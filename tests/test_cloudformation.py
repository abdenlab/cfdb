"""Tests for environment-scoping invariants in the CloudFormation templates.

The four templates under ``cloudformation/`` are deployed once per
environment (dev, prod) into the *same* AWS account and region, so any
resource whose physical name is pinned to a literal collides on the second
deploy. These tests pin the invariant that every such name is derived from
``${AWS::StackName}``, supplied by a template parameter, or imported from
another stack's (already stack-scoped) export.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "cloudformation"
_TEMPLATES = ("network.yml", "database.yml", "workers.yml", "backend.yml")

# Resource-type -> the property that sets an account/region-unique physical
# name. Keyed by type rather than by property name so that same-named
# properties which are NOT physical names are not swept up: the DocDB
# parameter group's ``Family: docdb5.0`` is an engine family, and the VPC
# endpoint's ``ServiceName: com.amazonaws...`` names an AWS service.
_PHYSICAL_NAME_PROPERTIES: dict[str, str] = {
    "AWS::ElasticLoadBalancingV2::TargetGroup": "Name",
    "AWS::ElasticLoadBalancingV2::LoadBalancer": "Name",
    "AWS::ECS::Cluster": "ClusterName",
    "AWS::ECS::Service": "ServiceName",
    "AWS::ECS::TaskDefinition": "Family",
    "AWS::Logs::LogGroup": "LogGroupName",
    "AWS::SecretsManager::Secret": "Name",
    "AWS::DocDB::DBCluster": "DBClusterIdentifier",
    "AWS::DocDB::DBInstance": "DBInstanceIdentifier",
    "AWS::DocDB::DBSubnetGroup": "DBSubnetGroupName",
    "AWS::S3::Bucket": "BucketName",
    "AWS::IAM::Role": "RoleName",
    "AWS::ECR::Repository": "RepositoryName",
}


class _CfnLoader(yaml.SafeLoader):
    """SafeLoader that tolerates CloudFormation short-form intrinsic tags."""


def _cfn_multi_constructor(loader: yaml.Loader, tag_suffix: str, node: yaml.Node) -> Any:
    """Represent a ``!Tag`` node as its long-form ``{"Fn::Tag": value}`` dict.

    ``!Ref`` is special-cased to the bare ``Ref`` key because that is its
    real long form; every other intrinsic takes the ``Fn::`` prefix.
    """
    key = "Ref" if tag_suffix == "Ref" else f"Fn::{tag_suffix}"
    if isinstance(node, yaml.ScalarNode):
        return {key: loader.construct_scalar(node)}
    if isinstance(node, yaml.SequenceNode):
        return {key: loader.construct_sequence(node, deep=True)}
    return {key: loader.construct_mapping(node, deep=True)}


_CfnLoader.add_multi_constructor("!", _cfn_multi_constructor)


def _load_template(name: str) -> dict[str, Any]:
    """Parse the named template in ``cloudformation/`` into a plain dict."""
    return yaml.load((_TEMPLATE_DIR / name).read_text(), Loader=_CfnLoader)


def _is_environment_scoped(value: Any, parameters: set[str], resources: set[str]) -> bool:
    """Report whether a physical-name value varies per environment.

    A name qualifies when it resolves a declared template parameter, is
    imported from another stack's export (itself stack-scoped by
    construction), or interpolates ``${AWS::StackName}``, a parameter, or a
    sibling resource. Interpolating a resource counts because CloudFormation
    generates that resource's physical name per stack, so the derived name is
    unique too. ``${AWS::Region}`` and ``${AWS::AccountId}`` deliberately do
    not count: both are identical for two environments in the same account
    and region, which is exactly the case under test.
    """
    if not isinstance(value, dict):
        # A bare string literal is identical in every environment.
        return False
    if "Ref" in value:
        return value["Ref"] in parameters
    if "Fn::ImportValue" in value:
        return True
    if "Fn::Sub" in value:
        body = value["Fn::Sub"]
        # !Sub accepts either a bare string or [template, {vars}].
        template = body[0] if isinstance(body, list) else body
        varying = {"AWS::StackName", *parameters, *resources}
        return any(f"${{{name}}}" in template for name in varying)
    return False


def _named_resources(template: dict[str, Any]) -> list[tuple[str, str, Any]]:
    """Yield (logical_id, resource_type, name_value) for each pinned name.

    Resources that leave their name property unset are skipped: CloudFormation
    then generates a unique physical name, which cannot collide.
    """
    found = []
    for logical_id, resource in (template.get("Resources") or {}).items():
        resource_type = resource.get("Type")
        prop = _PHYSICAL_NAME_PROPERTIES.get(resource_type)
        if prop is None:
            continue
        properties = resource.get("Properties") or {}
        if prop not in properties:
            continue
        found.append((logical_id, resource_type, properties[prop]))
    return found


@pytest.mark.parametrize("template_name", _TEMPLATES)
def test_physical_names_should_be_environment_scoped(template_name: str):
    """Test that no pinned physical name is a hard-coded literal.

    Given:
        A CloudFormation template that is deployed once per environment into
        the same AWS account and region.
    When:
        Every resource that pins an account/region-unique physical name is
        inspected.
    Then:
        Each such name should derive from ${AWS::StackName}, a template
        parameter, or a cross-stack import, so a second environment does not
        collide with the first.
    """
    # Arrange
    template = _load_template(template_name)
    parameters = set((template.get("Parameters") or {}).keys())
    resources = set((template.get("Resources") or {}).keys())

    # Act
    offenders = [
        (logical_id, resource_type, value)
        for logical_id, resource_type, value in _named_resources(template)
        if not _is_environment_scoped(value, parameters, resources)
    ]

    # Assert
    assert offenders == [], (
        f"{template_name} pins environment-invariant physical name(s): {offenders}. "
        "Derive the name from ${AWS::StackName} or expose it as a parameter."
    )


def test_backend_target_group_name_should_be_parameterized():
    """Test that the backend target group name comes from a parameter.

    Given:
        The backend template, whose target group name must be unique per
        account and region.
    When:
        The TargetGroupName parameter and the target group resource are read.
    Then:
        The parameter should exist and the resource should reference it, so a
        prod stack can supply its own name without disturbing dev's.
    """
    # Arrange
    template = _load_template("backend.yml")

    # Act
    parameters = template.get("Parameters") or {}
    target_group = template["Resources"]["TargetGroup"]["Properties"]

    # Assert
    assert "TargetGroupName" in parameters
    assert target_group["Name"] == {"Ref": "TargetGroupName"}


def test_backend_per_environment_parameters_should_be_documented():
    """Test that environment-specific parameters carry a Description.

    Given:
        The backend template's DNS parameters, whose defaults match dev's
        domain rather than any environment deployed today.
    When:
        Each per-environment parameter is inspected.
    Then:
        Each should document that it must be overridden per environment, so
        the misleading default cannot be adopted silently.
    """
    # Arrange
    template = _load_template("backend.yml")
    parameters = template.get("Parameters") or {}

    # Act & assert
    for name in ("HostedZoneName", "HostedZoneId", "DomainName", "TargetGroupName"):
        assert name in parameters, f"{name} is not a template parameter"
        assert parameters[name].get("Description", "").strip(), (
            f"{name} needs a Description marking it as a per-environment value"
        )


def test_worker_task_role_should_allow_the_worker_to_tag_its_own_task():
    """Test that the worker task role can publish its metadata.

    Given:
        The workers template, whose container publishes its wool version
        and TLS flag by tagging its own ECS task.
    When:
        The worker task role's policies are read.
    Then:
        They should grant ``ecs:TagResource``, without which the worker
        exits at startup and EcsDiscovery never advertises a worker.
    """
    # Arrange
    template = _load_template("workers.yml")

    # Act
    policies = template["Resources"]["WorkerTaskRole"]["Properties"]["Policies"]
    actions = [
        statement.get("Action")
        for policy in policies
        for statement in policy["PolicyDocument"]["Statement"]
    ]

    # Assert
    assert "ecs:TagResource" in actions


def test_worker_tag_grant_should_be_scoped_to_the_cluster_and_wool_keys():
    """Test that the tagging grant cannot reach the other environment.

    Given:
        The workers template's ecs:TagResource statement. These tags are
        load-bearing — discovery trusts them to build the metadata wool
        gates admission on — and dev and prod share the account and
        region, so an account-wide grant would let a compromised dev
        worker rewrite prod's tags and silently empty its pool.
    When:
        The statement's Resource and Condition are read.
    Then:
        The Resource should be confined to the parameterized cluster
        (not a bare ``task/*``) and the Condition should confine the
        writable keys to exactly the two ``wool.*`` tags.
    """
    # Arrange
    template = _load_template("workers.yml")
    policies = template["Resources"]["WorkerTaskRole"]["Properties"]["Policies"]

    # Act
    statement = next(
        statement
        for policy in policies
        for statement in policy["PolicyDocument"]["Statement"]
        if statement.get("Action") == "ecs:TagResource"
    )
    resource_template = statement["Resource"]["Fn::Sub"]
    tag_keys = statement["Condition"]["ForAllValues:StringEquals"]["aws:TagKeys"]

    # Assert
    assert "task/${BackendClusterName}/*" in resource_template
    assert "task/*" not in resource_template.replace(
        "task/${BackendClusterName}/*", ""
    )
    assert sorted(tag_keys) == ["wool.secure", "wool.version"]
    assert "BackendClusterName" in (template.get("Parameters") or {})


def test_backend_cluster_name_should_have_no_default():
    """Test that a deploy cannot silently mis-scope the tagging grant.

    Given:
        The workers template's BackendClusterName parameter, which
        scopes the ecs:TagResource grant to one environment's cluster.
    When:
        The parameter is inspected for a default.
    Then:
        It should have none, so CloudFormation rejects a deploy that
        omits it. A placeholder default deploys cleanly and attaches the
        policy to a cluster that does not exist, leaving every worker
        failing the same AccessDenied the grant exists to prevent — with
        UPDATE_COMPLETE on screen and no signal that anything is wrong.
        Every other parameter here may carry a placeholder because a
        wrong value fails visibly; this one may not.
    """
    # Arrange
    template = _load_template("workers.yml")

    # Act
    parameter = template["Parameters"]["BackendClusterName"]

    # Assert
    assert "Default" not in parameter, (
        "BackendClusterName must have no default — see the comment in "
        "workers.yml. A default makes a forgotten override deploy "
        "successfully while granting nothing usable."
    )


def test_worker_container_should_declare_aws_region():
    """Test that the worker container is told its region.

    Given:
        The workers template's container definition. The worker's
        metadata publish builds a boto3 client, and Fargate injects no
        region — without this variable (or the code-side ARN fallback)
        every worker exits at startup with NoRegionError and the fleet
        stays empty.
    When:
        The container's Environment entries are read.
    Then:
        ``AWS_REGION`` should be present and derive from the stack's
        own region, mirroring the backend container.
    """
    # Arrange
    template = _load_template("workers.yml")
    container = template["Resources"]["WorkerTaskDefinition"]["Properties"][
        "ContainerDefinitions"
    ][0]

    # Act
    plain_env = {
        entry["Name"]: entry.get("Value")
        for entry in container["Environment"]
        if isinstance(entry, dict) and "Name" in entry
    }

    # Assert
    assert plain_env.get("AWS_REGION") == {"Ref": "AWS::Region"}


def test_worker_tls_identity_should_only_render_when_mtls_is_enabled():
    """Test that the worker identity env var is gated on mTLS.

    Given:
        The workers template's container definition and its
        WorkerTlsIdentity parameter — the worker's half of identity
        verification on wool's graceful-stop channel.
    When:
        The conditional Environment entry is read.
    Then:
        ``CFDB_WORKER_TLS_IDENTITY`` should render under the
        WorkerMtlsEnabled condition and reference the parameter, so the
        plaintext template stays unchanged while an mTLS deployment can
        align the worker with the backend stack's identity.
    """
    # Arrange
    template = _load_template("workers.yml")
    container = template["Resources"]["WorkerTaskDefinition"]["Properties"][
        "ContainerDefinitions"
    ][0]

    # Act
    conditional = next(
        entry["Fn::If"]
        for entry in container["Environment"]
        if isinstance(entry, dict) and "Fn::If" in entry
    )
    condition_name, enabled_value, disabled_value = conditional

    # Assert
    assert condition_name == "WorkerMtlsEnabled"
    assert enabled_value == {
        "Name": "CFDB_WORKER_TLS_IDENTITY",
        "Value": {"Ref": "WorkerTlsIdentity"},
    }
    assert disabled_value == {"Ref": "AWS::NoValue"}


def test_worker_tls_identity_defaults_should_agree_across_both_stacks():
    """Test that the two templates cannot drift on the identity default.

    Given:
        The WorkerTlsIdentity parameters of the backend and workers
        templates. The API verifies the worker leaf's SAN against the
        backend value; the worker verifies the same SAN on its own
        graceful-stop channel via the workers value.
    When:
        Both defaults are compared to the application constant.
    Then:
        All three should be equal — a drift between them fails only at
        runtime, as a force-reaped worker losing in-flight work, with
        no TLS error anywhere client-side.
    """
    # Arrange
    from cfdb.workflows.constants import DEFAULT_TLS_IDENTITY

    backend = _load_template("backend.yml")["Parameters"]["WorkerTlsIdentity"]
    workers = _load_template("workers.yml")["Parameters"]["WorkerTlsIdentity"]

    # Act & assert
    assert backend["Default"] == DEFAULT_TLS_IDENTITY
    assert workers["Default"] == DEFAULT_TLS_IDENTITY


def test_worker_tls_identity_pattern_should_reject_ip_literals():
    """Test that the identity patterns reject what cannot be a SAN.

    Given:
        The identical AllowedPattern on both templates' WorkerTlsIdentity
        parameters. The cert generator refuses an IP-literal identity
        because it would mint DNS:<ip>, which gRPC never matches against
        an address — so the deploy boundary must refuse it too, or the
        one symptom is NoWorkersAvailable.
    When:
        The pattern is evaluated against representative values.
    Then:
        It should accept DNS-safe names (with at least one letter, in
        any position) and the empty opt-out, and reject IP literals,
        all-numeric names, and separator-edged names.
    """
    # Arrange
    import re

    backend = _load_template("backend.yml")["Parameters"]["WorkerTlsIdentity"]
    workers = _load_template("workers.yml")["Parameters"]["WorkerTlsIdentity"]
    pattern = re.compile(backend["AllowedPattern"])

    # Act & assert
    assert backend["AllowedPattern"] == workers["AllowedPattern"]
    for accepted in ("", "cfdb-worker", "a.b-c", "9lives", "cfdb-worker-prod"):
        assert pattern.fullmatch(accepted), f"{accepted!r} should be accepted"
    for rejected in ("10.0.0.5", "1.2.3.4", "1234", ".abc", "abc.", "-a", "a b"):
        assert not pattern.fullmatch(rejected), f"{rejected!r} should be rejected"


def test_cert_generator_default_identity_should_match_the_application_default():
    """Test that the cert script and the code agree on the default identity.

    Given:
        The ``IDENTITY=`` default in certs/generate-worker-certs.sh —
        the value that decides what SAN locally-minted worker leaves
        actually carry.
    When:
        The default is extracted from the script source.
    Then:
        It should equal DEFAULT_TLS_IDENTITY, because a drift means the
        API verifies against a name the certificates do not carry, and
        the only symptom is NoWorkersAvailable.
    """
    # Arrange
    import re

    from cfdb.workflows.constants import DEFAULT_TLS_IDENTITY

    script = (
        Path(__file__).resolve().parents[1] / "certs" / "generate-worker-certs.sh"
    ).read_text()

    # Act
    match = re.search(r'^IDENTITY="([^"]*)"', script, flags=re.MULTILINE)

    # Assert
    assert match is not None, "IDENTITY= default not found in the script"
    assert match.group(1) == DEFAULT_TLS_IDENTITY


def test_backend_worker_tls_identity_should_match_the_application_default():
    """Test that the template and the code agree on the default identity.

    Given:
        The backend template's WorkerTlsIdentity parameter and the
        application constant the API reads.
    When:
        The parameter's default is compared to DEFAULT_TLS_IDENTITY.
    Then:
        They should be equal — a drift means the API expects a name the
        deployed certificates do not carry, and every handshake fails
        with nothing in the error naming TLS.
    """
    # Arrange
    from cfdb.workflows.constants import DEFAULT_TLS_IDENTITY

    template = _load_template("backend.yml")

    # Act
    parameter = template["Parameters"]["WorkerTlsIdentity"]

    # Assert
    assert parameter["Default"] == DEFAULT_TLS_IDENTITY


def test_backend_worker_tls_identity_should_only_render_when_mtls_is_enabled():
    """Test that the identity env var is gated on the mTLS condition.

    Given:
        The backend template's API container definition.
    When:
        Its Environment entries are searched for the identity variable.
    Then:
        It should appear inside an Fn::If over ApiMtlsEnabled, so a
        plaintext deployment does not advertise a setting that has no
        effect.
    """
    # Arrange
    template = _load_template("backend.yml")
    container = template["Resources"]["TaskDefinition"]["Properties"][
        "ContainerDefinitions"
    ][0]

    # Act
    conditional = [
        entry
        for entry in container["Environment"]
        if isinstance(entry, dict) and "Fn::If" in entry
    ]

    # Assert
    identity_entries = [
        entry
        for entry in conditional
        if entry["Fn::If"][0] == "ApiMtlsEnabled"
        and entry["Fn::If"][1].get("Name") == "CFDB_WORKER_TLS_IDENTITY"
    ]
    assert len(identity_entries) == 1
    assert identity_entries[0]["Fn::If"][1]["Value"] == {"Ref": "WorkerTlsIdentity"}
