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
