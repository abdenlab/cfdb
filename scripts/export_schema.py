"""Regenerate the checked-in GraphQL SDL from the Strawberry schema.

``schema.graphql`` is a generated artifact — edit the Python types and run
this script (``make schema``) rather than editing the SDL by hand.
``tests/test_schema.py`` fails when the two drift apart.
"""

from pathlib import Path

from strawberry.printer import print_schema

from cfdb.api.gql.schema import schema


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.graphql"


def render() -> str:
    """Return the SDL exactly as it is written to ``schema.graphql``."""
    return print_schema(schema) + "\n"


def main() -> None:
    SCHEMA_PATH.write_text(render())
    print(f"Wrote {SCHEMA_PATH}")


if __name__ == "__main__":
    main()
