#!/usr/bin/env python3
"""
Post-write hook for Alembic migrations.
Automatically replaces hardcoded schema references with resolve_schema() calls.
"""

import re
import sys


def add_utils_import(content: str) -> str:
    """Add import for resolve_schema from utils module."""
    # Check if already imported
    if "from chalicelib.alembic_tenant.utils import resolve_schema" in content:
        return content

    # Find the alembic import line
    pattern = r"(from alembic import.*?\n)"
    match = re.search(pattern, content)

    if match:
        # Insert after alembic import
        pos = match.end()
        import_line = "\nfrom chalicelib.alembic_tenant.utils import resolve_schema\n"
        content = content[:pos] + import_line + content[pos:]
    else:
        # Fallback: add after sqlalchemy import
        pattern = r"(import sqlalchemy as sa\n)"
        match = re.search(pattern, content)
        if match:
            pos = match.end()
            import_line = "\nfrom chalicelib.alembic_tenant.utils import resolve_schema\n"
            content = content[:pos] + import_line + content[pos:]

    return content


def replace_schema_references(content: str, placeholder: str = "per_tenant") -> str:
    """Replace hardcoded schema references with resolve_schema() calls."""

    # Pattern 1: schema='uuid' or schema="uuid" or schema='per_tenant'
    # Matches UUID patterns, migrations, or the placeholder itself
    schema_pattern = r'schema=["\'](per_tenant)["\']'

    # Replace with resolve_schema call
    content = re.sub(schema_pattern, f'schema=resolve_schema("{placeholder}")', content)

    return content


def extract_public_enums(content: str) -> tuple[str, dict[str, str]]:
    """
    Extract ENUMs with schema='public' from inline definitions and create module-level variables.

    Returns:
        tuple: (modified_content, dict of {enum_var_name: enum_definition})
    """
    enum_definitions = {}

    # Pattern to match sa.Enum(..., name='enum_name', schema='public')
    enum_pattern = r"sa\.Enum\(([^)]+),\s*name=['\"](\w+)['\"],\s*schema=['\"]public['\"]\)"

    def replace_enum(match):
        values_and_name = match.group(1)
        enum_name = match.group(2)

        # Create variable name from enum name
        var_name = f"{enum_name}"

        # Store the enum definition
        enum_definitions[var_name] = (
            f'sa.Enum({values_and_name}, name="{enum_name}", schema="public")'
        )

        # Replace inline enum with variable reference
        return var_name

    # Replace all inline enums with variable references
    modified_content = re.sub(enum_pattern, replace_enum, content)

    return modified_content, enum_definitions


def add_postgresql_using_to_enum_alters(content: str) -> str:
    """
    Add postgresql_using parameter to op.alter_column() calls that change ENUM types.

    This is needed because PostgreSQL can't automatically cast between different ENUM types.
    The pattern added is: postgresql_using="column_name::text::public.enum_name"
    """
    # Match op.alter_column that has:
    # 1. type_= with either sa.Enum(..., name='xxx') OR enum_variable_name
    # 2. existing_nullable=...
    # 3. schema= (at the end)
    # 4. Does NOT already have postgresql_using

    # Pattern matches multiline alter_column calls
    alter_pattern = r"op\.alter_column\(([^,]+),\s*(['\"])(\w+)\2,\s*\n\s+existing_type=([^\n]+),\s*\n\s+type_=(enum_\w+|sa\.Enum\([^)]+name=['\"](\w+)['\"][^)]*\)),\s*\n\s+existing_nullable=([^,]+),\s*\n\s+schema="

    def add_using(match):
        full_match = match.group(0)
        column_name = match.group(3)
        type_value = match.group(5)

        # Check if postgresql_using is already present
        if "postgresql_using" in full_match:
            return full_match

        enum_name = type_value

        # Rebuild the alter_column with postgresql_using
        result = (
            f"op.alter_column({match.group(1)}, {match.group(2)}{column_name}{match.group(2)},\n"
            f"               existing_type={match.group(4)},\n"
            f"               type_={type_value},\n"
            f"               existing_nullable={match.group(7)},\n"
            f'               postgresql_using=\'"{column_name}"::text::public."{enum_name}"\',\n'
            f"               schema="
        )

        return result

    content = re.sub(alter_pattern, add_using, content)

    return content


def add_enum_definitions(content: str, enum_definitions: dict[str, str]) -> str:
    """
    Add enum variable definitions at module level, after revision identifiers.
    Also add enum.create() calls in upgrade function.
    """
    if not enum_definitions:
        return content

    # Find where to insert enum definitions (after depends_on line)
    pattern = r"(depends_on.*?= .*?\n)"
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        return content

    pos = match.end()

    # Build enum definitions block
    enum_block = ""
    for var_name, enum_def in enum_definitions.items():
        enum_block += f"{var_name} = {enum_def}\n"
    enum_block += "\n"

    # Insert enum definitions
    content = content[:pos] + enum_block + content[pos:]

    # Now add .create() calls in upgrade function
    # Find the upgrade function
    upgrade_pattern = (
        r"(def upgrade\(\):.*?\n.*?# ### commands auto generated by Alembic - please adjust! ###\n)"
    )
    upgrade_match = re.search(upgrade_pattern, content, re.DOTALL)

    if upgrade_match:
        pos = upgrade_match.end()

        # Build create calls
        create_block = ""
        for var_name in enum_definitions:
            create_block += f"    {var_name}.create(op.get_bind(), checkfirst=True)\n"

        # Insert after the auto-generated comment
        content = content[:pos] + create_block + content[pos:]

    return content


def process_migration_file(filepath: str, placeholder: str = "per_tenant") -> None:
    """Process a migration file to add resolve_schema() support and refactor public ENUMs."""

    # Read the file
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    original_content = content
    changes = []

    # Apply transformations
    content = add_utils_import(content)
    if content != original_content:
        changes.append("Added resolve_schema import from utils")

    content = replace_schema_references(content, placeholder)
    if content != original_content and "Added resolve_schema" not in changes:
        changes.append("Replaced hardcoded schemas with dynamic resolution")

    # Extract and refactor public ENUMs
    modified_content, enum_definitions = extract_public_enums(content)
    if enum_definitions:
        content = add_enum_definitions(modified_content, enum_definitions)
        changes.append(
            f"Extracted {len(enum_definitions)} public ENUM(s) to module level with checkfirst=True"
        )

    # Add postgresql_using to enum alter_column operations
    old_content = content
    content = add_postgresql_using_to_enum_alters(content)
    if content != old_content:
        changes.append("Added postgresql_using to ENUM alter_column operations")

    # Only write if changes were made
    if content != original_content:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"✓ Post-processed migration: {filepath}")
        for change in changes:
            print(f"  - {change}")
    else:
        print(f"✓ Migration file unchanged: {filepath}")


def main():
    """Main entry point for the post-write hook."""
    if len(sys.argv) < 2:
        print("Usage: post_write_hook.py <migration_file>")
        sys.exit(1)

    filepath = sys.argv[1]
    process_migration_file(filepath)


if __name__ == "__main__":
    main()
