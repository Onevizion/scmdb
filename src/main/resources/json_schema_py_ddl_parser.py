#!/usr/bin/env python3
"""
Utility: Oracle DDL -> JSON Schema Draft-07.

Usage:
    python json_schema_py_ddl_parser.py <path_to_sql_file> [output_file]

If output_file is omitted, the schema is printed to stdout.
"""

import re
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Oracle SQL type -> JSON Schema type with compact nullable pattern
# ---------------------------------------------------------------------------

def map_sql_type(sql_type: str, type_params: str | None, is_not_null: bool = False) -> dict:
    """
    Return JSON Schema property with compact type array for nullable types.

    Example:
    {
        "type": ["integer", "null"],
        "extendedType": "integer",
        "sqlPrecision": 1
    }
    """
    sql_type = sql_type.upper()
    prop = {}
    extended_type = None
    base_json_type = None

    if sql_type in ('NUMBER', 'INTEGER', 'INT', 'SMALLINT', 'BINARY_INTEGER', 'PLS_INTEGER'):
        precision = None
        scale = 0
        if type_params:
            parts = type_params.strip('()').split(',')
            if len(parts) >= 1:
                try:
                    precision = int(parts[0].strip())
                except ValueError:
                    pass
            if len(parts) == 2:
                try:
                    scale = int(parts[1].strip())
                except ValueError:
                    pass

        if scale > 0:
            base_json_type = 'number'
            extended_type = 'number'
        else:
            base_json_type = 'integer'
            extended_type = 'integer'

        if precision is not None and precision <= 10:
            prop['sqlPrecision'] = precision

    elif sql_type in ('FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE'):
        base_json_type = 'number'
        extended_type = 'number'

    elif sql_type in ('VARCHAR2', 'VARCHAR', 'NVARCHAR2', 'CHAR', 'NCHAR'):
        base_json_type = 'string'
        extended_type = 'string'
        if type_params:
            # Handle VARCHAR2(128 CHAR) or VARCHAR2(128)
            match = re.match(r'\((\d+)(?:\s+(?:CHAR|BYTE))?\)', type_params.strip())
            if match:
                try:
                    max_len = int(match.group(1))
                    prop['maxLength'] = max_len
                except ValueError:
                    pass

    elif sql_type == 'DATE':
        base_json_type = 'string'
        prop['format'] = 'date'
        extended_type = 'string'

    elif sql_type.startswith('TIMESTAMP'):
        base_json_type = 'string'
        prop['format'] = 'date-time'
        extended_type = 'string'

    elif sql_type in ('CLOB', 'NCLOB', 'LONG'):
        base_json_type = 'string'
        extended_type = 'string'

    elif sql_type == 'XMLTYPE':
        base_json_type = 'string'
        extended_type = 'string'

    elif sql_type == 'BLOB':
        base_json_type = 'string'
        extended_type = 'binary'

    else:
        base_json_type = 'string'
        extended_type = 'string'

    # Construct compact type array: ["integer", "null"]
    if is_not_null:
        prop['type'] = base_json_type
    else:
        prop['type'] = [base_json_type, 'null']

    prop['extendedType'] = extended_type

    return prop


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def split_by_top_comma(text: str) -> list[str]:
    """Split text by commas that are not inside parentheses."""
    parts, current, depth = [], [], 0
    for ch in text:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            parts.append(''.join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append(''.join(current))
    return parts


def extract_balanced_parens(text: str, start: int) -> str:
    """
    Given text and position of an opening '(', return the content
    between that '(' and its matching ')'.
    """
    assert text[start] == '('
    depth, i = 0, start
    for i in range(start, len(text)):
        if text[i] == '(':
            depth += 1
        elif text[i] == ')':
            depth -= 1
            if depth == 0:
                return text[start + 1:i]
    return text[start + 1:]


def find_check_expr(line: str) -> str | None:
    """Extract the content of CHECK(...) handling nested parens."""
    m = re.search(r'\bCHECK\s*\(', line, re.IGNORECASE)
    if not m:
        return None
    paren_start = m.end() - 1  # position of '('
    return extract_balanced_parens(line, paren_start)


def to_title_case_oracle(name: str) -> str:
    """Convert table_name to T_TABLE_NAME (Oracle type convention)."""
    return 'T_' + name.upper()


def parse_literal(value: str):
    """Try to convert a SQL literal to a Python scalar."""
    value = value.strip().strip("'")
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


# ---------------------------------------------------------------------------
# Trigger analysis
# ---------------------------------------------------------------------------

def extract_before_insert_triggers(content: str) -> list[str]:
    """Return bodies of all BEFORE INSERT triggers found in the content."""
    bodies = []
    for m in re.finditer(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+\w+.*?BEFORE\s+INSERT.*?(?:FOR\s+EACH\s+ROW\s+)?(?:DECLARE\s+.*?)?BEGIN(.*?)END\s*;',
        content, re.IGNORECASE | re.DOTALL
    ):
        bodies.append(m.group(1))
    return bodies


def parse_trigger_defaults(trigger_bodies: list[str]) -> dict[str, dict]:
    """
    Scan BEFORE INSERT trigger bodies for patterns that set :new.col values.

    Returns a dict: col_name -> {'default': value, 'source': 'trigger'} or
                                {'auto_increment': True, 'sequence': seq_name, 'source': 'trigger'}
    """
    result: dict[str, dict] = {}

    for body in trigger_bodies:
        # Pattern 1 & 2: IF :new.col IS NULL THEN :new.col := <value>; END IF;
        for m in re.finditer(
            r'if\s+:new\.(\w+)\s+is\s+null\s+then\s+'
            r':new\.\w+\s*:=\s*([^;]+?)\s*;',
            body, re.IGNORECASE | re.DOTALL
        ):
            col = m.group(1).upper()  # Keep UPPER_CASE
            raw_val = m.group(2).strip()
            _classify_assignment(col, raw_val, result)

        # Pattern 3: unconditional :new.col := <simple_literal>;
        for m in re.finditer(
            r':new\.(\w+)\s*:=\s*([^;]+?)\s*;',
            body, re.IGNORECASE | re.DOTALL
        ):
            col = m.group(1).upper()
            raw_val = m.group(2).strip()
            if col not in result:
                _classify_assignment(col, raw_val, result, conditional=False)

        # Pattern 4: SELECT <seq>.nextval INTO :new.col
        for m in re.finditer(
            r'SELECT\s+(\w+)\.nextval\s+INTO\s+:new\.(\w+)',
            body, re.IGNORECASE
        ):
            col = m.group(2).upper()
            seq = m.group(1)
            if col not in result:
                result[col] = {'auto_increment': True, 'sequence': seq, 'source': 'trigger'}

    return result


def _classify_assignment(col: str, raw_val: str, result: dict, conditional: bool = True):
    """Determine if raw_val is a literal, NULL, or a sequence, and store it."""
    # sequence.nextval
    m_seq = re.match(r'(\w+)\.nextval$', raw_val, re.IGNORECASE)
    if m_seq:
        result[col] = {'auto_increment': True, 'sequence': m_seq.group(1), 'source': 'trigger'}
        return

    # NULL literal
    if raw_val.upper() == 'NULL':
        return

    # Quoted string literal
    m_str = re.match(r"^'(.*)'$", raw_val)
    if m_str:
        lit = m_str.group(1).replace("''", "'")
        if col not in result or conditional:
            result[col] = {'default': lit, 'source': 'trigger'}
        return

    # Numeric literal
    m_num = re.match(r'^-?\d+(\.\d+)?$', raw_val)
    if m_num:
        lit = parse_literal(raw_val)
        if col not in result or conditional:
            result[col] = {'default': lit, 'source': 'trigger'}
        return

    # Function call / expression
    if conditional:
        if col not in result:
            result[col] = {'fills': True, 'source': 'trigger'}


def find_referenced_columns(expr: str, column_names: set[str]) -> set[str]:
    """Return the subset of column_names that appear as whole words in expr."""
    found = set()
    for col in column_names:
        if re.search(r'\b' + re.escape(col) + r'\b', expr, re.IGNORECASE):
            found.add(col)
    return found


# ---------------------------------------------------------------------------
# Foreign Key parser
# ---------------------------------------------------------------------------

def parse_foreign_keys(table_body: str) -> dict[str, dict]:
    """Parse FOREIGN KEY constraints from table definition."""
    fk_info: dict[str, dict] = {}

    pattern = r'CONSTRAINT\s+(\w+)\s+FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)'

    for m in re.finditer(pattern, table_body, re.IGNORECASE):
        column_name = m.group(2).upper()
        ref_table = m.group(3)
        ref_column = m.group(4).upper()

        fk_info[column_name] = {
            'table': ref_table,
            'column': ref_column
        }

    return fk_info


# ---------------------------------------------------------------------------
# DDL DEFAULT parser
# ---------------------------------------------------------------------------

def parse_ddl_default(col_def: str) -> tuple[str | None, bool]:
    """
    Parse DEFAULT clause from a column definition.
    Returns (raw_value, is_on_null)
    """
    # DEFAULT ON NULL <value>
    m = re.search(r'\bDEFAULT\s+ON\s+NULL\s+(\S+)', col_def, re.IGNORECASE)
    if m:
        return m.group(1), True

    # Plain DEFAULT <value>
    m = re.search(r'\bDEFAULT\s+(\S+)', col_def, re.IGNORECASE)
    if m:
        return m.group(1), False

    return None, False


# ---------------------------------------------------------------------------
# Metadata generation
# ---------------------------------------------------------------------------

# Common patterns for fields that should be ignored during import/export
IGNORE_PATTERNS = {
    # Environment-specific IDs
    'PROGRAM_ID': 'Environment-specific program reference',
    'COMPONENT_ID': 'Environment-specific component ID',
    'SYSTEM_ID': 'Environment-specific system ID',

    # Package membership
    'COMPONENT_PACKAGES': 'Package membership metadata, managed by platform',
    'COMPONENTS_PACKAGE_ID': 'Package membership, managed by platform',
}


def generate_metadata(table_name: str, column_defs: list[tuple[str, str]], 
                     trigger_defaults: dict[str, dict], 
                     check_constraints: dict[str, dict],
                     raw_check_list: list[tuple[str | None, str]] = None,
                     column_name_set: set[str] = None) -> dict:
    """
    Generate schema-level metadata: required fields and table constraints.
    Returns dict with 'required' and optionally 'x-table-constraints' keys.
    """
    metadata = {}
    
    # required: fields with NOT NULL (excluding auto-increment PKs and those with DEFAULT)
    required = []
    table_upper = table_name.upper()
    for col_name, col_def in column_defs:
        # Check if field is NOT NULL
        is_not_null = bool(re.search(r'\bNOT\s+NULL\b', col_def, re.IGNORECASE))
        
        if not is_not_null:
            continue
        
        # Skip primary key: <TABLE_NAME>_ID (usually auto-increment)
        if col_name == f'{table_upper}_ID':
            continue
        
        # Skip fields in IGNORE_PATTERNS (PROGRAM_ID, etc.)
        if col_name in IGNORE_PATTERNS:
            continue
        
        # Skip auto-increment fields (from triggers)
        if col_name in trigger_defaults and 'auto_increment' in trigger_defaults[col_name]:
            continue
        
        # Skip fields with DEFAULT value in DDL
        has_default = bool(re.search(r'\bDEFAULT\b', col_def, re.IGNORECASE))
        if has_default:
            continue
        
        # Add to required
        required.append(col_name)
    
    if required:
        metadata['required'] = required  # Standard JSON Schema field (no underscore)

    # Process multi-column constraints
    if raw_check_list and column_name_set:
        table_constraints = []
        for c_name, expr in raw_check_list:
            refs = find_referenced_columns(expr, column_name_set)
            if len(refs) > 1:  # Multi-column constraint
                constraint_info = {
                    'expression': expr,
                    'columns': sorted(list(refs))
                }
                if c_name:
                    constraint_info['name'] = c_name
                table_constraints.append(constraint_info)
        
        if table_constraints:
            metadata['x-table-constraints'] = table_constraints

    return metadata


def should_ignore_field(col_name: str, trigger_defaults: dict[str, dict]) -> tuple[bool, str | None]:
    """
    Check if field should be ignored during import/export.
    Returns (should_ignore, reason)
    """
    # Check IGNORE_PATTERNS
    if col_name in IGNORE_PATTERNS:
        return True, IGNORE_PATTERNS[col_name]
    
    # Auto-increment PKs should be ignored
    if col_name in trigger_defaults and 'auto_increment' in trigger_defaults[col_name]:
        reason = f"Auto-increment from {trigger_defaults[col_name].get('sequence', 'unknown')}"
        return True, reason
    
    return False, None


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_ddl(content: str, schema_filename: str | None = None) -> dict:
    """Parse DDL and return JSON Schema Draft-07.
    
    Args:
        content: SQL DDL content
        schema_filename: Name for schema file (used for file path detection)
    """

    # 1. Extract CREATE TABLE block
    m = re.search(
        r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)\s*;',
        content, re.IGNORECASE | re.DOTALL
    )
    if not m:
        raise ValueError("No CREATE TABLE statement found in the file")

    table_name: str = m.group(1)
    table_body: str = m.group(2)

    # 2. COMMENT ON TABLE
    m_tbl_comment = re.search(
        r"COMMENT\s+ON\s+TABLE\s+\w+\s+IS\s+'(.*?)'",
        content, re.IGNORECASE | re.DOTALL
    )
    table_description = m_tbl_comment.group(1).replace("''", "'") if m_tbl_comment else None

    # 3. COMMENT ON COLUMN
    column_comments: dict[str, str] = {}
    for m_col in re.finditer(
        r"COMMENT\s+ON\s+COLUMN\s+\w+\.(\w+)\s+IS\s+'(.*?)'",
        content, re.IGNORECASE | re.DOTALL
    ):
        column_comments[m_col.group(1).upper()] = m_col.group(2).replace("''", "'")

    # 4. Parse triggers and foreign keys
    trigger_bodies = extract_before_insert_triggers(content)
    trigger_defaults = parse_trigger_defaults(trigger_bodies)
    foreign_keys = parse_foreign_keys(table_body)

    # 5. Parse table body: columns + constraints
    lines = split_by_top_comma(table_body)

    column_defs: list[tuple[str, str]] = []
    check_constraints: dict[str, dict] = {}
    raw_check_list: list[tuple[str | None, str]] = []

    CONSTRAINT_KEYWORDS = ('CONSTRAINT', 'PRIMARY', 'UNIQUE', 'FOREIGN', 'CHECK')

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        upper = line.upper().lstrip()

        # Table-level CONSTRAINT ... CHECK (...)
        is_constraint = re.match(r'CONSTRAINT\s+\w+\s+CHECK\b', line, re.IGNORECASE)
        if is_constraint:
            m_cname = re.match(r'CONSTRAINT\s+(\w+)', line, re.IGNORECASE)
            c_name = m_cname.group(1) if m_cname else None

            expr_raw = find_check_expr(line)
            if expr_raw is None:
                continue
            expr = expr_raw.strip()

            # Simple patterns
            m_in = re.match(
                r'(?:nvl\s*\(\s*)?(\w+)(?:\s*,\s*[^)]+\))?\s+in\s*\(([^)]+)\)\s*$',
                expr, re.IGNORECASE
            )
            m_neq = re.match(r'(\w+)\s+(?:<>|!=)\s*(\S+)', expr, re.IGNORECASE)
            m_gte = re.match(r'(\w+)\s+>=\s*(\S+)', expr, re.IGNORECASE)
            m_gt  = re.match(r'(\w+)\s+>\s*(\S+)',  expr, re.IGNORECASE)
            m_lte = re.match(r'(\w+)\s+<=\s*(\S+)', expr, re.IGNORECASE)
            m_lt  = re.match(r'(\w+)\s+<\s*(\S+)',  expr, re.IGNORECASE)
            m_between = re.match(r'(\w+)\s+BETWEEN\s+(\S+)\s+AND\s+(\S+)', expr, re.IGNORECASE)

            if m_in:
                col = m_in.group(1).upper()
                raw_vals = [v.strip() for v in m_in.group(2).split(',')]
                check_constraints[col] = {'enum': [parse_literal(v) for v in raw_vals]}
            elif m_between:
                col = m_between.group(1).upper()
                min_val = parse_literal(m_between.group(2))
                max_val = parse_literal(m_between.group(3))
                check_constraints[col] = {'minimum': min_val, 'maximum': max_val}
            elif m_neq:
                col = m_neq.group(1).upper()
                val = parse_literal(m_neq.group(2))
                check_constraints[col] = {'neq': val}
            elif m_gte:
                col = m_gte.group(1).upper()
                val = parse_literal(m_gte.group(2))
                check_constraints[col] = {'minimum': val}
            elif m_gt:
                col = m_gt.group(1).upper()
                val = parse_literal(m_gt.group(2))
                check_constraints[col] = {'exclusiveMinimum': val}
            elif m_lte:
                col = m_lte.group(1).upper()
                val = parse_literal(m_lte.group(2))
                check_constraints[col] = {'maximum': val}
            elif m_lt:
                col = m_lt.group(1).upper()
                val = parse_literal(m_lt.group(2))
                check_constraints[col] = {'exclusiveMaximum': val}
            else:
                raw_check_list.append((c_name, expr))
            continue

        # Skip other constraint keywords
        if any(upper.startswith(kw) for kw in CONSTRAINT_KEYWORDS):
            continue

        # Column definition
        m_col = re.match(r'(\w+)\s+(.*)', line, re.IGNORECASE | re.DOTALL)
        if m_col:
            col_name = m_col.group(1).upper()  # UPPER_CASE
            col_def  = m_col.group(2).strip()
            column_defs.append((col_name, col_def))

    # 6. Assign complex CHECK constraints
    column_name_set = {col for col, _ in column_defs}
    single_col_checks: dict[str, list[tuple[str | None, str]]] = {}

    for c_name, expr in raw_check_list:
        refs = find_referenced_columns(expr, column_name_set)
        if len(refs) == 1:
            col = next(iter(refs)).upper()
            single_col_checks.setdefault(col, []).append((c_name, expr))

    # 7. Build JSON Schema properties
    properties: dict[str, dict] = {}

    for col_name, col_def in column_defs:
        # Parse type
        m_type = re.match(r'(\w+)(\([\d,\s]+(?:\s+(?:CHAR|BYTE))?\))?', col_def, re.IGNORECASE)
        if not m_type:
            continue

        sql_type   = m_type.group(1)
        type_params = m_type.group(2)

        is_not_null = bool(re.search(r'\bNOT\s+NULL\b', col_def, re.IGNORECASE))

        prop = map_sql_type(sql_type, type_params, is_not_null)

        # Add description from comments
        if col_name in column_comments:
            prop['description'] = column_comments[col_name]

        # Add DEFAULT value from DDL
        default_raw, is_on_null = parse_ddl_default(col_def)
        if default_raw and default_raw.upper() != 'NULL':
            default_val = parse_literal(default_raw)
            prop['default'] = default_val
            if is_on_null:
                prop['x-source'] = f'DEFAULT ON NULL {default_raw}'
            else:
                prop['x-source'] = f'DEFAULT {default_raw}'
        else:
            # Add default for auto-increment fields
            if col_name in trigger_defaults and 'auto_increment' in trigger_defaults[col_name]:
                prop['default'] = 0
                prop['x-source'] = f"Trigger: auto-increment from {trigger_defaults[col_name].get('sequence', 'unknown')}"
            elif col_name in trigger_defaults:
                # Add x-source for other trigger behaviors
                trigger_info = trigger_defaults[col_name]
                if 'default' in trigger_info:
                    prop['x-source'] = f"Trigger: default = {trigger_info['default']}"
                elif 'fills' in trigger_info:
                    prop['x-source'] = 'Trigger: fills value conditionally'

        # Add enum constraint from CHECK
        if col_name in check_constraints:
            constraint = check_constraints[col_name]
            if 'enum' in constraint:
                enum_values = constraint['enum']
                # Add null to enum if field is nullable
                if not is_not_null:
                    enum_values = enum_values + [None]
                prop['enum'] = enum_values
            elif 'minimum' in constraint or 'maximum' in constraint:
                if 'minimum' in constraint:
                    prop['minimum'] = constraint['minimum']
                if 'maximum' in constraint:
                    prop['maximum'] = constraint['maximum']
            elif 'exclusiveMinimum' in constraint:
                prop['exclusiveMinimum'] = constraint['exclusiveMinimum']
            elif 'exclusiveMaximum' in constraint:
                prop['exclusiveMaximum'] = constraint['exclusiveMaximum']
            
            if 'neq' in constraint:
                # For != constraints, add to x-constraints on property level
                prop['x-constraint-neq'] = constraint['neq']

        # Add x-ignore if field should be ignored
        should_ignore, ignore_reason = should_ignore_field(col_name, trigger_defaults)
        if should_ignore:
            prop['x-ignore'] = ignore_reason

        # Add x-checks for complex constraints
        if col_name in single_col_checks:
            prop['x-checks'] = [e for _, e in single_col_checks[col_name]]

        # Add FK references with $ref to schema
        if col_name in foreign_keys:
            fk = foreign_keys[col_name]
            fk_table = fk['table'].lower()
            prop['x-fk-table'] = fk['table']
            prop['x-fk-column'] = fk['column']
            prop['$ref'] = f'{fk_table}.schema.json'

        # Universal label reference detection: any field ending with _LABEL_ID
        # For user configurations import/export, always reference label_program
        elif col_name.endswith('_LABEL_ID'):
            prop['x-fk-table'] = 'LABEL_PROGRAM'
            prop['x-fk-column'] = ['LABEL_PROGRAM_ID', 'APP_LANG_ID']
            prop['x-fk-composite-key'] = True
            prop['x-fk-unique-index'] = 'UN1_LABEL_PROGRAM'
            prop['$ref'] = 'label_program.schema.json'
            prop['$comment'] = (
                'Composite FK via unique index UN1_LABEL_PROGRAM (label_program_id, app_lang_id); '
                'program_id determined from import context'
            )

        properties[col_name] = prop

    # 8. Assemble schema
    schema: dict = {
        '$schema': 'http://json-schema.org/draft-07/schema#',
        'title': to_title_case_oracle(table_name),
        'type': 'object',
    }

    if schema_filename:
        schema['$id'] = schema_filename

    if table_description:
        schema['description'] = table_description

    # Add schema-level metadata (required fields and table constraints)
    metadata = generate_metadata(table_name, column_defs, trigger_defaults, check_constraints,
                                 raw_check_list, column_name_set)
    schema.update(metadata)

    schema['properties'] = properties

    return schema


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------

def to_json_pretty(obj) -> str:
    """Serialize to JSON with indent=2. Compact 'enum' and 'type' arrays to single line."""
    json_str = json.dumps(obj, indent=2, ensure_ascii=False)
    
    # Compact enum arrays to single line.
    json_str = re.sub(
        r'"enum":\s*\[\s*([^\]]+)\s*\]',
        lambda m: '"enum": [' + ', '.join(x.strip() for x in m.group(1).split(',')) + ']',
        json_str,
        flags=re.DOTALL
    )
    
    # Compact type arrays to single line.
    json_str = re.sub(
        r'"type":\s*\[\s*([^\]]+)\s*\]',
        lambda m: '"type": [' + ', '.join(x.strip() for x in m.group(1).split(',')) + ']',
        json_str,
        flags=re.DOTALL
    )
    
    return json_str


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    sql_path = Path(sys.argv[1])
    if not sql_path.exists():
        print(f"Error: file not found: {sql_path}", file=sys.stderr)
        sys.exit(1)

    content = sql_path.read_text(encoding='utf-8')

    # Determine schema filename and output path
    if len(sys.argv) >= 3 and not sys.argv[2].startswith('--'):
        output_path = Path(sys.argv[2])
        schema_filename = output_path.name
    else:
        # Use input filename to derive schema name
        schema_filename = f"{sql_path.stem}.schema.json"
        output_path = sql_path.parent / schema_filename

    schema = parse_ddl(content, schema_filename)
    schema_json = to_json_pretty(schema)

    if len(sys.argv) >= 3 and not sys.argv[2].startswith('--'):
        output_path.write_text(schema_json, encoding='utf-8')
        print(f"Schema written to {output_path}")
    else:
        print(schema_json)


if __name__ == '__main__':
    main()
