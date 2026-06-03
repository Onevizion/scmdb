#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path

import json_schema_py_ddl_parser as ddl_parser


def parse_args():
    parser = argparse.ArgumentParser(description="Generate JSON schemas from SCMDB table DDL files.")
    parser.add_argument("--ddl-root", required=True, help="Path to db/ddl directory")
    parser.add_argument("--output-dir", required=True, help="Directory where *.schema.json files will be written")
    parser.add_argument("--tables", default="", help="Comma-separated table names to generate")
    parser.add_argument("--all", action="store_true", help="Generate schemas for all table DDL files")
    return parser.parse_args()


def table_names(args, tables_dir):
    if args.all:
        return sorted(path.stem.upper() for path in tables_dir.glob("*.sql"))
    return sorted({name.strip().upper() for name in args.tables.split(",") if name.strip()})


def is_temporary_table(ddl_text):
    return re.search(r"\bCREATE\s+GLOBAL\s+TEMPORARY\s+TABLE\b", ddl_text, re.IGNORECASE) is not None


def main():
    args = parse_args()
    ddl_root = Path(args.ddl_root)
    tables_dir = ddl_root / "tables"
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not tables_dir.is_dir():
        print(f"DDL tables directory not found: {tables_dir}", file=sys.stderr)
        return 1

    names = table_names(args, tables_dir)
    if not names:
        print("No tables to generate JSON schemas for")
        return 0

    generated = 0
    skipped = 0
    failed = 0

    print(f"Generating JSON schemas into {output_dir}")
    for table_name in names:
        ddl_file = tables_dir / f"{table_name}.sql"
        if not ddl_file.exists():
            print(f"  SKIP {table_name}: DDL file not found")
            skipped += 1
            continue

        output_file = output_dir / f"{table_name.lower()}.schema.json"
        try:
            ddl_text = ddl_file.read_text(encoding="utf-8")
            if is_temporary_table(ddl_text):
                print(f"  SKIP {table_name}: temporary table")
                skipped += 1
                continue

            schema = ddl_parser.parse_ddl(
                ddl_text,
                output_file.name,
            )
            output_file.write_text(json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"  OK   {table_name} -> {output_file.name}")
            generated += 1
        except Exception as exc:
            print(f"  FAIL {table_name}: {exc}", file=sys.stderr)
            failed += 1

    print(f"JSON schema generation complete: generated={generated}, skipped={skipped}, failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
