#!/usr/bin/env python3

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import requests
import subprocess
import sys
from dataclasses import dataclass
from typing import Any

GITHUB_OWNER = "IKAMTeam"
GITHUB_REPO = "ov"

# Timeout for git operations in seconds
GIT_TIMEOUT = 300  # 5 minutes

def _log(p_msg: str) -> None:
    print(p_msg, file=sys.stderr)


# GitHub API

def _gh_headers(p_token: str) -> dict[str, str]:
    return {
        "Authorization": f"token {p_token}",
        "Accept": "application/vnd.github.v3+json"
    }


def _http_get_json(p_url: str, p_headers: dict[str, str]) -> Any:

    try:
        resp = requests.get(
            p_url,
            headers=p_headers,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    except requests.exceptions.HTTPError as e:
        body = ""
        try:
            body = e.response.text
        except Exception:
            pass
        raise RuntimeError(
            f"GitHub API HTTPError {e.response.status_code} for {p_url}: {body}"
        ) from e

    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"GitHub API RequestException for {p_url}: {e}"
        ) from e


def _get_pr_data(
    p_token: str,
    p_pr_number: int,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Returns:
      head_branch_name, commits[], files[]
    """
    headers = _gh_headers(p_token)

    PR_URL_TEMPLATE = "https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
    pr_url = PR_URL_TEMPLATE.format(owner=GITHUB_OWNER, repo=GITHUB_REPO, pr_number=p_pr_number)
    pr_data = _http_get_json(pr_url, headers)
    branch_name = pr_data["head"]["ref"]

    # 100 units per page is the max allowed by GitHub API, by default it is 30
    commits_url = f"{pr_url}/commits?per_page=100"
    commits = _http_get_json(commits_url, headers)

    files_url = f"{pr_url}/files?per_page=100"
    files = _http_get_json(files_url, headers)

    return branch_name, commits, files


def _filter_commits(p_commits: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    kept: list[str] = []
    skipped: list[str] = []

    for c in p_commits:
        sha = c.get("sha")
        msg = c.get("commit", {}).get("message", "")

        if "Merge branch 'master' into" in msg:
            if sha:
                skipped.append(sha)
        else:
            if sha:
                kept.append(sha)

    return kept, skipped


@dataclass(frozen=True)
class RepoLayout:
    repo_root: str
    db_dir_abs: str
    db_dir_name: str

    @property
    def scripts_abs(self) -> str:
        return os.path.join(self.db_dir_abs, "scripts")

    @property
    def ddl_abs(self) -> str:
        return os.path.join(self.db_dir_abs, "ddl")

    @property
    def packages_abs(self) -> str:
        return os.path.join(self.ddl_abs, "packages")

    @property
    def db_rel_prefix(self) -> str:
        return f"{self.db_dir_name}/"

    @property
    def scripts_rel_prefix(self) -> str:
        return f"{self.db_dir_name}/scripts/"

    @property
    def ddl_rel_prefix(self) -> str:
        return f"{self.db_dir_name}/ddl/"

    @property
    def packages_rel_prefix(self) -> str:
        return f"{self.db_dir_name}/ddl/packages/"


def _infer_repo_layout(p_db_path: str) -> RepoLayout:
    db_abs = os.path.abspath(p_db_path)
    if not os.path.isdir(db_abs):
        raise RuntimeError(f"DB path does not exist or not a directory: {db_abs}")

    db_dir_name = os.path.basename(db_abs.rstrip("/\\"))
    repo_root = os.path.abspath(os.path.join(db_abs, os.pardir))

    git_dir = os.path.join(repo_root, ".git")
    if not os.path.isdir(git_dir):
        raise RuntimeError(
            f"Cannot find .git in repo root inferred from DB path.\n"
            f"DB path: {db_abs}\n"
            f"Inferred repo root: {repo_root}\n"
            f"Expected: {git_dir}"
        )

    return RepoLayout(
        repo_root=repo_root,
        db_dir_abs=db_abs,
        db_dir_name=db_dir_name,
    )


def _summarize_pr_changes(
    p_files: list[dict[str, Any]],
    p_layout: RepoLayout,
) -> tuple[list[str], list[str], list[str], bool, list[str]]:
    """
    Returns:
      db_files, script_files, package_files, has_non_package_ddl, warnings
    """
    db_file_names: list[str] = []
    scripts_file_names: list[str] = []
    packages_file_names: list[str] = []
    has_non_package_ddl = False
    warnings: list[str] = []

    for f in p_files:
        filename = str(f.get("filename", ""))

        if filename.startswith(p_layout.db_rel_prefix):
            db_file_names.append(filename)
            if filename.startswith(p_layout.ddl_rel_prefix) and not filename.startswith(p_layout.packages_rel_prefix):
                has_non_package_ddl = True

        if filename.startswith(p_layout.scripts_rel_prefix):
            scripts_file_names.append(filename)

        if filename.startswith(p_layout.packages_rel_prefix):
            packages_file_names.append(filename)

    if has_non_package_ddl:
        warnings.append("Non-package DDL changes detected under <DB>/ddl/ outside of ddl/packages/.")

    return db_file_names, scripts_file_names, packages_file_names, has_non_package_ddl, warnings


def _detect_packages_from_files(p_packages_file_names: list[str]) -> set[str]:
    """
    Input are PR changed filenames under <DB>/ddl/packages/.
    - *_spec.sql -> package name without suffix
    - *.sql      -> package body file (already package name)
    """
    package_names: set[str] = set()

    for pkg_file in p_packages_file_names:
        base_name = pkg_file.split("/")[-1]

        if base_name.endswith("_spec.sql"):
            package_names.add(base_name[:-9].lower())
        elif base_name.endswith(".sql"):
            # body file
            package_names.add(base_name[:-4].lower())

    return package_names


# Git helpers

def _run_git(p_repo_root: str, p_args: list[str]) -> str:
    cmd = ["git", "-C", p_repo_root] + p_args
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=GIT_TIMEOUT)
    if res.returncode != 0:
        raise RuntimeError(f"Git command failed: {' '.join(cmd)}\n{res.stderr.strip()}")
    return res.stdout


def _cherry_pick_commits(p_repo_root: str, p_remote_branch_name: str, p_commit_ids: list[str]) -> None:
    if not p_commit_ids:
        return

    # Fetch branch to ensure commits exist locally
    _run_git(p_repo_root, ["fetch", "origin", p_remote_branch_name])

    original_head = _run_git(p_repo_root, ["rev-parse", "HEAD"]).strip()

    try:
        for sha in p_commit_ids:
            _run_git(p_repo_root, ["cherry-pick", sha])
    except Exception as e:
        try:
            subprocess.run(["git", "-C", p_repo_root, "cherry-pick", "--abort"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=GIT_TIMEOUT)
        except Exception:
            pass

        try:
            _run_git(p_repo_root, ["reset", "--hard", original_head])
        except Exception:
            pass

        raise RuntimeError(f"Cherry-pick failed. Repository reset to {original_head}. Details: {e}") from e


# Script generation

def _replace_date_2003_range(p_text: str) -> str:
    current_year = _dt.datetime.now().year
    return re.sub(r"2003-20\d\d", f"2003-{current_year}", p_text)


ROLLBACK_SUFFIX = "_rollback.sql"

def _parse_script_file_name(p_script_file_name: str) -> tuple[str, str, str, bool]:
    base_name = p_script_file_name.split("/")[-1]
    if not base_name.endswith(".sql"):
        raise ValueError(f"Invalid script name (not .sql): {p_script_file_name}")

    is_rollback = base_name.endswith(ROLLBACK_SUFFIX)
    clean_name = base_name[:-len(ROLLBACK_SUFFIX)] if is_rollback else base_name[:-4]

    parts = clean_name.split("_")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse script name: {p_script_file_name}")

    script_number = parts[0]
    branch_name = parts[1]  # Can be branch name (e.g., 'master') or branch_name number (e.g., 'Admin-266038', 'DB-242327-182469')
    # Package name is optional - scripts without package name are also valid
    package_name = "_".join(parts[2:]).lower() if len(parts) > 2 else ""

    return script_number, branch_name, package_name, is_rollback


def _create_script_file(
    p_layout: RepoLayout,
    p_package_name: str,
    p_script_number: int,
    p_branch_name: str,
    p_is_rollback: bool,
) -> tuple[str, str]:
    file_prefix = f"{p_script_number}_{p_branch_name}_{p_package_name}"
    spec_path = os.path.join(p_layout.packages_abs, f"{p_package_name}_spec.sql")
    body_path = os.path.join(p_layout.packages_abs, f"{p_package_name}.sql")

    if p_is_rollback:
        out_name = f"{file_prefix}_rollback.sql"
    else:
        out_name = f"{file_prefix}.sql"

    if not os.path.isfile(spec_path):
        raise FileNotFoundError(f"Package spec file not found: {spec_path}")

    with open(spec_path, "r", encoding="utf-8") as f:
        package_spec = f.read()

    if os.path.isfile(body_path):
        with open(body_path, "r", encoding="utf-8") as f:
            package_body = f.read()
        has_body = True
    else:
        package_body = ""
        has_body = False

    package_spec = package_spec.rstrip("\n").rstrip(" ")
    script = package_spec + ("\n\n" if package_spec.endswith("/") else "/\n\n")

    if has_body:
        package_body = package_body.rstrip(" ").rstrip("\n").rstrip(" ")
        script += package_body
        if not script.endswith("\n"):
            script += "\n"

    if not p_is_rollback:
        script = _replace_date_2003_range(script)

    return out_name, script


def _collect_scripts_for_packages(
    p_scripts_file_names: list[str],
    p_package_names: set[str],
    p_is_rollback: bool,
) -> dict[str, tuple[str, str, int, str]]:
    """
    Returns dict keyed by package_name:
      package_name -> (script_file_name, content, script_number, branch_name)
    """
    scripts: dict[str, tuple[str, str, int, str]] = {}

    for script_file in p_scripts_file_names:
        base_name = script_file.split("/")[-1]

        if p_is_rollback and not base_name.endswith(ROLLBACK_SUFFIX):
            continue
        if (not p_is_rollback) and base_name.endswith(ROLLBACK_SUFFIX):
            continue

        # Parse script file name to get exact package name
        try:
            script_number_s, branch_name, package_name, _ = _parse_script_file_name(base_name)
            script_number = int(script_number_s)
        except (ValueError, IndexError):
            continue

        # Skip scripts without package name - they are DDL scripts (tables, views, etc.)
        # that will be executed by SCMDB as-is, not regenerated from package sources
        if not package_name:
            continue

        # Check if parsed package name matches one of the expected packages
        if package_name not in p_package_names:
            continue

        scripts[package_name] = (base_name, "", script_number, branch_name)

    return scripts


def _write_script(p_layout: RepoLayout, p_file_name: str, p_content: str) -> None:
    os.makedirs(p_layout.scripts_abs, exist_ok=True)
    full_path = os.path.join(p_layout.scripts_abs, p_file_name)
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(p_content)


# Main

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="backport_pipeline",
        description="Backport PR: cherry-pick commits and regenerate package scripts. SCMDB is not executed here.",
    )

    p.add_argument("pr", type=int, help="Pull Request number to backport")

    p.add_argument("--gh-token", required=True, help="GitHub token (passed from Java)")
    p.add_argument("--db-path", required=True, help="Absolute path to the repo <DB>/ directory (passed from Java)")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    result: dict[str, Any] = {
        "should_run_scmdb": False,
        "scripts_written": 0,
        "rollback_scripts_written": 0,
        "renamed": 0,
        "warnings": [],
    }

    try:
        layout = _infer_repo_layout(args.db_path)

        token = args.gh_token
        pr = args.pr

        package_names: set[str] = set()

        branch, commits, files = _get_pr_data(token, pr)

        _log(f"PR #{pr}: head branch = {branch}")

        db_files, scripts_file_names, packages_file_names, has_non_package_ddl, warnings = _summarize_pr_changes(files, layout)
        result["warnings"].extend(warnings)

        _log(f"changed files total: {len(files)}")
        _log(f"changed under {layout.db_dir_name}/: {len(db_files)}")
        _log(f"changed under {layout.db_dir_name}/scripts/: {len(scripts_file_names)}")
        _log(f"changed under {layout.db_dir_name}/ddl/packages/: {len(packages_file_names)}")
        if files:
            _log("changed files:")
            for f in files:
                filename = str(f.get("filename", ""))
                if filename:
                    _log(f"- {filename}")

        if has_non_package_ddl:
            msg = "Non-package DDL changes detected."

        if not files:
            print(json.dumps(result, ensure_ascii=False))
            return 0

        commit_ids, skipped_commit_ids = _filter_commits(commits)

        _log("Commits from PR:")
        for c in commits:
            sha = str(c.get("sha", ""))
            msg = str(c.get("commit", {}).get("message", ""))
            first_line = msg.splitlines()[0] if msg else ""
            _log(f"- {sha[:7]} {first_line}")

        if skipped_commit_ids:
            _log(f"commits skipped by filter: {len(skipped_commit_ids)}")

        # 1) Cherry-pick commits to current branch
        if commit_ids:
            _cherry_pick_commits(layout.repo_root, branch, commit_ids)

        # 2) Determine packages from changed package files
        package_names = _detect_packages_from_files(packages_file_names)
        if package_names:
            _log(f"packages detected from ddl/packages changes: {len(package_names)}")
            for p in sorted(package_names):
                _log(f"- {p}")
        else:
            _log("packages detected from ddl/packages changes: 0")

        # 3) Recreate scripts
        rollback_candidates = _collect_scripts_for_packages(scripts_file_names, package_names, p_is_rollback=True)
        direct_candidates = _collect_scripts_for_packages(scripts_file_names, package_names, p_is_rollback=False)

        # Generate + write rollback scripts
        for pkg, (_, _, script_number, branch_name) in rollback_candidates.items():
            out_name, out_text = _create_script_file(layout, pkg, script_number, branch_name, p_is_rollback=True)
            _write_script(layout, out_name, out_text)
            result["rollback_scripts_written"] += 1

        # Generate + write direct scripts
        for pkg, (_, _, script_number, branch_name) in direct_candidates.items():
            out_name, out_text = _create_script_file(layout, pkg, script_number, branch_name, p_is_rollback=False)
            _write_script(layout, out_name, out_text)
            result["scripts_written"] += 1

        # SCMDB decision: run if PR contains any scripts
        result["should_run_scmdb"] = (len(scripts_file_names) > 0) or ((result["scripts_written"] + result["rollback_scripts_written"]) > 0)

        print(json.dumps(result, ensure_ascii=False))
        return 0

    except Exception as e:
        result["error"] = str(e)
        print(json.dumps(result, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())