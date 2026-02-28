"""Deploy all Prefect flows defined in prefect.yaml.

Usage:
    python manage.py deploy_flows            # deploy all flows
    python manage.py deploy_flows --dry-run  # show what would be deployed
    python manage.py deploy_flows --list     # list deployments in prefect.yaml
    python manage.py deploy_flows --name monthly-orchestrator  # deploy one flow

Requires:
    - prefect >= 3.0 installed (pip install prefect)
    - Prefect server or Prefect Cloud running (prefect server start)
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import yaml
from django.core.management.base import BaseCommand, CommandError


# Resolve project root (where prefect.yaml lives)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_PREFECT_YAML = _PROJECT_ROOT / "prefect.yaml"


class Command(BaseCommand):
    help = "Deploy all Prefect flows defined in prefect.yaml"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate prefect.yaml and show what would be deployed without deploying",
        )
        parser.add_argument(
            "--list",
            action="store_true",
            dest="list_deployments",
            help="List all deployments defined in prefect.yaml",
        )
        parser.add_argument(
            "--name",
            type=str,
            default="",
            help="Deploy only the named deployment (e.g. 'monthly-orchestrator')",
        )

    def handle(self, *args, **options):
        dry_run: bool = options["dry_run"]
        list_only: bool = options["list_deployments"]
        deploy_name: str = options["name"]

        # ------------------------------------------------------------------
        # 1. Validate prefect.yaml exists and is valid YAML
        # ------------------------------------------------------------------
        if not _PREFECT_YAML.is_file():
            raise CommandError(
                f"prefect.yaml not found at {_PREFECT_YAML}\n"
                f"Expected location: project root ({_PROJECT_ROOT})"
            )

        try:
            with open(_PREFECT_YAML) as fh:
                config = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise CommandError(f"Invalid YAML in {_PREFECT_YAML}: {exc}")

        if not config or "deployments" not in config:
            raise CommandError(
                f"prefect.yaml is missing the 'deployments' key.\n"
                f"See: https://docs.prefect.io/3.0/deploy/infrastructure-concepts/prefect-yaml"
            )

        deployments = config["deployments"]
        self.stdout.write(
            self.style.SUCCESS(
                f"Found {len(deployments)} deployment(s) in {_PREFECT_YAML}"
            )
        )

        # ------------------------------------------------------------------
        # 2. Validate entrypoints -- check that each flow file exists and
        #    contains a function with the expected name decorated with @flow
        # ------------------------------------------------------------------
        errors: list[str] = []
        for dep in deployments:
            name = dep.get("name", "<unnamed>")
            entrypoint = dep.get("entrypoint", "")

            if ":" not in entrypoint:
                errors.append(
                    f"  [{name}] Invalid entrypoint format: '{entrypoint}' "
                    f"(expected 'path/to/file.py:function_name')"
                )
                continue

            file_path_str, func_name = entrypoint.rsplit(":", 1)
            flow_file = _PROJECT_ROOT / file_path_str

            if not flow_file.is_file():
                errors.append(f"  [{name}] Flow file not found: {flow_file}")
                continue

            # Parse the file's AST to verify the function exists
            try:
                source = flow_file.read_text()
                tree = ast.parse(source, filename=str(flow_file))
            except SyntaxError as exc:
                errors.append(f"  [{name}] Syntax error in {flow_file}: {exc}")
                continue

            func_names = [
                node.name
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]

            if func_name not in func_names:
                errors.append(
                    f"  [{name}] Function '{func_name}' not found in {flow_file}. "
                    f"Available functions: {', '.join(func_names)}"
                )

        if errors:
            self.stderr.write(self.style.ERROR("Entrypoint validation errors:"))
            for err in errors:
                self.stderr.write(self.style.ERROR(err))
            raise CommandError("Fix the errors above before deploying.")

        self.stdout.write(self.style.SUCCESS("All entrypoints validated successfully"))

        # ------------------------------------------------------------------
        # 3. List mode -- just print deployments and exit
        # ------------------------------------------------------------------
        if list_only:
            self.stdout.write("")
            self.stdout.write(self.style.MIGRATE_HEADING("Deployments:"))
            for dep in deployments:
                name = dep.get("name", "<unnamed>")
                desc = dep.get("description", "")
                schedule = dep.get("schedule", {})
                cron = schedule.get("cron", "no schedule")
                tz = schedule.get("timezone", "UTC")
                tags = ", ".join(dep.get("tags", []))
                self.stdout.write(
                    f"  {self.style.SQL_KEYWORD(name)}\n"
                    f"    entrypoint: {dep.get('entrypoint', '')}\n"
                    f"    schedule:   {cron} ({tz})\n"
                    f"    tags:       {tags}\n"
                    f"    {desc}"
                )
                self.stdout.write("")
            return

        # ------------------------------------------------------------------
        # 4. Dry-run mode -- validate only
        # ------------------------------------------------------------------
        if dry_run:
            self.stdout.write("")
            self.stdout.write(
                self.style.SUCCESS("DRY RUN -- would deploy the following flows:")
            )
            for dep in deployments:
                name = dep.get("name", "<unnamed>")
                schedule = dep.get("schedule", {})
                cron = schedule.get("cron", "no schedule")
                self.stdout.write(f"  - {name} (cron: {cron})")
            self.stdout.write("")
            self.stdout.write(
                "Run without --dry-run to deploy, or use "
                "'prefect deploy --all' directly."
            )
            return

        # ------------------------------------------------------------------
        # 5. Check that Prefect CLI is available
        # ------------------------------------------------------------------
        try:
            version_result = subprocess.run(
                [sys.executable, "-m", "prefect", "version"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if version_result.returncode != 0:
                raise FileNotFoundError
            prefect_version = version_result.stdout.strip()
            self.stdout.write(f"Prefect version: {prefect_version}")
        except (FileNotFoundError, subprocess.TimeoutExpired):
            self.stderr.write(
                self.style.ERROR(
                    "Prefect CLI not found. Install it with:\n"
                    "  pip install prefect\n\n"
                    "Then start the Prefect server:\n"
                    "  prefect server start\n\n"
                    "Or configure Prefect Cloud:\n"
                    "  prefect cloud login"
                )
            )
            raise CommandError("Prefect CLI is required to deploy flows.")

        # ------------------------------------------------------------------
        # 6. Deploy
        # ------------------------------------------------------------------
        cmd = [sys.executable, "-m", "prefect", "deploy"]

        if deploy_name:
            cmd.extend(["-n", deploy_name])
        else:
            cmd.append("--all")

        self.stdout.write(f"Running: {' '.join(cmd)}")
        self.stdout.write(f"Working directory: {_PROJECT_ROOT}")
        self.stdout.write("")

        result = subprocess.run(
            cmd,
            cwd=str(_PROJECT_ROOT),
            timeout=120,
        )

        if result.returncode == 0:
            self.stdout.write(
                self.style.SUCCESS(
                    "\nAll flows deployed successfully. "
                    "Use 'prefect deployment ls' to verify."
                )
            )
        else:
            raise CommandError(
                f"Prefect deploy exited with code {result.returncode}.\n"
                f"Check the output above for details.\n\n"
                f"Common fixes:\n"
                f"  - Ensure Prefect server is running: prefect server start\n"
                f"  - Or log in to Prefect Cloud: prefect cloud login\n"
                f"  - Check prefect.yaml syntax: python -c \"import yaml; yaml.safe_load(open('prefect.yaml'))\""
            )
