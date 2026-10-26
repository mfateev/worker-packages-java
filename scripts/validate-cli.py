#!/usr/bin/env python3
"""Smoke-test the installed CLI against a temporary local Temporal test service.

First run from the plugin root:
    ./gradlew packageExamples installDist
    python3 scripts/validate-cli.py

This script never invokes Gradle. It records commands, stdout, stderr, server and
worker logs, and summary.json under build/verification/cli-smoke. Child processes
are bounded and terminated on success, failure, or interruption. Python 3 and the
built Java 8-compatible CLI distribution are the only prerequisites.
"""

import csv
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "build/verification/cli-smoke"
CLI = ROOT / "build/install/temporal-worker-packages/bin/temporal-worker-packages"
PACKAGES = ROOT / "build/examples"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=OUTPUT))
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith("TEMPORAL_")
    }
    commands = []
    children = []
    phases = []
    started = time.monotonic()
    summary = {"success": False, "logs": str(run_dir.relative_to(ROOT))}

    def write_json(path, value):
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def record(entry):
        commands.append(entry)
        write_json(run_dir / "commands.json", commands)

    def command(name, *arguments, timeout=45, allow_failure=False):
        index = len(commands) + 1
        stdout_path = run_dir / f"{index:02d}-{name}.stdout.log"
        stderr_path = run_dir / f"{index:02d}-{name}.stderr.log"
        invocation = [str(CLI), *map(str, arguments)]
        entry = {
            "name": name, "argv": invocation, "shell": shlex.join(invocation),
            "stdout": stdout_path.name, "stderr": stderr_path.name,
        }
        began = time.monotonic()
        with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
            process = subprocess.Popen(invocation, cwd=ROOT, env=environment,
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except BaseException:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
                entry.update(exitCode=process.returncode, interrupted=True,
                             elapsedSeconds=round(time.monotonic() - began, 3))
                record(entry)
                raise
        entry.update(exitCode=code, elapsedSeconds=round(time.monotonic() - began, 3))
        record(entry)
        output = stdout_path.read_text(encoding="utf-8")
        error = stderr_path.read_text(encoding="utf-8")
        if code != 0 and not allow_failure:
            raise RuntimeError(f"CLI {name} exited {code}; see {stderr_path}\n{error[-4000:]}")
        return code, output

    def launch(name, *arguments):
        stdout_path, stderr_path = run_dir / f"{name}.stdout.log", run_dir / f"{name}.stderr.log"
        invocation = [str(CLI), *map(str, arguments)]
        stdout, stderr = stdout_path.open("w", encoding="utf-8"), stderr_path.open("w", encoding="utf-8")
        try:
            process = subprocess.Popen(invocation, cwd=ROOT, env=environment,
                                       stdout=stdout, stderr=stderr, start_new_session=True)
        except BaseException:
            stdout.close()
            stderr.close()
            raise
        child = {"name": name, "process": process, "streams": (stdout, stderr),
                 "stdout": stdout_path, "stderr": stderr_path}
        children.append(child)
        record({"name": name, "argv": invocation, "shell": shlex.join(invocation),
                "pid": process.pid, "stdout": stdout_path.name, "stderr": stderr_path.name})
        return child

    def ready(child, evidence, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            require(child["process"].poll() is None,
                    f"{child['name']} exited before readiness; inspect {child['stderr']}")
            if evidence in child["stdout"].read_text(encoding="utf-8", errors="replace"):
                return
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        raise TimeoutError(f"No {evidence!r} readiness evidence in {child['stdout']}")

    try:
        require(CLI.is_file() and os.access(CLI, os.X_OK),
                "Installed CLI missing; first run ./gradlew packageExamples installDist")
        require((PACKAGES / "greetings-1.twp").is_file() and (PACKAGES / "greetings-2.twp").is_file(),
                "Example archives missing; first run ./gradlew packageExamples")

        _, help_text = command("help", "help")
        require("Usage:" in help_text and "worker" in help_text and "pack" in help_text,
                "CLI help omitted documented commands")
        phases.append("installed CLI help")

        _, listing = command("list", "list", "--packages", PACKAGES)
        rows = list(csv.DictReader(io.StringIO(listing), delimiter="\t"))
        greetings = [row for row in rows if row["PACKAGE"] == "greetings"]
        require([row["VERSION"] for row in greetings] == ["1", "2"], "Expected installed versions 1,2")

        require(all(re.fullmatch(r"[0-9a-f]{64}", row["ARCHIVE CHECKSUM"]) for row in greetings),
                "Listing omitted full archive digests")
        summary["packageDigests"] = {row["VERSION"]: row["ARCHIVE CHECKSUM"] for row in greetings}
        phases.append("string package version listing")

        archive = PACKAGES / "greetings-2.twp"
        _, validation = command("validate", "validate", "--archive", archive)
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        require(f"greetings version 2 archive checksum {digest}" in validation,
                "CLI validation did not report the expected package identity")
        require(summary["packageDigests"]["2"] == digest, "Listing and validation digests differ")
        phases.append("archive validation with full digest")

        extraction = run_dir / "pack-input"
        extraction.mkdir()
        with zipfile.ZipFile(archive) as source:
            manifest_bytes = source.read("workflow-package.json")
            manifest = json.loads(manifest_bytes)
            (extraction / "workflow-package.json").write_bytes(manifest_bytes)
            for entry in manifest["classpath"]:
                path = PurePosixPath(entry)
                require(not path.is_absolute() and all(p not in ("", ".", "..") for p in path.parts),
                        f"Unsafe validated classpath entry: {entry}")
                target = extraction.joinpath(*path.parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source.read(entry))
        repacked = [run_dir / "repacked-one.twp", run_dir / "repacked-two.twp"]
        for index, output in enumerate(repacked, 1):
            _, pack_output = command(f"pack-{index}", "pack", "--manifest", extraction / "workflow-package.json",
                                     "--input-root", extraction, "--output", output)
            require(f"archive checksum {hashlib.sha256(output.read_bytes()).hexdigest()}" in pack_output,
                    "CLI packaging omitted the archive checksum label or value")
        require(repacked[0].read_bytes() == repacked[1].read_bytes() == archive.read_bytes(),
                "CLI packaging did not preserve deterministic archive bytes")
        phases.append("deterministic CLI repack of the validated example archive")

        patch_directory = run_dir / "patch-only"
        patch_directory.mkdir()
        for version, patched in (("patch1", "2"), ("patch2", "patch1")):
            patch_manifest = dict(manifest, packageVersion=version, patchesVersion=patched)
            (extraction / "workflow-package.json").write_text(json.dumps(patch_manifest), encoding="utf-8")
            patch_archive = patch_directory / f"greetings-{patched}-{version}.twp"
            command(f"pack-{version}", "pack", "--manifest", extraction / "workflow-package.json",
                    "--input-root", extraction, "--output", patch_archive)
            _, patch_validation = command(f"validate-{version}", "validate", "--archive", patch_archive)
            require(f"Patches version {patched}" in patch_validation,
                    "CLI validation omitted the patch target")
        _, patch_listing = command("list-patches", "list", "--packages", patch_directory)
        patch_rows = list(csv.DictReader(io.StringIO(patch_listing), delimiter="\t"))
        require([(row["VERSION"], row["PATCHES VERSION"]) for row in patch_rows]
                == [("patch1", "2"), ("patch2", "patch1")],
                "CLI listing omitted patch-chain metadata")
        for row in patch_rows:
            require(row["ARCHIVE CHECKSUM"] == hashlib.sha256(Path(row["ARCHIVE"]).read_bytes()).hexdigest(),
                    "CLI patch listing has an incorrect archive checksum")
        phases.append("CLI patch packaging, canonical names, validation and patch-target listing")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as allocation:
            allocation.bind(("127.0.0.1", 0))
            port = allocation.getsockname()[1]
        endpoint = f"127.0.0.1:{port}"
        suffix = uuid.uuid4().hex
        queue, workflow_id = f"cli-smoke-{suffix}", f"cli-greeting-{suffix}"
        common = ["--endpoint", endpoint, "--namespace", "default", "--task-queue", queue]
        server = launch("server", "server", "--port", port)
        ready(server, f"Local test server listening on port {port}")
        worker = launch("worker", "worker", "--package-version", "2", "--packages", patch_directory, "--cache", run_dir / "cache", *common)
        ready(worker, f"Worker polling {queue}")
        phases.append("installed CLI test server and patch-only chain worker startup")

        _, start_output = command("start", "start", "--package-id", "greetings", "--type", "GreetingWorkflow",
                                  "--workflow-id", workflow_id, "--input",
                                  json.dumps([]), *common)
        execution = json.loads(start_output)
        require(execution.get("workflowId") == workflow_id and execution.get("runId"),
                "CLI start did not return a workflow execution")
        target = ["--workflow-id", workflow_id, "--run-id", execution["runId"], *common]
        summary["workflow"] = execution

        phases.append("header-routed zero-argument workflow start")

        _, signalled = command("signal", "signal", "--name", "proceed", "--input",
                               json.dumps([]), *target)
        require(json.loads(signalled) == "signal accepted", "CLI signal was not accepted")
        _, result = command("result", "result", "--timeout-seconds", "30", *target, timeout=60)
        expected = "Hi, multi-versioned worker!"
        require(json.loads(result) == expected, f"CLI workflow/activity result differs from {expected!r}")
        summary.update(selectedVersion="2", effectivePatchVersion="patch2", result=json.loads(result))
        phases.append("signal and result prove changed Greet signature and added GetName activity")
        summary["success"] = True
    except BaseException as failure:
        summary.update(error=f"{type(failure).__name__}: {failure}", traceback=traceback.format_exc())
        print(summary["error"], file=sys.stderr)
    finally:
        cleanup = []
        for child in reversed(children):
            process = child["process"]
            code_before = process.poll()
            forced = False
            if code_before is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    forced = True
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
                except ProcessLookupError:
                    process.wait(timeout=5)
            for stream in child["streams"]:
                stream.close()
            accepted = code_before is None and not forced and process.returncode in (0, 143, -signal.SIGTERM)
            cleanup.append({"name": child["name"], "exitCode": process.returncode,
                            "forcedKill": forced, "passed": accepted})
            if not accepted:
                summary["success"] = False
                summary.setdefault("error", f"{child['name']} exited unexpectedly or required forced cleanup")
        summary.update(phases=phases, cleanup=cleanup,
                       elapsedSeconds=round(time.monotonic() - started, 3))
        write_json(run_dir / "summary.json", summary)
        write_json(OUTPUT / "summary.json", summary)
        print(json.dumps(summary, indent=2, sort_keys=True))
        print(f"CLI smoke logs: {run_dir}")
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
