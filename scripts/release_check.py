#!/usr/bin/env python3
"""Run the local release validation gate."""

from __future__ import annotations

import argparse
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import venv
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
INIT_FILE = ROOT / "KISTI_DB_Manager" / "__init__.py"
RUST_MANIFEST = ROOT / "crates" / "kisti_json_rs" / "Cargo.toml"
PACKAGE_STAGING_DIRS = (
    ROOT / "build",
    ROOT / "dist",
    ROOT / "kisti_db_manager.egg-info",
)
EXPECTED_SDIST_PATHS = {
    "tests/test_profile_artifact_contracts.py",
    "tests/test_review_schema_artifact_contracts.py",
    "tests/fixtures/profile_contract/sample.csv",
    "tests/fixtures/profile_contract/expected_description_desc_v2.csv",
    "tests/fixtures/profile_contract/expected_description_profile_v2.json",
    "tests/fixtures/profile_contract/expected_dataset_profile_v1.json",
    "tests/fixtures/review_schema_contract/config.json",
    "tests/fixtures/review_schema_contract/description_profile.json",
    "tests/fixtures/review_schema_contract/report.json",
    "tests/fixtures/review_schema_contract/expected_schema_artifact_contract.json",
}


@dataclass(frozen=True)
class StepResult:
    name: str
    elapsed_sec: float


def _quote_cmd(cmd: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def _run_step(
    name: str,
    cmd: list[str],
    *,
    capture: bool = False,
    echo_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    print(f"[release-check] {name}: {_quote_cmd(cmd)}", flush=True)
    start = time.perf_counter()
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        check=False,
    )
    elapsed = time.perf_counter() - start
    if capture and proc.stdout and echo_output:
        print(proc.stdout.rstrip(), flush=True)
    if proc.returncode != 0:
        if capture and proc.stdout and not echo_output:
            print(proc.stdout.rstrip(), flush=True)
        raise SystemExit(f"[release-check] {name} failed after {elapsed:.2f}s")
    print(f"[release-check] {name}: ok ({elapsed:.2f}s)", flush=True)
    return proc


def _runner(args: argparse.Namespace) -> str:
    if args.runner == "python":
        return "python"
    if args.runner == "uv":
        if shutil.which("uv") is None:
            raise SystemExit("uv was requested but is not on PATH")
        return "uv"
    return "uv" if shutil.which("uv") else "python"


def _python_cmd(runner: str, *args: str) -> list[str]:
    if runner == "uv":
        return ["uv", "run", "--all-extras", *args]
    return [sys.executable, *args]


def _package_cmd(runner: str) -> list[str]:
    if runner == "uv":
        return ["uv", "run", "--with", "build", "python", "-m", "build", "--sdist", "--wheel"]
    return [sys.executable, "-m", "build", "--sdist", "--wheel"]


def _read_project_version() -> str:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def _read_init_version() -> str:
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', INIT_FILE.read_text(encoding="utf-8"), re.M)
    if not match:
        raise SystemExit(f"could not find __version__ in {INIT_FILE}")
    return match.group(1)


def _check_versions() -> str:
    project_version = _read_project_version()
    init_version = _read_init_version()
    if project_version != init_version:
        raise SystemExit(
            f"version mismatch: pyproject.toml has {project_version}, "
            f"KISTI_DB_Manager/__init__.py has {init_version}"
        )
    print(f"[release-check] version: {project_version}", flush=True)
    return project_version


def _require_clean_worktree() -> None:
    proc = subprocess.run(
        ["git", "status", "--short"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stdout.rstrip() or "git status failed")
    if proc.stdout.strip():
        raise SystemExit("working tree is not clean:\n" + proc.stdout.rstrip())


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _clean_package_outputs() -> None:
    for path in PACKAGE_STAGING_DIRS:
        _remove_path(path)


def _wheel_path(version: str) -> Path:
    candidates = sorted((ROOT / "dist").glob(f"kisti_db_manager-{version}-*.whl"))
    if not candidates:
        raise SystemExit(f"built wheel not found for version {version}")
    if len(candidates) > 1:
        raise SystemExit(f"expected one wheel for version {version}, found {len(candidates)}")
    return candidates[0]


def _sdist_path(version: str) -> Path:
    path = ROOT / "dist" / f"kisti_db_manager-{version}.tar.gz"
    if not path.exists():
        raise SystemExit(f"built sdist not found: {path}")
    return path


def _check_sdist_contains_fixtures(version: str) -> None:
    sdist = _sdist_path(version)
    prefix = f"kisti_db_manager-{version}/"
    with tarfile.open(sdist, "r:gz") as tf:
        names = {name.removeprefix(prefix) for name in tf.getnames() if name.startswith(prefix)}
    missing = sorted(EXPECTED_SDIST_PATHS - names)
    if missing:
        raise SystemExit("sdist is missing release regression fixtures:\n" + "\n".join(missing))
    print("[release-check] sdist fixture check: ok", flush=True)


def _venv_python(venv_dir: Path) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _wheel_smoke(version: str) -> None:
    wheel = _wheel_path(version)
    with tempfile.TemporaryDirectory(prefix="kisti-wheel-smoke-") as td:
        venv_dir = Path(td)
        venv.EnvBuilder(with_pip=True).create(venv_dir)
        py = _venv_python(venv_dir)
        _run_step("wheel smoke: upgrade pip", [str(py), "-m", "pip", "install", "--upgrade", "pip", "-q"])
        _run_step("wheel smoke: install wheel", [str(py), "-m", "pip", "install", str(wheel), "-q"])
        version_proc = _run_step(
            "wheel smoke: cli version",
            [str(py), "-m", "KISTI_DB_Manager.cli", "version"],
            capture=True,
        )
        actual = (version_proc.stdout or "").strip().splitlines()[-1]
        if actual != version:
            raise SystemExit(f"wheel CLI version mismatch: expected {version}, got {actual}")
        _run_step(
            "wheel smoke: cli help",
            [str(py), "-m", "KISTI_DB_Manager.cli", "--help"],
            capture=True,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", choices=("auto", "uv", "python"), default="auto")
    parser.add_argument("--require-clean", action="store_true", help="Fail if git status is not clean before checks")
    parser.add_argument("--skip-python", action="store_true")
    parser.add_argument("--skip-rust", action="store_true")
    parser.add_argument("--skip-docs", action="store_true")
    parser.add_argument("--skip-package", action="store_true")
    parser.add_argument("--skip-wheel-smoke", action="store_true")
    parser.add_argument("--keep-generated-uv-lock", action="store_true")
    parser.add_argument("--cleanup-package-outputs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    runner = _runner(args)
    uv_lock_existed = (ROOT / "uv.lock").exists()
    start = time.perf_counter()

    if args.require_clean:
        _require_clean_worktree()

    version = _check_versions()
    _run_step("whitespace", ["git", "diff", "--check"])

    if not args.skip_python:
        _run_step("python tests", _python_cmd(runner, "pytest", "-q"))

    if not args.skip_rust:
        _run_step("rust fmt", ["cargo", "fmt", "--manifest-path", str(RUST_MANIFEST), "--check"])
        _run_step("rust check", ["cargo", "check", "--manifest-path", str(RUST_MANIFEST)])
        _run_step(
            "rust tests",
            ["cargo", "test", "--manifest-path", str(RUST_MANIFEST), "--no-default-features"],
        )

    if not args.skip_docs:
        _run_step("docs", _python_cmd(runner, "mkdocs", "build", "--strict"))

    if not args.skip_package:
        _clean_package_outputs()
        _run_step("package build", _package_cmd(runner), capture=True, echo_output=False)
        _check_sdist_contains_fixtures(version)
        if not args.skip_wheel_smoke:
            _wheel_smoke(version)
        if args.cleanup_package_outputs:
            _clean_package_outputs()

    uv_lock = ROOT / "uv.lock"
    if runner == "uv" and uv_lock.exists() and not uv_lock_existed and not args.keep_generated_uv_lock:
        uv_lock.unlink()
        print("[release-check] removed generated uv.lock", flush=True)

    elapsed = time.perf_counter() - start
    print(f"[release-check] all checks passed in {elapsed:.2f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
