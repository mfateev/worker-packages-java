#!/usr/bin/env python3
"""Verify the plugin as an independent Git clone with a fresh sdk-java main checkout and no samples checkout."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, help='Evidence directory; defaults to build/verification/clean-checkout')
    options = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = (options.output or root / 'build/verification/clean-checkout').resolve()
    output.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix='worker-packages-clean-', dir='/private/tmp' if Path('/private/tmp').exists() else None))
    snapshot = workspace / 'snapshot'
    clone = workspace / 'fresh/worker-packages-java'
    snapshot.mkdir()
    env = dict(os.environ)
    env.pop('TEMPORAL_SDK_DIR', None)
    env.pop('GRADLE_USER_HOME', None)
    env.pop('WORKER_PACKAGES_DIR', None)
    for key in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE'):
        env.pop(key, None)
    env['GIT_TERMINAL_PROMPT'] = '0'
    commands = []

    def run(args, cwd, timeout=900):
        args = [str(x) for x in args]
        record = {'argv': args, 'cwd': str(cwd)}
        commands.append(record)
        number = len(commands)
        log = output / f'{number:02d}.log'
        print(f'[{number}] {" ".join(args)}', flush=True)
        with log.open('w') as stream:
            result = subprocess.run(args, cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=timeout)
        record.update(exitCode=result.returncode, log=str(log))
        if result.returncode:
            raise RuntimeError(f'Command exited {result.returncode}; see {log}')

    git = ['git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', '-c', 'user.name=Validation fixture', '-c', 'user.email=validation@example.invalid']
    result = {'workspace': str(workspace), 'clone': str(clone), 'commands': commands, 'status': 'failed'}
    try:
        listing = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=root)
        owned_roots = {'.github', 'AGENTS.md', 'CONTRIBUTING.md', 'LICENSE', '.gitignore',
                       'ACCEPTANCE_REPORT.md', 'README.md', 'SPEC.md', 'build-logic',
                       'build.gradle', 'docs', 'gradle.properties', 'gradle',
                       'gradlew', 'gradlew.bat', 'gradle.lockfile', 'scripts', 'settings.gradle', 'src'}
        names = sorted({os.fsdecode(name) for name in listing.split(b'\0') if name
                        and os.fsdecode(name).split('/', 1)[0] in owned_roots})
        copied_sources = 0
        for name in names:
            source = root / name
            if source.is_symlink():
                raise RuntimeError(f'Expected independent source file: {source}')
            # git ls-files also reports tracked files deleted by the current change.
            if not source.exists():
                continue
            if not source.is_file():
                raise RuntimeError(f'Expected independent source file: {source}')
            destination = snapshot / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            copied_sources += 1
        result['sourceFileCount'] = copied_sources
        # Only the temporary snapshot is committed; the user's original repository stays untouched.
        run(git + ['init', '-b', 'main'], snapshot)
        run(git + ['add', '--all'], snapshot)
        run(git + ['commit', '-m', 'Temporary validation source snapshot'], snapshot)
        clone.parent.mkdir()
        run(git + ['clone', '--no-local', str(snapshot), str(clone)], workspace)
        assert not (clone.parent / 'sdk-java').exists()
        assert not (clone.parent / 'worker-packages-java-samples').exists()
        run(['./scripts/bootstrap-sdk.sh'], clone)
        run(['./gradlew', '--no-daemon', 'build', 'installDist', 'writeDependencyReport'], clone)
        for location in (clone,):
            status = subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all'], cwd=location, text=True)
            if status.strip():
                raise RuntimeError(f'Validation modified tracked source or created unignored outputs in {location}: {status}')
        assert not (clone.parent / 'worker-packages-java-samples').exists()
        result['samplesCheckoutAbsent'] = True
        result['status'] = 'passed'
        print(f'Clean clone, fresh SDK bootstrap, independent library build/tests and CLI distribution passed without a samples checkout. Evidence: {output}', flush=True)
    finally:
        (output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
        # Preserve the clone and reports for review; it lives outside the implementation repository.


if __name__ == '__main__':
    main()
