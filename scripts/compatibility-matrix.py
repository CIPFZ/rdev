#!/usr/bin/env python3
"""Actual source-defined predecessor checks; not full release certification."""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import shutil
import signal
import subprocess
import stat
import time
import uuid

REPO = pathlib.Path(__file__).resolve().parents[1]
PREVIOUS = 'e73a38bcd94c6ffb4dbe379578c3d46ff70fa50e'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=REPO, text=True).strip()


def digest(path):
    with pathlib.Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def artifact_paths(root):
    """Accept a release bundle or the existing clean checkout build layout."""
    root = pathlib.Path(root).resolve(strict=True)
    if (root / 'bin/rdev').is_file():
        return {'cli': root / 'bin/rdev', 'broker': root / 'bin/rdevd',
                'agent_dir': root / 'cmd/rdev/agents'}
    return {'cli': root / 'rdev', 'broker': root / 'rdevd', 'agent_dir': root}


def freeze_artifacts(go, source, inputs, destination):
    """Snapshot each actual executable and validate its own clean VCS identity."""
    destination.mkdir(mode=0o700)
    agents = destination / 'agents'
    agents.mkdir(mode=0o700)
    paths = {'cli': inputs['cli'], 'broker': inputs['broker']}
    for platform in ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64'):
        paths['agent-' + platform] = inputs['agent_dir'] / ('rdev-agent-' + platform)
    identities = {}
    for component, path in paths.items():
        if not path.is_file() or path.is_symlink():
            raise SystemExit('regular immutable component required: ' + component)
        target = agents / path.name if component.startswith('agent-') else destination / component
        shutil.copyfile(path, target)
        target.chmod(0o500)
        metadata = subprocess.check_output([go, 'version', '-m', str(target)], text=True)
        settings = dict(re.findall(r'^\s*build\s+(\S+?)=(.*)$', metadata, re.MULTILINE))
        if settings.get('vcs.revision') != source or settings.get('vcs.modified') != 'false':
            raise SystemExit('component/source identity mismatch: ' + component)
        identities[component] = {'source_commit': source, 'sha256': digest(target),
                                 'goos': settings.get('GOOS'), 'goarch': settings.get('GOARCH')}
        expected_os, expected_arch = component.removeprefix('agent-').split('-') if component.startswith('agent-') else ('linux', 'amd64')
        expected_package = 'github.com/CIPFZ/rdev/cmd/' + ('rdev-agent' if component.startswith('agent-') else 'rdev' if component == 'cli' else 'rdevd')
        package = re.search(r'^\s*path\s+(\S+)$', metadata, re.MULTILINE)
        if (settings.get('GOOS'), settings.get('GOARCH')) != (expected_os, expected_arch) or not package or package.group(1) != expected_package:
            raise SystemExit('component platform/package mismatch: ' + component)
        identities[component]['package'] = expected_package
        (destination / (component + '.build.txt')).write_text(metadata)
    # Both real CLIs expose 12-hex embedded agent digest prefixes. This detects
    # accidental stale agents; it is explicitly not full embedded-byte proof.
    # Full candidate embedded bytes were verified by the release gate.
    version = subprocess.check_output([str(destination / 'cli'), 'version'], text=True)
    for platform in ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64'):
        expected = identities['agent-' + platform]['sha256'][:12]
        if not re.search(r'^\s*rdev-agent-' + platform + r'\s+' + expected + r'\s+\d+ bytes$', version, re.MULTILINE):
            raise SystemExit('CLI embedded agent identity mismatch: ' + platform)
    (destination / 'version.txt').write_text(version)
    return {'cli': str(destination / 'cli'), 'broker': str(destination / 'broker'),
            'agent_dir': str(agents)}, identities


def cases(current, previous):
    base = {'RDEV_TEST_CLI_BINARY': current['cli'], 'RDEV_TEST_DAEMON_BINARY': current['broker'],
            'RDEV_TEST_AGENT_DIR': current['agent_dir']}
    return [
        ('job-upgrade-retained-supervisors', 'TestRemoteBrokerJobUpgrade', base,
         {'client': 'test harness', 'broker': 'previous -> current', 'agent': 'previous -> current'}),
        ('old-cli-mcp-new-broker-new-agent', 'TestRemoteBrokerFrontendBoundary',
         {**base, 'RDEV_TEST_CLI_BINARY': previous['cli']},
         {'client': 'previous', 'broker': 'current', 'agent': 'current'}),
        ('new-cli-mcp-old-broker-old-agent', 'TestRemoteBrokerFrontendBoundary',
         {**base, 'RDEV_TEST_DAEMON_BINARY': previous['broker'], 'RDEV_TEST_AGENT_DIR': previous['agent_dir']},
         {'client': 'current', 'broker': 'previous', 'agent': 'previous'}),
        ('new-broker-refuses-legacy-agent-artifact', 'TestRemotePhase8SharedCompatibility',
         {**base, 'RDEV_TEST_AGENT_DIR': previous['agent_dir']},
         {'client': 'current CLI/MCP and current test wire', 'broker': 'current', 'agent': 'previous unchanged; missing candidate release identity rejected before business'}),
        ('old-broker-new-agent', 'TestRemotePhase8SharedCompatibility',
         {**base, 'RDEV_TEST_DAEMON_BINARY': previous['broker']},
         {'client': 'current CLI/MCP and current test wire', 'broker': 'previous', 'agent': 'current'}),
        ('standalone-new-cli-mcp-upgrades-old-agent', 'TestRemotePhase8StandaloneCompatibility',
         {**base, 'RDEV_TEST_COMPAT_OLD_CLIENT': '0'},
         {'client': 'current', 'broker': 'none during frontend calls', 'agent': 'previous -> current (authorized unsigned dev upgrade)'}),
        ('standalone-old-cli-mcp-refuses-new-agent', 'TestRemotePhase8StandaloneCompatibility',
         {**base, 'RDEV_TEST_CLI_BINARY': previous['cli'], 'RDEV_TEST_COMPAT_OLD_CLIENT': '1'},
         {'client': 'previous', 'broker': 'none during frontend calls', 'agent': 'current unchanged; downgrade rejected before business'}),
        ('fleet-approval-attempt-upgrade-continuity', 'TestRemotePhase8FleetCompatibility', base,
         {'client': 'current test wire', 'broker': 'previous -> current -> previous -> current',
          'agent': 'previous -> current; broker rollback retains current agent'}),
        ('secret-sync-outcome-upgrade-continuity', 'TestRemotePhase8RetainedStateCompatibility', base,
         {'client': 'current test wire', 'broker': 'previous creates archive/outcomes -> current recovers old pre-ACK outcome -> SIGKILL/restart -> previous broker-only rollback -> current',
          'agent': 'previous -> current -> SIGKILL/reconnect; no agent rollback or schema migration'}),
    ]


def process_start(pid):
    try:
        value = pathlib.Path('/proc', str(pid), 'stat').read_text()
        fields = value[value.rindex(')') + 1:].split()
        return fields[19] if fields[0] != 'Z' else None
    except (OSError, ValueError, IndexError):
        return None


def close_controls(root, env):
    """Only this case's private sockets; no fallback network connection."""
    directory = root / 'rdev-ctl'
    if not directory.exists():
        return True
    for path in directory.iterdir():
        if not stat.S_ISSOCK(path.lstat().st_mode):
            return False
        prefix = ['ssh']
        if env.get('RDEV_TEST_SSH_CONFIG'):
            prefix += ['-F', env['RDEV_TEST_SSH_CONFIG']]
        prefix += ['-S', str(path), '-O']
        try:
            check = subprocess.run(prefix + ['check', env['RDEV_TEST_REMOTE']],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10)
            match = re.search(rb'pid=(\d+)', check.stdout)
            if check.returncode or not match:
                return False
            pid = int(match[1])
            identity = process_start(pid)
            if identity is None:
                return False
            result = subprocess.run(prefix + ['exit', env['RDEV_TEST_REMOTE']],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        except subprocess.TimeoutExpired:
            return False
        if result.returncode:
            return False
        deadline = time.monotonic() + 5
        while path.exists() or process_start(pid) == identity:
            if time.monotonic() >= deadline:
                return False
            time.sleep(.02)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--previous', default=PREVIOUS)
    parser.add_argument('--artifact-source', help='clean candidate commit with unchanged product/build inputs')
    parser.add_argument('--current-artifacts', help='prebuilt release directory or clean checkout root')
    parser.add_argument('--previous-artifacts', help='prebuilt release directory or clean predecessor checkout root')
    parser.add_argument('--case', action='append', help='run selected named case; repeatable')
    args = parser.parse_args()
    if git('status', '--porcelain', '--untracked-files=all'):
        parser.error('clean source required')
    source = git('rev-parse', 'HEAD')
    candidate = git('rev-parse', (args.artifact_source or source) + '^{commit}')
    predecessor = git('rev-parse', args.previous + '^{commit}')
    if subprocess.run(['git', 'diff', '--quiet', candidate, source, '--', 'cmd', 'internal',
                       'go.mod', 'go.sum', 'Makefile', ':(exclude)**/*_test.go'], cwd=REPO).returncode:
        parser.error('candidate differs in product/build inputs')
    if os.environ.get('RDEV_RUN_REMOTE') != '1' or not os.environ.get('RDEV_TEST_REMOTE'):
        parser.error('explicit isolated authorized SSH target required')
    out = pathlib.Path(args.out).absolute()
    if out.exists():
        parser.error('new private output directory required')
    os.umask(0o077)
    out.mkdir(mode=0o700)
    old = pathlib.Path(args.previous_artifacts) if args.previous_artifacts else out / 'predecessor-source'
    if not args.previous_artifacts:
        subprocess.run(['git', 'clone', '--quiet', '--no-hardlinks', '--no-local', str(REPO), str(old)], check=True)
        subprocess.run(['git', '-C', str(old), 'checkout', '--quiet', '--detach', predecessor], check=True)
        with (out / 'build.log').open('w') as stream:
            subprocess.run(['make', 'GO=' + args.go, 'all', 'daemon'], cwd=old, stdout=stream, stderr=subprocess.STDOUT, check=True)
    current, current_ids = freeze_artifacts(args.go, candidate, artifact_paths(args.current_artifacts or REPO), out / 'current')
    previous, previous_ids = freeze_artifacts(args.go, predecessor, artifact_paths(old), out / 'previous')
    selected = cases(current, previous)
    if args.case:
        unknown = set(args.case) - {case[0] for case in selected}
        if unknown:
            parser.error('unknown case name')
        selected = [case for case in selected if case[0] in args.case]
    base = {**os.environ, 'RDEV_TEST_NAMESPACE_PREFIX': '.cache/rdev-phase5-matrix-' + uuid.uuid4().hex,
            'RDEV_TEST_CURRENT_AGENT_DIR': current['agent_dir'], 'RDEV_TEST_CURRENT_DAEMON_BINARY': current['broker'],
            'RDEV_TEST_PREDECESSOR_DAEMON_BINARY': previous['broker'],
            'RDEV_TEST_PREDECESSOR_AGENT_DIR': previous['agent_dir'], 'RDEV_TEST_SAFE_DIAGNOSTICS': '1'}
    result = {'schema_version': 2, 'source_commit': source, 'artifact_source_commit': candidate,
              'predecessor_commit': predecessor, 'predecessor_kind': 'Phase7 engineering baseline; no formal N-1 release claim',
              'started_at': timestamp(), 'toolchain': subprocess.check_output([args.go, 'version'], text=True).strip(),
              'components': {'current': current_ids, 'previous': previous_ids},
              'embedded_check': 'CLI version 12-hex SHA256 prefix comparison; not full embedded-byte proof', 'cases': [],
              'production_certification': 'pending', 'boundary': 'one authorized Linux amd64 SSH endpoint; private namespace per test; explicit unsigned-dev migration; actual CLI/MCP and state subsets only',
              'unverified_combinations': ['formal N/N-1 releases on every Tier1 platform', 'historical channel/pin/signed rollback matrix',
                                          'full schema migration/legacy writer fencing', 'formal historical secret archive/sync outcome agent rollback and schema migration',
                                          'peers actually lacking job_resource_envelope or durable_job_start; e73 has both']}
    for index, (name, test, overrides, components) in enumerate(selected):
        started = timestamp()
        temporary = out / ('tmp-' + str(index))
        temporary.mkdir(mode=0o700)
        env = {**base, **overrides, 'TMPDIR': str(temporary)}
        log = out / (name + '.log')
        with log.open('w') as stream:
            proc = subprocess.Popen([args.go, 'test', '-p=1', './cmd/rdevd', '-run', '^' + test + '$', '-count=1', '-timeout=5m', '-v'],
                                    cwd=REPO, env=env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                status = 'passed' if proc.wait(timeout=330) == 0 else 'failed'
            except subprocess.TimeoutExpired:
                status = 'failed'
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                    proc.wait(timeout=5)
                except ProcessLookupError:
                    pass
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
        # A missing environment or mount capability cannot become passing
        # certification merely because `go test` exits successfully on Skip.
        transcript = log.read_text()
        if status == 'passed' and re.search(r'^\s*--- SKIP:', transcript, re.MULTILINE):
            status = 'skipped'
        elif status == 'passed' and not re.search(r'^--- PASS: ' + re.escape(test) + r' \(', transcript, re.MULTILINE):
            status = 'not-run'
        controls_closed = close_controls(temporary, env)
        if not controls_closed:
            status = 'failed'
        result['cases'].append({'name': name, 'test': test, 'status': status, 'components': components,
                                'started_at': started, 'completed_at': timestamp(), 'log_sha256': digest(log), 'private_control_sockets_closed': controls_closed})
        if status != 'passed':
            (out / 'recovery.json').write_text(json.dumps({'namespace_prefix': base['RDEV_TEST_NAMESPACE_PREFIX'],
                'result': 'failed/skipped; process exit is not proof of cleanup',
                'action': 'Inspect only this private remote prefix for retained supervisors/jobs. Preserve unresolved outcomes.'}, indent=2) + '\n')
        result['completed_at'] = timestamp()
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    return 0 if all(case['status'] == 'passed' for case in result['cases']) else 1


if __name__ == '__main__':
    raise SystemExit(main())
