#!/usr/bin/env python3
"""Actual signed SSH upgrade/rollback using isolated test releases, never publication."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import uuid


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--remote', required=True, help='authorized isolated SSH alias')
    parser.add_argument('--ssh-config', type=Path, required=True)
    for role in ('previous', 'candidate', 'frozen'):
        parser.add_argument('--' + role + '-policy', type=Path, required=True,
                            help='private administrator test-root policy for a complete signed bundle')
    parser.add_argument('--allow-dirty-fixture', action='store_true', help='draft verification only; never final-source acceptance')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    source = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    dirty = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo))
    if dirty and not args.allow_dirty_fixture:
        parser.error('clean fixture source required')
    os.umask(0o077)
    args.out = args.out.resolve()
    if args.out.is_relative_to(repo.resolve()):
        parser.error('evidence directory must be outside the source checkout')
    args.out.mkdir(mode=0o700)
    bundles = {}
    before = {}
    env = {**os.environ, 'GOMAXPROCS': '2', 'GOFLAGS': '-p=1', 'RDEV_RUN_REMOTE': '1',
           'RDEV_TEST_REMOTE': args.remote, 'RDEV_TEST_SSH_CONFIG': str(args.ssh_config.resolve(strict=True)),
           'RDEV_TEST_NAMESPACE_PREFIX': os.environ.get('RDEV_TEST_NAMESPACE_PREFIX') or '.cache/rdev-phase5-signed-' + uuid.uuid4().hex,
           'RDEV_TEST_SAFE_DIAGNOSTICS': '1'}
    prefix = env['RDEV_TEST_NAMESPACE_PREFIX']
    if not re.fullmatch(r'\.cache/rdev-phase5-[A-Za-z0-9._/-]+', prefix) or any(part in ('', '.', '..') for part in prefix.split('/')):
        parser.error('private home-relative phase5-compatible namespace prefix required')
    for role in ('previous', 'candidate', 'frozen'):
        policy = getattr(args, role + '_policy').resolve(strict=True)
        config = json.loads(policy.read_text())
        bundle = Path(config['bundle_dir']).resolve(strict=True)
        manifest = json.loads((bundle / 'release.json').read_text())
        if not re.fullmatch(r'[a-f0-9]{40}', manifest['source']['commit']):
            parser.error('bundle source commit must be a complete immutable Git identity')
        bundles[role] = {'version': manifest['version'], 'source': manifest['source'],
                         'signer': manifest['signer'], 'policy_sha256': sha(policy),
                         'release_sha256': sha(bundle / 'release.json'), 'signature_sha256': sha(bundle / 'release.json.sig'),
                         'artifacts': {value['name']: value['sha256'] for value in manifest['binaries'] + manifest['metadata']}}
        for value in manifest['binaries'] + manifest['metadata']:
            path = bundle / value['name']
            if path.name != value['name'] or sha(path) != value['sha256']:
                parser.error('bundle artifact differs before actual signature verification')
            before[str(path)] = value['sha256']
        before[str(policy)] = sha(policy)
        before[str(bundle / 'release.json')] = sha(bundle / 'release.json')
        before[str(bundle / 'release.json.sig')] = sha(bundle / 'release.json.sig')
        env['RDEV_TEST_SIGNED_' + role.upper() + '_POLICY'] = str(policy)
        if role == 'frozen':
            env['RDEV_TEST_DAEMON_BINARY'] = str(bundle / 'rdevd')
            env['RDEV_TEST_AGENT_DIR'] = str(bundle)
    temporary = args.out / 'tmp'
    temporary.mkdir(mode=0o700)
    env['TMPDIR'] = str(temporary)
    report = {'schema_version': 1, 'source_commit': source, 'source_dirty': dirty,
              'draft_fixture': args.allow_dirty_fixture, 'started_at': now(), 'status': 'running',
              'runtime_test_sha256': sha(repo / 'cmd/rdevd/remote_phase8_signed_test.go'),
              'shared_fixture_sha256': sha(repo / 'cmd/rdevd/remote_phase8_compat_test.go'),
              'runner_sha256': sha(Path(__file__)), 'bundles': bundles, 'steps': [],
              'scope': {'ssh': 'one authorized Linux amd64 endpoint; unique private namespace',
                        'signer': 'administrator-selected isolated test root; no official release identity',
                        'historical_sources': 'source-defined test releases; implementation/schema migration is not inferred from source stamps or test versions; not formal N/N-1',
                        'candidate': 'additional test-only candidate; supplied frozen bundle is checked unchanged',
                        'runtime_broker': {'source_commit': bundles['frozen']['source']['commit'], 'version': bundles['frozen']['version'], 'sha256': bundles['frozen']['artifacts']['rdevd']},
                        'state': 'future/corrupt fixture manifests preserved on refusal, then exact fixture snapshot restored',
                        'production_certification': 'pending'}}
    try:
        diff = subprocess.check_output(['git', 'diff', '--name-only', bundles['previous']['source']['commit'],
                                        bundles['candidate']['source']['commit'], '--', 'cmd', 'internal',
                                        'go.mod', 'go.sum', 'Makefile', ':(exclude)**/*_test.go'],
                                       cwd=repo, text=True, stderr=subprocess.DEVNULL)
        report['historical_product_diff_paths'] = diff.splitlines()
    except subprocess.CalledProcessError:
        report['historical_product_diff_paths'] = 'not verified; supplied history unavailable in fixture checkout'
    def write():
        (args.out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    write()
    test_binary = args.out / 'runtime.test'
    commands = [('compile', [args.go, 'test', '-p=1', '-c', '-o', str(test_binary), './cmd/rdevd']),
                ('signed-ssh', [str(test_binary), '-test.run=^TestRemotePhase8SignedUpgradeRollback$', '-test.v', '-test.timeout=3m'])]
    for name, command in commands:
        row = {'name': name, 'command': command, 'started_at': now()}
        with (args.out / (name + '.log')).open('wb') as stream:
            process = subprocess.Popen(['nice', '-n', '10', *command], cwd=repo, env=env,
                                       stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=210)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                code = process.wait()
                row['timeout'] = True
        log = args.out / (name + '.log')
        row.update(exit_code=code, completed_at=now(), log_sha256=sha(log))
        report['steps'].append(row)
        write()
        if code:
            report['status'] = 'failed'
            break
        if name == 'compile':
            report['test_binary_sha256'] = sha(test_binary)
        if name == 'signed-ssh':
            text = log.read_text(errors='replace')
            passed = re.search(r'^--- PASS: TestRemotePhase8SignedUpgradeRollback \(', text, re.MULTILINE)
            report['status'] = 'passed' if passed and '--- SKIP:' not in text else 'not-run'
    runtime_log = args.out / 'signed-ssh.log'
    transcript = runtime_log.read_text(errors='replace') if runtime_log.exists() else ''
    scopes = [{'namespace': namespace, 'control_scope': control} for namespace, control in
              re.findall(r'RDEV_SIGNED_SCOPE namespace=([A-Za-z0-9._/-]+) control=(/tmp/rdev-compat-[A-Za-z0-9_-]+)', transcript)]
    report['observed_private_scopes'] = scopes
    report['runtime_subcases_passed'] = re.findall(r'--- PASS: TestRemotePhase8SignedUpgradeRollback/([a-z-]+) ', transcript)
    report['inputs_unchanged'] = all(sha(Path(path)) == digest for path, digest in before.items())
    if not report['inputs_unchanged']:
        report['status'] = 'failed'
    if report['status'] == 'passed':
        expected_subcases = {'force-without-authorization', 'wrong-target-authorization', 'wrong-digest-authorization',
                             'authorization-without-force', 'future-state', 'corrupt-state'}
        if set(report['runtime_subcases_passed']) != expected_subcases:
            report['status'] = 'failed'
    if report['status'] == 'passed':
        if len(scopes) != 1 or any(Path(scope['control_scope']).exists() for scope in scopes):
            report['status'] = 'failed'
        else:
            shutil.rmtree(temporary)
            report['cleanup'] = 'actual test namespace absence and exact control PID/start exit asserted; private compilation tmp removed'
    report['completed_at'] = now()
    if report['status'] != 'passed':
        report['recovery'] = {'namespace_prefix': env['RDEV_TEST_NAMESPACE_PREFIX'],
                              'observed_scopes': scopes,
                              'instruction': 'Inspect only this private namespace and observed exact test control scopes; preserve unresolved state. Process exit alone is not cleanup evidence.'}
    write()
    print(json.dumps({'status': report['status'], 'result': str(args.out / 'result.json'),
                      'draft_fixture': args.allow_dirty_fixture}))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
