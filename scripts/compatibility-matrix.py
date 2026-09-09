#!/usr/bin/env python3
"""Actual source-defined predecessor checks; not full release certification."""
import argparse, datetime, hashlib, json, os, pathlib, subprocess, signal, uuid

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--go',required=True)
p.add_argument('--out',required=True)
p.add_argument('--previous',default='e73a38bcd94c6ffb4dbe379578c3d46ff70fa50e')
a=p.parse_args()
repo=pathlib.Path(__file__).resolve().parents[1]
def git(*args):return subprocess.check_output(['git',*args],cwd=repo,text=True).strip()
if git('status','--porcelain','--untracked-files=all'):raise SystemExit('clean source required')
if os.environ.get('RDEV_RUN_REMOTE')!='1' or not os.environ.get('RDEV_TEST_REMOTE'):raise SystemExit('explicit isolated authorized SSH target required')
out=pathlib.Path(a.out).absolute()
if out.exists():raise SystemExit('new private output directory required')
out.mkdir(mode=0o700)
os.umask(0o077)
old=out/'previous';current=git('rev-parse','HEAD');previous=git('rev-parse',a.previous+'^{commit}')
subprocess.run(['git','clone','--quiet','--no-hardlinks','--no-local',str(repo),str(old)],check=True)
subprocess.run(['git','-C',str(old),'checkout','--quiet','--detach',previous],check=True)
with (out/'build.log').open('w') as f:
 subprocess.run(['make','GO='+a.go,'all'],cwd=old,stdout=f,stderr=subprocess.STDOUT,check=True)
 subprocess.run([a.go,'build','-trimpath','-o',str(old/'bin/rdevd'),'./cmd/rdevd'],cwd=old,stdout=f,stderr=subprocess.STDOUT,check=True)
def digest(path):
 with pathlib.Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
base=os.environ.copy();base['RDEV_TEST_NAMESPACE_PREFIX']='.cache/rdev-phase5-matrix-'+uuid.uuid4().hex;base['RDEV_TEST_DAEMON_BINARY']=str(repo/'bin/rdevd');base['RDEV_TEST_CLI_BINARY']=str(repo/'bin/rdev')
base['RDEV_TEST_CURRENT_AGENT_DIR']=str(repo/'cmd/rdev/agents')
base['RDEV_TEST_PREDECESSOR_DAEMON_BINARY']=str(old/'bin/rdevd');base['RDEV_TEST_PREDECESSOR_AGENT_DIR']=str(old/'cmd/rdev/agents')
cases=[
 ('job-upgrade-retained-supervisors','TestRemoteBrokerJobUpgrade',{}),
 ('old-cli-mcp-new-broker-new-agent','TestRemoteBrokerFrontendBoundary',{'RDEV_TEST_CLI_BINARY':str(old/'bin/rdev')}),
 ('new-cli-mcp-old-broker-old-agent','TestRemoteBrokerFrontendBoundary',{'RDEV_TEST_DAEMON_BINARY':str(old/'bin/rdevd'),'RDEV_TEST_AGENT_DIR':str(old/'cmd/rdev/agents')}),
]
result={'schema_version':1,'source_commit':current,'predecessor_commit':previous,'predecessor_kind':'Phase7 engineering baseline; no formal N-1 release claim','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'toolchain':subprocess.check_output([a.go,'version'],text=True).strip(),'binaries':{},'cases':[],'production_certification':'pending','boundary':'one authorized Linux SSH endpoint, private namespace per test; CLI/MCP shared read/policy subset and retained jobs; full standalone/migration/rollback/channel/approval/archive matrix remains pending'}
for prefix,root in [('current',repo),('previous',old)]:
 for path in ['bin/rdev','bin/rdevd','cmd/rdev/agents/rdev-agent-linux-amd64']:result['binaries'][prefix+'/'+path]=digest(root/path)
for name,test,overrides in cases:
 env={**base,**overrides}
 with (out/(name+'.log')).open('w') as f:
  proc=subprocess.Popen([a.go,'test','./cmd/rdevd','-run','^'+test+'$','-count=1','-timeout=5m','-v'],cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
  try:status='passed' if proc.wait(timeout=330)==0 else 'failed'
  except subprocess.TimeoutExpired:
   status='failed'
   try:os.killpg(proc.pid,signal.SIGTERM)
   except ProcessLookupError:pass
   try:proc.wait(timeout=5)
   except subprocess.TimeoutExpired:
    try:os.killpg(proc.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    proc.wait()
  if status!='passed':
   (out/'recovery.json').write_text(json.dumps({'ssh_alias':env['RDEV_TEST_REMOTE'],'namespace_prefix':base['RDEV_TEST_NAMESPACE_PREFIX'],'result':'failed; do not treat process exit as successful cleanup','action':'Inspect only this private remote prefix for retained supervisors/jobs before removing it. Preserve unresolved outcomes.'},indent=2)+'\n')
 result['cases'].append({'name':name,'test':test,'status':status})
 result['completed_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(0 if all(r['status']=='passed' for r in result['cases']) else 1)
