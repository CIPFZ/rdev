package main

import (
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestRemoteAgentReplayDigest(t *testing.T) {
	d, _, sshRun := newRemoteRuntime(t)
	owner := broker.Owner{ClientID: "digest-deploy", ProjectID: "phase5"}
	p := broker.NewPolicy()
	if err := p.GrantHost(owner.Key(), "runtime-host", "ping", "ping"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	w := d.dial(owner, d.token(owner, "2m"), true)
	if r := remoteWireCall(t, w, owner, &proto.Request{Op: proto.OpPing}); !r.OK {
		t.Fatal("agent bootstrap failed")
	}
	// A second real remote agent exercises the operation cache directly with
	// stable IDs. The local Client currently regenerates IDs, which is a separate
	// durable-intent gap; this test must reach the actual remote replay boundary.
	out, err := sshRun(`import os,sys,json,subprocess
root=os.path.expanduser('~/'+sys.argv[1]);state=root+'/digest-state'
os.mkdir(state,0o700)
p=subprocess.Popen([root+'/rdev-agent','-state',state],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
def send(r):
 p.stdin.write(json.dumps(r)+'\n');p.stdin.flush()
 while True:
  line=p.stdout.readline()
  assert line,'agent exited before response'
  result=json.loads(line)
  if result.get('terminal') or r['id']=='hello':return result
try:
 hello=send({'id':'hello','op':'ping','hello':{'min_version':3,'max_version':3,'features':['operation_id','deduplication']}})
 assert hello.get('ok'),hello
 for index,op in enumerate(['state_migrate','state_repair','capability_probe']):
  field='capability' if op=='capability_probe' else 'state'
  before={'refresh':False} if field=='capability' else {'dry_run':True}
  after={'refresh':True} if field=='capability' else {'dry_run':False}
  request={'id':'first-'+str(index),'operation_id':'op_digest_test_'+str(index),'client_id':'principal_digest_test','op':op,field:before}
  first=send(request);assert first.get('ok'),first
  request['id']='retry-'+str(index);request['replay']=True;request[field]=after
  replay=send(request)
  assert not replay.get('ok') and replay.get('error',{}).get('code')=='request.operation_id_conflict',replay
 assert not os.path.exists(state+'/manifest.json'),'dry-run substitution changed state'
 print('real remote replay: state_migrate/state_repair dry_run and capability refresh substitution rejected; state manifest remains absent')
finally:
 p.stdin.close()
 try:p.wait(timeout=5)
 except subprocess.TimeoutExpired:p.kill();p.wait()
`)
	if err != nil {
		t.Fatalf("real replay digest: %v %s", err, out)
	}
	t.Log(string(out))
}
