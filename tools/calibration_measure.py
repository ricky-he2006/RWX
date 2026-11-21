"""Collect predictions and CPU separately from ground truth."""
import os
import subprocess
import sys
from pathlib import Path

out = Path(sys.argv[1])
measure = """
import os, sys
from pathlib import Path
def cpu():
    root = Path('/sys/fs/cgroup')
    group = next(row[3:] for row in Path('/proc/self/cgroup').read_text().splitlines() if row.startswith('0::'))
    path = root / group.lstrip('/') / 'cpu.stat'
    return int(dict(row.split() for row in path.read_text().splitlines())['usage_usec'])
before = cpu()
pid = os.posix_spawnp('timeout', ['timeout','-s','KILL','180','python','/app/selector.py'], os.environ)
_, status = os.waitpid(pid,0)
print('CPU_US', cpu()-before, file=sys.stderr)
sys.exit(os.waitstatus_to_exitcode(status))
"""
refs = ['13c83c661aa9', 'cdcc32c32e03', 'b1aa23ed8d88', '60d182878a4f']
for batch in ['stress', 'throttled']:
    data = Path('/tmp/stress')
    for run in range(3):
        for ref in refs[run:] + refs[:run]:
            name = f'{batch}-{ref}-{run}'
            command = ['docker','run','--rm','--platform','linux/amd64','--network','none','--cpus','2' if batch == 'stress' else '0.8',
                       '--memory','4096m','--memory-swap','4096m','--pids-limit','256','--read-only',
                       '--tmpfs','/tmp:rw,size=64m,mode=1777','--user','65534:65534','--env','HOME=/tmp',
                       '--cap-drop','ALL','--security-opt','no-new-privileges',
                       '-v',f'{data}/commits:/data/commits:ro','-v',f'{data}/tests.json:/data/tests.json:ro',
                       'br-' + ref,'python','-c',measure]
            with (out / (name+'.jsonl')).open('wb') as predictions, (out / (name+'.log')).open('wb') as log:
                subprocess.run(command,stdout=predictions,stderr=log,check=True)
