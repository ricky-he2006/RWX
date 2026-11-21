"""Independently label each patch with the pinned checkout's pytest suite."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
sys.path.insert(0,'/project/app')
from patchlib import apply_hunks, parse_patch

expected = {t['id']:t['number'] for t in json.loads(Path('/project/sample/tests.json').read_text())['tests']}
plugin = '''
import json, os
from pathlib import Path
failed=set()
collected=set()
broken=set()
def pytest_collection_finish(session):
    collected.update(item.nodeid for item in session.items)
def pytest_collectreport(report):
    if report.failed:
        broken.add(report.nodeid)
def pytest_runtest_logreport(report):
    if report.failed:
        failed.add(report.nodeid)
def pytest_sessionfinish(session, exitstatus):
    Path(os.environ["REPORT"]).write_text(json.dumps([sorted(failed),sorted(collected),sorted(broken)]))
'''
for patch in sorted(Path('/patches').glob('*.patch'))[int(sys.argv[1])::3]:
    with tempfile.TemporaryDirectory() as tmp:
        base=Path(tmp)
        repo=base/'repo'
        shutil.copytree('/baseline',repo,ignore=shutil.ignore_patterns('.git','__pycache__'))
        for change in parse_patch(patch.read_text()):
            target=repo/change.path
            target.write_text(apply_hunks(target.read_text(),change))
        (base/'cal_plugin.py').write_text(plugin)
        report=base/'report.json'
        env=dict(os.environ,PYTHONPATH=f'{repo}:{base}',SKIP_INTEGRATION='0',REPORT=str(report))
        proc=subprocess.run([sys.executable,'-m','pytest','-q','--continue-on-collection-errors',
                             '-p','cal_plugin','--timeout=300','tests'],cwd=repo,env=env,stdout=subprocess.DEVNULL)
        if proc.returncode not in (0,1) or not report.exists():
            raise RuntimeError(f'{patch.name}: pytest exit {proc.returncode}')
        failed, collected, broken=json.loads(report.read_text())
        missing=set(expected)-set(collected)
        if any(n.split('::')[0] not in broken for n in missing) or set(failed)-set(expected):
            raise RuntimeError(f'{patch.name}: incomplete test accounting')
        tests=sorted(expected[n] for n in set(failed)|missing)
        Path('/results',patch.stem+'.json').write_text(json.dumps(tests))
        print(patch.stem,len(tests),flush=True)
