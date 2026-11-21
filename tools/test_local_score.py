#!/usr/bin/env python3
"""Local testing harness to verify Container CPU (< 10s) and Simulated makespan across all batches."""
import os, sys, time, json, subprocess
from pathlib import Path
import importlib.util

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('official_scorer', REPO / 'score.py')
scorer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scorer)

IMAGE = os.environ.get("IMAGE", "blast-radius-entry:arm64")

CGROUP_MEASURE = """
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

SUITES = [
    {
        "name": "sample",
        "commits_dir": REPO / "sample/commits",
        "tests_json": REPO / "sample/tests.json",
        "answers_json": REPO / "sample/answers.json",
    },
    {
        "name": "stress",
        "commits_dir": Path("/tmp/stress/commits"),
        "tests_json": Path("/tmp/stress/tests.json"),
        "answers_json": Path("/tmp/stress/answers.json"),
    },
    {
        "name": "fresh-1301",
        "commits_dir": Path("/tmp/br-calibrate.O3ZRq8/fresh1301/commits"),
        "tests_json": Path("/tmp/stress/tests.json"),
        "answers_json": None,
    },
    {
        "name": "fresh-1302",
        "commits_dir": Path("/tmp/br-calibrate.O3ZRq8/fresh1302/commits"),
        "tests_json": Path("/tmp/stress/tests.json"),
        "answers_json": None,
    },
]

print(f"{'Suite':<12} | {'Commits':<8} | {'Simulated':<11} | {'Container CPU':<14} | {'Misses':<8} | {'Total Score':<11}")
print("-" * 75)

for suite in SUITES:
    name = suite["name"]
    cdir = suite["commits_dir"]
    tjson = suite["tests_json"]
    if not cdir.exists() or not tjson.exists():
        continue
    n_commits = len(list(cdir.glob("*.patch")))
    cmd = [
        'docker','run','--rm','--network','none','--cpus','2',
        '--memory','4096m','--memory-swap','4096m','--pids-limit','256','--read-only',
        '--tmpfs','/tmp:rw,size=64m,mode=1777','--user','65534:65534','--env','HOME=/tmp',
        '--cap-drop','ALL','--security-opt','no-new-privileges',
        '-v',f'{cdir}:/data/commits:ro','-v',f'{tjson}:/data/tests.json:ro',
        IMAGE,'python','-c',CGROUP_MEASURE
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cpu_lines = [l for l in p.stderr.decode().splitlines() if 'CPU_US' in l]
    cpu_s = int(cpu_lines[-1].split()[1]) / 1e6 if cpu_lines else 0.0

    ans_file = suite["answers_json"]
    if ans_file and ans_file.exists():
        tests = json.loads(tjson.read_text())['tests']
        answers = json.loads(ans_file.read_text())['commits']
        res = scorer.evaluate_run(p.stdout, tests, answers)
        if res.get('ok'):
            sim_s = res['simulated_milliseconds'] / 1000.0
            misses = len(res['missed_commits'])
            tot_s = sim_s + cpu_s
            print(f"{name:<12} | {n_commits:<8} | {sim_s:>9.3f}s | {cpu_s:>12.2f}s | {misses:>8} | {tot_s:>9.2f}s")
        else:
            print(f"{name:<12} | {n_commits:<8} | {'ERR':>10} | {cpu_s:>12.2f}s | {'ERR':>8} | {res.get('code')}")
    else:
        print(f"{name:<12} | {n_commits:<8} | {'(no labels)':>10} | {cpu_s:>12.2f}s | {'-':>8} | {'-'}")

