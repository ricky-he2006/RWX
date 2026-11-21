"""Blast Radius selector.

1. Static analysis of every patch against a per-test coverage map of sqlglot at
   the pinned commit gives the candidate tests that could possibly fail.
2. A zygote process imports sqlglot and the candidate test modules once; for
   every commit a forked child applies the change in memory and runs the
   candidates in-process to find out which of them really fail.
3. Only failing tests (plus any candidate we could not afford to run) are
   printed, packed onto workers to minimise makespan + 2 s per worker.

Anything that goes wrong for a commit falls back to the static candidates.
"""
from __future__ import annotations

import json
import os
import re
import resource
import select
import signal
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

T_START = time.time()
DATA = Path(os.environ.get("BR_DATA", "/data"))
REPO = Path(os.environ.get("BR_REPO", "/opt/sqlglot"))
os.environ["BR_REPO"] = str(REPO)
MAP = Path(os.environ.get("BR_MAP", HERE / "map.json.gz"))

# keep the real stdout for answers only; everything else goes to stderr
_OUT_FD = os.dup(1)
os.dup2(2, 1)
sys.stdout = sys.stderr

from sched import best_schedule  # noqa: E402
from functools import partial

schedule_cost = partial(best_schedule, want_workers=False)
from selcore import Map, analyze  # noqa: E402

HEAVY_SECONDS = float(os.environ.get("BR_HEAVY", "1.5"))
SECONDS_PER_COMMIT = float(os.environ.get("BR_WALL_PER_COMMIT", "4.6"))
PARALLEL = int(os.environ.get("BR_PARALLEL", "2"))
CPU_FACTOR = float(os.environ.get("BR_CPU_FACTOR", "1.0"))  # our cpu / recorded seconds


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def emit(lines):
    data = "".join(json.dumps(l, separators=(",", ":")) + "\n" for l in lines).encode()
    while data:
        n = os.write(_OUT_FD, data)
        data = data[n:]


_TEST = dict(kv.split("=", 1) for kv in os.environ.get("BR_SELFTEST", "").split(",") if "=" in kv)
UNKNOWN_TESTS: set = set()
POISON: set = set()       # commits whose analysis killed an earlier attempt: all their tests
STATIC_ONLY = [False]     # last attempt: no test runs, static candidates only
_PROGRESS_FD = [None]
ANALYZE_TIMEOUT = float(os.environ.get("BR_ANALYZE_TIMEOUT", "25"))


def _progress(msg):
    fd = _PROGRESS_FD[0]
    if fd is not None:
        try:
            os.write(fd, (msg + "\n").encode())
        except OSError:
            pass


def supervise():
    """Run main() in a child and make sure every commit gets an answer line.

    If the child dies or hangs while analysing a patch, that commit is answered with
    every test and the work starts over without it. If it dies or overruns later, the
    static candidates are printed. If nothing works, every test of every commit is."""
    try:
        cids = [pf.stem for pf in sorted((DATA / "commits").glob("*.patch"))]
        n_tests = len(json.loads((DATA / "tests.json").read_text())["tests"])
    except Exception as e:  # noqa
        log("supervisor cannot read the inputs", repr(e))
        main()
        return
    n = len(cids)
    hard = T_START + max(25.0, SECONDS_PER_COMMIT * n) + 8.0 + 0.2 * n
    for attempt in range(8):
        rd, wr = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(rd)
            _PROGRESS_FD[0] = wr
            try:
                main()
            except BaseException as e:  # noqa
                log("main failed", repr(e))
            os._exit(1)     # main() leaves with os._exit(0) itself
        os.close(wr)
        cur, phase, last, buf = None, "A", time.time(), b""
        while True:
            now = time.time()
            limit = last + ANALYZE_TIMEOUT if phase == "A" else hard if phase == "R" else now + 60.0
            try:
                r, _, _ = select.select([rd], [], [], max(0.05, min(limit - now, 1.0)))
            except InterruptedError:
                continue
            if r:
                ch = os.read(rd, 4096)
                if not ch:
                    break
                buf += ch
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.decode("utf-8", "replace")
                    last = time.time()
                    if line.startswith("A "):
                        cur = line[2:]
                    elif line in ("R", "E"):
                        phase = line
                continue
            if time.time() > limit and phase != "E":
                log(f"supervisor: no progress in phase {phase} (commit {cur}); stopping the worker")
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
                break
        try:
            _, st = os.waitpid(pid, 0)
        except OSError:
            st = -1
        os.close(rd)
        if phase == "E":
            return
        if phase == "A" and cur is not None and cur not in POISON:
            log(f"supervisor: worker lost while analysing {cur} (status {st}); answering it with every test")
            POISON.add(cur)
        elif STATIC_ONLY[0]:
            break
        else:
            log(f"supervisor: worker lost in phase {phase} (status {st}); falling back to static candidates")
            STATIC_ONLY[0] = True
    log("supervisor: last resort, every test for every commit")
    workers = [list(range(i, n_tests, 8)) for i in range(8)] if n_tests >= 8 else [list(range(n_tests))]
    emit([{"commit": c, "workers": [w for w in workers if w]} for c in cids])


def main():
    tests = json.loads((DATA / "tests.json").read_text())["tests"]
    nodeids = [t["id"] for t in tests]
    ms = [round(t["seconds"] * 1000) for t in tests]
    patches = sorted((DATA / "commits").glob("*.patch"))
    n_commits = len(patches)
    deadline = T_START + max(25.0, SECONDS_PER_COMMIT * n_commits)
    global MAP_OBJ
    M = _pkl(MAP, lambda: Map(MAP))
    MAP_OBJ = M
    if M.tests != nodeids:
        # every other map is indexed by the test numbers of the pinned test list: with a
        # different list only the (remapped) coverage map can be trusted
        log("WARNING: map test list differs from tests.json; remapping, static candidates only")
        STATIC_ONLY[0] = True
        known = set(M.tests)
        UNKNOWN_TESTS.update(i for i, t in enumerate(nodeids) if t not in known)
        idx = {t: i for i, t in enumerate(nodeids)}
        remap = [idx.get(t) for t in M.tests]
        M.sets = [frozenset(remap[x] for x in s if remap[x] is not None) for s in M.sets]
        M.opened = {p: {remap[x] for x in s if remap[x] is not None} for p, s in M.opened.items()}
        M.tests = nodeids
        M.n = len(nodeids)
        M.file_of = [t.split("::", 1)[0] for t in nodeids]
        M._file_users = {}
        M._tests_by_file = {}
        for i, f in enumerate(M.file_of):
            M._tests_by_file.setdefault(f, set()).add(i)
    all_tests = set(range(len(tests)))

    answers = {}
    jobs = []
    maybe_equiv = {}
    for pf in patches:
        cid = pf.stem
        _progress("A " + cid)
        if cid in POISON:
            answers[cid] = sorted(all_tests)
            log(cid, "ALL (its analysis did not survive an earlier attempt)")
            continue
        n_jobs = len(jobs)
        try:
            if _TEST.get("crash") == cid:      # self-test of the supervisor
                os.kill(os.getpid(), signal.SIGKILL)
            if _TEST.get("hang") == cid:
                time.sleep(10 ** 6)
            if _TEST.get("raise") == cid:
                raise RuntimeError("self-test")
            try:
                D = analyze(pf.read_text(encoding="utf-8", errors="replace"), REPO, M)
            except Exception as e:  # noqa
                log(cid, "analyze failed", repr(e))
                answers[cid] = sorted(all_tests)
                continue
            if not D.all and D.files:
                # only the cheap screen here; the proof itself runs after the tests
                try:
                    from equiv import maybe_equivalent
                    if maybe_equivalent(D.files, REPO):
                        maybe_equiv[cid] = D.files
                except Exception as e:  # noqa
                    log(cid, "equiv screen failed", repr(e))
            if D.all:
                answers[cid] = sorted(all_tests)
                log(cid, "ALL", " ".join(D.reasons)[:200])
                continue
            cands = sorted(t for t in D.cands if t is not None)
            answers[cid] = cands  # static fallback
            hot = [r for r, c in D.files.items() if r.endswith(".py") and c is not None and (REPO / r).exists()]
            olds = {r: (REPO / r).read_text(encoding="utf-8") for r in hot}
            if not cands and not D.import_change:
                log(cid, "none", " ".join(D.reasons)[:200])
                continue
            if not cands:
                # import-time state changed in a file no test runs code from (e.g. a module of
                # class definitions): the readers are only known after the state diff. If the
                # child cannot finish, everything is selected.
                answers[cid] = sorted(all_tests)
            instr = {}
            n_sites = 0
            if not D.import_change and len(hot) == 1 and all(r.endswith(".py") for r in D.files):
                from patchlib import instrument
                for r in hot:
                    try:
                        res = instrument(olds[r], D.files[r], D.parsed.pop(r, None))
                    except Exception:  # noqa
                        res = None
                    if res is None:
                        instr = {}
                        break
                    instr[r] = res[0]
                    n_sites = res[1]
            subf = {}
            if not D.import_change and all(r.split(" ", 1)[0] in ("rt", "ast-same", "annotation") for r in D.reasons):
                subf = submit_filter(D, cands)
            CPU_OVR.clear()
            CPU_OVR.update(sub_cpu(subf))
            memo_duck = all((f.startswith("sqlglot/") and f.endswith(".py")) or
                            (f.startswith("tests/fixtures/") and f.endswith(".sql")) for f in D.files)
            pure_src = all(f.startswith("sqlglot/") and f.endswith(".py") for f in D.files)
            # code lines the patch touches; fixture-only .sql edits touch none (a changed
            # query no longer matches its baseline plan, so it is executed for real)
            ch_lines = {f: sorted(v) for f, v in D.ch_lines.items()} if memo_duck else None
            iterf = {}
            opt_aff = None
            if pure_src and not D.import_change:
                iterf = iter_filter(cands, D.ch_lines)
                iter_cpu(iterf)
                opt_aff = opt_affected(D.ch_lines)
                opt_cpu(opt_aff, cands)
            memo_skip = []
            if memo_duck and any(t in D.cands for t in memo_ids(nodeids)):
                if not D.import_change:
                    memo_skip = memo_skippable(ch_lines, nodeids)
                memo_cpu(memo_skip, nodeids, subf)
            if D.import_change:
                # decided in the child once the import-time state diff is known; the static
                # candidates are no safe answer for such a change, so if the child cannot
                # finish everything is selected
                answers[cid] = sorted(all_tests)
                light, heavy = [], []
            else:
                light, heavy, est_run, static_cost = plan_split(cands, ms, CPU, False, bool(instr))
            if light and not D.import_change and len(cands) == 1 and \
                    all(r.split(" ", 1)[0] in ("fixture", "ast-same", "annotation", "ignored", "fixture-unused")
                        for r in D.reasons):
                # a lone test reading an edited fixture nearly always fails: running it
                # only pays when it is cheap next to the chance of dropping it
                if cpu_of(cands[0], ms) + OVERHEAD_HOT > (1.0 - FIX_PFAIL) * static_cost / 1000.0:
                    light, heavy = [], cands
            force = False
            if pure_src and not D.import_change and not light and len(cands) >= 2:
                ins, _, est = replay_scope(cands, ch_lines)
                force = len(ins) >= 2 and est <= REPLAY_MAX and est < static_cost / 1000.0
            if not light and not D.import_change and not force:
                log(cid, f"static cost={static_cost/1000:.2f}", " ".join(D.reasons)[:150])
                if not D.import_change:
                    continue
            jobs.append(dict(
                cid=cid, files=D.files, olds=olds, hot=hot, overlay=bool(D.import_change),
                light=light, heavy=heavy, instr=instr, n_sites=n_sites, subf=subf, cpu_ovr=dict(CPU_OVR), iterf={t: sorted(v) for t, v in iterf.items()}, opt_aff=opt_aff,
                memo_duck=memo_duck, memo_skip=memo_skip, ch_lines=ch_lines, pure_src=pure_src,
                precise=sorted(D.precise), broad=sorted(D.cands), was_import=bool(D.import_change),
                reasons=" ".join(D.reasons)[:200],
            ))
        except Exception as e:  # noqa
            log(cid, "planning failed", repr(e), "-> all tests")
            del jobs[n_jobs:]
            answers[cid] = sorted(all_tests)

    CPU_OVR.clear()
    import patchlib
    patchlib._PARSED.clear()
    log(f"analysis done in {time.time() - T_START:.2f}s; {len(jobs)} commits to run")
    _progress("R")

    if jobs and not STATIC_ONLY[0]:
        if _TEST.get("runcrash"):
            os.kill(os.getpid(), signal.SIGKILL)
        run_jobs(jobs, answers, nodeids, ms, deadline)
    # The proof of equivalence parses much of the checkout, so it is kept for the commits
    # that still have tests selected. By now this process has sqlglot imported if any
    # commit was run.
    pending = {c: f for c, f in maybe_equiv.items() if answers.get(c)}
    if pending:
        prove_equivalent(pending, answers)

    lines = []
    for pf in patches:
        cid = pf.stem
        sel = answers.get(cid, sorted(all_tests))
        if UNKNOWN_TESTS:
            sel = sorted(set(sel) | UNKNOWN_TESTS)   # tests no map knows are always selected
        workers, cost = best_schedule(sel, ms)
        lines.append({"commit": cid, "workers": workers})
    _progress("E")
    emit(lines)
    ru = resource.getrusage(resource.RUSAGE_SELF)
    rc = resource.getrusage(resource.RUSAGE_CHILDREN)
    log(f"done in {time.time() - T_START:.1f}s cpu self={ru.ru_utime + ru.ru_stime:.1f} children={rc.ru_utime + rc.ru_stime:.1f}")
    sys.stderr.flush()
    os._exit(0)  # answers are flushed; skip interpreter teardown


CPU_PATH = Path(os.environ.get("BR_CPU", HERE / "cpu.json"))
try:
    _cpu = json.loads(CPU_PATH.read_text())
except Exception:  # noqa
    _cpu = {"test": {}, "class": {}}
CPU = _cpu
OVERHEAD_HOT = float(os.environ.get("BR_OVH_HOT", "0.25"))
OVERHEAD_OVERLAY = float(os.environ.get("BR_OVH_OV", "2.0"))
FAIL_PENALTY = float(os.environ.get("BR_FAIL_PEN", "1.5"))


CPU_OVR = {}
SUBMAP_PATH = Path(os.environ.get("BR_SUBMAP", HERE / "submap.json.gz"))
SUB_BASE = float(os.environ.get("BR_SUB_BASE", "0.15"))
_SUBMAP = None


def _submap():
    global _SUBMAP
    if _SUBMAP is None:
        import gzip
        def build():
            with gzip.open(SUBMAP_PATH, "rt") as fh:
                raw = json.load(fh)
            return {int(t): (v["n"], {f: set(ls) for f, ls in v["main"].items()},
                             {f: {int(l): s for l, s in d.items()} for f, d in v["subs"].items()})
                    for t, v in raw.items()}
        try:
            _SUBMAP = _pkl(SUBMAP_PATH, build)
        except Exception:  # noqa
            _SUBMAP = {}
    return _SUBMAP


def submit_filter(D, cands):
    """For pool-based tests, which submitted queries can reach the changed lines."""
    out = {}
    if not D.rt_lines or (1106 not in cands and 1107 not in cands):
        return out
    for t, (n, main, subs) in _submap().items():
        if t not in cands:
            continue
        allow = set()
        ok = True
        for f, lines in D.rt_lines.items():
            if lines & main.get(f, set()):
                ok = False
                break
            sf = subs.get(f, {})
            for l in lines:
                allow.update(sf.get(l, ()))
        if ok and len(allow) < n:
            out[t] = allow
    return out


def sub_cpu(subf):
    o = {}
    for t, allow in subf.items():
        n = _submap()[t][0]
        v = CPU["test"].get(str(t), 0.0)
        o[t] = v * (SUB_BASE + (1 - SUB_BASE) * len(allow) / max(1, n))
    return o


OPT_PREFIX = os.environ.get("BR_OPT", str(HERE / "optmemo"))
OPT_ON = os.environ.get("BR_OPT_ON", "1") == "1"
OPT_PER_CALL = os.environ.get("BR_OPT_PER_CALL", "0") == "1"   # needs per-call line sets in the memo (not built)
_OPT_LOADED = [False]


def opt_affected(lines):
    """Optimizer pipeline stages that may run a changed line, or None if the optimizer
    memo cannot be used. `lines`: {file: lines} of a change to function bodies only."""
    if not OPT_ON or lines is None:
        return None
    import optmemo
    if not _OPT_LOADED[0]:
        _OPT_LOADED[0] = True
        optmemo.load(OPT_PREFIX)
    return optmemo.affected(lines)


OPT_FRAC_PATH = Path(os.environ.get("BR_OPT_FRAC", HERE / "optfrac.json"))
_OPT_FRAC = None


def opt_cpu(aff, cands):
    """With the optimizer memo on, tests that mostly call optimize() get much cheaper."""
    global _OPT_FRAC
    if aff is None or 0 in aff:
        return
    if _OPT_FRAC is None:
        try:
            _OPT_FRAC = {int(t): f for t, f in json.loads(OPT_FRAC_PATH.read_text()).items()}
        except Exception:  # noqa
            _OPT_FRAC = {}
    save = 0.8 if aff else 0.95
    for t in cands:
        f = _OPT_FRAC.get(t)
        if f:
            base = CPU_OVR.get(t, CPU["test"].get(str(t)))
            if base is not None:
                CPU_OVR[t] = base * (1.0 - f * save)


def _pkl(path, build):
    """Processed form of a map: pickled next to it at image build time (prep.py)."""
    import pickle
    import gc
    pk = str(path) + ".pkl"
    try:
        gc.disable()
        with open(pk, "rb") as fh:
            obj = pickle.load(fh)
        gc.freeze()
        return obj
    except Exception:  # noqa
        pass
    finally:
        gc.enable()
    if build is None:
        return None
    gc.disable()
    obj = build()
    gc.freeze()
    gc.enable()
    if os.environ.get("BR_PREP") == "1":
        with open(pk, "wb") as fh:
            pickle.dump(obj, fh, protocol=5)
    return obj


ITER_PATH = Path(os.environ.get("BR_ITER", HERE / "iter.json.gz"))
ITER_ON = os.environ.get("BR_ITER_ON", "1") == "1"
ITER_BASE = float(os.environ.get("BR_ITER_BASE", "0.15"))
ITER_MAXFRAC = float(os.environ.get("BR_ITER_MAXFRAC", "0.8"))
_ITERMAP = None
_ITERMAP_CACHE = {}


def _itermap():
    """Per-iteration line coverage of fixture-driven tests (see builditer.py)."""
    global _ITERMAP
    if _ITERMAP is None:
        import gzip

        def build():
            with gzip.open(ITER_PATH, "rt") as fh:
                raw = json.load(fh)
            tests_meta = {}
            by_file_raw = {}
            for t, v in raw.items():
                t = int(t)
                tests_meta[t] = (v["n"], {f: frozenset(ls) for f, ls in v["main"].items() if ls})
                for f, d in v["its"].items():
                    sub = by_file_raw.setdefault(f, {})
                    for l, ks in d.items():
                        if ks:
                            sub.setdefault(int(l), []).append((t, tuple(ks)))
            import pickle
            for f in list(by_file_raw.keys()):
                by_file_raw[f] = pickle.dumps(by_file_raw[f], protocol=pickle.HIGHEST_PROTOCOL)
            return (tests_meta, by_file_raw)

        try:
            _ITERMAP = _pkl(ITER_PATH, build)
        except Exception as e:  # noqa
            log("iteration map unavailable", repr(e))
            _ITERMAP = ({}, {})
    return _ITERMAP


ITER_TEST_IDS = {1082, 1083, 1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1094, 1095, 1096, 1097, 1098, 1099, 1100, 1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1115, 1116, 1117, 1118, 1119, 1120, 1121, 1122, 1123, 1124, 1125, 1126, 1127, 1128, 1129, 1130, 1131, 1132, 1133, 1134, 1135, 1136, 1137, 1138, 1139, 1140, 1141, 1295, 1302, 1313, 1314, 1324, 1325, 1327, 1335, 1343, 1352, 1353, 1355, 1362, 1375, 1377, 1378, 1382, 1383, 1384, 1417, 1420, 1434, 1440, 1441, 1454, 1561, 1599, 1606, 1607}


def iter_filter(cands, lines):
    """For fixture-driven candidates: the iterations that reach a changed line. Only
    valid when nothing but function bodies changed. Returns {test: set of iterations}."""
    if not ITER_ON or not lines:
        return {}
    cs = set(cands)
    if not cs.intersection(ITER_TEST_IDS):
        return {}
    tests_meta, by_file_raw = _itermap()
    bad_tests = set()
    for f, ls in lines.items():
        for t in cs:
            if t in tests_meta:
                main = tests_meta[t][1]
                if f in main and any(l in main[f] for l in ls):
                    bad_tests.add(t)
    per_test = {t: set() for t in cs if t in tests_meta and t not in bad_tests}
    import pickle
    for f, ls in lines.items():
        if f not in _ITERMAP_CACHE:
            raw = by_file_raw.get(f)
            _ITERMAP_CACHE[f] = pickle.loads(raw) if raw else {}
        flines = _ITERMAP_CACHE[f]
        if flines:
            for l in ls:
                entries = flines.get(l)
                if entries:
                    for t, ks in entries:
                        if t in per_test:
                            per_test[t].update(ks)
    out = {}
    for t, allow in per_test.items():
        n = tests_meta[t][0]
        if allow and len(allow) <= ITER_MAXFRAC * n:
            out[t] = allow
    return out


def iter_cpu(iterf):
    tests_meta = _itermap()[0]
    for t, allow in iterf.items():
        base = CPU["test"].get(str(t))
        if base is not None:
            n = tests_meta.get(t, (1,))[0]
            CPU_OVR[t] = base * (ITER_BASE + (1 - ITER_BASE) * len(allow) / max(1, n))



REPLAY_PATH = Path(os.environ.get("BR_REPLAY", HERE / "replay.json.gz"))
REPLAY_ON = os.environ.get("BR_REPLAY_ON", "1") == "1"
REPLAY_FACTOR = float(os.environ.get("BR_REPLAY_FACTOR", "0.9"))  # recorded seconds -> cpu here
REPLAY_FRAC = float(os.environ.get("BR_REPLAY_FRAC", "0.6"))
REPLAY_BOUND_DIV = float(os.environ.get("BR_REPLAY_BOUND_DIV", "1.15"))
REPLAY_MAX = float(os.environ.get("BR_REPLAY_MAX", "18"))
_REPLAY = None


def _replay():
    """Recorded parse-phase calls of every test at the pinned commit (see replay.py)."""
    global _REPLAY
    if _REPLAY is None:
        import gzip

        def build():
            with gzip.open(REPLAY_PATH, "rt") as fh:
                d = json.load(fh)
            d["tests"] = {int(t): v for t, v in d["tests"].items()}
            d["bad"] = set(d["bad"])
            d["sets"] = [frozenset(x) for x in d["sets"]]
            d["out"] = {f: {int(l): i for l, i in m.items()} for f, m in d["out"].items()}
            d["snaps_tuple"] = {k: tuple(v) for k, v in d.pop("snaps", {}).items()}
            try:
                with gzip.open(str(REPLAY_PATH).replace(".json.gz", "") + ".fk.gz", "rb") as fh:
                    d["fk"] = fh.read()
            except Exception:
                d["fk"] = None
            return d
        try:
            d = _pkl(REPLAY_PATH, build)
            try:
                import replay
                replay.SNAP_INDEX.update(d.get("snaps_tuple") or {k: tuple(v) for k, v in d.pop("snaps", {}).items()})
                replay.SNAP_PATH[0] = str(REPLAY_PATH).replace(".json.gz", "") + ".snap"
            except Exception as e:  # noqa
                log("snapshots unavailable", repr(e))
            if "fk" not in d:
                try:
                    with gzip.open(str(REPLAY_PATH).replace(".json.gz", "") + ".fk.gz", "rb") as fh:
                        d["fk"] = fh.read()
                except Exception:
                    d["fk"] = None
            _REPLAY = d
        except Exception as e:  # noqa
            log("replay map unavailable", repr(e))
            _REPLAY = {}
    return _REPLAY


def replay_scope(cands, ch_lines, outside0=None, extra_lines=None):
    """Candidates that run the changed lines only inside recorded parse phases.
    Returns (tests, key ids, estimated cpu seconds)."""
    R = _replay() if REPLAY_ON else {}
    if not R or ch_lines is None:
        return [], [], 0.0
    outside = set(outside0 or ())
    allines = {f: set(v) for f, v in ch_lines.items()}
    for f, v in (extra_lines or {}).items():
        allines.setdefault(f, set()).update(v)
    for f, lines in allines.items():
        m = R["out"].get(f)
        if m:
            for l in lines:
                i = m.get(l)
                if i is not None:
                    outside |= R["sets"][i]
    inside = [t for t in cands if t in R["tests"] and t not in R["bad"] and t not in outside]
    keys = set()
    for t in inside:
        keys.update(R["tests"][t])
    est = sum(R["ms"][k] for k in keys) / 1000.0 * REPLAY_FACTOR + 0.05
    return inside, sorted(keys), est


def _key_mask(R, lines_by_file):
    """Bitmap of the recorded inputs that enter a function containing a changed line."""
    fk = R.get("fk")
    if not fk or not R.get("funcs"):
        return None
    nb = (len(R["dig"]) + 7) // 8
    mask = 0
    for i, (f, lo, hi) in enumerate(R["funcs"]):
        ls = lines_by_file.get(f)
        if ls and any(lo <= l <= hi for l in ls):
            mask |= int.from_bytes(fk[i * nb:(i + 1) * nb], "little")
    return mask


_AK = None
GEN_INIT_LINE = 1005    # sqlglot/generator.py at the pinned commit: inside Generator.__init__


def _attr_mask(R, attrs):
    """Bitmap of the recorded inputs that read any of the given (owner, attribute) pairs."""
    global _AK
    base = str(REPLAY_PATH).replace(".json.gz", "")
    if _AK is None:
        import gzip
        try:
            with gzip.open(base + ".ak.json.gz", "rt") as fh:
                _AK = json.load(fh)
            if _AK.get("nkeys") != len(R["dig"]):
                _AK = False
        except Exception:  # noqa
            _AK = False
    if not _AK:
        return None
    import zlib
    mask = 0
    fd = os.open(base + ".ak", os.O_RDONLY)
    try:
        for owner, attr in attrs:
            e = _AK["attrs"].get(f"{owner}|{attr}")
            if e:
                mask |= int.from_bytes(zlib.decompress(os.pread(fd, e[1], e[0])), "little")
    finally:
        os.close(fd)
    return mask


def replay_narrow(cands, ch_lines, ms, outside0=None, extra_lines=None, light0=None, pure=False, attrs=None,
                  disp=False):
    """Replay the recorded phase inputs under the patched code; drop the candidates whose
    inputs all still produce their baseline output. Returns (candidates, note)."""
    inside, keys, est = replay_scope(cands, ch_lines, outside0, extra_lines)
    if len(inside) < 2:
        return cands, ""
    R = _replay()
    tkeys = {t: R["tests"][t] for t in inside}
    if pure:
        # only function bodies changed: an input that never enters a changed function
        # runs exactly the old code, so only the inputs that do are replayed. (The tests
        # here run the changed lines inside phases only; one with no such input never
        # runs them at all.)
        allines = {f: set(v) for f, v in ch_lines.items()}
        for f, v in (extra_lines or {}).items():
            allines.setdefault(f, set()).update(v)
        mask = _key_mask(R, allines) if os.environ.get("BR_REPLAY_FK", "1") == "1" else None
        if mask is not None:
            tkeys = {t: [k for k in ks if (mask >> k) & 1] for t, ks in tkeys.items()}
            keys = sorted({k for ks in tkeys.values() for k in ks})
            est = sum(R["ms"][k] for k in keys) / 1000.0 * REPLAY_FACTOR + 0.05
    elif attrs is not None and os.environ.get("BR_REPLAY_AK", "1") == "1":
        # class attributes changed: only the inputs that read one of them (or enter a
        # changed function) can behave differently. A test with no such input is not
        # explained by the trace and simply stays a candidate.
        allines = {f: set(v) for f, v in ch_lines.items()}
        for f, v in (extra_lines or {}).items():
            allines.setdefault(f, set()).update(v)
        m1 = _attr_mask(R, attrs)
        m2 = _key_mask(R, allines)
        # inputs that create a generator (they enter Generator.__init__, where the dispatch
        # table is looked up)
        m3 = _key_mask(R, {"sqlglot/generator.py": {GEN_INIT_LINE}}) if disp else 0
        if disp and not m3:
            m1 = None       # cannot tell: replay every input of the tests in scope
        if m1 is not None and m2 is not None:
            mask = m1 | m2 | m3
            tkeys = {t: [k for k in ks if (mask >> k) & 1] for t, ks in tkeys.items()}
            tkeys = {t: ks for t, ks in tkeys.items() if ks}
            inside = [t for t in inside if t in tkeys]
            keys = sorted({k for ks in tkeys.values() for k in ks})
            est = sum(R["ms"][k] for k in keys) / 1000.0 * REPLAY_FACTOR + 0.05
            if len(inside) < 2:
                return cands, ""
    ins0 = set(inside)
    # what replay can buy: cpu of the tests that would otherwise be run, or the
    # schedule time freed if every replayed test turns out unaffected
    would_run = ins0 if light0 is None else ins0.intersection(light0)
    worth = sum(cpu_of(t, ms) for t in would_run)
    rest = [t for t in cands if t not in ins0]
    bound = (schedule_cost(cands, ms)[1] - (schedule_cost(rest, ms)[1] if rest else 0)) / 1000.0
    if est > REPLAY_MAX or est > max(0.2, REPLAY_FRAC * worth, bound / REPLAY_BOUND_DIV):
        return cands, f" replay-skip(est={est:.1f} worth={worth:.1f} bound={bound:.1f})"
    import replay
    t0 = time.time()
    c0 = time.process_time()
    import runner
    if os.environ.get("BR_REPLAY_DEBUG"):
        for dt in [int(x) for x in os.environ["BR_REPLAY_DEBUG"].split(",")]:
            allk = R["tests"].get(dt, [])
            mk = tkeys.get(dt)
            res = []
            for k in allk:
                try:
                    ch = replay.run_key(R["keys"][k]) != R["dig"][k]
                except BaseException as e:  # noqa
                    ch = repr(e)[:40]
                if ch:
                    res.append((k, ch, k in (mk or ()), str(R["keys"][k])[:300]))
            log("REPLAYDEBUG", dt, "in_cands", dt in cands, "inside", dt in inside, "bad", dt in R["bad"], "keys", len(allk),
                "masked", None if mk is None else len(mk), "changed(all keys)", res[:6])
    signal.signal(signal.SIGALRM, runner._alarm)
    limit = t0 + 3.0 * est + 5.0
    state = {}   # key -> True when its output differs from the baseline
    n_changed = 0
    ins = set(inside)
    kept = set()
    kms = R["ms"]
    # a test is affected as soon as one of its inputs differs, so its other inputs are
    # not replayed; tests with few inputs go first (their results are shared)
    for t in sorted(inside, key=lambda t: len(tkeys[t])):
        hit = False
        ks = tkeys[t]
        for k in ks:
            if state.get(k):
                hit = True
                break
        if not hit:
            for k in sorted((k for k in ks if k not in state), key=kms.__getitem__):
                if time.time() > limit:
                    return cands, " replay-abort(slow)"
                # a patched parser may loop forever on an input: the tests would hang the
                # same way, so nothing more is replayed or run and every candidate stays
                signal.setitimer(signal.ITIMER_REAL, max(KEY_T_MIN, kms[k] / 1000.0 * 30))
                try:
                    ch = replay.run_key(R["keys"][k]) != R["dig"][k]
                except runner.Timeout:
                    HANG[0] = True
                    return cands, " replay-abort(hang)"
                except BaseException:  # noqa
                    ch = True
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                state[k] = ch
                if ch:
                    n_changed += 1
                    hit = True
                    break
        if hit:
            kept.add(t)
    out = [t for t in cands if t not in ins or t in kept]
    PROF["replay"] = PROF.get("replay", 0.0) + time.process_time() - c0
    return out, (f" replay(tests={len(inside)} keys={len(state)}/{len(keys)} changed={n_changed} "
                 f"kept={len(out)}/{len(cands)} {time.time() - t0:.2f}s est={est:.1f})")


MEMO_PATH = Path(os.environ.get("BR_MEMO", HERE / "memo.pkl"))
MEMO_LITE_PATH = Path(os.environ.get("BR_MEMO_LITE", HERE / "memo_lite.pkl"))
CHEAP_MARGIN = float(os.environ.get("BR_CHEAP_MARGIN", "3.0"))
C_SKIP = float(os.environ.get("BR_C_SKIP", "5.0"))        # cpu of a TPC run that executes no query
DUCK_FRAC = float(os.environ.get("BR_DUCK_FRAC", "0.55"))  # share of TPC cpu left once references are reused
_MEMO_IDS = None
_MEMO_LITE = None


def _memo_lite():
    global _MEMO_LITE
    if _MEMO_LITE is None:
        if MEMO_LITE_PATH.exists():
            try:
                import pickle, gc
                gc.disable()
                with open(MEMO_LITE_PATH, "rb") as fh:
                    _MEMO_LITE = pickle.load(fh)
                gc.freeze()
            except Exception:
                _MEMO_LITE = None
            finally:
                gc.enable()
        if _MEMO_LITE is None:
            import runner
            if runner.MEMO is None:
                runner.load_memo(str(MEMO_PATH))
            if runner.MEMO is not None:
                _MEMO_LITE = runner.MEMO
    return _MEMO_LITE


def memo_ids(nodeids):
    """test number -> id for the tests the baseline memo knows."""
    global _MEMO_IDS
    if _MEMO_IDS is None:
        ml = _memo_lite()
        _MEMO_IDS = {}
        if ml is not None:
            pos = {n: i for i, n in enumerate(nodeids)}
            _MEMO_IDS = {pos[tid]: tid for tid in ml.get("rlines", {}) if tid in pos}
        try:
            import runner
            runner.MEMO_IDS = _MEMO_IDS
        except Exception:
            pass
    return _MEMO_IDS


def memo_skippable(ch_lines, nodeids):
    """Tests whose queries may skip execution: the patch only touches code that
    runs inside parse/optimize/transpile for them."""
    ml = _memo_lite()
    if ch_lines is None or ml is None:
        return []
    out = []
    for t, tid in memo_ids(nodeids).items():
        if not ml.get("opt", {}).get(tid):
            continue
        rl = ml["rlines"][tid]
        if all(not (rl.get(f) and rl[f].intersection(ls)) for f, ls in ch_lines.items()):
            out.append(t)
    return out


def memo_cpu(memo_skip, nodeids, subf=None):
    ml = _memo_lite()
    if ml is None:
        return
    for t, tid in memo_ids(nodeids).items():
        base = CPU["test"].get(str(t))
        if base is None:
            continue
        if t in memo_skip:
            CPU_OVR[t] = min(C_SKIP, base)
        elif ml.get("opt", {}).get(tid):
            CPU_OVR[t] = sub_cpu({t: subf[t]} if subf and t in subf else {}).get(t, base) * DUCK_FRAC



KEY_T_MIN = float(os.environ.get("BR_KEY_T_MIN", "1.5"))
HANG = [False]     # the patched code was seen to hang: run nothing, keep every candidate
T_MIN = float(os.environ.get("BR_T_MIN", "3"))
T_MAX = float(os.environ.get("BR_T_MAX", "30"))
CHILD_CPU_MAX = int(os.environ.get("BR_CHILD_CPU_MAX", "60"))
SETUP_TIMEOUT = float(os.environ.get("BR_SETUP_TIMEOUT", "25"))


def test_timeout(t, ms):
    """Wall seconds a test may take under a patch before it counts as hanging (failed)."""
    return min(T_MAX, max(T_MIN, 3.0 * max(cpu_of(t, ms), ms[t] / 1000.0) + 2.0))


def cpu_of(t, ms):
    v = CPU_OVR.get(t)
    if v is not None:
        return v * CPU_FACTOR
    v = CPU["test"].get(str(t))
    if v is None:
        v = ms[t] / 1000.0 * 1.2
    return v * CPU_FACTOR


OV_MULT = float(os.environ.get("BR_OV_MULT", "1.6"))
MAX_CHILD_CPU = float(os.environ.get("BR_MAX_CHILD", "4.0"))
BROAD_CANDS = int(os.environ.get("BR_BROAD_CANDS", "400"))
BROAD_FAILS = int(os.environ.get("BR_BROAD", "3"))
HEAVY_CPU = float(os.environ.get("BR_HEAVY_CPU", "1.0"))
FIX_PFAIL = float(os.environ.get("BR_FIX_PFAIL", "0.9"))
P_EQUIV = float(os.environ.get("BR_P_EQUIV", "0.6"))
LATE_MARGIN = float(os.environ.get("BR_LATE_MARGIN", "1.2"))
P2_PASS = float(os.environ.get("BR_P2_PASS", "0.7"))
P2_PASS_FAILED = float(os.environ.get("BR_P2_PASS_F", "0.3"))
REPLAN = os.environ.get("BR_REPLAN", "1") == "1"
PRESET_MIN = int(os.environ.get("BR_PRESET_MIN", "1"))
PRESET_CACHE = Path(os.environ.get("BR_PRESET_CACHE", HERE / "preset_executor.pkl"))
DENSE_MS = float(os.environ.get("BR_DENSE_MS", "500"))
DENSE_MIN = float(os.environ.get("BR_DENSE_MIN", "2.0"))
Q_MAX = float(os.environ.get("BR_Q_MAX", "0.7"))
Q_PRIOR = float(os.environ.get("BR_Q_PRIOR", "1.0"))
RP_MARGIN = float(os.environ.get("BR_RP_MARGIN", "1.1"))


def plan_split(cands, ms, cpu, overlay, can_diff=False, overhead=None, mult=1.0, base=()):
    """Choose which candidates to run (R) and which to select blindly (U).

    U is a prefix of the heavy tests, taken either by cpu or by cpu per recorded
    second (tests that are cheap to run but long on the clock are worth running).
    Returns (run_list, unrun_list, est_cost_ms, static_cost_ms)."""
    base = list(base)
    floor = schedule_cost(base, ms)[1] if base else 0   # tests selected whatever happens
    static_cost = schedule_cost(list(cands) + base, ms)[1] - floor
    ovh = (OVERHEAD_OVERLAY if overlay else OVERHEAD_HOT) if overhead is None else overhead
    total = sum(cpu_of(t, ms) for t in cands) * mult
    heavy = [t for t in cands if cpu_of(t, ms) >= HEAVY_CPU or ms[t] >= HEAVY_CPU * 1000]
    best = (static_cost, None)
    orders = [sorted(heavy, key=lambda t: -cpu_of(t, ms)),
              sorted(heavy, key=lambda t: -cpu_of(t, ms) / max(ms[t], 1.0))]
    for order in orders:
        run_cpu = total
        for k in range(0, len(order) + 1):
            if k:
                run_cpu -= cpu_of(order[k - 1], ms) * mult
            if k == len(cands):
                break
            U = order[:k]
            u_cost = (schedule_cost(list(U) + base, ms)[1] - floor) if U else 0
            if can_diff and U:
                u_cost *= (1.0 - P_EQUIV)
            pen = FAIL_PENALTY * 1000 if not U else 500
            cost = u_cost + (run_cpu + ovh) * 1000 + pen
            if cost < best[0]:
                best = (cost, U)
    # run only a few tests that are cheap for their clock time, select the rest unrun
    cheap = sorted((t for t in heavy if cpu_of(t, ms) * mult < ms[t] / 1000.0),
                   key=lambda t: cpu_of(t, ms) / max(ms[t], 1.0))[:6]
    for j in range(1, len(cheap) + 1):
        rs = set(cheap[:j])
        U = [t for t in cands if t not in rs]
        if not U:
            break
        u_cost = schedule_cost(U + base, ms)[1] - floor
        cost = u_cost + (sum(cpu_of(t, ms) for t in rs) * mult + ovh) * 1000 + 500
        if cost + CHEAP_MARGIN * 1000 < best[0]:
            best = (cost, U)
    if best[1] is None:
        return [], sorted(cands, key=lambda t: -cpu_of(t, ms)), static_cost, static_cost
    U = list(best[1])
    us = set(U)
    R = sorted((t for t in cands if t not in us), key=lambda t: cpu_of(t, ms))
    return R, U, best[0], static_cost


READS_PATH = Path(os.environ.get("BR_READS", HERE / "reads.json.gz"))
NOISE_PATH = Path(os.environ.get("BR_NOISE", HERE / "noise.json"))
_READS = None


OUTREADS_PATH = Path(os.environ.get("BR_OUTREADS", HERE / "outreads.json.gz"))
_OUTREADS = None


def load_outreads():
    """Like the reads map, but only reads made outside parse phases."""
    global _OUTREADS
    if _OUTREADS is None:
        import gzip
        def build():
            with gzip.open(OUTREADS_PATH, "rt") as fh:
                R = json.load(fh)
            sets = [frozenset(x) for x in R["sets"]]
            byattr = {}
            for k, i in R["reads"].items():
                o, a, key = k.split("|", 2)
                byattr.setdefault((o, a), []).append((key, i))
            return (sets, byattr, set(R["untraced"]))
        try:
            _OUTREADS = _pkl(OUTREADS_PATH, build)
        except Exception as e:  # noqa
            log("out-of-phase reads map unavailable", repr(e))
            _OUTREADS = False
    return _OUTREADS


def load_reads():
    global _READS
    if _READS is None:
        import gzip
        def build():
            with gzip.open(READS_PATH, "rt") as fh:
                R = json.load(fh)
            sets = [frozenset(x) for x in R["sets"]]
            byattr = {}
            for k, i in R["reads"].items():
                o, a, key = k.split("|", 2)
                byattr.setdefault((o, a), []).append((key, i))
            noise = {tuple(x) for x in json.loads(NOISE_PATH.read_text())}
            return (sets, byattr, set(R["untraced"]), noise)
        try:
            _READS = _pkl(READS_PATH, build)
        except Exception as e:  # noqa
            log("reads map unavailable", repr(e))
            _READS = False
    return _READS


IMPORTS_PATH = Path(os.environ.get("BR_IMPORTS", HERE / "imports.json.gz"))
IMPFAIL_ON = os.environ.get("BR_IMPFAIL", "1") == "1"
_IMPORTS = None


def load_imports():
    """test -> the sqlglot modules it imports, directly or lazily (buildimports.py)."""
    global _IMPORTS
    if _IMPORTS is None:
        import gzip

        def build():
            with gzip.open(IMPORTS_PATH, "rt") as fh:
                d = json.load(fh)
            by_mod = {}
            for t, si in d["tests"].items():
                for m in d["sets"][si]:
                    by_mod.setdefault(d["mods"][m], set()).add(int(t))
            return {"by_mod": by_mod, "known": {int(t) for t in d["tests"]}}
        try:
            _IMPORTS = _pkl(IMPORTS_PATH, build)
        except Exception as e:  # noqa
            log("imports map unavailable", repr(e))
            _IMPORTS = False
    return _IMPORTS


_ALL_MOD_NAMES = None


def _all_mod_names():
    global _ALL_MOD_NAMES
    if _ALL_MOD_NAMES is None:
        import pkgutil
        import sqlglot
        _ALL_MOD_NAMES = [m.name for m in pkgutil.walk_packages(sqlglot.__path__, "sqlglot.") if not m.name.endswith("__main__")]
    return _ALL_MOD_NAMES


def _import_all():
    import importlib
    failed = []
    for name in _all_mod_names():
        try:
            importlib.import_module(name)
        except BaseException as e:  # noqa
            failed.append((name, repr(e)[:80]))
    return failed


def _modname(rel):
    name = rel[:-3].replace("/", ".")
    if name.endswith(".__init__"):
        name = name[: -len(".__init__")]
    return name


SCOPE_ON = os.environ.get("BR_SCOPE", "0") == "1"
_IMPORT_RE = re.compile(r"^[ \t]*(?:from[ \t]+([.\w]+)[ \t]+import[ \t]+(\([^)]*\)|[^\n]*)|import[ \t]+([^\n]+))", re.M)
_IMPORT_GRAPH = None


def _static_imports(mod, is_pkg, text, known):
    """sqlglot modules named by any import statement of a module's text (function-level
    ones too), with the parent packages an import of them also runs."""
    out = set()
    pkg = mod if is_pkg else mod.rpartition(".")[0]

    def add(name):
        while name:
            if name in known:
                out.add(name)
            name = name.rpartition(".")[0]

    for m in _IMPORT_RE.finditer(text):
        src, names, plain = m.groups()
        if plain is not None:
            for part in plain.split("#")[0].split(","):
                add(part.strip().split(" ")[0])
            continue
        if src.startswith("."):
            dots = len(src) - len(src.lstrip("."))
            base = pkg
            for _ in range(dots - 1):
                base = base.rpartition(".")[0]
            rest = src[dots:]
            src = base + "." + rest if rest else base
        add(src)
        for part in re.sub(r"#[^\n]*", "", names).strip("()").split(","):
            part = part.strip().split(" ")[0]
            if part:
                add(src + "." + part)
    add(mod.rpartition(".")[0])
    out.discard(mod)
    return out


def _import_graph():
    """module -> sqlglot modules its source imports, at the pinned commit."""
    global _IMPORT_GRAPH
    if _IMPORT_GRAPH is None:
        _preload_src_text()
        mods = {}
        for rel, text in _SRC_TEXT.items():
            if rel.startswith("sqlglot/"):
                mods[_modname(rel)] = (rel.endswith("/__init__.py"), text)
        known = set(mods)
        _IMPORT_GRAPH = {m: _static_imports(m, pk, text, known) for m, (pk, text) in mods.items()}
    return _IMPORT_GRAPH


def _diff_scope(old_mods, new_mods, seeds, files=None):
    """Modules whose import-time state a change to `seeds` can reach.

    That is the changed modules themselves, anything they import or inherit (their
    top-level code can write those), and anything that imports them, however far out:
    an importer can copy or derive a value at import time, and its own importers can do
    the same with that. None means the comparison should cover every module.
    """
    import types
    if not SCOPE_ON:
        return None
    seeds = {s for s in seeds if s in old_mods or s in new_mods}
    if not seeds:
        return None
    graph = dict(_import_graph())
    known = set(old_mods) | set(new_mods) | set(graph)
    for rel, text in (files or {}).items():
        if text is not None and rel.endswith(".py") and rel.startswith("sqlglot/"):
            m = _modname(rel)
            graph[m] = graph.get(m, set()) | _static_imports(m, rel.endswith("/__init__.py"), text, known)
    scope = set(seeds)

    def absorb(mod):
        if mod is None:
            return
        try:
            vals = list(vars(mod).values())
        except Exception:  # noqa
            return
        for v in vals:
            if isinstance(v, types.ModuleType) and v.__name__ in known:
                scope.add(v.__name__)
            elif isinstance(v, type) and getattr(v, "__module__", None) in known:
                scope.add(v.__module__)
        try:
            import statediff
            for c in statediff._classes_of(mod):
                for b in c.__mro__:
                    m = getattr(b, "__module__", None)
                    if m in known:
                        scope.add(m)
        except Exception:  # noqa
            pass

    for s in seeds:
        absorb(new_mods.get(s) or old_mods.get(s))
        scope |= graph.get(s, set())
    # importers, by import statement
    rev = {}
    for m, deps in graph.items():
        for d in deps:
            rev.setdefault(d, set()).add(m)
    todo = list(seeds)
    reached = set(seeds)
    while todo:
        for m in rev.get(todo.pop(), ()):
            if m not in reached:
                reached.add(m)
                todo.append(m)
    # holders of a seed's module, class or function (e.g. a package that caches its
    # lazily loaded members); these are not followed further out
    for mods in (old_mods, new_mods):
        for name, mod in mods.items():
            if name in reached:
                continue
            try:
                vals = list(vars(mod).values())
            except Exception:  # noqa
                continue
            for v in vals:
                if isinstance(v, types.ModuleType):
                    d = v.__name__
                elif isinstance(v, (type, types.FunctionType)):
                    d = getattr(v, "__module__", None)
                else:
                    continue
                if d in seeds:
                    scope.add(name)
                    break
    return scope | reached


def overlay_candidates(j, old_mods, nodeids):
    """After re-importing the changed sqlglot: what changed in import-time state, and
    which tests read it."""
    broad = set(j["broad"])
    precise = set(j["precise"])
    R = load_reads()
    failed_imports = _import_all()
    old_failed = set(OLD_IMPORT_FAILURES)
    newly = [f for f in failed_imports if f[0] not in old_failed]
    forced = set()
    fail_note = ""
    if newly:
        # modules that no longer import: every test that imports one of them (even
        # lazily) is selected unrun; the other tests see the remaining state as usual
        IM = load_imports() if IMPFAIL_ON else False
        if not IM or not R:
            return {"all": True, "note": f"import-fail {newly[0]}"}
        n_tests = len(nodeids)
        forced = {t for t in range(n_tests) if t not in IM["known"]}
        for name, _ in newly:
            forced |= IM["by_mod"].get(name, set())
        fail_note = f"import-fail {len(newly)} {newly[0]} forced={len(forced)} "
        if len(forced) >= n_tests:
            return {"all": True, "note": fail_note}
    if not R:
        return {"cands": broad | precise, "note": "no-reads-map"}
    sets, byattr, untraced, noise = R
    import statediff
    new_mods = {k: v for k, v in sys.modules.items() if k == "sqlglot" or k.startswith("sqlglot.")}
    seeds = [_modname(r) for r in j.get("files", {}) if r.endswith(".py") and r.startswith("sqlglot/")]
    scope = _diff_scope(old_mods, new_mods, seeds, j.get("files"))
    try:
        delta = statediff.diff_state(old_mods, new_mods, only=scope)
        delta = [c for c in delta if c not in noise]
    except Exception as e:  # noqa
        return {"cands": broad | precise, "note": f"diff-failed {e!r}"}
    cands = set(precise)
    unknown = []
    M = MAP_OBJ
    OR = load_outreads() if REPLAY_ON else False
    outside = set(OR[2]) if OR else set()   # tests that touch a change outside parse phases
    fn_lines = {}
    n_glob = 0
    for kind, owner, attr, key in delta:
        if kind == "cls":
            if attr in ("*", "__mro__"):
                unknown.append((kind, owner, attr))
                continue
            for k2, i in byattr.get((owner, attr), []):
                if key is None or k2 == key or k2 == "*":
                    cands |= sets[i]
            if OR:
                for k2, i in OR[1].get((owner, attr), []):
                    if key is None or k2 == key or k2 == "*":
                        outside |= OR[0][i]
        elif kind in ("meth", "methcode"):
            if kind == "meth":
                for k2, i in byattr.get((owner, attr), []):
                    cands |= sets[i]
                if OR:
                    for k2, i in OR[1].get((owner, attr), []):
                        outside |= OR[0][i]
            rng = _function_range(old_mods, owner, attr)
            if rng is not None:
                fn_lines.setdefault(rng[0], set()).update(range(rng[1], rng[2] + 1))
            if kind == "meth" or not precise:
                if rng is None:
                    unknown.append((kind, owner, attr))
                else:
                    cands |= M.covering(*rng)
            elif rng is None:
                OR = False
        else:
            refs = _global_refs(attr, nodeids) if (kind == "mod" and GLOBAL_REFS) else None
            if refs is None:
                unknown.append((kind, owner, attr))
            else:
                # a module global: its readers are the lines that name it
                for f, ls in refs[0].items():
                    fn_lines.setdefault(f, set()).update(ls)
                    for l in ls:
                        cands |= M.covering(f, l, l)
                cands |= refs[1]
                outside |= refs[1]
                n_glob += 1
    note = fail_note + f"delta={len(delta)} {[d[1].split(':')[-1] + '.' + str(d[2]) for d in delta][:3]} scope={0 if scope is None else len(scope)}"
    if n_glob:
        note += f" globals={n_glob}"
    if unknown:
        cands |= broad
        note += f" unknown={unknown[:2]}"
        if len(cands) > BROAD_CANDS:
            return {"all": True, "note": note + " broad"}
    if delta:
        cands |= untraced & (broad | precise)
    # A generator class's dispatch table is built once per process from TRANSFORMS and the
    # *_sql methods and then reused: the per-input attribute trace only saw those reads
    # for the first input that built it. For such a change every input that creates a
    # generator counts as a reader.
    disp = False
    try:
        gen_base = old_mods["sqlglot.generator"].Generator
        for d in delta:
            if d[0] not in ("cls", "meth"):
                continue
            modname, _, qual = d[1].partition(":")
            obj = old_mods.get(modname)
            try:
                for part in (x for x in qual.split(".") if x):
                    obj = vars(obj)[part]
                if isinstance(obj, type) and not issubclass(obj, gen_base):
                    continue
            except Exception:  # noqa
                pass
            disp = True
            break
    except Exception:  # noqa
        disp = True
    return {"cands": cands, "note": note, "unknown": bool(unknown), "forced": forced, "disp": disp,
            "pure": all(d[0] == "methcode" for d in delta) and not forced,
            "replay_ok": bool(OR) and not unknown, "outside": outside, "fn_lines": fn_lines,
            "attrs": sorted({(d[1], d[2]) for d in delta if d[0] in ("cls", "meth")})}


GLOBAL_REFS = os.environ.get("BR_GLOBAL_REFS", "1") == "1"
_SRC_TEXT = {}


def _preload_src_text():
    if not _SRC_TEXT:
        for top in ("sqlglot", "tests"):
            for path in sorted((REPO / top).rglob("*.py")):
                _SRC_TEXT[str(path.relative_to(REPO))] = path.read_text(encoding="utf-8", errors="replace")


def _global_refs(name, nodeids):
    """Where a module-level name is mentioned at the pinned commit: ({sqlglot file: lines},
    tests of the test files that mention it). None when that cannot be told."""
    import re
    try:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) or not name:
            return None
        _preload_src_text()
        pat = re.compile(r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(name))
        src, tests = {}, set()
        by_file = None
        for rel, text in _SRC_TEXT.items():
            if name not in text:
                continue
            lines = {i for i, line in enumerate(text.split("\n"), 1) if pat.search(line)}
            if not lines:
                continue
            if rel.startswith("sqlglot/"):
                src[rel] = lines
            else:
                if by_file is None:
                    by_file = {}
                    for t, nid in enumerate(nodeids):
                        by_file.setdefault(nid.split("::", 1)[0], set()).add(t)
                if rel not in by_file:
                    return None     # a shared test helper: its users are not known
                tests |= by_file[rel]
        return src, tests
    except Exception:  # noqa
        return None


def _function_range(mods, owner, attr):
    modname, _, qual = owner.partition(":")
    obj = mods.get(modname)
    try:
        for part in (p for p in qual.split(".") if p):
            obj = vars(obj)[part]
        fn = vars(obj)[attr]
        fn = getattr(fn, "__func__", fn)
        code = fn.__code__
        lines = [l for _, _, l in code.co_lines() if l]
        return os.path.relpath(code.co_filename, str(REPO)), min(lines), max(lines)
    except Exception:  # noqa
        return None


def _function_coverage(mods, owner, attr, M):
    modname, _, qual = owner.partition(":")
    mod = mods.get(modname)
    obj = mod
    try:
        for part in (p for p in qual.split(".") if p):
            obj = vars(obj)[part]
        fn = vars(obj)[attr]
        fn = getattr(fn, "__func__", fn)
        code = fn.__code__
        lines = [l for _, _, l in code.co_lines() if l]
        rel = os.path.relpath(code.co_filename, str(REPO))
        return M.covering(rel, min(lines), max(lines))
    except Exception:  # noqa
        return None


MAP_OBJ = None
OLD_IMPORT_FAILURES = []


def prove_equivalent(maybe_equiv, answers):
    """Answer the commits whose edit provably changes no behavior with no test at all."""
    try:
        from equiv import proven_equivalent
    except Exception as e:  # noqa
        log("equiv unavailable", repr(e))
        return
    for cid, files in maybe_equiv.items():
        try:
            if proven_equivalent(files, REPO):
                answers[cid] = []
                log(cid, "none equiv")
        except Exception as e:  # noqa
            log(cid, "equiv check failed", repr(e))


def run_jobs(jobs, answers, nodeids, ms, deadline):
    import runner

    if MEMO_PATH.is_file() and any(j["memo_duck"] for j in jobs):
        memo_ids(nodeids)
        if runner.MEMO is None:
            runner.load_memo(str(MEMO_PATH))
    modules = set()
    for j in jobs:
        for t in j["light"] + j["heavy"]:
            modules.add(runner.module_name_for(nodeids[t].split("::")[0]))
    runner.zygote_import(modules)
    if any(j["overlay"] for j in jobs):
        _all_mod_names()
        OLD_IMPORT_FAILURES[:] = [failure[0] for failure in _import_all()]
        load_reads()
        load_imports()
        _preload_src_text()
        _import_graph()
    if os.environ.get("BR_PRESET", "1") == "1":
        # executor tests wanted by at least two commits: load their class fixtures once
        want = 0
        for j in jobs:
            if not j["overlay"] and j.get("pure_src") and any(
                    nodeids[t].startswith("tests/test_executor.py::TestExecutor::") for t in j["light"] + j["heavy"]):
                want += 1
        if want >= PRESET_MIN:
            ok = runner.prebuild_class("tests.test_executor", "TestExecutor", cache=str(PRESET_CACHE))
            log(f"shared executor fixtures for {want} commits: {ok}")
    # maps every child would otherwise load for itself
    try:
        _submap()
        if REPLAY_ON and len(jobs) >= 2 and any(j.get("pure_src") for j in jobs):
            R = _replay()
            if R:
                _attr_mask(R, ())
            if any(j["overlay"] for j in jobs):
                load_outreads()
        if len(jobs) >= 2 and any(j["overlay"] and j.get("pure_src") for j in jobs):
            _itermap()
            opt_affected({})
    except Exception as e:  # noqa
        log("preload failed", repr(e))
    import gc
    gc.collect(1)
    gc.freeze()  # children neither scan nor copy the zygote's heap
    log(f"zygote ready at {time.time() - T_START:.2f}s")

    # cheapest jobs first so that a tight budget hurts the fewest commits
    def est(j):
        return sum(cpu_of(t, ms) for t in j["light"]) * 1000
    pending = sorted(jobs, key=est)
    running = {}  # pid -> (job, fd, started)
    tmp_root = tempfile.mkdtemp(prefix="br")
    total_est_left = sum(est(j) for j in pending) / 1000.0 * CPU_FACTOR

    while pending or running:
        now = time.time()
        # launch
        while pending and len(running) < PARALLEL:
            j = pending.pop(0)
            remaining = deadline - time.time()
            need = est(j) / 1000.0 * CPU_FACTOR
            total_est_left -= need
            # allow heavy runs only when the rest of the batch still fits
            heavy_budget = remaining - 3.0 - max(0.0, total_est_left) / PARALLEL
            light_cost = est(j) / 1000.0
            if remaining < 4.0 or light_cost > remaining - 3.0:
                log(j["cid"], "skipped (budget)")
                continue
            j["tmp"] = os.path.join(tmp_root, j["cid"])
            os.makedirs(j["tmp"], exist_ok=True)
            j["heavy_budget"] = heavy_budget
            rd, wr = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(rd)
                code = 0
                _CHILD_FD[0] = wr
                try:
                    out = child(j, nodeids, ms, runner, modules)
                except BaseException as e:  # noqa
                    out = {"error": repr(e)}
                try:
                    data = json.dumps(out).encode()
                    while data:
                        n = os.write(wr, data)
                        data = data[n:]
                finally:
                    os._exit(code)
            os.close(wr)
            running[pid] = (j, rd, time.time(), [])
        if not running:
            continue
        # collect
        fds = {v[1]: pid for pid, v in running.items()}
        timeout = max(0.05, deadline - time.time())
        r, _, _ = select.select(list(fds), [], [], min(timeout, 1.0))
        for fd in r:
            pid = fds[fd]
            j, _, started, buf = running[pid]
            chunk = os.read(fd, 65536)
            if chunk:
                buf.append(chunk)
                continue
            os.close(fd)
            os.waitpid(pid, 0)
            del running[pid]
            raw = b"".join(buf)
            reimported = raw.startswith(_REIMPORT_MARK)
            if reimported:
                raw = raw[len(_REIMPORT_MARK):]
            try:
                out = json.loads(raw or b"{}")
            except Exception:  # noqa
                out = {"error": "bad json"}
            if "error" in out or "failed" not in out:
                if reimported or j["overlay"] or j.get("was_import"):
                    # the patched package was re-imported: nothing static bounds its effect
                    answers[j["cid"]] = list(range(len(nodeids)))
                    log(j["cid"], "child error", out.get("error"), "-> all tests")
                else:
                    log(j["cid"], "child error", out.get("error"), "-> static")
                continue
            if out.get("all"):
                sel = list(range(len(nodeids)))
            else:
                sel = sorted(set(out["failed"]) | set(out.get("unrun", [])))
            st = schedule_cost(answers[j["cid"]], ms)[1] / 1000.0
            fin = schedule_cost(sel, ms)[1] / 1000.0
            answers[j["cid"]] = sel
            log(f"{j['cid']} st={st:.1f} fin={fin:.1f} light={len(j['light'])} heavy={len(j['heavy'])} failed={len(out['failed'])} "
                f"unrun={len(out.get('unrun', []))} {time.time() - started:.2f}s ov={int(j['overlay'])} {out.get('note', '')} | {j['reasons'][:80]}")
        if time.time() > deadline:
            for pid, (j, fd, started, buf) in list(running.items()):
                try:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                except Exception:  # noqa
                    pass
                try:
                    while True:     # what it wrote before it was stopped
                        chunk = os.read(fd, 65536)
                        if not chunk:
                            break
                        buf.append(chunk)
                except Exception:  # noqa
                    pass
                os.close(fd)
                if b"".join(buf).startswith(_REIMPORT_MARK) or j["overlay"] or j.get("was_import"):
                    answers[j["cid"]] = list(range(len(nodeids)))
                    log(j["cid"], "killed at deadline -> all tests")
                else:
                    log(j["cid"], "killed at deadline -> static")
            running.clear()
            for j in pending:
                log(j["cid"], "not started -> static")
            pending = []


def _sampler():
    """Debug: cpu-time stack sampling of a child (BR_SAMPLE=1)."""
    import collections
    self_c, incl_c = collections.Counter(), collections.Counter()

    def h(sig, frame):
        seen = set()
        f = frame
        first = True
        while f is not None:
            co = f.f_code
            k = f"{co.co_filename.rsplit('/', 2)[-2]}/{co.co_filename.rsplit('/', 1)[-1]}:{co.co_name}"
            if first:
                self_c[k + f":{f.f_lineno}"] += 1
                first = False
            if k not in seen:
                seen.add(k)
                incl_c[k] += 1
            f = f.f_back
    signal.signal(signal.SIGPROF, h)
    signal.setitimer(signal.ITIMER_PROF, 0.004, 0.004)

    def done(cid):
        signal.setitimer(signal.ITIMER_PROF, 0, 0)
        n = sum(self_c.values())
        log("SAMPLE", cid, n, "samples\nINCL", "\n".join(f"{v:6d} {k}" for k, v in incl_c.most_common(70)),
            "\nSELF", "\n".join(f"{v:6d} {k}" for k, v in self_c.most_common(40)))
    return done


def child(j, nodeids, ms, runner, modules):
    c_start = time.process_time()
    smp = _sampler() if os.environ.get("BR_SAMPLE") else None
    out = _child(j, nodeids, ms, runner, modules)
    if smp:
        smp(j["cid"])
    if FORCED and "failed" in out and not out.get("all"):
        out["failed"] = sorted(set(out["failed"]) | FORCED)
    try:
        tc = runner.TEST_CPU
        slow = {t: round(v, 2) for t, v in sorted(tc.items(), key=lambda kv: -kv[1])[:5] if v >= 0.25}
        out["note"] = (f" prof(cpu={time.process_time() - c_start:.2f} prep={PROF.get('prep', 0):.2f} "
                       f"ovc={PROF.get('ovc', 0):.2f} replay={PROF.get('replay', 0):.2f} tests={sum(tc.values()):.2f} slow={slow} "
                       f"start={c_start:.2f} marks={PROF.get('marks')})"
                       + out.get("note", ""))
    except Exception:  # noqa
        pass
    return out


PROF = {}
_CHILD_FD = [None]
_REIMPORT_MARK = b"REIMPORT\n"
FORCED: set = set()   # tests selected without running (they import a module that no longer imports)


def _child(j, nodeids, ms, runner, modules):
    os.environ["TMPDIR"] = j["tmp"]
    try:
        # backstop against patched code that never returns (import loops and the like)
        resource.setrlimit(resource.RLIMIT_CPU, (CHILD_CPU_MAX, CHILD_CPU_MAX + 5))
    except Exception:  # noqa
        pass
    signal.signal(signal.SIGALRM, runner._alarm)
    signal.setitimer(signal.ITIMER_REAL, SETUP_TIMEOUT)     # applying the patch must not hang
    CPU_OVR.clear()
    CPU_OVR.update(j.get("cpu_ovr") or {})
    runner.SUBMIT_FILTER.clear()
    runner.SUBMIT_FILTER.update(j.get("subf") or {})
    runner.SUBMIT_LAZY.update(_submap())
    import optmemo
    optmemo.ON[0] = j.get("opt_aff") is not None and not j["overlay"]
    optmemo.AFFECTED[:] = j.get("opt_aff") or []
    optmemo.set_changed(j.get("ch_lines") if (optmemo.ON[0] and OPT_PER_CALL) else None)
    runner.ITER_FILTER.clear()
    runner.ITER_FILTER.update({int(t): set(v) for t, v in (j.get("iterf") or {}).items()})
    runner.MEMO_DUCK = bool(j.get("memo_duck")) and runner.MEMO is not None
    if runner.MEMO_DUCK and all(p.startswith("tests/fixtures/") and p.endswith(".sql") for p in j["files"]):
        runner.FIXTURE_CALLS = runner.MEMO.get("pool_results")
    runner.MEMO_SKIP.clear()
    runner.MEMO_SKIP.update(j.get("memo_skip") or [])
    tempfile.tempdir = j["tmp"]
    if not j["overlay"]:
        for r in j["hot"]:
            if r.startswith("sqlglot/") or r.startswith("tests/"):
                name = r[:-3].replace("/", ".")
                if name.endswith(".__init__"):
                    name = name[: -len(".__init__")]
                try:
                    __import__(name)
                except Exception:  # noqa
                    pass
    note = ""
    old_mods = {k: v for k, v in sys.modules.items() if k == "sqlglot" or k.startswith("sqlglot.")}
    c0 = time.process_time()
    try:
        prepared = runner.prepare(j, modules)
    except runner.NeedReimport as e:
        try:
            os.write(_CHILD_FD[0], _REIMPORT_MARK)   # the parent must not fall back to static
        except Exception:  # noqa
            pass
        j["overlay"] = True
        note = f"reimport({e})"
        prepared = runner.prepare(j, modules)
    PROF["prep"] = time.process_time() - c0
    PROF["marks"] = [("prep", round(time.process_time(), 2))]
    unrun = list(j["heavy"])
    if runner.PRESETS:
        kept = runner.apply_presets(not j["overlay"] and bool(j.get("pure_src")) and not j.get("was_import"),
                                    j.get("ch_lines"))
        if kept:
            note += " preset"
    if j["overlay"]:
        optmemo.ON[0] = False   # decided again once the state diff is known
        optmemo.set_changed(None)
        c0 = time.process_time()
        res = overlay_candidates(j, old_mods, nodeids)
        PROF["ovc"] = time.process_time() - c0
        if res.get("all"):
            return {"failed": [], "unrun": [], "all": True, "note": res["note"]}
        forced = set(res.get("forced") or ())
        FORCED.update(forced)
        cands = sorted(set(res["cands"]) - forced)
        j["unknown"] = res.get("unknown", False)
        if res.get("replay_ok") and j.get("pure_src"):
            cands, rn = replay_narrow(cands, j.get("ch_lines"), ms, res.get("outside"), res.get("fn_lines"),
                                      light0=() if len(cands) > 300 else None, pure=bool(res.get("pure")),
                                      attrs=None if res.get("pure") else res.get("attrs"),
                                      disp=bool(res.get("disp", True)))
            note += rn
            if HANG[0]:
                return {"failed": [], "unrun": sorted(cands), "note": note + " " + res["note"]}
            if not cands:
                return {"failed": [], "unrun": [], "note": note}
        if res.get("pure") and j.get("pure_src") and j.get("ch_lines") is not None:
            lines = {f: set(v) for f, v in j["ch_lines"].items()}
            for f, v in (res.get("fn_lines") or {}).items():
                lines.setdefault(f, set()).update(v)
            aff = opt_affected(lines)
            if aff is not None:
                optmemo.ON[0] = True
                optmemo.AFFECTED[:] = aff
                optmemo.set_changed(lines if OPT_PER_CALL else None)
                opt_cpu(aff, cands)
            itf = iter_filter(cands, lines)
            iter_cpu(itf)
            runner.ITER_FILTER.update(itf)
            if itf:
                note += f" iter={ {t: len(v) for t, v in itf.items()} }"[:120]
        runner.MEMO_SKIP.clear()
        if runner.MEMO_DUCK:
            if res.get("pure"):
                # import-time state is unchanged: only function bodies differ
                runner.MEMO_SKIP.update(memo_skippable(j.get("ch_lines"), nodeids))
            memo_cpu(runner.MEMO_SKIP, nodeids, j.get("subf"))
        note += " " + res["note"]
        if not cands:
            return {"failed": [], "unrun": [], "note": note}
        light, heavy, est, static = plan_split(cands, ms, CPU, True, False, overhead=0.0, mult=OV_MULT,
                                               base=sorted(forced))
        budget = j.get("heavy_budget", 1e9)
        est_light = sum(cpu_of(t, ms) for t in light) * OV_MULT
        if est_light > min(budget, MAX_CHILD_CPU):
            light, heavy = [], cands
        note += f" cands={len(cands)} run={len(light)} est={est/1000:.1f}/{static/1000:.1f}"
        note += f" cpu={sum(cpu_of(t, ms) for t in cands):.1f} top={[(t, round(cpu_of(t, ms), 1)) for t in sorted(cands, key=lambda t: -cpu_of(t, ms))[:3]]} skip={sorted(runner.MEMO_SKIP)} bud={budget:.0f}"
        j["light"], j["heavy"] = light, heavy
        unrun = list(heavy)
    elif j.get("pure_src") and not j.get("was_import"):
        allc = sorted(set(j["light"]) | set(j["heavy"]))
        cands, rn = replay_narrow(allc, j.get("ch_lines"), ms, light0=j["light"], pure=True)
        note += rn
        if HANG[0]:
            return {"failed": [], "unrun": allc, "note": note}
        if len(cands) != len(allc) or not j["light"]:
            if not cands:
                return {"failed": [], "unrun": [], "note": note}
            light, heavy, est, static = plan_split(cands, ms, CPU, False, prepared is True)
            j["light"], j["heavy"] = light, heavy
            unrun = list(heavy)
    signal.setitimer(signal.ITIMER_REAL, 0)
    runner.TIMEOUTS[0] = 0
    PROF["marks"].append(("plan", round(time.process_time(), 2)))

    # tests that clear much schedule time per cpu second go first; then the rest grouped
    # by class (cheapest classes first) so class setup is shared
    def density(t):
        return ms[t] / 1000.0 / max(cpu_of(t, ms), 0.002)

    if prepared is True:
        # Differential probing is active: prioritize fast tests by class so probe
        # sites are exercised rapidly with minimal CPU, triggering early equivalence stops.
        groups = {}
        for t in j["light"]:
            groups.setdefault(nodeids[t].rsplit("::", 1)[0], []).append(t)
        for g in groups.values():
            g.sort(key=lambda t: cpu_of(t, ms))
        ordered = [t for g in sorted(groups.values(), key=lambda g: sum(cpu_of(t, ms) for t in g)) for t in g]
    else:
        tier1 = sorted((t for t in j["light"] if ms[t] >= DENSE_MS and density(t) >= DENSE_MIN), key=lambda t: -density(t))
        t1 = set(tier1)
        groups = {}
        for t in tier1:
            groups.setdefault(nodeids[t].rsplit("::", 1)[0], []).append(t)
        ordered = [t for g in groups.values() for t in g]
        groups = {}
        for t in j["light"]:
            if t not in t1:
                groups.setdefault(nodeids[t].rsplit("::", 1)[0], []).append(t)
        ordered += [t for g in sorted(groups.values(), key=lambda g: sum(cpu_of(t, ms) for t in g)) for t in g]
    n_total = len(ordered)
    rp = {"dropped": set(), "n": 0}

    def should_stop(cur_failed, rest):
        if j["overlay"] and j.get("unknown") and len(cur_failed) >= BROAD_FAILS:
            return True
        if prepared is True and not cur_failed and not runner.DIFF_BAD:
            hits = runner.DIFF_HITS
            n_sites = j["n_sites"]
            n_ran = n_total - len(rest) - len(rp["dropped"])
            if n_ran >= 8 and hits and all(hits.get(k, 0) >= 30 for k in range(n_sites)):
                return True
        if not cur_failed:
            return False
        base = sorted(set(unrun) | set(cur_failed))
        delta = schedule_cost(base + rest, ms)[1] - schedule_cost(base, ms)[1]
        return delta / 1000.0 <= sum(cpu_of(t, ms) for t in rest) + 0.2

    def replan(cur_failed, rest):
        """Another test failed: which of the remaining tests are still worth their cpu?"""
        if j["overlay"] and j.get("unknown") and len(cur_failed) >= BROAD_FAILS:
            return []
        rp["n"] += 1
        base = sorted(set(unrun) | set(cur_failed) | rp["dropped"] | FORCED)
        n_run = max(1, n_total - len(rest) - len(rp["dropped"]))
        q = min(Q_MAX, (len(cur_failed) + Q_PRIOR) / (n_run + 2.0 * Q_PRIOR))
        order = sorted(rest, key=lambda t: -density(t))
        full = schedule_cost(base + order, ms)[1]
        n = len(order)
        ks = sorted(set(list(range(0, min(n, 12) + 1)) + [int(n * f) for f in (0.2, 0.35, 0.5, 0.65, 0.8, 0.9, 1.0)]))
        best_k, best_net = 0, 0.0
        cpu_pref = [0.0]
        for t in order:
            cpu_pref.append(cpu_pref[-1] + cpu_of(t, ms))
        for k in ks:
            if k == 0:
                continue
            saved = (full - schedule_cost(base + order[k:], ms)[1]) / 1000.0
            net = saved * (1.0 - q) - RP_MARGIN * cpu_pref[k] - 0.1
            if net > best_net:
                best_k, best_net = k, net
        rp["dropped"].update(order[best_k:])
        return order[:best_k]

    skipped = []
    if os.environ.get("BR_PROFILE"):
        import cProfile, pstats, io
        pr = cProfile.Profile()
        pr.enable()
        tt0 = time.time()
        per = []
        for t in ordered:
            c0 = time.process_time()
            runner.run_tests([t], nodeids, timeouts=[600.0])
            per.append((round(time.process_time() - c0, 2), nodeids[t][-50:]))
        pr.disable()
        buf = io.StringIO()
        pstats.Stats(pr, stream=buf).sort_stats("cumulative").print_stats(45)
        log("PROFILE", j["cid"], "tests", len(ordered), sorted(per, reverse=True)[:25], buf.getvalue()[:6000])
    failed = set(runner.run_tests(ordered, nodeids, timeouts=[test_timeout(t, ms) for t in ordered],
                                  replan=replan if REPLAN else None, should_stop=should_stop,
                                  skipped=skipped))
    PROF["marks"].append(("ran", round(time.process_time(), 2)))
    if skipped:
        unrun.extend(skipped)
        note += f" early-stop({len(skipped)}/{rp['n']})"
    if j["overlay"] and j.get("unknown") and len(failed) >= BROAD_FAILS:
        # an import-time change that breaks many tests: its reach is beyond what
        # the coverage map can tell, so run nothing more and select everything
        return {"failed": sorted(failed), "unrun": [], "all": True, "note": note + " broad-import-break"}
    if prepared is True:
        hits, bad = dict(runner.DIFF_HITS), set(runner.DIFF_BAD)
        n_sites = j["n_sites"]
        note += f" diff hits={hits} bad={sorted(bad)}"
        n_ran = len(ordered) - len(skipped)
        # equivalence is only trusted when most candidates exercised the change
        if unrun and not failed and hits and not bad and all(hits.get(k, 0) > 0 for k in range(n_sites)) \
                and (n_ran >= 0.5 * (n_ran + len(unrun)) or (n_ran >= 8 and all(hits.get(k, 0) >= 30 for k in range(n_sites)))):
            note += " EQUIV->drop heavy"
            unrun = []
    # second phase: the light tests are done; a heavy test left unrun is run after
    # all when the schedule time it would free outweighs its cpu
    if unrun and not (j["overlay"] and j.get("unknown")):
        budget = min(j.get("heavy_budget", 1e9), MAX_CHILD_CPU)
        mult = OV_MULT if j["overlay"] else 1.0
        p_pass = P2_PASS if not failed else P2_PASS_FAILED
        late = []
        big = [t for t in unrun if ms[t] >= HEAVY_CPU * 1000]
        for t in sorted(big, key=lambda t: cpu_of(t, ms) / max(ms[t], 1.0))[:8]:
            c = cpu_of(t, ms) * mult
            if c > budget or (ms[t] / 1000.0 + 2.0) * p_pass <= LATE_MARGIN * c + 0.1:
                continue
            cur = sorted(set(unrun) | failed | FORCED)
            gain = (schedule_cost(cur, ms)[1] - schedule_cost([x for x in cur if x != t], ms)[1]) / 1000.0
            if gain * p_pass <= LATE_MARGIN * c + 0.1:
                continue
            if runner.TIMEOUTS[0]:
                break       # the patched code hangs: no more runs
            f = runner.run_tests([t], nodeids, timeouts=[test_timeout(t, ms)])
            budget -= c
            unrun.remove(t)
            failed |= set(f)
            late.append(t)
        if late:
            note += f" late-run{late}"
    if any(runner.MEMO_STAT):
        note += f" memo={runner.MEMO_STAT} memoT={[round(x, 2) for x in runner.MEMO_T]}"
    if any(optmemo.STAT):
        note += f" opt={optmemo.STAT}/{optmemo.AFFECTED} optT={[round(x, 2) for x in optmemo.TIMES]}"
    return {"failed": sorted(failed), "unrun": unrun, "note": note}


if __name__ == "__main__":
    if os.environ.get("BR_SUPERVISE", "1") == "1":
        supervise()
    else:
        main()
