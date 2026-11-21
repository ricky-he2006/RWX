"""Static candidate selection: which tests could a patch possibly break."""
from __future__ import annotations

import ast
import gzip
import json
import os
import re
from pathlib import Path

from patchlib import apply_hunks, ast_changes, changed_old_lines, parse_patch

DIALECT_DIRS = ("sqlglot/dialects/", "sqlglot/parsers/", "sqlglot/generators/",
                "sqlglot/tokenizers/", "sqlglot/typing/")


class Map:
    def __init__(self, path):
        with gzip.open(path, "rt") as fh:
            d = json.load(fh)
        self.tests = d["tests"]
        self.n = len(self.tests)
        self.sets = [frozenset(s) for s in d["sets"]]
        self.lines = {f: {int(l): i for l, i in m.items()} for f, m in d["lines"].items()}
        self.importlines = {f: set(ls) for f, ls in d["importlines"].items()}
        self.opened = {p: set(s) for p, s in d["opened"].items()}
        self.file_of = [t.split("::", 1)[0] for t in self.tests]
        self._file_users = {}
        self._tests_by_file = {}
        for i, f in enumerate(self.file_of):
            self._tests_by_file.setdefault(f, set()).add(i)

    def covering(self, f, start, end):
        m = self.lines.get(f)
        out = set()
        if not m:
            return out
        if end - start > 5000:
            for l, i in m.items():
                if start <= l <= end:
                    out |= self.sets[i]
            return out
        for l in range(start, end + 1):
            i = m.get(l)
            if i is not None:
                out |= self.sets[i]
        return out

    def file_users(self, f):
        """Tests that executed any line of file f at test time."""
        if f not in self._file_users:
            s = set()
            for i in set(self.lines.get(f, {}).values()):
                s |= self.sets[i]
            self._file_users[f] = s
        return self._file_users[f]

    def tests_in_file(self, tf):
        return set(self._tests_by_file.get(tf, ()))


def dialect_name(path):
    for d in DIALECT_DIRS:
        if path.startswith(d):
            return Path(path).stem
    return None


class Decision:
    def __init__(self):
        self.cands: set[int] = set()
        self.precise: set[int] = set()   # candidates not coming from broad import-time guesses
        self.all = False
        self.parsed = {}  # patched trees reused when instrumenting this change
        self.reasons: list[str] = []
        self.import_change = False   # something changed at import time
        self.files: dict[str, str] = {}  # path -> new content (None = deleted)
        self.rt_lines: dict[str, set] = {}  # runtime-changed old lines per sqlglot file
        self.ch_lines: dict[str, set] = {}  # every changed old line per non-test python file

    def add(self, s, why, broad=False):
        self.cands |= s
        if not broad:
            self.precise |= s
        self.reasons.append(f"{why}:{len(s)}")


def analyze(patch_text: str, repo: Path, M: Map) -> Decision:
    D = Decision()
    try:
        fps = parse_patch(patch_text)
    except Exception as e:  # noqa
        D.all = True
        D.reasons.append(f"parse-fail {e!r}")
        return D
    if not fps:
        D.reasons.append("empty patch")
        return D
    for fp in fps:
        _analyze_file(fp, repo, M, D)
    return D


def _read(repo, rel):
    p = repo / rel
    try:
        return p.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def _analyze_file(fp, repo, M, D):
    old_path = fp.old_path
    new_path = fp.new_path
    path = fp.path or old_path or new_path
    if fp.binary:
        D.all = True
        D.reasons.append(f"binary {path}")
        return
    old_text = _read(repo, old_path) if old_path and not fp.is_new else None
    try:
        new_text = apply_hunks(old_text, fp) if fp.hunks or fp.is_deleted else old_text
    except Exception as e:
        D.all = True
        D.reasons.append(f"apply-fail {path} {e!r}")
        return
    renamed = old_path and new_path and old_path != new_path
    if renamed:
        D.files[old_path] = None
        D.files[new_path] = new_text
    else:
        D.files[path] = new_text

    if renamed or fp.is_new or fp.is_deleted:
        # file appears/disappears: anything importing or globbing it
        _structural(path, old_path, new_path, fp, M, D)
        if fp.is_new and not renamed:
            return

    p = old_path or path
    if p.endswith(".py"):
        _python(p, old_text, new_text, fp, M, D)
    elif p.startswith("tests/fixtures/") or p.startswith("tests/"):
        _fixture(p, fp, M, D)
    elif p.startswith("sqlglot/"):
        # non-python file inside the package (e.g. data file)
        users = _opened_users(p, M)
        D.add(users, f"pkgdata {p}")
        if not users:
            D.all = True
            D.reasons.append(f"unknown pkg file {p}")
    else:
        # docs, setup, CI configs etc. Things that tests could read
        users = _opened_users(p, M)
        if users:
            D.add(users, f"opened {p}")
        elif p in ("setup.py", "setup.cfg", "pyproject.toml") or p.startswith("sqlglotc/"):
            D.add(M.tests_in_file("tests/test_sqlglotc.py") | M.tests_in_file("tests/test_build.py"), f"build {p}")
            if p in ("setup.cfg", "pyproject.toml"):
                # pytest config could live there
                D.import_change = True
                D.reasons.append(f"config {p}")
        else:
            D.reasons.append(f"ignored {p}")


def _opened_users(p, M):
    s = set()
    for q, users in M.opened.items():
        if q == p or (q.endswith("/") and p.startswith(q)):
            s |= users
    if "tests/fixtures/optimizer/tpc-ds" in p:
        s = {u for u in s if not (M.file_of[u] == "tests/test_executor.py" and "test_execute_tpcds" not in M.tests[u])}
    elif "tests/fixtures/optimizer/tpc-h" in p:
        s = {u for u in s if not (M.file_of[u] == "tests/test_executor.py" and "test_execute_tpch" not in M.tests[u] and "test_optimized_tpch" not in M.tests[u])}
    return s


def _structural(path, old_path, new_path, fp, M, D):
    for p in (old_path, new_path):
        if not p:
            continue
        if p.endswith(".py"):
            if p.startswith("tests/") and (p.endswith(".py") and Path(p).name.startswith("test_")):
                D.add(M.tests_in_file(p), f"testfile-struct {p}")
            else:
                D.import_change = True
                D.add(M.file_users(p) if p in M.lines else set(), f"py-struct {p}")
                if p.startswith("sqlglot/") or p.startswith("tests/"):
                    D.all = True
                    D.reasons.append(f"module add/remove {p}")
        else:
            users = _opened_users(p, M)
            parent = str(Path(p).parent) + "/"
            for q, u in M.opened.items():
                if q == parent:
                    users |= u
            D.add(users, f"file-struct {p}")


def _fixture(p, fp, M, D):
    users = _opened_users(p, M)
    if users:
        D.add(users, f"fixture {p}")
    else:
        D.reasons.append(f"fixture-unused {p}")


def _python(p, old_text, new_text, fp, M, D):
    if new_text is None:
        return
    if old_text is None:
        return
    try:
        tree = ast.parse(new_text)
        same, changes = ast_changes(old_text, new_text, tree)
        if not same:
            D.parsed[p] = tree
    except SyntaxError:
        # module cannot be imported any more
        D.import_change = True
        D.reasons.append(f"syntax-error {p}")
        _import_break(p, M, D)
        return
    if same:
        # comments / formatting only. Line numbers may shift; only tests that
        # inspect source positions could notice.
        D.reasons.append(f"ast-same {p}")
        return
    imp_lines = M.importlines.get(p, set())
    is_test = p.startswith("tests/")
    n_lines = old_text.count("\n") + 1
    for ch in changes:
        s, e = ch.lines
        e = min(e, n_lines)     # "to the end of the file" is recorded as a huge line number
        s = min(s, e)
        if ch.kind == "annotation":
            # with "from __future__ import annotations" these are never evaluated
            if "from __future__ import annotations" in old_text:
                D.reasons.append(f"annotation {p}:{s}")
                continue
            ch.kind = "import"
        if is_test:
            _test_change(p, ch, M, D)
            continue
        D.ch_lines.setdefault(p, set()).update(range(s, e + 1))
        cov = M.covering(p, s, e)
        if ch.kind == "runtime":
            D.rt_lines.setdefault(p, set()).update(range(s, e + 1))
            D.add(cov, f"rt {p}:{s}-{e}")
            if any(l in imp_lines for l in range(s, e + 1)):
                D.import_change = True
                _import_change(p, ch, M, D)
        else:
            D.import_change = True
            D.add(cov, f"cov-imp {p}:{s}-{e}", broad=True)
            _import_change(p, ch, M, D)


def _test_change(p, ch, M, D):
    s, e = ch.lines
    if p.endswith(".py") and Path(p).name.startswith("test_"):
        if ch.kind == "runtime" and len(ch.scope) >= 1:
            # a change inside a test method or helper: tests covering those lines
            cov = M.covering(p, s, e)
            D.add(cov, f"testline {p}:{s}")
            # the test method itself (in case coverage missed the line)
            name = ch.scope[-1] if ch.scope[-1] != "<lambda>" else None
            if len(ch.scope) >= 2 and name and name.startswith("test"):
                cls = ch.scope[-2]
                for i in M.tests_in_file(p):
                    parts = M.tests[i].split("::")
                    if parts[-1] == name and (len(parts) < 3 or parts[-2] == cls):
                        D.cands.add(i)
                        D.precise.add(i)
            elif len(ch.scope) == 1 and name and name.startswith("test"):
                for i in M.tests_in_file(p):
                    if M.tests[i].split("::")[-1] == name:
                        D.cands.add(i)
                        D.precise.add(i)
        else:
            D.add(M.tests_in_file(p), f"testfile {p}")
            D.add(M.covering(p, s, e), f"testfile-cov {p}")
    else:
        # helpers.py, __init__.py, etc.
        cov = M.covering(p, s, e)
        D.add(cov, f"helper {p}:{s}")
        if ch.kind != "runtime":
            D.import_change = True
            _import_break(p, M, D)


def _module_importers(p, M):
    """Tests that use module p in any way at runtime (rough)."""
    return M.file_users(p)


def _import_change(p, ch, M, D):
    """A change evaluated at import time: constant, class attribute, default, decorator."""
    users = set(_module_importers(p, M))
    dn = dialect_name(p)
    if dn:
        for d in DIALECT_DIRS:
            users |= M.file_users(f"{d}{dn}.py")
        # tests named after the dialect
        users |= M.tests_in_file(f"tests/dialects/test_{dn}.py")
        D.add(users, f"imp-dialect {dn}", broad=True)
    else:
        D.add(users, f"imp-module {p}", broad=True)
    D.reasons.append("IMPORT")


def _import_break(p, M, D):
    # a module that fails to import: everything that imports it (practically all)
    D.all = True
    D.reasons.append(f"import-break {p}")
