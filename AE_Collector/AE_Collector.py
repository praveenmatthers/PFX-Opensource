from __future__ import annotations

import ctypes
import json
import math
import os
import queue
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

APP_NAME    = "AE_Collector CLI"
APP_VERSION = "0.4.2"

WORKERS           = 10
CHUNK_SIZE        = 8  * 1024 * 1024
LARGE_FILE_THRESH = 64 * 1024 * 1024
SKIP_SEQ_STAT     = True
BAR_WIDTH         = 30
LINE_W            = 78


# ─────────────────────────────────────────────────────────────
# ANSI COLORS  (Windows 10+ VT support enabled via ctypes)
# ─────────────────────────────────────────────────────────────
def _enable_win_ansi() -> None:
    try:
        kernel32 = ctypes.windll.kernel32       # type: ignore
        handle   = kernel32.GetStdHandle(-11)   # STD_OUTPUT_HANDLE
        mode     = ctypes.c_ulong()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
    except Exception:
        pass

_enable_win_ansi()

class C:
    RST    = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    BLACK  = "\033[30m"
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    BLUE   = "\033[94m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"
    GRAY   = "\033[90m"
    BG_BAR = "\033[48;5;236m"   # dark gray bar fill
    BG_ACC = "\033[48;5;25m"    # accent blue fill


def c(text: str, *codes: str) -> str:
    return "".join(codes) + str(text) + C.RST


# ─────────────────────────────────────────────────────────────
# TERMINAL HELPERS
# ─────────────────────────────────────────────────────────────
def sep(char: str = "─", color: str = C.GRAY) -> None:
    print(c(char * LINE_W, color))

def blank() -> None:
    print()

def hdr_line(text: str, color: str = C.CYAN) -> None:
    print(c(f"  {text}", C.BOLD, color))

def kv(key: str, val: str, key_color: str = C.GRAY, val_color: str = C.WHITE) -> None:
    print(f"  {c(f'{key:<28}', key_color)}{c(val, val_color)}")

def tag_ok(text: str)   -> str: return c(f"  ✓  {text}", C.GREEN)
def tag_err(text: str)  -> str: return c(f"  ✗  {text}", C.RED)
def tag_warn(text: str) -> str: return c(f"  ⚠  {text}", C.YELLOW)
def tag_info(text: str) -> str: return c(f"  ·  {text}", C.GRAY)

def banner() -> None:
    line = f"  {c('══', C.BLUE)}  {c('AE COLLECTOR', C.BOLD, C.WHITE)}  {c(APP_VERSION, C.CYAN)}  {c('·', C.GRAY)}  {c('© 2026 Praveen Brijwal', C.DIM, C.GRAY)}  {c('══', C.BLUE)}"
    print()
    print(line)
    print()
def progress_bar(done: int, total: int) -> str:
    pct    = done / total if total else 0
    filled = int(BAR_WIDTH * pct)
    empty  = BAR_WIDTH - filled
    bar    = (c("█" * filled, C.CYAN) +
              c("░" * empty,  C.GRAY))
    pct_s  = c(f"{pct*100:5.1f}%", C.WHITE, C.BOLD)
    return f"[{bar}] {pct_s}"

def fmt_bytes(n: float) -> str:
    for unit in ('B','KB','MB','GB','TB'):
        if n < 1024 or unit == 'TB': return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} B"

def fmt_time(secs: float) -> str:
    s = max(0, int(secs))
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

def ask(prompt: str, choices: tuple = ("y","n"), default: str = "") -> str:
    choices_str = "/".join(c(x.upper(), C.WHITE, C.BOLD) if x == default else x for x in choices)
    while True:
        raw = input(f"\n  {c(prompt, C.YELLOW)}  [{choices_str}]:  ").strip().lower()
        if raw == "" and default: return default
        if raw in choices: return raw
        print(tag_warn(f"Enter one of: {', '.join(choices)}"))


# ─────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────
@dataclass
class ManifestItem:
    item_name:        str
    item_type:        str
    source_path:      str
    exists:           bool
    is_missing:       bool
    is_sequence_like: bool
    extension:        Optional[str]
    is_proxy:         bool          = False
    used_in:          List[str]     = field(default_factory=list)
    comment:          Optional[str] = None


@dataclass
class CopyRecord:
    source:     Path
    dest:       Path
    size_bytes: int
    item_name:  str
    used_in:    List[str]
    kind:       str            = "file"
    group_name: Optional[str] = None

    @property
    def is_sequence_member(self) -> bool:
        return self.kind == "sequence_member"


# ─────────────────────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────────────────────
class CopyStats:
    def __init__(self) -> None:
        self.total_bytes:  int   = 0
        self.copied_bytes: int   = 0
        self.total_files:  int   = 0
        self.done_files:   int   = 0
        self.failed_files: int   = 0
        self.start_time:   Optional[float] = None
        self.end_time:     Optional[float] = None
        self._lock = threading.Lock()

    def add_bytes(self, n: int) -> None:
        with self._lock: self.copied_bytes += n

    def finish_file(self, ok: bool) -> None:
        with self._lock:
            self.done_files += 1
            if not ok: self.failed_files += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def norm_path(p: str) -> str:
    return os.path.normcase(os.path.normpath(p))

def safe_name(name: str) -> str:
    cleaned = ''.join('_' if ch in r'<>:"/\|?*' else ch for ch in str(name)).strip()
    return cleaned or 'unnamed'

def split_name(filename: str) -> Tuple[str, str]:
    p = Path(filename); return p.stem, p.suffix

def sequence_group_base(path: Path) -> str:
    m = re.match(r'^(.*?)(\d+)$', path.stem)
    if m:
        base = m.group(1).rstrip('._- ')
        if base: return safe_name(base)
    return safe_name(path.stem)

def detect_sequence_members(path: Path) -> List[Path]:
    stem = path.stem; i = len(stem) - 1
    while i >= 0 and stem[i].isdigit(): i -= 1
    prefix = stem[:i+1]
    if not (len(stem)-i-1): return [path]
    ext = path.suffix.lower(); candidates: List[Path] = []
    try:
        for child in path.parent.iterdir():
            if not child.is_file(): continue
            if child.suffix.lower() != ext: continue
            cstem = child.stem
            if not cstem.startswith(prefix): continue
            if cstem[len(prefix):].isdigit(): candidates.append(child)
    except OSError: return [path]
    candidates.sort(); return candidates if candidates else [path]


# ─────────────────────────────────────────────────────────────
# NAME ALLOCATOR
# ─────────────────────────────────────────────────────────────
class NameAllocator:
    def __init__(self) -> None:
        self._files: Dict[str, str] = {}
        self._dirs:  Dict[str, str] = {}

    def file_name(self, desired: str) -> str:
        desired = safe_name(desired); base, ext = split_name(desired); cand, n = desired, 2
        while norm_path(cand) in self._files: cand = f"{base}__dup{n}{ext}"; n += 1
        self._files[norm_path(cand)] = cand; return cand

    def dir_name(self, desired: str) -> str:
        desired = safe_name(desired); cand, n = desired, 2
        while norm_path(cand) in self._dirs: cand = f"{desired}__seq{n}"; n += 1
        self._dirs[norm_path(cand)] = cand; return cand


# ─────────────────────────────────────────────────────────────
# PLAN
# ─────────────────────────────────────────────────────────────
class CollectorPlan:
    def __init__(self, manifest: dict) -> None:
        self.manifest     = manifest
        proj              = manifest["project"]
        self.project_path = Path(proj["project_path"])
        self.project_dir  = Path(proj["project_dir"])
        self.project_name = proj["name"]
        self.coll_root    = self.project_dir / "AE_Collection"
        self.assets_root  = self.coll_root   / "assets"
        self.proj_dir_out = self.coll_root   / "project"
        self.logs_dir     = self.coll_root   / "logs"
        self.records:          List[CopyRecord]              = []
        self.missing:          List[ManifestItem]            = []
        self.duplicates:       Dict[str, List[ManifestItem]] = {}
        self.renamed_files:    Dict[str, str]                = {}
        self.renamed_dirs:     Dict[str, str]                = {}
        self.source_to_dest:   Dict[str, str]                = {}
        self.unique_dest_dirs: Set[Path]                     = set()

    @staticmethod
    def from_json(path: Path) -> "CollectorPlan":
        return CollectorPlan(json.loads(path.read_text(encoding="utf-8")))

    def build(self) -> None:
        self.records.clear(); self.missing.clear(); self.duplicates.clear()
        self.renamed_files.clear(); self.renamed_dirs.clear()
        self.source_to_dest.clear(); self.unique_dest_dirs.clear()
        raw = []
        for it in self.manifest.get("items", []):
            valid = {k: v for k, v in it.items() if k in ManifestItem.__dataclass_fields__}
            raw.append(ManifestItem(**valid))
        by_norm: Dict[str, List[ManifestItem]] = {}
        for item in raw:
            if item.source_path: by_norm.setdefault(norm_path(item.source_path), []).append(item)
        alloc = NameAllocator()
        for nk, items in by_norm.items():
            if len(items) > 1: self.duplicates[nk] = items
            item = items[0]; src = Path(item.source_path)
            if item.is_missing or not src.exists() or not src.is_file():
                self.missing.append(item); continue
            if item.is_sequence_like:
                members = detect_sequence_members(src)
                grp_base = sequence_group_base(src); grp_name = alloc.dir_name(grp_base)
                if grp_name != grp_base: self.renamed_dirs[grp_base] = grp_name
                for m in members:
                    if not m.exists() or not m.is_file(): continue
                    dest = self.assets_root / grp_name / safe_name(m.name)
                    self.unique_dest_dirs.add(dest.parent)
                    self.records.append(CopyRecord(source=m, dest=dest,
                        size_bytes=m.stat().st_size, item_name=item.item_name,
                        used_in=item.used_in, kind="sequence_member", group_name=grp_name))
                    self.source_to_dest[str(m)] = str(dest)
            else:
                desired = safe_name(src.name); final_name = alloc.file_name(desired)
                if final_name != desired: self.renamed_files[str(src)] = final_name
                dest = self.assets_root / final_name
                self.unique_dest_dirs.add(dest.parent)
                self.records.append(CopyRecord(source=src, dest=dest,
                    size_bytes=src.stat().st_size, item_name=item.item_name,
                    used_in=item.used_in, kind="file_proxy" if item.is_proxy else "file"))
                self.source_to_dest[str(src)] = str(dest)
        self.records.sort(key=lambda r: r.size_bytes, reverse=True)

    def precreate_dirs(self) -> None:
        for d in self.unique_dest_dirs: d.mkdir(parents=True, exist_ok=True)
        self.assets_root.mkdir(parents=True, exist_ok=True)

    def export_audit_files(self, manifest_path: Path) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        total = sum(r.size_bytes for r in self.records)
        large = sum(1 for r in self.records if r.size_bytes >= LARGE_FILE_THRESH)
        seqs  = sorted({r.group_name for r in self.records if r.group_name})
        summary = {
            "project": self.project_name, "total_files": len(self.records),
            "total_size": fmt_bytes(total), "total_bytes": total,
            "large_files_os_copy": large, "small_files_chunked": len(self.records)-large,
            "sequence_groups": len(seqs), "missing": len(self.missing),
            "duplicates": len(self.duplicates),
            "renamed_files": len(self.renamed_files),
            "renamed_dirs":  len(self.renamed_dirs),
            "destination": str(self.coll_root),
        }
        audit = {"summary": summary,
                 "missing": [i.__dict__ for i in self.missing],
                 "duplicates": {k: [i.__dict__ for i in v] for k, v in self.duplicates.items()},
                 "renamed_files": self.renamed_files,
                 "renamed_sequence_dirs": self.renamed_dirs,
                 "source_to_dest": self.source_to_dest}
        (self.logs_dir/"manifest_copy.json").write_text(json.dumps(self.manifest, indent=2), encoding="utf-8")
        (self.logs_dir/"manifest_audit.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
        rows = ["source_path,dest_path,size_bytes,item_name,kind,group_name,used_in"]
        for r in self.records:
            used = "|".join(r.used_in).replace('"',"''")
            row = [str(r.source),str(r.dest),str(r.size_bytes),r.item_name,r.kind,r.group_name or "",used]
            rows.append(",".join('"'+str(v).replace('"','""')+'"' for v in row))
        (self.logs_dir/"preview.csv").write_text("\n".join(rows), encoding="utf-8")
        if manifest_path.exists(): shutil.copy2(manifest_path, self.logs_dir/manifest_path.name)


# ─────────────────────────────────────────────────────────────
# PREWARM
# ─────────────────────────────────────────────────────────────
def prewarm_cache(records: List[CopyRecord], cancel: threading.Event) -> None:
    total = sum(r.size_bytes for r in records)
    print(tag_warn(f"Pre-warming {len(records)} files ({fmt_bytes(total)}) into RAM cache…"))
    read = 0; t0 = time.time()
    for i, rec in enumerate(records, 1):
        if cancel.is_set(): break
        try:
            with rec.source.open("rb") as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk: break
                    read += len(chunk)
        except OSError: pass
        pct = read/total*100 if total else 0
        print(f"\r  {c('Warming', C.YELLOW)}  {progress_bar(read,total)}"
              f"  {c(fmt_bytes(read), C.CYAN)} / {fmt_bytes(total)}"
              f"  {c(f'{i}/{len(records)} files', C.GRAY)}"
              + " " * 4, end="", flush=True)
    elapsed = time.time() - t0
    print()
    print(tag_ok(f"Cache warm done  {fmt_bytes(read)} in {fmt_time(elapsed)}"
                 f"  avg {fmt_bytes(read/max(elapsed,0.001))}/s"))


# ─────────────────────────────────────────────────────────────
# COPY FUNCTION
# ─────────────────────────────────────────────────────────────
def copy_one(record: CopyRecord, stats: CopyStats,
             q: queue.Queue, cancel: threading.Event) -> bool:
    if cancel.is_set():
        q.put(("cancelled", str(record.source))); return False
    copied = 0
    try:
        if record.size_bytes >= LARGE_FILE_THRESH:
            shutil.copy2(record.source, record.dest)
            copied = record.size_bytes; stats.add_bytes(copied)
        else:
            with record.source.open("rb") as sf, record.dest.open("wb") as df:
                while True:
                    if cancel.is_set(): raise RuntimeError("Cancelled")
                    chunk = sf.read(CHUNK_SIZE)
                    if not chunk: break
                    df.write(chunk); copied += len(chunk); stats.add_bytes(len(chunk))
            if not (SKIP_SEQ_STAT and record.is_sequence_member):
                shutil.copystat(record.source, record.dest)
        stats.finish_file(True)
        q.put(("done", str(record.source), copied, str(record.dest)))
        return True
    except Exception as exc:
        stats.finish_file(False)
        try:
            if record.dest.exists(): record.dest.unlink()
        except OSError: pass
        if cancel.is_set(): q.put(("cancelled", str(record.source)))
        else:              q.put(("error", str(record.source), str(exc)))
        return False


# ─────────────────────────────────────────────────────────────
# LIVE PROGRESS PRINTER THREAD
# ─────────────────────────────────────────────────────────────
def progress_printer(stats: CopyStats, done_evt: threading.Event) -> None:
    while not done_evt.is_set():
        snap = stats.snapshot()
        cb,tb = snap["copied_bytes"], snap["total_bytes"]
        df,tf = snap["done_files"],   snap["total_files"]
        ff    = snap["failed_files"]; st = snap["start_time"] or time.time()
        elapsed = max(time.time()-st, 0.001)
        speed   = cb/elapsed; remain = max(tb-cb,0)
        eta     = remain/speed if speed>0 else math.inf
        eta_s   = fmt_time(eta) if math.isfinite(eta) else "--:--:--"

        bar    = progress_bar(cb, tb)
        failed = c(f"  {ff} failed", C.RED) if ff else ""
        line   = (f"\r  {bar}"
                  f"  {c(f'{df}/{tf}', C.WHITE)} files"
                  f"  {c(fmt_bytes(speed)+'/s', C.GREEN)}"
                  f"  elapsed {c(fmt_time(elapsed), C.CYAN)}"
                  f"  ETA {c(eta_s, C.YELLOW)}"
                  f"{failed}")
        # Strip ANSI for length calc
        import re as _re
        raw_len = len(_re.sub(r'\033\[[0-9;]*m','', line))
        print(line + " " * max(0, LINE_W - raw_len), end="", flush=True)
        time.sleep(0.25)
    print()


# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# MANIFEST LOADER
# ─────────────────────────────────────────────────────────────
MANIFEST_PATH = Path.home() / "Documents" / "AE_Collector" / "manifest.json"

def pick_manifest() -> Path:
    blank()
    if MANIFEST_PATH.exists():
        age = time.strftime("%Y-%m-%d  %H:%M", time.localtime(MANIFEST_PATH.stat().st_mtime))
        sep()
        hdr_line("Manifest", C.CYAN)
        sep()
        blank()
        print(f"  {c(str(MANIFEST_PATH), C.WHITE)}")
        print(f"  {c('Last saved: ' + age, C.GRAY)}")
        blank()
        return MANIFEST_PATH
    print(tag_warn("No manifest found:"))
    print(f"  {c(str(MANIFEST_PATH), C.GRAY)}")
    print(tag_info("Run AE_Collector_ExportManifest.jsx inside After Effects first."))
    blank()
    sys.exit(1)
# ─────────────────────────────────────────────────────────────
# PRINT SUMMARY
# ─────────────────────────────────────────────────────────────
def print_plan(plan: CollectorPlan, prewarm: bool) -> None:
    total = sum(r.size_bytes for r in plan.records)
    large = sum(1 for r in plan.records if r.size_bytes >= LARGE_FILE_THRESH)
    seqs  = len({r.group_name for r in plan.records if r.group_name})
    blank(); sep()
    hdr_line("Plan Summary", C.CYAN); sep(); blank()
    kv("Project",           plan.project_name)
    kv("Total files",       str(len(plan.records)))
    kv("Total size",        fmt_bytes(total),              val_color=C.GREEN)
    kv("Large → OS copy",  f"{large} files (≥ {LARGE_FILE_THRESH//(1024*1024)} MB)")
    kv("Small → chunked",  f"{len(plan.records)-large} files")
    kv("Sequence groups",   str(seqs))
    kv("Missing",           str(len(plan.missing)),        val_color=C.RED   if plan.missing  else C.GRAY)
    kv("Duplicates",        str(len(plan.duplicates)),     val_color=C.YELLOW if plan.duplicates else C.GRAY)
    kv("Renamed files",     str(len(plan.renamed_files)),  val_color=C.YELLOW if plan.renamed_files else C.GRAY)
    kv("Renamed seq dirs",  str(len(plan.renamed_dirs)),   val_color=C.YELLOW if plan.renamed_dirs else C.GRAY)
    kv("Destination",       str(plan.coll_root),           val_color=C.CYAN)
    blank()
    kv("Threads",           str(WORKERS))
    kv("Chunk size",        fmt_bytes(CHUNK_SIZE))
    kv("OS-copy threshold", f"≥ {fmt_bytes(LARGE_FILE_THRESH)}")
    kv("Skip seq copystat", str(SKIP_SEQ_STAT))
    kv("Pre-warm RAM cache",str(prewarm), val_color=C.GREEN if prewarm else C.GRAY)
    blank()

    if plan.missing:
        hdr_line(f"Missing media ({len(plan.missing)})", C.RED)
        for m in plan.missing[:8]: print(tag_err(m.source_path))
        if len(plan.missing) > 8: print(c(f"  … and {len(plan.missing)-8} more", C.GRAY))
        blank()

    if plan.renamed_files:
        hdr_line(f"Renamed files — collisions ({len(plan.renamed_files)})", C.YELLOW)
        for src, nm in list(plan.renamed_files.items())[:5]:
            print(f"  {c(Path(src).name, C.GRAY)}  {c('→', C.YELLOW)}  {c(nm, C.WHITE)}")
        if len(plan.renamed_files) > 5: print(c(f"  … and {len(plan.renamed_files)-5} more", C.GRAY))
        blank()

    sep()


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main() -> None:
    banner()

    manifest_path = (
        Path(sys.argv[1]) if len(sys.argv) > 1 and Path(sys.argv[1]).exists()
        else pick_manifest()
    )

    print(tag_info(f"Loading  {manifest_path}"))
    plan = CollectorPlan.from_json(manifest_path)
    plan.build()

    if not plan.records:
        print(tag_warn("No files found in manifest.")); return

    # ── Prompt
    prewarm_ans = ask("Pre-warm RAM cache before copying?", ("y","n"), default="n")
    prewarm     = prewarm_ans == "y"
    overwrite   = ask("Overwrite existing files?",          ("y","n"), default="n") == "y"

    print_plan(plan, prewarm)

    start = ask("Start collection?", ("y","n"), default="y")
    if start != "y":
        blank(); print(c("  Aborted.", C.YELLOW)); blank(); return

    # ── Setup
    blank()
    for d in (plan.coll_root, plan.assets_root, plan.proj_dir_out, plan.logs_dir):
        d.mkdir(parents=True, exist_ok=True)
    plan.export_audit_files(manifest_path)
    plan.precreate_dirs()
    print(tag_ok(f"Pre-created {len(plan.unique_dest_dirs)} destination folders"))

    proj_dest = plan.proj_dir_out / (plan.project_path.stem + "_collected" + plan.project_path.suffix)
    shutil.copy2(plan.project_path, proj_dest)
    print(tag_ok(f"Project  →  {proj_dest}"))

    to_copy = (plan.records if overwrite else [r for r in plan.records if not r.dest.exists()])
    skipped = len(plan.records) - len(to_copy)
    if skipped: print(tag_warn(f"Skipped {skipped} already-existing files"))

    # ── Pre-warm
    cancel = threading.Event()
    if prewarm:
        blank()
        try:
            prewarm_cache(to_copy, cancel)
        except KeyboardInterrupt:
            blank(); print(tag_warn("Pre-warm interrupted — starting copy anyway…"))
            cancel.clear()

    # ── Copy
    total_copy = sum(r.size_bytes for r in to_copy)
    blank(); sep()
    hdr_line(f"Copying  {len(to_copy)} files  ({fmt_bytes(total_copy)})", C.GREEN)
    sep(); blank()

    stats       = CopyStats()
    stats.total_files = len(to_copy)
    stats.total_bytes = total_copy
    stats.start_time  = time.time()
    event_q     = queue.Queue()
    done_evt    = threading.Event()
    log_lines:  List[str] = []
    errors:     List[str] = []

    printer = threading.Thread(target=progress_printer, args=(stats, done_evt), daemon=True)
    printer.start()

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(copy_one, rec, stats, event_q, cancel): rec for rec in to_copy}
            for fut in as_completed(futures):
                if cancel.is_set():
                    pool.shutdown(wait=False, cancel_futures=True); break
                try: fut.result()
                except Exception as exc:
                    event_q.put(("error", str(futures[fut].source), str(exc)))
    except KeyboardInterrupt:
        cancel.set(); blank(); blank(); print(tag_warn("KeyboardInterrupt — cancelling…"))

    done_evt.set(); printer.join(timeout=1.5)

    # Drain queue
    while not event_q.empty():
        try:
            ev = event_q.get_nowait()
            if ev[0] == "error":
                errors.append(f"{ev[1]}  |  {ev[2]}")
                log_lines.append(f"[ERROR] {ev[1]} | {ev[2]}\n")
            elif ev[0] == "done":
                log_lines.append(f"[DONE]  {ev[1]}  ({fmt_bytes(ev[2])})\n")
            elif ev[0] == "cancelled":
                log_lines.append(f"[CANC]  {ev[1]}\n")
        except queue.Empty: break

    # ── Final report
    elapsed = max(time.time() - stats.start_time, 0.001)
    avg_spd = stats.copied_bytes / elapsed
    status  = "CANCELLED" if cancel.is_set() else "DONE"
    blank(); sep("═", C.BLUE)
    col = C.GREEN if status == "DONE" else C.YELLOW
    hdr_line(f"Collection {status}", col)
    sep("═", C.BLUE); blank()
    kv("Files done",     f"{stats.done_files} / {stats.total_files}", val_color=C.GREEN)
    kv("Failed",         str(stats.failed_files), val_color=C.RED if stats.failed_files else C.GRAY)
    kv("Copied",         fmt_bytes(stats.copied_bytes), val_color=C.CYAN)
    kv("Elapsed",        fmt_time(elapsed))
    kv("Avg speed",      fmt_bytes(avg_spd)+"/s", val_color=C.GREEN)
    blank()

    if errors:
        hdr_line(f"Errors ({len(errors)})", C.RED)
        for e in errors[:10]: print(tag_err(e))
        if len(errors) > 10: print(c(f"  … and {len(errors)-10} more", C.GRAY))
        blank()

    # Write logs
    (plan.logs_dir/"relink_map.json").write_text(json.dumps(plan.source_to_dest, indent=2), encoding="utf-8")
    (plan.logs_dir/"collect_log.txt").write_text("".join(log_lines), encoding="utf-8")
    kv("Logs",   str(plan.logs_dir),   val_color=C.CYAN)
    kv("Assets", str(plan.assets_root),val_color=C.CYAN)
    blank(); sep("═", C.BLUE); blank()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        blank(); print(c("\n  Interrupted.\n", C.YELLOW)); sys.exit(1)
    finally:
        input(c("\n  Press Enter to exit…", C.GRAY))