#!/usr/bin/env python3
"""
AE_RenderSlave.py  —  AEREN Render Farm Slave  v5.0.0
=======================================================
File-based render node.  Zero ports.  Zero firewall.  Zero IT dept.
All communication via JSON files on a shared network folder.

CONFIGURE:  Set FARM_ROOT below.
RUN:        python AE_RenderSlave.py
            python AE_RenderSlave.py --name "MY-NODE-01"
"""

# ═══════════════════════════════════════════════════════════════════════
#  ▶  ONLY SETTING YOU NEED TO CHANGE
FARM_ROOT = r"\\DESKTOP-3BK9PQH\Projects\AE_RenderManager\AEREN_DATA_LOGS"
# ═══════════════════════════════════════════════════════════════════════

SLAVE_VERSION  = "5.0.0"
POLL_SEC       = 3       # seconds between queue scans (when idle)
HB_SEC         = 5       # heartbeat write interval
SLAVE_TIMEOUT  = 45      # seconds without heartbeat → slave declared offline
NET_RETRY_SEC  = 10      # pause when network share is unreachable

import os, sys, json, socket, time, shutil, threading, subprocess
import re, traceback, platform, argparse, signal
from datetime import datetime
from pathlib  import Path

# ── ANSI Colors ───────────────────────────────────────────────────────────────
os.system("")  # enable ANSI on Windows CMD
R="\033[91m"; G="\033[92m"; Y="\033[93m"; C="\033[96m"
W="\033[97m";  DG="\033[90m"; RESET="\033[0m"
TS = lambda: datetime.now().strftime("%H:%M:%S")

def clog(msg, tag="INFO"):
    col = {"OK":G,"INFO":C,"RENDER":Y,"WARN":Y,"ERROR":R,"STATUS":W}.get(tag, W)
    print(f"{DG}[{TS()}]{RESET} {col}[{tag}]{RESET} {msg}")
    sys.stdout.flush()

# ── Path helpers ──────────────────────────────────────────────────────────────
def mk_paths(root):
    r = Path(root)
    d = {k: r/k for k in ("jobs","queue","done","failed","slaves","history")}
    d["root"] = r
    return d

def jread(path, default=None):
    try:
        with open(str(path), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def jwrite(path, data):
    """Atomic JSON write via temp-file rename."""
    tmp = str(path) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        try:
            os.replace(tmp, str(path))
        except Exception:
            os.rename(tmp, str(path))
        return True
    except Exception as e:
        try: os.remove(tmp)
        except: pass
        clog(f"jwrite failed: {e}", "WARN")
        return False

def mkdir_p(path):
    try: Path(path).mkdir(parents=True, exist_ok=True)
    except Exception as e: clog(f"mkdir_p failed: {e}", "WARN")

def safe_mv(src, dst):
    try: shutil.move(str(src), str(dst))
    except Exception as e: clog(f"Move failed {src}→{dst}: {e}", "WARN")

# ── System detection ──────────────────────────────────────────────────────────
def find_aerender():
    for year in range(2030, 2018, -1):
        for suffix in ["", " (Beta)", " 2"]:
            p = Path(rf"C:\Program Files\Adobe\Adobe After Effects {year}{suffix}\Support Files\aerender.exe")
            if p.exists():
                return str(p), str(year)
    return None, None

def detect_plugins():
    for year in range(2030, 2018, -1):
        base = Path(rf"C:\Program Files\Adobe\Adobe After Effects {year}\Support Files\Plug-ins")
        if base.exists():
            return [f.stem for f in base.rglob("*.aex")]
    return []

def get_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]; s.close(); return ip
    except: return "127.0.0.1"

def sys_stats():
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=None)
        vm  = psutil.virtual_memory()
        return cpu, round(vm.used/1e9, 1), round(vm.total/1e9, 1)
    except: return 0.0, 0.0, 0.0

def check_network(paths):
    """Return True if the farm root is accessible."""
    return paths["slaves"].parent.exists()

# ── RenderSlave ───────────────────────────────────────────────────────────────
class RenderSlave:
    def __init__(self, hostname, farm_root):
        self.hostname  = hostname
        self.ip        = get_ip()
        self.paths     = mk_paths(farm_root)
        self.aerender, self.ae_ver = find_aerender()
        self.plugins   = detect_plugins()
        self.os_str    = f"{platform.system()} {platform.release()}"

        self._lock           = threading.Lock()
        self.status          = "IDLE"
        self.cur_job         = None
        self.cur_chunk       = None
        self.cur_frame       = 0
        self.pct             = 0
        self.frames_done     = []
        self._rthread        = None
        self._stop           = threading.Event()   # stop current render
        self._gstop          = threading.Event()   # global shutdown
        self._last_frame_ts  = 0.0
        self._proc           = None

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    def write_hb(self, status=None):
        if status:
            with self._lock: self.status = status
        cpu, ram_used, ram_tot = sys_stats()
        with self._lock:
            data = {
                "hostname": self.hostname,   "ip": self.ip,
                "status": self.status,       "current_job": self.cur_job,
                "current_chunk": self.cur_chunk, "current_frame": self.cur_frame,
                "progress_pct": self.pct,    "frames_done": list(self.frames_done),
                "cpu_pct": cpu,              "ram_used_gb": ram_used,
                "ram_total_gb": ram_tot,     "ae_version": self.ae_ver or "unknown",
                "aerender_path": self.aerender or "NOT FOUND",
                "plugins": self.plugins,     "os": self.os_str,
                "last_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_seen_epoch": time.time(),
                "slave_version": SLAVE_VERSION,
            }
        jwrite(self.paths["slaves"] / f"{self.hostname}.json", data)

    # ── Stop signal from manager ──────────────────────────────────────────────
    def check_stop_signal(self):
        sf = self.paths["slaves"] / f"{self.hostname}_STOP.json"
        if sf.exists():
            data = jread(sf, {})
            clog(f"STOP signal received  chunk={data.get('chunk')}", "WARN")
            self._stop.set()
            with self._lock:
                p = getattr(self, "_proc", None)
                if p:
                    clog("Asynchronously killing aerender process...", "WARN")
                    try: p.terminate()
                    except: pass
                    try: p.kill()
                    except: pass
            try: sf.unlink()
            except: pass

    # ── Claim a chunk (atomic) ────────────────────────────────────────────────
    def claim_chunk(self):
        q = self.paths["queue"]
        if not q.exists(): return None
        try:
            files = [f for f in q.iterdir()
                     if f.suffix == ".json" and ".CLAIMED_" not in f.name]
            # Sort by priority (highest first) then by creation time
            files.sort(key=lambda f: (-(jread(f, {}).get("priority", 5)), f.name))
        except Exception: return None

        for cf in files:
            d = jread(cf)
            if not d: continue
            eligible = d.get("eligible_slaves")
            if eligible and self.hostname not in eligible: continue

            claimed = cf.parent / (cf.stem + f".CLAIMED_{self.hostname}.json")
            try:
                os.rename(str(cf), str(claimed))       # atomic on Windows
                d.update({
                    "status":     "RENDERING",
                    "claimed_by": self.hostname,
                    "claimed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                jwrite(claimed, d)
                clog(f"Claimed  chunk={d.get('chunk_id')}  job={d.get('job_id')}", "OK")
                return claimed, d
            except (FileExistsError, OSError, PermissionError):
                continue   # another slave got it first
        return None

    # ── Verify output files exist ─────────────────────────────────────────────
    def _verify_out(self, output, sf, ef, out_type):
        if not output or out_type == "VIDEO":
            p = Path(output) if output else None
            if p and not (p.exists() and p.stat().st_size > 0):
                return ["video_output"]
            return []
        missing = []
        bracket = re.search(r'\[#+\]', output)
        if bracket:
            pad = bracket.group().count("#")
            for f in range(sf, ef + 1):
                fp = re.sub(r'\[#+\]', str(f).zfill(pad), output)
                try:
                    if not (os.path.exists(fp) and os.path.getsize(fp) > 0):
                        missing.append(f)
                except: missing.append(f)
        return missing

    # ── Render worker (runs in its own thread) ────────────────────────────────
    def _render_worker(self, cpath, cd):
        job_id   = cd["job_id"];    chunk_id = cd.get("chunk_id", "chunk_unknown")
        sf       = cd["start_frame"]; ef = cd["end_frame"]
        project  = cd.get("project_path","")
        output   = cd.get("output_path","")
        comp     = cd.get("comp_name","")
        rqi      = int(cd.get("rq_index", 1))
        out_type = cd.get("output_type","SEQUENCE")
        total    = max(ef - sf + 1, 1)

        with self._lock:
            self.cur_job=job_id; self.cur_chunk=chunk_id
            self.cur_frame=sf;   self.pct=0;  self.frames_done=[]
            self._last_frame_ts = time.time()

        clog(f"START  job={job_id}  chunk={chunk_id}  frames={sf}–{ef}", "RENDER")

        success = False; err = None; log_lines = []; rendered = []

        try:
            if not self.aerender:
                raise RuntimeError("aerender.exe not found on this machine")
            if not os.path.exists(project):
                unc = project.startswith("\\\\") or project.startswith("//")
                hint = " (Is the network share mounted?)" if unc else ""
                raise RuntimeError(f"Project not found: {project}{hint}")

            # Create output dir
            if output:
                od = re.sub(r'\[#+\]', '0000', output)
                mkdir_p(Path(od).parent)

            cmd = [self.aerender,
                   "-project", project, "-comp", comp,
                   "-s", str(sf),       "-e",    str(ef),
                   "-rqindex", str(rqi)]
            if output: cmd += ["-output", output]

            clog("CMD: " + " ".join(cmd), "STATUS")
            self._stop.clear()
            flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    text=True, bufsize=1, creationflags=flags)
            with self._lock: self._proc = proc

            for raw in iter(proc.stdout.readline, ""):
                # honour stop requests
                if self._stop.is_set() or self._gstop.is_set():
                    err = "Stopped by signal"
                    break

                line = raw.rstrip()
                if not line: continue
                log_lines.append(line)
                clog(line, "ERROR" if "error" in line.lower() else "RENDER")

                # Parse aerender progress (handles AE 2019–2025 output formats)
                fn = None
                m = re.search(r'(\d+)\s+of\s+\d+', line, re.IGNORECASE)
                if m: fn = int(m.group(1))
                if fn is None:
                    m2 = re.search(r'PROGRESS[:\s]+(\d+)', line, re.IGNORECASE)
                    if m2: fn = int(m2.group(1))
                if fn is None:
                    m3 = re.search(r'Frame\s+(\d+)', line, re.IGNORECASE)
                    if m3:
                        abs_fn = int(m3.group(1))
                        fn = abs_fn - sf + 1 if abs_fn >= sf else None

                if fn is not None and fn > 0:
                    actual = sf + fn - 1
                    pct    = min(int(fn / total * 100), 100)
                    with self._lock:
                        self.cur_frame = actual; self.pct = pct
                        self._last_frame_ts = time.time()
                        if actual not in rendered:
                            rendered.append(actual)
                            self.frames_done = rendered[:]
                    cd.update({"current_frame": actual, "progress_pct": pct,
                               "frames_done": rendered[:]})
                    jwrite(cpath, cd)

            proc.wait()

            if proc.returncode == 0 and not (self._stop.is_set() or self._gstop.is_set()):
                missing = self._verify_out(output, sf, ef, out_type)
                if missing:
                    err = f"Output files missing: {missing[:10]}"
                    cd["frames_failed"] = missing
                else:
                    success = True
            elif not err:
                err = f"aerender exit code {proc.returncode}"

        except Exception:
            err = traceback.format_exc()
            clog(f"Exception:\n{err}", "ERROR")

        finally:
            # Always reset state
            with self._lock:
                self.status=("IDLE"); self.cur_job=None; self.cur_chunk=None
                self.cur_frame=0;     self.pct=0;        self.frames_done=[]
                self._proc = None

        # ── Finalize chunk file ────────────────────────────────────────────────
        cd["finished_at"]  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cd["aerender_log"] = "\n".join(log_lines[-100:])

        claimed_suffix = f".CLAIMED_{self.hostname}"

        if success:
            cd.update({"status":"DONE","error":None,
                       "frames_done": list(range(sf, ef+1))})
            clog(f"DONE   job={job_id}  chunk={chunk_id}", "OK")
            dst = self.paths["done"] / cpath.name.replace(claimed_suffix, "")
            jwrite(cpath, cd); safe_mv(cpath, dst)
        else:
            rc = cd.get("retry_count", 0) + 1
            mr = cd.get("max_retries", 3)
            cd.update({"retry_count": rc, "error": err,
                       "claimed_by": None, "claimed_at": None})
            if rc >= mr:
                cd["status"] = "FAILED"
                clog(f"FAIL   job={job_id}  chunk={chunk_id}  (max retries {rc})", "ERROR")
                dst = self.paths["failed"] / cpath.name.replace(claimed_suffix, "")
                jwrite(cpath, cd); safe_mv(cpath, dst)
            else:
                cd["status"] = "WAITING"
                clog(f"RETRY  job={job_id}  chunk={chunk_id}  ({rc}/{mr})", "WARN")
                unclaimed = self.paths["queue"] / cpath.name.replace(claimed_suffix, "")
                jwrite(cpath, cd)
                try: os.rename(str(cpath), str(unclaimed))
                except: safe_mv(cpath, unclaimed)

    # ── Recover orphaned chunks from a previous crash ─────────────────────────
    def recover_orphans(self):
        q = self.paths["queue"]
        if not q.exists(): return
        marker = f".CLAIMED_{self.hostname}"
        try:
            for f in list(q.iterdir()):
                if f.is_file() and marker in f.name:
                    clog(f"Recovering orphaned chunk: {f.name}", "WARN")
                    d = jread(f, {})
                    d.update({"claimed_by": None, "claimed_at": None,
                               "status": "WAITING",
                               "retry_count": d.get("retry_count", 0) + 1})
                    unclaimed = q / f.name.replace(marker, "")
                    jwrite(f, d)
                    try: os.rename(str(f), str(unclaimed))
                    except: safe_mv(f, unclaimed)
        except Exception as e:
            clog(f"Recovery scan error: {e}", "WARN")

    # ── Main loop ─────────────────────────────────────────────────────────────
    def run(self):
        print()
        print(f"  \033[97m╔══════════════════════════════════════════════════════╗\033[0m")
        print(f"  \033[97m║       AEREN  Render Slave  v{SLAVE_VERSION}                   ║\033[0m")
        print(f"  \033[97m║       2026 - Copyright Reserved - Praveen Brijwal    ║\033[0m")
        print(f"  \033[97m╚══════════════════════════════════════════════════════╝\033[0m")
        clog(f"Hostname  : {self.hostname}", "INFO")
        clog(f"IP        : {self.ip}", "INFO")
        clog(f"OS        : {self.os_str}", "INFO")
        clog(f"aerender  : {self.aerender or 'NOT FOUND — renders will fail'}", "INFO" if self.aerender else "WARN")
        clog(f"AE Ver    : {self.ae_ver or 'unknown'}", "INFO")
        clog(f"Plugins   : {len(self.plugins)} found", "INFO")
        clog(f"Farm root : {self.paths['root']}", "INFO")
        print()

        # Ensure dirs
        for d in self.paths.values(): mkdir_p(d)

        # Recover any chunks left from a previous crash
        self.recover_orphans()

        net_ok    = True
        last_hb   = 0.0
        last_poll = 0.0

        while not self._gstop.is_set():
            now = time.time()

            # Network check
            try:
                reachable = check_network(self.paths)
            except Exception:
                reachable = False

            if not reachable:
                if net_ok:
                    clog("Network share unreachable — render paused. Waiting...", "WARN")
                    net_ok = False
                time.sleep(NET_RETRY_SEC)
                continue
            else:
                if not net_ok:
                    clog("Network share restored. Resuming.", "OK")
                    net_ok = True

            # Heartbeat
            if now - last_hb >= HB_SEC:
                try: self.write_hb()
                except Exception as e: clog(f"HB write failed: {e}", "WARN")
                last_hb = now

            # Stop signal check
            try: self.check_stop_signal()
            except: pass

            # Claim work if idle
            if now - last_poll >= POLL_SEC:
                last_poll = now
                with self._lock:
                    idle = (self.status == "IDLE")
                if idle and (self._rthread is None or not self._rthread.is_alive()):
                    try:
                        result = self.claim_chunk()
                        if result:
                            cpath, cd = result
                            with self._lock: self.status = "RENDERING"
                            self._rthread = threading.Thread(
                                target=self._render_worker,
                                args=(cpath, cd), daemon=True)
                            self._rthread.start()
                    except Exception as e:
                        clog(f"Claim error: {e}", "WARN")

            time.sleep(1)

        # Shutdown
        clog("Shutting down...", "WARN")
        try: self.write_hb("OFFLINE")
        except: pass


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="AEREN Render Farm Slave v5")
    ap.add_argument("--name", default=socket.gethostname(), help="Node display name")
    ap.add_argument("--farm", default=FARM_ROOT,            help="Farm root UNC path")
    args = ap.parse_args()

    slave = RenderSlave(args.name, args.farm)

    def _sig(sig, frame):
        clog("Interrupt — shutting down...", "WARN")
        slave._gstop.set()
    signal.signal(signal.SIGINT, _sig)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _sig)

    slave.run()

if __name__ == "__main__":
    main()