#!/usr/bin/env python3
"""
AE_RenderSlave.py  —  AEREN Render Farm Slave
Version : 3.0.0  |  AEREN - 2026  |  Praveen Brijwal

Usage:
    python AE_RenderSlave.py --manager <MANAGER_IP> [--name NODENAME] [--port PORT]

The slave:
  • Registers with the manager on startup and sends heartbeats
  • Listens for RENDER / STOP / PING / PREFLIGHT / STATUS commands
  • Streams aerender progress back to the manager in real-time
  • Handles graceful shutdown on SIGINT / SIGTERM
"""

import sys, os, json, socket, threading, time, subprocess
import platform, argparse, signal, re, logging, traceback
from datetime import datetime

# ── Optional psutil ───────────────────────────────────────────────────────────
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
SLAVE_VERSION    = "3.0.0"
MANAGER_PORT     = 9876
SLAVE_PORT       = 9877
HEARTBEAT_SEC    = 5

AERENDER_PRIMARY = r"C:\Program Files\Adobe\Adobe After Effects 2024\Support Files\aerender.exe"
AERENDER_FALLBACK = [
    r"C:\Program Files\Adobe\Adobe After Effects 2025\Support Files\aerender.exe",
    r"C:\Program Files\Adobe\Adobe After Effects 2023\Support Files\aerender.exe",
    r"C:\Program Files\Adobe\Adobe After Effects 2022\Support Files\aerender.exe",
    r"C:\Program Files\Adobe\Adobe After Effects 2021\Support Files\aerender.exe",
    "/Applications/Adobe After Effects 2025/aerender",
    "/Applications/Adobe After Effects 2024/aerender",
    "/Applications/Adobe After Effects 2023/aerender",
    "/Applications/Adobe After Effects 2021/aerender",
]

PLUGIN_DIRS_WINDOWS = [
    r"C:\Program Files\Adobe\Adobe After Effects 2025\Support Files\Plug-ins",
    r"C:\Program Files\Adobe\Adobe After Effects 2024\Support Files\Plug-ins",
    r"C:\Program Files\Adobe\Adobe After Effects 2023\Support Files\Plug-ins",
    r"C:\Program Files\Common Files\Adobe\Plug-ins\7.0",
]
PLUGIN_DIRS_MAC = [
    "/Applications/Adobe After Effects 2024/Plug-ins",
    "/Applications/Adobe After Effects 2023/Plug-ins",
    "/Library/Application Support/Adobe/Plug-ins/7.0",
]
AE_PLUGIN_EXTS = {".aex", ".plugin", ".flt", ".8bf"}

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("AERenderSlave")

ANSI = {
    "INFO":   "\033[96m",
    "OK":     "\033[92m",
    "WARN":   "\033[93m",
    "ERROR":  "\033[91m",
    "RENDER": "\033[95m",
    "STATUS": "\033[94m",
    "RESET":  "\033[0m",
}

def ts():
    return datetime.now().strftime("%H:%M:%S")

def clog(msg: str, tag: str = "INFO"):
    c = ANSI.get(tag, ANSI["RESET"])
    print(f"{c}[{ts()}][{tag:6s}]{ANSI['RESET']} {msg}", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def find_aerender() -> str:
    if os.path.exists(AERENDER_PRIMARY):
        return AERENDER_PRIMARY
    for p in AERENDER_FALLBACK:
        if os.path.exists(p):
            return p
    return ""

def get_local_ip(manager_ip: str = None) -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

def get_ae_version(aerender: str) -> str:
    m = re.search(r"After Effects (\d{4})", aerender, re.IGNORECASE)
    if m: return m.group(1)
    return "Unknown"

def cpu_pct() -> float:
    if HAS_PSUTIL:
        try: return round(psutil.cpu_percent(interval=None), 1)
        except: pass
    return 0.0

def ram_used_gb() -> float:
    if HAS_PSUTIL:
        try: return round(psutil.virtual_memory().used / 1e9, 1)
        except: pass
    return 0.0

def cpu_cores() -> int:
    if HAS_PSUTIL:
        try: return psutil.cpu_count(logical=True) or 0
        except: pass
    return 0

def ram_total_gb() -> float:
    if HAS_PSUTIL:
        try: return round(psutil.virtual_memory().total / 1e9, 1)
        except: pass
    return 0.0

def scan_installed_plugins() -> list:
    """
    Scan known AE plugin directories and return a set of lowercase plugin basenames.
    """
    dirs = PLUGIN_DIRS_WINDOWS if platform.system() == "Windows" else PLUGIN_DIRS_MAC
    found = set()
    for d in dirs:
        if not os.path.isdir(d): continue
        try:
            for root, _, files in os.walk(d):
                for fn in files:
                    ext = os.path.splitext(fn)[1].lower()
                    if ext in AE_PLUGIN_EXTS:
                        found.add(os.path.splitext(fn)[0].lower())
        except Exception as e:
            log.debug(f"Plugin scan error in {d}: {e}")
    return list(found)

def check_plugins(required: list, installed_set: set) -> dict:
    """
    Heuristically match required effect matchNames against installed plugin filenames.
    Built-in ADBE effects are always present.
    """
    result = {}
    for eff in required:
        name = eff.lower() if isinstance(eff, str) else ""
        if name.startswith("adbe "):
            # Built-in After Effects effect — always available
            result[eff] = True
            continue
        # Substring match
        matched = any(name in plug or plug in name for plug in installed_set)
        result[eff] = matched
    return result


# ══════════════════════════════════════════════════════════════════════════════
# SLAVE STATE
# ══════════════════════════════════════════════════════════════════════════════
class SlaveState:
    def __init__(self, manager_ip: str, name: str, port: int, secret: str = ""):
        self.manager_ip        = manager_ip
        self.secret            = secret
        self.listen_port       = port
        self.hostname          = name or socket.gethostname()
        self.local_ip          = get_local_ip(manager_ip)  # use manager route to pick correct NIC
        self.os_info           = f"{platform.system()} {platform.release()}"
        self.aerender          = find_aerender()
        self.ae_version        = get_ae_version(self.aerender) if self.aerender else "N/A"
        self.installed_plugins = set(scan_installed_plugins())

        self._lock              = threading.Lock()
        self.status             = "Idle"
        self.current_job_id     = None
        self.current_job_name   = ""
        self.current_frame      = 0
        self.progress           = 0
        self._proc              = None
        self._global_stop       = False   # set True on SIGINT/SIGTERM
        self._render_stop       = False   # set True per STOP command

    # ── Status payload ────────────────────────────────────────────────────────
    def _build_payload(self, extra: dict = None) -> dict:
        with self._lock:
            p = dict(
                secret        = self.secret,
                type          = "SLAVE_STATUS",
                hostname      = self.hostname,
                ip            = self.local_ip,
                port          = self.listen_port,
                os            = self.os_info,
                status        = self.status,
                current_job   = self.current_job_id   or "--",
                job_name      = self.current_job_name or "--",
                current_frame = self.current_frame,
                progress      = self.progress,
                aerender_ok   = bool(self.aerender),
                ae_version    = self.ae_version,
                cpu_pct       = cpu_pct(),
                ram_gb        = ram_used_gb(),
                cpu_cores     = cpu_cores(),
                ram_total_gb  = ram_total_gb(),
            )
        if extra: p.update(extra)
        return p

    # ── Send to manager ───────────────────────────────────────────────────────
    def send_status(self, extra: dict = None):
        """Send heartbeat / progress update to manager via HTTP POST."""
        payload = self._build_payload(extra)
        # Choose endpoint: first contact uses /register, ongoing use /heartbeat
        msg_type = payload.get("type", "")
        endpoint = "/register" if msg_type == "SLAVE_CONNECT" else "/heartbeat"
        url = f"http://{self.manager_ip}:{MANAGER_PORT}{endpoint}"
        body = json.dumps(payload).encode()
        try:
            import urllib.request
            req = urllib.request.Request(
                url, data=body,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=4):
                pass
        except Exception as e:
            # Only log at WARN level after repeated failures to avoid startup noise
            self._hb_fail_count = getattr(self, "_hb_fail_count", 0) + 1
            if self._hb_fail_count >= 3:
                log.warning(f"Manager unreachable ({self._hb_fail_count}x): {e}")
            else:
                log.debug(f"Heartbeat attempt failed: {e}")
            return
        self._hb_fail_count = 0  # reset on success

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    def heartbeat_loop(self):
        # Brief startup delay — manager HTTP server may still be binding its port.
        # The /register call in main() already happened; these are ongoing heartbeats.
        time.sleep(2)
        while not self._global_stop:
            self.send_status()
            time.sleep(HEARTBEAT_SEC)

    # ── RENDER ────────────────────────────────────────────────────────────────
    def render_job(self, job: dict):
        with self._lock:
            if self.status == "Rendering":
                clog("Already rendering — rejecting.", "WARN")
                return
            job_id   = str(job.get("job_id",       "UNKNOWN"))
            comp     = str(job.get("comp_name",    ""))
            project  = str(job.get("project_path", ""))
            output   = str(job.get("output_path",  ""))
            start_f  = int(job.get("start_frame",  0))
            end_f    = int(job.get("end_frame",     0))
            rq_index = int(job.get("rq_index",      1))
            self.status           = "Rendering"
            self.current_job_id   = job_id
            self.current_job_name = comp
            self.current_frame    = start_f
            self.progress         = 0
            self._render_stop     = False

        clog(f"Job {job_id}: {comp}  [{start_f}–{end_f}]", "RENDER")

        # Validation
        if not self.aerender:
            clog("aerender not found.", "ERROR")
            self._finish(job_id, False)
            return
        # Check project file exists — give a helpful message for UNC/network paths
        if not os.path.exists(project):
            if project.startswith("\\") or project.startswith("//"):
                clog(f"Network project path not accessible: {project}", "ERROR")
                clog("Check: (1) network share is mounted, (2) path spelling, "
                     "(3) this machine has read access to the share", "ERROR")
            else:
                clog(f"Project file not found: {project}", "ERROR")
            self._finish(job_id, False)
            return

        # Create output directory
        if output:
            out_dir = os.path.dirname(output)
            if out_dir:
                try:
                    os.makedirs(out_dir, exist_ok=True)
                except Exception as e:
                    clog(f"Could not create output dir: {e}", "WARN")

        # Build command
        cmd = [
            self.aerender,
            "-project", project,
            "-comp",    comp,
            "-s",       str(start_f),
            "-e",       str(end_f),
            "-rqindex", str(rq_index),
        ]
        if output:
            cmd += ["-output", output]

        clog("CMD: " + " ".join(cmd), "STATUS")
        total = max(end_f - start_f + 1, 1)

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, bufsize=1
            )
            with self._lock:
                self._proc = proc

            for raw in iter(proc.stdout.readline, ""):
                # Check stop flags
                with self._lock:
                    stop_now = self._render_stop or self._global_stop
                if stop_now:
                    clog("Stopping render on request.", "WARN")
                    try: proc.terminate()
                    except: pass
                    self._finish(job_id, False, stopped=True)
                    return

                line = raw.rstrip()
                if not line: continue

                tag = "ERROR" if "error" in line.lower() else "RENDER"
                clog(line, tag)

                # Parse aerender progress — multiple output formats across AE versions:
                #   AE 2021 and older : "X of Y"
                #   AE 2022-2024      : "PROGRESS:  X of Y" or "aerender: PROGRESS: X"
                #   AE 2024+          : "Frame X (Y%)" or just a frame number line
                cur = None
                m = re.search(r'(\d+)\s+of\s+(\d+)', line, re.IGNORECASE)
                if m:
                    cur = int(m.group(1))
                if cur is None:
                    m2 = re.search(r'PROGRESS[:\s]+(\d+)', line, re.IGNORECASE)
                    if m2: cur = int(m2.group(1))
                if cur is None:
                    m3 = re.search(r'Frame\s+(\d+)', line, re.IGNORECASE)
                    if m3:
                        fn_abs = int(m3.group(1))
                        cur = fn_abs - start_f + 1
                if cur is not None and cur > 0:
                    pct = min(int(cur / total * 100), 100)
                    fn  = start_f + cur - 1
                    with self._lock:
                        self.current_frame = fn
                        self.progress      = pct
                    self.send_status({
                        "type":          "PROGRESS",
                        "job_id":        job_id,
                        "current_frame": fn,
                        "progress":      pct,
                    })

            proc.wait()
            success = (proc.returncode == 0)
            clog(f"Job {job_id} {'COMPLETE' if success else 'FAILED'} "
                 f"(exit {proc.returncode})",
                 "OK" if success else "ERROR")
            self._finish(job_id, success)

        except Exception:
            clog(f"Render exception:\n{traceback.format_exc()}", "ERROR")
            self._finish(job_id, False)

    def _finish(self, job_id: str, success: bool, stopped: bool = False):
        msg_type = "JOB_DONE" if success else ("JOB_STOPPED" if stopped else "JOB_FAILED")
        with self._lock:
            self.status           = "Idle"
            self.current_job_id   = None
            self.current_job_name = ""
            self.current_frame    = 0
            self.progress         = 0
            self._proc            = None
        self.send_status({"type": msg_type, "job_id": job_id})
        clog(f"{msg_type} — slave now idle.", "STATUS")

    # ── Stop current render ───────────────────────────────────────────────────
    def stop_render(self):
        with self._lock:
            self._render_stop = True
            proc = self._proc
        if proc:
            # First try graceful termination
            try: proc.terminate()
            except: pass
            # On Windows aerender sometimes ignores SIGTERM — hard kill after 3s
            def _force_kill():
                import time as _t
                _t.sleep(3)
                try:
                    if proc.poll() is None:  # still alive
                        proc.kill()
                        clog("aerender force-killed after terminate timeout", "WARN")
                except: pass
            threading.Thread(target=_force_kill, daemon=True).start()

    # ── Preflight ─────────────────────────────────────────────────────────────
    def handle_preflight(self, required: list) -> dict:
        clog(f"Preflight: checking {len(required)} effects", "INFO")
        result  = check_plugins(required, self.installed_plugins)
        missing = [k for k, v in result.items() if not v]
        if missing:
            clog(f"MISSING: {missing}", "WARN")
        else:
            clog("All plugins OK.", "OK")
        return result

    # ── Shutdown ──────────────────────────────────────────────────────────────
    def shutdown(self):
        self._global_stop = True
        self.stop_render()
        self.send_status({"type": "SLAVE_DISCONNECT"})
        clog("Slave disconnected.", "STATUS")


# ══════════════════════════════════════════════════════════════════════════════
# TCP SERVER
# ══════════════════════════════════════════════════════════════════════════════
def handle_connection(conn: socket.socket, addr, slave: SlaveState):
    data = b""
    try:
        conn.settimeout(5)
        while True:
            chunk = conn.recv(65536)
            if not chunk: break
            data += chunk
    except: pass

    response = None
    if data:
        try:
            msg    = json.loads(data.decode())
            action = msg.get("action", "")

            if slave.secret and msg.get("secret") != slave.secret:
                clog(f"Unauthorized connection attempt from {addr[0]}", "WARN")
                response = json.dumps({"status": "unauthorized", "error": "unauthorized"}).encode()
                conn.sendall(response)
                conn.close()
                return

            if action == "RENDER":
                clog(f"RENDER from {addr[0]}", "INFO")
                with slave._lock:
                    already = (slave.status == "Rendering")
                if already:
                    clog("Already rendering — rejecting with 'busy'", "WARN")
                    response = json.dumps({"status": "busy",
                                           "msg": "slave already rendering"}).encode()
                else:
                    t = threading.Thread(
                        target=slave.render_job, args=(msg,), daemon=True)
                    t.start()
                    response = json.dumps({"status": "ok"}).encode()

            elif action == "STOP":
                clog(f"STOP from {addr[0]}", "WARN")
                slave.stop_render()
                response = json.dumps({"status": "ok"}).encode()

            elif action == "PING":
                response = json.dumps({
                    "status":        "ok",
                    "hostname":      slave.hostname,
                    "slave_status":  slave.status,
                }).encode()

            elif action == "PREFLIGHT":
                required = msg.get("required", [])
                result   = slave.handle_preflight(required)
                response = json.dumps({"status": "ok", "plugins": result}).encode()

            elif action == "STATUS":
                response = json.dumps(slave._build_payload()).encode()

            else:
                clog(f"Unknown action '{action}' from {addr[0]}", "WARN")
                response = json.dumps({"error": f"unknown action: {action}"}).encode()

        except json.JSONDecodeError as e:
            clog(f"Bad JSON from {addr[0]}: {e}", "ERROR")
            response = json.dumps({"error": "bad JSON"}).encode()
        except Exception:
            clog(f"Handler exception:\n{traceback.format_exc()}", "ERROR")
            response = json.dumps({"error": "internal error"}).encode()

    if response:
        try: conn.sendall(response)
        except: pass
    try: conn.close()
    except: pass


def run_server(slave: SlaveState, ready_event: threading.Event = None):
    try:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", slave.listen_port))
        srv.listen(10)
        srv.settimeout(1.0)
        clog(f"Listening on 0.0.0.0:{slave.listen_port}", "INFO")
        if ready_event:
            ready_event.set()   # signal that port is bound and ready
    except Exception as e:
        clog(f"Could not bind to port {slave.listen_port}: {e}", "ERROR")
        if ready_event:
            ready_event.set()   # unblock even on failure so main doesn't hang
        sys.exit(1)

    while not slave._global_stop:
        try:
            conn, addr = srv.accept()
            threading.Thread(
                target=handle_connection,
                args=(conn, addr, slave),
                daemon=True
            ).start()
        except socket.timeout:
            pass
        except Exception as e:
            if not slave._global_stop:
                clog(f"Accept error: {e}", "ERROR")

    try: srv.close()
    except: pass


# ══════════════════════════════════════════════════════════════════════════════
# BANNER
# ══════════════════════════════════════════════════════════════════════════════
def check_connectivity(slave) -> tuple:
    """Test reachability to manager and verify our listen port is bindable."""
    can_reach_mgr = False
    try:
        import urllib.request as _ur
        r = _ur.urlopen(
            f"http://{slave.manager_ip}:{MANAGER_PORT}/ping", timeout=4)
        can_reach_mgr = (r.status == 200)
    except Exception:
        pass

    # Verify our listen port isn't blocked by another process
    srv_ok = False
    try:
        test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        test.bind(("0.0.0.0", slave.listen_port))
        test.close()
        srv_ok = True   # port free — server will bind it
    except OSError:
        srv_ok = True   # port in use — means our server is already bound (good)
    except Exception:
        pass

    note = ""
    if not can_reach_mgr:
        note = (f"Cannot reach manager at {slave.manager_ip}:{MANAGER_PORT}. "
                f"Check: (1) Manager is running, "
                f"(2) --manager flag has the correct LAN IP (not localhost), "
                f"(3) Windows Firewall allows port {MANAGER_PORT}.")
    return can_reach_mgr, srv_ok, note


def print_banner(slave) -> bool:
    """Print startup banner and connectivity check. Returns True if manager reachable."""
    ae  = slave.aerender or "NOT FOUND"
    ps  = "installed" if HAS_PSUTIL else "NOT installed  (pip install psutil)"

    can_reach_mgr, srv_ok, conn_note = check_connectivity(slave)
    mgr_col = "\033[92m" if can_reach_mgr else "\033[91m"
    mgr_txt = "REACHABLE" if can_reach_mgr else "UNREACHABLE"
    srv_txt = "OK" if srv_ok else "PORT CONFLICT"
    ip_warn = ""
    if slave.local_ip.startswith("127."):
        ip_warn = ("  \033[93m<-- WARNING: loopback detected. "
                   "Manager cannot dial back. Use --ip <your_lan_ip>\033[0m")

    print(f"""
\033[97m╔══════════════════════════════════════════════════════╗
║         AEREN  Render Slave  v{SLAVE_VERSION}                ║
╚══════════════════════════════════════════════════════╝\033[0m
\033[96m  Hostname      :\033[92m {slave.hostname}
\033[96m  This Machine  :\033[92m {slave.local_ip}{ip_warn}
\033[96m  OS            :\033[92m {slave.os_info}
\033[96m  aerender      :\033[92m {ae}
\033[96m  AE Version    :\033[92m {slave.ae_version}
\033[96m  Manager       :\033[92m {slave.manager_ip}:{MANAGER_PORT}
\033[96m  Listen Port   :\033[92m {slave.listen_port}
\033[96m  CPU Cores     :\033[92m {cpu_cores()}
\033[96m  RAM Total     :\033[92m {ram_total_gb()} GB
\033[96m  Plugins found :\033[92m {len(slave.installed_plugins)}
\033[96m  psutil        :\033[92m {ps}

\033[96m  -- Network Check --
\033[96m  Manager       : {mgr_col}{mgr_txt}\033[0m
\033[96m  Listen port   : {"\033[92m" if srv_ok else "\033[91m"}{srv_txt}\033[0m
\033[90m  AEREN 2026 -- Praveen Brijwal\033[0m
""")
    if conn_note and slave.manager_ip not in ("localhost", "127.0.0.1"):
        print(f"  \033[91m[NETWORK ERROR] {conn_note}\033[0m\n")
    elif not can_reach_mgr and slave.manager_ip in ("localhost", "127.0.0.1"):
        print(f"  \033[90m[INFO] Manager not reachable via localhost — "
              f"normal if manager starts after slave. Will keep retrying.\033[0m\n")
    if not slave.aerender:
        print("  \033[93m[WARN] aerender not found. Render jobs WILL fail on this node.\033[0m\n")

    return can_reach_mgr


# ==============================================================================
# ENTRY POINT
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="AEREN Render Slave v3.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples (replace 192.168.1.10 with your manager machine's LAN IP):
  python AE_RenderSlave.py --manager 192.168.1.10
  python AE_RenderSlave.py --manager 192.168.1.10 --name RENDER-PC-01
  python AE_RenderSlave.py --manager 192.168.1.10 --ip 192.168.1.25
  python AE_RenderSlave.py --manager 192.168.1.10 --port 9877

NOTE: --manager must be the LAN IP of the manager machine.
      Using localhost only works when both run on the same machine.
""")
    parser.add_argument("--manager", default="localhost",
                        help="LAN IP of the manager machine  (e.g. 192.168.1.10)")
    parser.add_argument("--name",    default=None,
                        help="Override this node's display name in the Manager UI")
    parser.add_argument("--port",    type=int, default=SLAVE_PORT,
                        help=f"Port this slave listens on for job dispatch (default: {SLAVE_PORT})")
    parser.add_argument("--ip",      default=None,
                        help=("Override the IP this machine advertises to the manager. "
                              "Use when auto-detection picks the wrong network adapter. "
                              "Example: --ip 192.168.1.25"))
    parser.add_argument("--secret",  default="",
                        help="Shared secret for API and TCP authentication")
    args = parser.parse_args()

    # Warn clearly if --manager was not set for a multi-machine studio setup
    if args.manager in ("localhost", "127.0.0.1"):
        print("\033[93m" + "=" * 60)
        print("  WARNING: --manager is set to localhost / 127.0.0.1")
        print("  This only works if the Manager runs on THIS machine.")
        print("  For a studio render farm, pass the Manager's LAN IP:")
        print("    python AE_RenderSlave.py --manager 192.168.1.10")
        print("=" * 60 + "\033[0m\n")

    slave = SlaveState(
        manager_ip = args.manager,
        name       = args.name,
        port       = args.port,
        secret     = args.secret,
    )

    # Manual IP override — useful for multi-NIC machines or VPN environments
    if args.ip:
        slave.local_ip = args.ip
        clog(f"IP override applied: advertising {slave.local_ip} to manager", "INFO")

    # Print banner and run connectivity check
    manager_ok = print_banner(slave)

    def handle_exit(sig, frame):
        clog(f"Signal {sig} received — shutting down.", "WARN")
        slave.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT,  handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # Start TCP server in a background thread FIRST so we are ready to accept
    # job dispatches before registering — avoids race where manager dispatches
    # immediately after SLAVE_CONNECT but our port isn't bound yet.
    server_ready = threading.Event()

    def _run_server_bg():
        run_server(slave, ready_event=server_ready)

    srv_thread = threading.Thread(target=_run_server_bg, daemon=False, name="tcp_server")
    srv_thread.start()

    # Wait until the TCP server has actually bound its port (up to 5s)
    if not server_ready.wait(timeout=5):
        clog("WARNING: TCP server did not start in time — registration may race", "WARN")

    # Start heartbeat thread
    threading.Thread(
        target=slave.heartbeat_loop, daemon=True, name="heartbeat"
    ).start()

    # Now register with manager — server is ready to accept dispatches
    slave.send_status({"type": "SLAVE_CONNECT"})
    if manager_ok:
        clog(f"Registered with manager @ {slave.manager_ip}:{MANAGER_PORT}", "OK")
    else:
        clog(f"Registration sent but manager unreachable — will keep retrying", "WARN")

    # Wait for server thread to finish (runs until global stop)
    srv_thread.join()


if __name__ == "__main__":
    main()