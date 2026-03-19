#!/usr/bin/env python3
"""
AE_RenderManager.py  —  AEREN Render Farm Manager
Version : 4.0.0  |  AEREN - 2026  |  Praveen Brijwal

Run: python AE_RenderManager.py
"""

# ══════════════════════════════════════════════════════════════════════════════
#
#  PRODUCTION CONFIG  —  Edit these values before deployment
#  All paths, ports, and tunable constants live here.
#
# ══════════════════════════════════════════════════════════════════════════════

# ── Network ───────────────────────────────────────────────────────────────────
MANAGER_PORT  = 9876        # Port the manager listens on (HTTP + slave TCP)
SLAVE_PORT    = 9877        # Port each slave listens on for job dispatch

# ── aerender executable ───────────────────────────────────────────────────────
# Primary path — set this to your AE version first
AERENDER_PATH = r"C:\Program Files\Adobe\Adobe After Effects 2024\Support Files\aerender.exe"
# Fallback list — searched in order if primary is missing
AERENDER_FALLBACKS = [
    r"C:\Program Files\Adobe\Adobe After Effects 2025\Support Files\aerender.exe",
    r"C:\Program Files\Adobe\Adobe After Effects 2023\Support Files\aerender.exe",
    r"C:\Program Files\Adobe\Adobe After Effects 2022\Support Files\aerender.exe",
    r"C:\Program Files\Adobe\Adobe After Effects 2021\Support Files\aerender.exe",
    "/Applications/Adobe After Effects 2025/aerender",
    "/Applications/Adobe After Effects 2024/aerender",
    "/Applications/Adobe After Effects 2023/aerender",
]

# ── Job drop-file watch directory ─────────────────────────────────────────────
# AE_Submit.jsx writes JSON files here; manager picks them up automatically.
# Default = OS temp folder.  Override with a shared network path if needed.
JOB_WATCH_DIR = r""          # e.g. r"\\SERVER\AEREN\jobs"  — leave "" for OS temp

# ── Render history / persistence ──────────────────────────────────────────────
# Full path to the JSON file that stores job history across restarts.
HISTORY_FILE  = r""          # e.g. r"\\SERVER\AEREN\ae_render_history.json"
                              # Leave "" = same folder as this script

# ── Farm behaviour ────────────────────────────────────────────────────────────
SLAVE_TIMEOUT  = 20          # Seconds of silence before a slave is marked Offline
MAX_RETRIES    = 3           # Auto-debug max retries per frame before marking Failed
HEARTBEAT_SEC  = 5           # How often slaves send heartbeats (must match slave config)
STALL_TIMEOUT  = 120         # Seconds without frame progress = stalled render

# ══════════════════════════════════════════════════════════════════════════════
#  END OF PRODUCTION CONFIG  —  Do not edit below unless you know what you're doing
# ══════════════════════════════════════════════════════════════════════════════

import sys, os, json, socket, threading, time, uuid, glob, platform
import subprocess, re, logging, traceback
from datetime import datetime
import socketserver
from http.server import HTTPServer, BaseHTTPRequestHandler

class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    """HTTPServer that handles each request in its own thread."""
    daemon_threads = True

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTableWidget, QTableWidgetItem, QHeaderView, QPushButton,
    QLabel, QFrame, QProgressBar, QTextEdit, QStatusBar, QToolBar,
    QMessageBox, QDialog, QFormLayout, QLineEdit, QSpinBox,
    QMenu, QAbstractItemView, QListWidget, QListWidgetItem,
    QDialogButtonBox, QComboBox, QSizePolicy
)
from PyQt5.QtCore  import Qt, QTimer, QThread, pyqtSignal
from PyQt5.QtGui   import QColor, QPalette, QBrush, QFont

# ── Resolve config defaults ───────────────────────────────────────────────────
MANAGER_VERSION = "4.0.0"
APP_NAME        = "AEREN"
JOB_PATTERN     = "ae_render_job_*.json"

if not JOB_WATCH_DIR:
    JOB_WATCH_DIR = os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp"))
if not HISTORY_FILE:
    HISTORY_FILE = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "ae_render_history.json")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("AEREN")

def get_current_user():
    for v in ("USERNAME", "USER", "LOGNAME"):
        val = os.environ.get(v, "")
        if val: return val
    try:
        import getpass; return getpass.getuser()
    except: return "unknown"

CURRENT_USER   = get_current_user()
LOCAL_HOSTNAME = socket.gethostname()

def find_aerender():
    if os.path.exists(AERENDER_PATH): return AERENDER_PATH
    for p in AERENDER_FALLBACKS:
        if os.path.exists(p): return p
    return ""

# ── Status constants ──────────────────────────────────────────────────────────
class JS:
    PENDING   = "Pending"
    RENDERING = "Rendering"
    PAUSED    = "Paused"
    COMPLETED = "Completed"
    FAILED    = "Failed"
    STOPPED   = "Stopped"

DONE_STATUSES   = {JS.COMPLETED, JS.FAILED, JS.STOPPED}
ACTIVE_STATUSES = {JS.RENDERING, JS.PAUSED}

STATUS_COLOR = {
    JS.RENDERING: "#3CBA54",
    JS.COMPLETED: "#66CC77",
    JS.PENDING:   "#F0C674",
    JS.PAUSED:    "#FFCC66",
    JS.FAILED:    "#FF5555",
    JS.STOPPED:   "#888888",
}
GHOST_FG  = QColor(110, 118, 128)
ACTIVE_FG = QColor(220, 225, 230)

# ══════════════════════════════════════════════════════════════════════════════
#  STYLESHEET  — faithful to original provided by user
# ══════════════════════════════════════════════════════════════════════════════
def apply_palette(app):
    app.setStyle("Fusion")
    p = QPalette()
    p.setColor(QPalette.Window,          QColor(8,   8,   8))
    p.setColor(QPalette.WindowText,      QColor(220, 225, 230))
    p.setColor(QPalette.Base,            QColor(12,  12,  12))
    p.setColor(QPalette.AlternateBase,   QColor(18,  18,  18))
    p.setColor(QPalette.ToolTipBase,     QColor(10,  10,  10))
    p.setColor(QPalette.ToolTipText,     QColor(230, 235, 240))
    p.setColor(QPalette.Text,            QColor(220, 225, 230))
    p.setColor(QPalette.Button,          QColor(20,  20,  20))
    p.setColor(QPalette.ButtonText,      QColor(220, 225, 230))
    p.setColor(QPalette.BrightText,      Qt.red)
    p.setColor(QPalette.Link,            QColor(80,  180, 255))
    p.setColor(QPalette.Highlight,       QColor(25,  100, 200))
    p.setColor(QPalette.HighlightedText, Qt.white)
    p.setColor(QPalette.Disabled, QPalette.ButtonText, QColor(100, 105, 112))
    p.setColor(QPalette.Disabled, QPalette.Text,       QColor(100, 105, 112))
    app.setPalette(p)

SS = """
* { font-family: 'Segoe UI', Arial, sans-serif; font-size: 12px; }
QMainWindow, QDialog { background: #080808; }
QWidget { background: #080808; color: #E0E4EA; }

QMenuBar { background: #000000; color: #C4CAD4; border-bottom: 1px solid #181818;
           padding: 2px 4px; }
QMenuBar::item { padding: 4px 10px; border-radius: 3px; }
QMenuBar::item:selected { background: #1E1E1E; color: #FFFFFF; }
QMenu { background: #101010; color: #E0E4EA; border: 1px solid #303030; }
QMenu::item { padding: 5px 24px 5px 12px; }
QMenu::item:selected { background: #145A9E; }
QMenu::separator { height: 1px; background: #262626; margin: 3px 0; }

QToolBar { background: #000000; border: none; border-bottom: 1px solid #181818;
           spacing: 4px; padding: 4px 8px; }

QTableWidget { background: #0C0C0C; gridline-color: #1C1C1C;
               color: #E0E4EA; border: none; outline: none; }
QTableWidget::item { padding: 2px 6px; border: none; }
QTableWidget::item:selected { background: #0F3C72; color: #FFFFFF; }
QTableWidget::item:alternate { background: #111111; }
QHeaderView { background: #000000; }
QHeaderView::section { background: #000000; color: #8A94A4; padding: 5px 6px;
                        border: none; border-right: 1px solid #1C1C1C;
                        border-bottom: 1px solid #1C1C1C;
                        font-size: 11px; font-weight: bold; text-transform: uppercase; }
QHeaderView::section:last { border-right: none; }

QPushButton { background: #181818; color: #D8DCE4; border: 1px solid #2E2E2E;
              border-radius: 3px; padding: 4px 12px; min-width: 70px; }
QPushButton:hover   { background: #242424; border-color: #4FA8FF; color: #FFFFFF; }
QPushButton:pressed { background: #0E2E52; }
QPushButton:disabled { color: #606870; border-color: #202020; background: #111111; }

QPushButton#btn_render  { background:#0A1E3A; border-color:#2255AA; color:#7AB8FF; font-weight:bold; }
QPushButton#btn_render:hover  { background:#2255AA; color:#FFFFFF; }
QPushButton#btn_render:disabled { background:#070E18; border-color:#111E2E; color:#303848; }
QPushButton#btn_pause   { background:#2A1800; border-color:#AA6600; color:#FFCC66; }
QPushButton#btn_pause:hover   { background:#AA6600; color:#FFFFFF; }
QPushButton#btn_resume  { background:#0A2210; border-color:#228844; color:#88FFAA; }
QPushButton#btn_resume:hover  { background:#228844; color:#FFFFFF; }
QPushButton#btn_stop    { background:#220A0A; border-color:#882222; color:#FF9090; }
QPushButton#btn_stop:hover    { background:#882222; color:#FFFFFF; }

QProgressBar { background: #141414; border: 1px solid #282828; border-radius: 3px;
               height: 12px; text-align: center; color: #C0C8D0; font-size: 10px; }
QProgressBar::chunk { background: #228844; border-radius: 2px; }
QProgressBar[done="true"]::chunk   { background: #3A4040; }
QProgressBar[failed="true"]::chunk { background: #7A2020; }

QTextEdit { background: #050505; color: #A0EEA0; border: 1px solid #222222;
            font-family: Consolas, 'Courier New', monospace; font-size: 11px; }

QLineEdit, QSpinBox, QComboBox {
    background: #0E0E0E; color: #E0E4EA; border: 1px solid #2E2E2E;
    border-radius: 3px; padding: 3px 6px; }
QLineEdit:focus, QSpinBox:focus, QComboBox:focus { border-color: #4FA8FF; }
QComboBox::drop-down { border: none; }
QComboBox QAbstractItemView { background: #0E0E0E; border: 1px solid #2E2E2E;
                               color: #E0E4EA; }

QScrollBar:vertical   { background: #000000; width: 9px; margin: 0; border: none; }
QScrollBar:horizontal { background: #000000; height: 9px; margin: 0; border: none; }
QScrollBar::handle:vertical, QScrollBar::handle:horizontal
    { background: #303030; border-radius: 4px; min-height: 18px; min-width: 18px; }
QScrollBar::handle:vertical:hover, QScrollBar::handle:horizontal:hover
    { background: #4FA8FF; }
QScrollBar::add-line, QScrollBar::sub-line { height: 0; width: 0; }

QStatusBar { background: #000000; color: #8A94A4; font-size: 11px;
             border-top: 1px solid #181818; }
QStatusBar::item { border: none; }
QSplitter::handle { background: #080808; }
QListWidget { background: #0E0E0E; color: #E0E4EA; border: 1px solid #2E2E2E; }
QListWidget::item { padding: 4px 8px; }
QListWidget::item:selected { background: #0F3C72; }
QListWidget::item:hover    { background: #1A1A1A; }
QLabel#section_hdr { color: #7A8494; font-size: 11px; font-weight: bold;
                     background: #000000; padding: 4px 8px; }
QCheckBox { color: #9098A8; background: transparent; }
QCheckBox::indicator { width: 13px; height: 13px; border: 1px solid #3A3A3A;
                       border-radius: 2px; background: #0E0E0E; }
QCheckBox::indicator:checked { background: #228844; border-color: #33AA55; }
QToolTip { background: #101010; color: #E0E4EA; border: 1px solid #303030; padding: 4px; }
"""

# ══════════════════════════════════════════════════════════════════════════════
#  DATA MODEL
# ══════════════════════════════════════════════════════════════════════════════
class RenderJob:
    def __init__(self, data: dict):
        self.id              = data.get("id",             str(uuid.uuid4())[:8].upper())
        self.comp_name       = data.get("comp_name",      "Unknown")
        self.project_path    = data.get("project_path",   "")
        self.output_path     = data.get("output_path",    "")
        self.start_frame     = int(data.get("start_frame",    0))
        self.end_frame       = int(data.get("end_frame",      0))
        self.fps             = float(data.get("fps",          25.0))
        self.width           = int(data.get("width",          1920))
        self.height          = int(data.get("height",         1080))
        self.duration        = int(data.get("duration_frames",0))
        self.rq_index        = int(data.get("rq_index",       1))
        self.status          = data.get("status",          JS.PENDING)
        self.progress        = int(data.get("progress",    0))
        self.submitted_at    = data.get("submitted_at",   datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.started_at      = data.get("started_at",    "")
        self.finished_at     = data.get("finished_at",   "")
        self.assigned_to     = data.get("assigned_to",   "Local")
        self.current_frame   = int(data.get("current_frame", data.get("start_frame", 0)))
        self.log_lines       = data.get("log_lines",     [])
        self.source_machine  = data.get("machine",        LOCAL_HOSTNAME)
        self.source_user     = data.get("user",           CURRENT_USER)
        self.errors          = int(data.get("errors",     0))
        self.hostname        = data.get("hostname",       "")
        self.frame_status    = {int(k): v for k, v in data.get("frame_status", {}).items()}
        self.assigned_workers= data.get("assigned_workers", [])
        self.submitted_epoch = data.get("submitted_epoch", time.time())
        self.priority        = int(data.get("priority",   5))
        self.auto_debug      = bool(data.get("auto_debug", True))
        self.is_video        = bool(data.get("is_video",  False))
        self.required_effects= data.get("required_effects", [])
        self.preflight_report= data.get("preflight_report", {})
        self.frame_retries   = {int(k): int(v) for k, v in data.get("frame_retries", {}).items()}
        self.process         = None
        self._table_row      = -1
        self._pause_event    = threading.Event()
        self._pause_event.set()
        self._stop_flag      = False

    @property
    def frame_range(self):  return f"{self.start_frame}-{self.end_frame}"
    @property
    def resolution(self):   return f"{self.width}x{self.height}"
    @property
    def total_frames(self): return max(self.end_frame - self.start_frame + 1, 0)
    @property
    def elapsed(self):
        if not self.started_at: return "--"
        fmt = "%Y-%m-%d %H:%M:%S"
        try:
            s   = datetime.strptime(self.started_at, fmt)
            e   = datetime.strptime(self.finished_at, fmt) if self.finished_at else datetime.now()
            sec = int((e - s).total_seconds())
            return f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
        except: return "--"

    def to_dict(self):
        return dict(
            id=self.id, comp_name=self.comp_name, project_path=self.project_path,
            output_path=self.output_path, start_frame=self.start_frame,
            end_frame=self.end_frame, fps=self.fps, width=self.width,
            height=self.height, duration_frames=self.duration, rq_index=self.rq_index,
            status=self.status, progress=self.progress, submitted_at=self.submitted_at,
            started_at=self.started_at, finished_at=self.finished_at,
            assigned_to=self.assigned_to, current_frame=self.current_frame,
            log_lines=self.log_lines[-500:], machine=self.source_machine,
            user=self.source_user, errors=self.errors, hostname=self.hostname,
            frame_status={str(k): v for k, v in self.frame_status.items()},
            assigned_workers=self.assigned_workers, submitted_epoch=self.submitted_epoch,
            priority=self.priority, auto_debug=self.auto_debug, is_video=self.is_video,
            required_effects=self.required_effects, preflight_report=self.preflight_report,
            frame_retries={str(k): v for k, v in self.frame_retries.items()},
        )

# ══════════════════════════════════════════════════════════════════════════════
#  PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════
MAX_HISTORY_JOBS = 200   # max completed jobs kept in history file

_history_lock = threading.Lock()
_save_thread = None

def save_history(jobs: dict):
    global _save_thread
    try:
        # Serialize the jobs synchronously to avoid dictionary mutation errors
        # during iteration by the background thread.
        all_jobs = [j.to_dict() for j in jobs.values()]

        def background_save():
            with _history_lock:
                try:
                    # Prune: keep ALL active/failed/stopped jobs + newest N completed
                    active  = [d for d in all_jobs if d.get("status") != "Completed"]
                    done    = [d for d in all_jobs if d.get("status") == "Completed"]
                    # Sort completed by submitted_epoch descending, keep newest
                    done.sort(key=lambda d: d.get("submitted_epoch", 0), reverse=True)
                    pruned  = active + done[:MAX_HISTORY_JOBS]
                    with open(HISTORY_FILE, "w") as f:
                        json.dump(pruned, f, indent=2)
                except Exception as e:
                    log.error(f"Save history background task failed: {e}")

        _save_thread = threading.Thread(target=background_save, daemon=True)
        _save_thread.start()
    except Exception as e:
        log.error(f"Save history dispatch failed: {e}")

def load_history():
    if not os.path.exists(HISTORY_FILE): return []
    try:
        with open(HISTORY_FILE) as f: return json.load(f)
    except Exception as e:
        log.error(f"Load history failed: {e}"); return []

# ══════════════════════════════════════════════════════════════════════════════
#  PREFLIGHT — ask a slave if plugins are installed
# ══════════════════════════════════════════════════════════════════════════════
def check_slave_plugins(slave_ip: str, slave_port: int,
                        required: list, timeout: int = 8) -> dict:
    if not required: return {}
    payload = json.dumps({"action": "PREFLIGHT", "required": required}).encode()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((slave_ip, slave_port))
        s.sendall(payload)
        data = b""
        while True:
            try:
                chunk = s.recv(65536)
                if not chunk: break
                data += chunk
            except: break
        s.close()
        if data:
            return json.loads(data.decode()).get("plugins", {})
    except Exception as e:
        log.warning(f"Preflight failed for {slave_ip}: {e}")
    return {}

# ══════════════════════════════════════════════════════════════════════════════
#  JOB FILE WATCHER
# ══════════════════════════════════════════════════════════════════════════════
class JobWatcher(QThread):
    new_job = pyqtSignal(dict)

    def __init__(self):
        super().__init__()
        self._seen = set()
        self._run  = True

    def run(self):
        while self._run:
            try:
                for fp in glob.glob(os.path.join(JOB_WATCH_DIR, JOB_PATTERN)):
                    if fp not in self._seen:
                        self._seen.add(fp)
                        try:
                            with open(fp) as f: data = json.load(f)
                            for jd in data.get("jobs", []):
                                jd["machine"]          = data.get("machine", CURRENT_USER)
                                jd["user"]             = data.get("user",    CURRENT_USER)
                                jd["submitted_at"]     = data.get("submitted_at", "")
                                jd["project_path"]     = data.get("project", jd.get("project_path", ""))
                                jd["submitted_epoch"]  = time.time()
                                jd["priority"]         = int(data.get("priority", 5))
                                jd["required_effects"] = data.get("required_effects", [])
                                self.new_job.emit(jd)
                            try: os.remove(fp)
                            except: pass
                        except Exception as e:
                            log.error(f"Job file error: {e}")
            except Exception as e:
                log.error(f"JobWatcher error: {e}")
            time.sleep(2)

    def stop(self): self._run = False

# ══════════════════════════════════════════════════════════════════════════════
#  HTTP SERVER  (receives /submit from AE_Submit.jsx curl call)
# ══════════════════════════════════════════════════════════════════════════════
class ManagerHTTPHandler(BaseHTTPRequestHandler):
    # Callbacks set by HTTPServerThread before server starts
    job_callback    = None   # called with job dict on /submit
    slave_callback  = None   # called with slave dict on /heartbeat or /register

    def log_message(self, fmt, *args): pass  # suppress default stdout logging

    def send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length).decode())

    def do_POST(self):
        try:
            data       = self._read_json()
            client_ip  = self.client_address[0]

            # ── Job submission from AE_Submit.jsx (curl) ──────────────────────
            if self.path == "/submit":
                if ManagerHTTPHandler.job_callback:
                    ManagerHTTPHandler.job_callback(data)
                self.send_json({"status": "ok"})

            # ── Slave registration ────────────────────────────────────────────
            elif self.path == "/register":
                data["host"]      = client_ip
                data["last_seen"] = time.time()
                if not data.get("type"):
                    data["type"] = "SLAVE_CONNECT"
                if ManagerHTTPHandler.slave_callback:
                    ManagerHTTPHandler.slave_callback(data)
                self.send_json({"status": "ok"})

            # ── Slave heartbeat / progress / done ─────────────────────────────
            elif self.path == "/heartbeat":
                data["host"]      = client_ip
                data["last_seen"] = time.time()
                if ManagerHTTPHandler.slave_callback:
                    ManagerHTTPHandler.slave_callback(data)
                self.send_json({"status": "ok"})

            else:
                self.send_json({"error": "not found"}, 404)

        except Exception as e:
            log.debug(f"HTTP handler error on {self.path}: {e}")
            try: self.send_json({"error": str(e)}, 500)
            except: pass

    def do_GET(self):
        if self.path == "/ping":
            self.send_json({"status": "ok", "manager": APP_NAME,
                            "version": MANAGER_VERSION})
        else:
            self.send_json({"error": "not found"}, 404)


class HTTPServerThread(QThread):
    """
    Single HTTP server on MANAGER_PORT.
    Handles:
      POST /submit    — job from AE_Submit.jsx
      POST /register  — slave first connection
      POST /heartbeat — slave status / progress / done
      GET  /ping      — health check
    """
    http_job     = pyqtSignal(dict)   # new render job received
    slave_update = pyqtSignal(dict)   # slave heartbeat / status received

    def __init__(self, port=MANAGER_PORT):
        super().__init__()
        self.port    = port
        self._server = None

    def run(self):
        # Wire job callback
        def job_cb(data):
            for jd in data.get("jobs", []):
                jd["machine"]          = data.get("machine", "")
                jd["user"]             = data.get("user",    "")
                jd["submitted_at"]     = data.get("submitted_at", "")
                jd["project_path"]     = data.get("project", jd.get("project_path", ""))
                jd["submitted_epoch"]  = time.time()
                jd["priority"]         = int(data.get("priority", 5))
                jd["required_effects"] = data.get("required_effects", [])
                self.http_job.emit(jd)

        # Wire slave callback
        def slave_cb(data):
            self.slave_update.emit(data)

        ManagerHTTPHandler.job_callback   = job_cb
        ManagerHTTPHandler.slave_callback = slave_cb

        try:
            self._server = ThreadingHTTPServer(("0.0.0.0", self.port), ManagerHTTPHandler)
            log.info(f"HTTP server listening on port {self.port} "
                     f"(jobs + slave heartbeats)")
            self._server.serve_forever()
        except Exception as e:
            log.error(f"HTTP server error: {e}")

    def stop(self):
        if self._server:
            try: self._server.shutdown()
            except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  LOCAL RENDER WORKER
# ══════════════════════════════════════════════════════════════════════════════
class RenderWorker(QThread):
    sig_progress   = pyqtSignal(str, int, int)  # job_id, frame, pct
    sig_log        = pyqtSignal(str, str)
    sig_status     = pyqtSignal(str, str)
    sig_frame_done = pyqtSignal(str, int)

    def __init__(self, job: RenderJob, aerender: str,
                 start_frame: int = None, end_frame: int = None):
        super().__init__()
        self.job      = job
        self.aerender = aerender
        self.sf       = start_frame if start_frame is not None else job.start_frame
        self.ef       = end_frame   if end_frame   is not None else job.end_frame
        self._stop    = False
        self._paused  = False
        self._pev     = threading.Event()
        self._pev.set()

    def run(self):
        job = self.job
        if not job.started_at:
            job.started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.sig_status.emit(job.id, JS.RENDERING)
        self.sig_log.emit(job.id,
            f"[{job.started_at}] Starting: {job.comp_name}  frames {self.sf}-{self.ef}")

        if not self.aerender or not os.path.exists(self.aerender):
            self.sig_log.emit(job.id, f"[ERROR] aerender not found: {self.aerender}")
            self.sig_status.emit(job.id, JS.FAILED); return

        if not os.path.exists(job.project_path):
            self.sig_log.emit(job.id, f"[ERROR] Project not found: {job.project_path}")
            self.sig_status.emit(job.id, JS.FAILED); return

        if job.output_path:
            out_dir = os.path.dirname(job.output_path)
            if out_dir:
                try: os.makedirs(out_dir, exist_ok=True)
                except Exception as e:
                    self.sig_log.emit(job.id, f"[WARN] Output dir: {e}")

        cmd = [self.aerender,
               "-project",  job.project_path,
               "-comp",     job.comp_name,
               "-s",        str(self.sf),
               "-e",        str(self.ef),
               "-rqindex",  str(job.rq_index)]
        if job.output_path: cmd += ["-output", job.output_path]

        self.sig_log.emit(job.id, "CMD: " + " ".join(cmd))
        total = max(self.ef - self.sf + 1, 1)

        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
            job.process = proc
            for raw in iter(proc.stdout.readline, ""):
                if self._paused:
                    self._pev.wait()
                if self._stop:
                    try: proc.terminate()
                    except: pass
                    self.sig_log.emit(job.id, "[STOPPED]")
                    self.sig_status.emit(job.id, JS.STOPPED); return

                line = raw.rstrip()
                if not line: continue
                if "error" in line.lower(): job.errors += 1
                self.sig_log.emit(job.id, line)

                m = re.search(r'(\d+)\s+of\s+(\d+)', line, re.IGNORECASE)
                if m:
                    cur = int(m.group(1))
                    pct = min(int(cur / total * 100), 100)
                    fn  = self.sf + cur - 1
                    job.frame_status[fn] = JS.COMPLETED
                    self.sig_progress.emit(job.id, fn, pct)
                    self.sig_frame_done.emit(job.id, fn)

            proc.wait()
            job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if proc.returncode == 0:
                for f in range(self.sf, self.ef + 1):
                    job.frame_status.setdefault(f, JS.COMPLETED)
                self.sig_progress.emit(job.id, self.ef, 100)
                self.sig_log.emit(job.id, f"[{job.finished_at}] COMPLETE  rc=0")
                self.sig_status.emit(job.id, JS.COMPLETED)
            else:
                self.sig_log.emit(job.id,
                    f"[{job.finished_at}] FAILED  exit={proc.returncode}")
                self.sig_status.emit(job.id, JS.FAILED)
        except Exception:
            self.sig_log.emit(job.id, f"[EXCEPTION]\n{traceback.format_exc()}")
            self.sig_status.emit(job.id, JS.FAILED)

    def pause(self):
        self._paused = True
        self._pev.clear()
        if self.job.process:
            try:
                if platform.system() == "Windows":
                    import ctypes
                    h = ctypes.windll.kernel32.OpenProcess(0x001F0FFF, False,
                                                           self.job.process.pid)
                    ctypes.windll.kernel32.SuspendThread(h)
                    ctypes.windll.kernel32.CloseHandle(h)
                else:
                    import signal as _s
                    os.kill(self.job.process.pid, _s.SIGSTOP)
            except: pass

    def resume(self):
        if self.job.process:
            try:
                if platform.system() == "Windows":
                    import ctypes
                    h = ctypes.windll.kernel32.OpenProcess(0x001F0FFF, False,
                                                           self.job.process.pid)
                    ctypes.windll.kernel32.ResumeThread(h)
                    ctypes.windll.kernel32.CloseHandle(h)
                else:
                    import signal as _s
                    os.kill(self.job.process.pid, _s.SIGCONT)
            except: pass
        self._paused = False
        self._pev.set()

    def stop(self):
        self._stop = True
        self._pev.set()
        if self.job.process:
            try: self.job.process.terminate()
            except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  FRAME FILE WATCHER
# ══════════════════════════════════════════════════════════════════════════════
class FrameWatcher(QThread):
    frame_update = pyqtSignal(str, int)
    IMG_EXTS = {'.png','.jpg','.jpeg','.exr','.tif','.tiff',
                '.dpx','.hdr','.bmp','.tga','.psd','.cin'}

    def __init__(self):
        super().__init__()
        self._jobs = {}
        self._lock = threading.Lock()
        self._run  = True

    def register(self, jid, output_path, sf, ef):
        with self._lock: self._jobs[jid] = (output_path, sf, ef)

    def unregister(self, jid):
        with self._lock: self._jobs.pop(jid, None)

    def run(self):
        while self._run:
            with self._lock: snapshot = dict(self._jobs)
            for jid, (out_path, sf, ef) in snapshot.items():
                if not out_path: continue
                out_dir = os.path.dirname(out_path)
                if not out_dir or not os.path.isdir(out_dir): continue
                try:
                    count = sum(1 for fn in os.listdir(out_dir)
                                if os.path.splitext(fn)[1].lower() in self.IMG_EXTS)
                    if count > 0: self.frame_update.emit(jid, count)
                except: pass
            time.sleep(3)

    def stop(self): self._run = False

# ══════════════════════════════════════════════════════════════════════════════
#  SLAVE DISPATCH
# ══════════════════════════════════════════════════════════════════════════════
def dispatch_to_slave(host: str, job: RenderJob,
                      start_frame: int = None, end_frame: int = None,
                      port: int = SLAVE_PORT,
                      reported_ip: str = None) -> bool:
    """
    Send a RENDER command to a slave over TCP.
    Tries `host` (client_address) first, then `reported_ip` (slave's self-reported IP)
    as a fallback for multi-NIC / NAT studio environments.
    """
    sf = start_frame if start_frame is not None else job.start_frame
    ef = end_frame   if end_frame   is not None else job.end_frame
    payload = json.dumps(dict(
        action="RENDER", job_id=job.id, comp_name=job.comp_name,
        project_path=job.project_path, output_path=job.output_path,
        start_frame=sf, end_frame=ef, rq_index=job.rq_index,
    )).encode()

    targets = [host]
    if reported_ip and reported_ip != host and not reported_ip.startswith("127."):
        targets.append(reported_ip)

    for target in targets:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(6)
            s.connect((target, port))
            s.sendall(payload)
            # Read slave's response to confirm it accepted (not "busy")
            s.settimeout(4)
            resp_data = b""
            try:
                while True:
                    chunk = s.recv(1024)
                    if not chunk: break
                    resp_data += chunk
                    if len(resp_data) > 512: break  # enough to parse status
            except: pass
            s.close()
            if resp_data:
                try:
                    resp = json.loads(resp_data.decode())
                    if resp.get("status") == "busy":
                        log.warning(f"dispatch_to_slave {target}: slave busy, trying next")
                        continue   # try next target
                except: pass
            if target != host:
                log.info(f"dispatch_to_slave: connected via reported_ip {target} (not {host})")
            return True
        except Exception as e:
            log.debug(f"dispatch_to_slave {target}:{port} failed: {e}")

    log.warning(f"dispatch_to_slave: all targets failed for slave {host}")
    return False

def stop_slave_render(host: str, port: int = SLAVE_PORT,
                      reported_ip: str = None):
    targets = [host]
    if reported_ip and reported_ip != host and not reported_ip.startswith("127."):
        targets.append(reported_ip)
    for target in targets:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(4); s.connect((target, port))
            s.sendall(json.dumps({"action": "STOP"}).encode()); s.close()
            return  # sent successfully
        except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  AUTO DEBUG ENGINE
# ══════════════════════════════════════════════════════════════════════════════
class AutoDebugEngine(QThread):
    sig_log    = pyqtSignal(str, str)
    sig_retry  = pyqtSignal(str, int, int, str)  # job_id, sf, ef, target
    sig_status = pyqtSignal(str, str)

    def __init__(self, jobs_ref: dict, slaves_ref: dict, aerender: str):
        super().__init__()
        self._jobs    = jobs_ref
        self._slaves  = slaves_ref
        self._aerender= aerender
        self._run     = True
        self._last_progress = {}  # job_id -> (frame, time)
        self._lock    = threading.Lock()

    def update_progress(self, job_id: str, frame: int):
        with self._lock:
            self._last_progress[job_id] = (frame, time.time())

    def run(self):
        while self._run:
            time.sleep(15)
            self._check_all()

    def _check_all(self):
        with self._lock:
            snap_jobs   = dict(self._jobs)
            snap_prog   = dict(self._last_progress)
            snap_slaves = dict(self._slaves)
        now = time.time()

        for jid, job in snap_jobs.items():
            if not job.auto_debug or job.status != JS.RENDERING: continue

            # Check stall
            last = snap_prog.get(jid)
            stalled = last and (now - last[1]) > STALL_TIMEOUT

            # Check assigned slave offline
            assigned = job.assigned_to
            slave_offline = (
                assigned and assigned != "Local" and
                assigned in snap_slaves and
                (now - snap_slaves[assigned].get("last_seen", 0)) > SLAVE_TIMEOUT
            )

            if stalled or slave_offline:
                reason = "stall detected" if stalled else f"slave {assigned} offline"
                retries = job.frame_retries.get(job.current_frame, 0)
                if retries >= MAX_RETRIES:
                    self.sig_log.emit(jid,
                        f"[AUTO-DEBUG] {reason} — MAX_RETRIES reached, marking FAILED")
                    self.sig_status.emit(jid, JS.FAILED)
                    continue

                job.frame_retries[job.current_frame] = retries + 1
                # Find a different idle slave
                alt = None
                for h, info in snap_slaves.items():
                    if (h != assigned and
                            info.get("status", "") == "Idle" and
                            (now - info.get("last_seen", 0)) < SLAVE_TIMEOUT):
                        alt = h; break
                if not alt:
                    self.sig_log.emit(jid,
                        f"[AUTO-DEBUG] {reason} — no idle slaves available, marking FAILED")
                    self.sig_status.emit(jid, JS.FAILED)
                    continue
                alt_hn = self._slaves.get(alt, {}).get("hostname", alt)
                self.sig_log.emit(jid,
                    f"[AUTO-DEBUG] {reason} — retry {retries+1}/{MAX_RETRIES} -> {alt_hn}")
                self.sig_retry.emit(jid, job.current_frame, job.end_frame, alt)

    def stop(self): self._run = False

# ══════════════════════════════════════════════════════════════════════════════
#  DIALOGS
# ══════════════════════════════════════════════════════════════════════════════
def section_header(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setObjectName("section_hdr")
    lbl.setFixedHeight(24)
    return lbl


class AssignWorkersDialog(QDialog):
    """
    Slave-only assign dialog.
    Shows all registered slaves with their current status.
    Render only dispatches to slave nodes — no local rendering.
    """
    def __init__(self, job: RenderJob, slaves: dict, parent=None):
        """
        slaves: dict of ip -> slave_info (from self.slaves in main window)
        """
        super().__init__(parent)
        self.setWindowTitle(f"Assign Workers  —  {job.comp_name}")
        self.setMinimumSize(480, 420)
        self.setStyleSheet(SS)
        lay = QVBoxLayout(self); lay.setSpacing(8)

        info = QLabel(
            f"Job      :  {job.comp_name}  [{job.id}]\n"
            f"Frames   :  {job.frame_range}  ({job.total_frames} frames)\n"
            f"Priority :  {job.priority}" +
            ("\nType     :  VIDEO  (will use first selected slave only)" if job.is_video else "")
        )
        info.setStyleSheet("color:#8090A8; background:transparent;")
        lay.addWidget(info)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("background:#222222; max-height:1px;"); lay.addWidget(sep)

        now = time.time()
        # Build slave entries: (display_label, ip, is_alive)
        self._entries = []
        for ip, info_d in slaves.items():
            alive   = (now - info_d.get("last_seen", 0)) < SLAVE_TIMEOUT
            status  = info_d.get("status", "Offline") if alive else "Offline"
            hostname= info_d.get("hostname", ip)
            cpu     = info_d.get("cpu_pct", "?")
            label   = f"{hostname}  —  {status}  (CPU {cpu}%)"
            self._entries.append((label, ip, alive, hostname))

        if not self._entries:
            no_slave = QLabel(
                "No slaves are connected.\n\n"
                "Start AE_RenderSlave.py on one or more render machines\n"
                "and make sure they can reach this manager.")
            no_slave.setStyleSheet("color:#FF9090; background:transparent;")
            no_slave.setWordWrap(True)
            lay.addWidget(no_slave)
            self._lw = None
        else:
            lay.addWidget(QLabel("Select slave machines for this job:"))
            self._lw = QListWidget()
            for label, ip, alive, hostname in self._entries:
                item = QListWidgetItem(label)
                item.setData(Qt.UserRole, ip)
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                # Pre-check if previously assigned, or check all idle ones
                was_assigned = ip in job.assigned_workers or hostname in job.assigned_workers
                default_check = was_assigned or (not job.assigned_workers and alive)
                item.setCheckState(Qt.Checked if default_check else Qt.Unchecked)
                if not alive:
                    item.setForeground(QBrush(QColor("#505868")))
                self._lw.addItem(item)
            lay.addWidget(self._lw)

            row = QHBoxLayout()
            ab = QPushButton("All");  ab.setMaximumWidth(55)
            nb = QPushButton("None"); nb.setMaximumWidth(55)
            ab.clicked.connect(lambda: [
                self._lw.item(i).setCheckState(Qt.Checked)
                for i in range(self._lw.count())])
            nb.clicked.connect(lambda: [
                self._lw.item(i).setCheckState(Qt.Unchecked)
                for i in range(self._lw.count())])
            row.addWidget(ab); row.addWidget(nb); row.addStretch()
            lay.addLayout(row)

        note = QLabel(
            "Renders run on slave machines only.  "
            "No selection = job stays Pending until a slave is available.")
        note.setStyleSheet("color:#505868; font-size:11px; background:transparent;")
        note.setWordWrap(True); lay.addWidget(note)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.button(QDialogButtonBox.Ok).setText("Assign & Render")
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def get_assigned_ips(self) -> list:
        """Returns list of slave IPs that were checked."""
        if not self._lw:
            return []
        return [self._lw.item(i).data(Qt.UserRole)
                for i in range(self._lw.count())
                if self._lw.item(i).checkState() == Qt.Checked]


class PreflightReportDialog(QDialog):
    def __init__(self, job: RenderJob, report: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Preflight Report  —  {job.comp_name}")
        self.setMinimumSize(620, 440); self.setStyleSheet(SS)
        lay = QVBoxLayout(self)

        title = QLabel(f"Plugin Preflight  —  {job.id}  ({job.comp_name})")
        title.setStyleSheet(
            "color:#7AB8FF; font-size:13px; font-weight:bold; background:transparent;")
        lay.addWidget(title)

        if not report:
            lay.addWidget(QLabel("No preflight data. Run preflight with slaves connected."))
        else:
            tbl = QTableWidget()
            tbl.setColumnCount(3)
            tbl.setHorizontalHeaderLabels(["Machine", "Plugin", "Available"])
            tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
            tbl.verticalHeader().setVisible(False)
            tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
            rows = [(m, p, ok)
                    for m, plugins in report.items()
                    for p, ok in plugins.items()]
            tbl.setRowCount(len(rows))
            for r, (machine, plugin, ok) in enumerate(rows):
                tbl.setItem(r, 0, QTableWidgetItem(machine))
                tbl.setItem(r, 1, QTableWidgetItem(plugin))
                si = QTableWidgetItem("YES" if ok else "MISSING")
                si.setForeground(QBrush(QColor("#22CC55") if ok else QColor("#FF4444")))
                tbl.setItem(r, 2, si)
                tbl.setRowHeight(r, 22)
            lay.addWidget(tbl)

        btns = QDialogButtonBox(QDialogButtonBox.Ok)
        btns.accepted.connect(self.accept); lay.addWidget(btns)


class JobDetailDialog(QDialog):
    def __init__(self, job: RenderJob, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"{APP_NAME}  —  {job.id}  ({job.comp_name})")
        self.setMinimumSize(860, 620); self.setStyleSheet(SS)
        lay = QVBoxLayout(self); lay.setSpacing(8)

        top = QHBoxLayout()

        def make_form():
            w = QWidget(); f = QFormLayout(w)
            f.setLabelAlignment(Qt.AlignRight)
            f.setHorizontalSpacing(14); f.setVerticalSpacing(5)
            return w, f

        def row(form, lbl, val):
            l = QLabel(lbl); l.setStyleSheet("color:#6A7894; background:transparent;")
            v = QLabel(str(val))
            v.setTextInteractionFlags(Qt.TextSelectableByMouse)
            v.setWordWrap(True); v.setStyleSheet("background:transparent;")
            form.addRow(l, v)

        lw, lf = make_form()
        row(lf, "Job ID",       job.id)
        row(lf, "Composition",  job.comp_name)
        row(lf, "Project",      job.project_path)
        row(lf, "Output",       job.output_path or "--")
        row(lf, "Frame Range",  job.frame_range)
        row(lf, "FPS",          f"{job.fps:.3f}")
        row(lf, "Resolution",   job.resolution)
        row(lf, "Type",         "Video (single machine)" if job.is_video else "Sequence")
        row(lf, "Submitted By", job.source_user)
        row(lf, "Machine",      job.hostname or job.source_machine)

        rw, rf = make_form()
        row(rf, "Status",    job.status)
        row(rf, "Progress",  f"{job.progress}%  (frame {job.current_frame})")
        row(rf, "Priority",  job.priority)
        row(rf, "Auto-Debug",job.auto_debug)
        row(rf, "Errors",    job.errors)
        row(rf, "Retries",   str(job.frame_retries) if job.frame_retries else "--")
        row(rf, "Submitted", job.submitted_at)
        row(rf, "Started",   job.started_at  or "--")
        row(rf, "Finished",  job.finished_at or "--")
        row(rf, "Elapsed",   job.elapsed)
        row(rf, "Workers",   getattr(job, "_display_workers", "Any"))

        top.addWidget(lw); top.addWidget(rw); lay.addLayout(top)
        sep = QFrame(); sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("background:#1E1E1E; max-height:1px;"); lay.addWidget(sep)

        ll = QLabel("Render Log")
        ll.setStyleSheet("color:#4A90D9; font-weight:bold; background:transparent;")
        lay.addWidget(ll)
        log_box = QTextEdit(); log_box.setReadOnly(True)
        log_box.setPlainText("\n".join(job.log_lines)); lay.addWidget(log_box)

        br = QHBoxLayout(); br.addStretch()
        cb = QPushButton("Close"); cb.setMinimumWidth(80); cb.clicked.connect(self.accept)
        br.addWidget(cb); lay.addLayout(br)


class PriorityDialog(QDialog):
    """Edit the priority of a job (0-10)."""
    def __init__(self, job: RenderJob, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Set Priority  —  {job.comp_name}")
        self.setFixedSize(320, 140); self.setStyleSheet(SS)
        lay = QVBoxLayout(self); lay.setSpacing(10)

        info = QLabel(f"Job: {job.comp_name}  [{job.id}]\nCurrent priority: {job.priority}")
        info.setStyleSheet("color:#8090A8; background:transparent;"); lay.addWidget(info)

        row = QHBoxLayout()
        row.addWidget(QLabel("New priority (0 = lowest, 10 = highest):"))
        self.spin = QSpinBox()
        self.spin.setRange(0, 10); self.spin.setValue(job.priority)
        self.spin.setFixedWidth(60); row.addWidget(self.spin); lay.addLayout(row)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def get_priority(self): return self.spin.value()


class FrameAssignDialog(QDialog):
    """Reassign selected frames to a specific slave machine."""
    def __init__(self, job: RenderJob, frames: list,
                 slaves: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Reassign Frames to Slave")
        self.setMinimumWidth(420); self.setStyleSheet(SS)
        self._ip = None
        lay = QVBoxLayout(self); lay.setSpacing(10)
        info = QLabel(
            f"Job    : {job.comp_name}\n"
            f"Frames : {min(frames)}-{max(frames)}  ({len(frames)} frames)")
        info.setStyleSheet("color:#8090A8; background:transparent;"); lay.addWidget(info)
        lay.addWidget(QLabel("Target slave machine:"))
        self.combo = QComboBox()
        self._ips = []
        now = time.time()
        for ip, info_d in slaves.items():
            alive    = (now - info_d.get("last_seen", 0)) < SLAVE_TIMEOUT
            hostname = info_d.get("hostname", ip)
            status   = info_d.get("status","Offline") if alive else "Offline"
            self.combo.addItem(f"{hostname}  ({status})", userData=ip)
            self._ips.append(ip)
        lay.addWidget(self.combo)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._ok); btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def _ok(self):
        self._ip = self.combo.currentData()
        self.accept()

    def get_result(self): return self._ip  # returns IP string

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN WINDOW
# ══════════════════════════════════════════════════════════════════════════════
class AERenderManager(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME}  —  After Effects Render Manager  v{MANAGER_VERSION}")
        self.setMinimumSize(1480, 920)

        self.jobs     = {}
        self.workers  = {}
        self.slaves   = {}
        self.aerender = find_aerender() or AERENDER_PATH
        self._sel_jid = None
        self._start_t = time.time()
        self._dirty   = False

        self._init_ui()
        self._load_history()
        self._start_services()

    # ── UI ────────────────────────────────────────────────────────────────────
    def _init_ui(self):
        self.setStyleSheet(SS)
        central = QWidget(); self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0); root.setSpacing(0)

        # Banner
        banner = QFrame()
        banner.setStyleSheet(
            "QFrame { background: #000000; border-bottom: 1px solid #181818; }")
        banner.setFixedHeight(40)
        bl = QHBoxLayout(banner); bl.setContentsMargins(14, 0, 14, 0)

        app_lbl = QLabel(APP_NAME)
        app_lbl.setFont(QFont("Segoe UI", 20, QFont.Bold))
        app_lbl.setStyleSheet(
            "color: #4FA8FF; background: transparent; letter-spacing: 3px;")

        sub_lbl = QLabel("After Effects Render Manager")
        sub_lbl.setStyleSheet(
            "color: #3A4458; font-size: 12px; background: transparent; padding-left: 10px;")

        self._farm_lbl = QLabel("Farm: Offline")
        self._farm_lbl.setStyleSheet(
            "color: #505870; font-size: 11px; background: transparent;")

        bl.addWidget(app_lbl); bl.addWidget(sub_lbl)
        bl.addStretch(); bl.addWidget(self._farm_lbl)
        root.addWidget(banner)

        # Menubar  (no File/Settings — config is code-only)
        mb = self.menuBar()

        vm = mb.addMenu("View")
        vm.addAction("Clear Completed", self._clear_done)

        sm = mb.addMenu("Scripts")
        sm.addAction("Approve All Pending",  self._approve_all_pending)
        sm.addAction("Retry All Failed",     self._retry_failed)
        sm.addAction("Pause All Rendering",  self._pause_all)
        sm.addAction("Resume All Paused",    self._resume_all)
        sm.addAction("Stop All",             self._stop_all)

        hm = mb.addMenu("Help")
        hm.addAction("System Info", self._show_sysinfo)
        hm.addAction("About", lambda: QMessageBox.about(
            self, f"About {APP_NAME}",
            f"{APP_NAME}  v{MANAGER_VERSION}\n\n"
            f"Distributed After Effects Render Manager\n\n"
            f"AEREN - 2026  —  Praveen Brijwal\n\n"
            f"User     : {CURRENT_USER}\n"
            f"Hostname : {LOCAL_HOSTNAME}"))

        # Toolbar  (no emojis, no Delete button)
        tb = QToolBar(); tb.setMovable(False); tb.setFloatable(False)
        self.addToolBar(tb)

        def tb_btn(label, obj_name, slot, tip=""):
            b = QPushButton(label); b.setMinimumWidth(86)
            if obj_name: b.setObjectName(obj_name)
            b.clicked.connect(slot)
            if tip: b.setToolTip(tip)
            tb.addWidget(b); return b

        self._btn_render = tb_btn("Render",   "btn_render",  self._render_selected,
                                  "Assign workers then render selected jobs")
        self._btn_pause  = tb_btn("Pause",    "btn_pause",   self._pause_selected)
        self._btn_resume = tb_btn("Resume",   "btn_resume",  self._resume_selected)
        tb_btn("Retry Failed", "",            self._retry_failed)
        self._btn_stop     = tb_btn("Stop",         "btn_stop",    self._stop_selected,
                                    "Permanently stop selected jobs")
        self._btn_priority = tb_btn("Set Priority", "",            self._set_priority_dialog,
                                    "Change priority (0-10) for selected jobs")
        tb.addSeparator()

        tb.addWidget(QLabel("  Filter: "))
        self._filter_edit = QLineEdit()
        self._filter_edit.setPlaceholderText("Filter by job name...")
        self._filter_edit.setMinimumWidth(200)
        self._filter_edit.textChanged.connect(self._filter_jobs)
        tb.addWidget(self._filter_edit)
        tb.addSeparator()

        tb.addWidget(QLabel("  Priority: "))
        self._prio_filter = QComboBox()
        self._prio_filter.addItems(["All", "0-3 Low", "4-6 Mid", "7-10 High"])
        self._prio_filter.setMinimumWidth(100)
        self._prio_filter.currentIndexChanged.connect(self._filter_jobs)
        tb.addWidget(self._prio_filter)

        root.addWidget(tb)

        # Count bar
        self._count_bar = QLabel("  Total: 0   Rendering: 0   Pending: 0")
        self._count_bar.setStyleSheet(
            "background:#000000; color:#50586A; font-size:11px;"
            " padding:2px 8px; border-bottom:1px solid #141414;")
        self._count_bar.setFixedHeight(20)
        root.addWidget(self._count_bar)

        # Main splitter
        main_spl = QSplitter(Qt.Vertical);   main_spl.setHandleWidth(2)
        top_spl  = QSplitter(Qt.Horizontal); top_spl.setHandleWidth(2)

        # Jobs table
        jp_w = QWidget(); jp = QVBoxLayout(jp_w)
        jp.setContentsMargins(0, 0, 0, 0); jp.setSpacing(0)
        jp.addWidget(section_header("JOBS"))

        self._JOB_COLS = [
            "JOB NAME", "SUBMITTED BY", "STATUS", "ERR",
            "WORKERS", "PRIORITY", "AUTO-DBG", "PROGRESS", "SUBMITTED AT"
        ]
        self.job_table = QTableWidget()
        self.job_table.setColumnCount(len(self._JOB_COLS))
        self.job_table.setHorizontalHeaderLabels(self._JOB_COLS)
        for i, w in enumerate([0, 120, 90, 38, 130, 62, 65, 160, 145]):
            if w: self.job_table.setColumnWidth(i, w)
        hh = self.job_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.Stretch)
        hh.setMinimumSectionSize(38)
        hh.setSortIndicatorShown(True)
        hh.sortIndicatorChanged.connect(self._sort_jobs)
        hh.setSortIndicator(8, Qt.DescendingOrder)
        self.job_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.job_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.job_table.setAlternatingRowColors(True)
        self.job_table.setShowGrid(True)
        self.job_table.verticalHeader().setVisible(False)
        self.job_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.job_table.customContextMenuRequested.connect(self._job_context_menu)
        self.job_table.doubleClicked.connect(self._show_job_detail_from_click)
        self.job_table.itemSelectionChanged.connect(self._on_job_selection_changed)
        jp.addWidget(self.job_table)
        top_spl.addWidget(jp_w)

        # Tasks table
        tp_w = QWidget(); tp = QVBoxLayout(tp_w)
        tp.setContentsMargins(0, 0, 0, 0); tp.setSpacing(0)
        self._tasks_hdr = section_header("TASKS  —  select a job")
        tp.addWidget(self._tasks_hdr)

        self._TASK_COLS = ["FRAME", "STATUS", "PROGRESS", "MACHINE", "RETRIES"]
        self.task_table = QTableWidget()
        self.task_table.setColumnCount(len(self._TASK_COLS))
        self.task_table.setHorizontalHeaderLabels(self._TASK_COLS)
        for i, w in enumerate([70, 86, 0, 140, 55]):
            if w: self.task_table.setColumnWidth(i, w)
        self.task_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.task_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.task_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.task_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.task_table.setAlternatingRowColors(True)
        self.task_table.setShowGrid(True)
        self.task_table.verticalHeader().setVisible(False)
        self.task_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.task_table.customContextMenuRequested.connect(self._task_context_menu)
        tp.addWidget(self.task_table)
        top_spl.addWidget(tp_w)
        top_spl.setSizes([980, 380])
        main_spl.addWidget(top_spl)

        # Workers table  (Hostname only — no IP column)
        wp_w = QWidget(); wp = QVBoxLayout(wp_w)
        wp.setContentsMargins(0, 0, 0, 0); wp.setSpacing(0)

        wh = QFrame()
        wh.setStyleSheet("QFrame{background:#000000;border-bottom:1px solid #141414;}")
        wh.setFixedHeight(26)
        whl = QHBoxLayout(wh); whl.setContentsMargins(8, 0, 8, 0)
        wh_lbl = QLabel("WORKERS")
        wh_lbl.setStyleSheet(
            "color:#7A8494;font-size:11px;font-weight:bold;background:transparent;")
        self._worker_total_lbl = QLabel("")
        self._worker_total_lbl.setStyleSheet(
            "color:#5A6474;font-size:11px;background:transparent;")
        whl.addWidget(wh_lbl); whl.addStretch(); whl.addWidget(self._worker_total_lbl)
        wp.addWidget(wh)

        self._W_COLS = ["NODE", "HOSTNAME", "STATUS", "CPU / RAM",
                        "AE VERSION", "CURRENT JOB", "LAST SEEN"]
        self.worker_table = QTableWidget()
        self.worker_table.setColumnCount(len(self._W_COLS))
        self.worker_table.setHorizontalHeaderLabels(self._W_COLS)
        for i, w in enumerate([70, 160, 86, 110, 86, 0, 72]):
            if w: self.worker_table.setColumnWidth(i, w)
        self.worker_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.worker_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.worker_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.worker_table.setAlternatingRowColors(True)
        self.worker_table.setShowGrid(True)
        self.worker_table.verticalHeader().setVisible(False)
        wp.addWidget(self.worker_table)
        main_spl.addWidget(wp_w)
        main_spl.setSizes([620, 220])
        root.addWidget(main_spl)

        # Status bar
        sb = QStatusBar(); self.setStatusBar(sb)
        self._sb_conn  = QLabel("Listening")
        self._sb_conn.setStyleSheet("color:#228844;font-size:11px;background:transparent;")
        self._sb_watch = QLabel(f"  Watch: {JOB_WATCH_DIR}")
        self._sb_watch.setStyleSheet("color:#303840;font-size:11px;background:transparent;")
        self._sb_up    = QLabel("")
        self._sb_up.setStyleSheet("color:#404858;font-size:11px;background:transparent;")
        self._sb_ver   = QLabel(f"  {APP_NAME} v{MANAGER_VERSION}  ")
        self._sb_ver.setStyleSheet("color:#303840;font-size:11px;background:transparent;")
        self._sb_user  = QLabel(f"  {CURRENT_USER}@{LOCAL_HOSTNAME}  ")
        self._sb_user.setStyleSheet("color:#3A5040;font-size:11px;background:transparent;")
        self._sb_copy  = QLabel("  AEREN 2026 — Praveen Brijwal  ")
        self._sb_copy.setStyleSheet("color:#282830;font-size:10px;background:transparent;")
        sb.addWidget(self._sb_conn); sb.addWidget(self._sb_watch)
        sb.addPermanentWidget(self._sb_up)
        sb.addPermanentWidget(self._sb_ver)
        sb.addPermanentWidget(self._sb_user)
        sb.addPermanentWidget(self._sb_copy)

    # ── SERVICES ──────────────────────────────────────────────────────────────
    def _start_services(self):
        self.watcher = JobWatcher()
        self.watcher.new_job.connect(self._on_new_job)
        self.watcher.start()

        self.http_thread = HTTPServerThread(MANAGER_PORT)
        self.http_thread.http_job.connect(self._on_new_job)
        self.http_thread.slave_update.connect(self._on_slave_update)
        self.http_thread.start()

        self.frame_watcher = FrameWatcher()
        self.frame_watcher.frame_update.connect(self._on_frame_file_update)
        self.frame_watcher.start()

        self.auto_debug_engine = AutoDebugEngine(self.jobs, self.slaves, self.aerender)
        self.auto_debug_engine.sig_log.connect(self._on_log)
        self.auto_debug_engine.sig_retry.connect(self._on_auto_retry)
        self.auto_debug_engine.sig_status.connect(self._on_status)
        self.auto_debug_engine.start()

        self._timer = QTimer(); self._timer.timeout.connect(self._tick)
        self._timer.start(1000)
        self._save_timer = QTimer()
        self._save_timer.timeout.connect(lambda: save_history(self.jobs))
        self._save_timer.start(15000)

    # ── HISTORY ───────────────────────────────────────────────────────────────
    def _load_history(self):
        data = load_history()
        data.sort(key=lambda d: d.get("submitted_epoch", 0), reverse=True)
        for d in data:
            job = RenderJob(d)
            if job.status == JS.RENDERING:
                job.status = JS.FAILED
                job.log_lines.append("[MANAGER RESTART] Interrupted — marked FAILED")
            self.jobs[job.id] = job
            self._add_job_row(job)
        self._update_counts()

    def _add_log(self, job: RenderJob, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        job.log_lines.append(f"[{ts}] {msg}")

    # ── INCOMING SIGNALS ──────────────────────────────────────────────────────
    def _on_new_job(self, data: dict):
        job = RenderJob(data)
        self.jobs[job.id] = job
        self._insert_job_row_top(job)
        self._update_counts()
        save_history(self.jobs)

    def _on_slave_update(self, msg: dict):
        # "host" = client_address[0] set by HTTP handler (the IP the request arrived from)
        host = msg.get("host", "")
        if not host: return
        msg["last_seen"] = time.time()
        # Also store slave's self-reported IP for multi-NIC / NAT environments.
        # We keep the dict keyed by client_address (most reliable for inbound connections)
        # but record reported_ip so dispatch can try it as a fallback.
        reported_ip = msg.get("ip", "")
        if reported_ip and not reported_ip.startswith("127."):
            msg["reported_ip"] = reported_ip
        # Merge with existing so we don't lose fields sent only on /register
        existing = self.slaves.get(host, {})
        existing.update(msg)
        self.slaves[host] = existing

        job_id   = msg.get("job_id", "")
        msg_type = msg.get("type",   "")
        if job_id and job_id in self.jobs:
            job = self.jobs[job_id]
            if msg_type == "JOB_DONE":
                job.status = JS.COMPLETED; job.progress = 100
                job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                hn = self._ip_to_hostname(host)
                self._add_log(job, f"Slave {hn} completed job.")
                self._update_job_row(job); self.frame_watcher.unregister(job_id)
                self._update_counts()
                # This slave is now idle — auto-dispatch next Pending job if any
                self._auto_dispatch_pending(host)

            elif msg_type == "JOB_STOPPED":
                # MGR-10: slave confirmed it stopped — ensure job reflects PAUSED/STOPPED
                if job.status == JS.RENDERING:
                    # Spontaneous stop (slave crash or OOM) — mark failed
                    self._add_log(job, f"Slave {self._ip_to_hostname(host)} stopped unexpectedly")
                    job.status = JS.FAILED
                    self._update_job_row(job); self._update_counts()
                # If job was already PAUSED by user action, leave it as PAUSED
            elif msg_type == "JOB_FAILED":
                job.errors += 1
                hn = self._ip_to_hostname(host)
                self._add_log(job, f"Slave {hn} reported FAILED.")
                if job.auto_debug:
                    # MGR-06: directly trigger retry — don't wait for AutoDebugEngine poll
                    retries = job.frame_retries.get(job.current_frame, 0)
                    if retries >= MAX_RETRIES:
                        self._add_log(job,
                            f"[AUTO-DEBUG] MAX_RETRIES={MAX_RETRIES} reached — marking FAILED")
                        job.status = JS.FAILED
                        self._update_job_row(job); self._update_counts()
                    else:
                        job.frame_retries[job.current_frame] = retries + 1
                        # Find an idle slave that is not the one that just failed
                        now2 = time.time()
                        alt = next(
                            (h for h, si in self.slaves.items()
                             if h != host and
                                si.get("status") == "Idle" and
                                (now2 - si.get("last_seen", 0)) < SLAVE_TIMEOUT),
                            None)
                        if alt:
                            self._add_log(job,
                                f"[AUTO-DEBUG] Retry {retries+1}/{MAX_RETRIES} "
                                f"-> {self._ip_to_hostname(alt)}")
                            info2 = self.slaves.get(alt, {})
                            self._on_auto_retry(job.id, job.current_frame,
                                                job.end_frame, alt)
                        else:
                            self._add_log(job,
                                "[AUTO-DEBUG] No idle slaves — marking FAILED")
                            job.status = JS.FAILED
                            self._update_job_row(job); self._update_counts()
                else:
                    job.status = JS.FAILED
                    self._update_job_row(job); self._update_counts()
                    # Slave is now idle — auto-dispatch next pending job
                    self._auto_dispatch_pending(host)
            elif msg_type == "PROGRESS":
                frame = int(msg.get("current_frame", job.current_frame))
                pct   = int(msg.get("progress",      job.progress))
                job.current_frame = frame; job.progress = pct
                job.frame_status[frame] = JS.COMPLETED
                self._update_progress_bar(job)
                self.auto_debug_engine.update_progress(job_id, frame)
                self._dirty = True

        self._refresh_workers()
        online = sum(1 for s in self.slaves.values()
                     if (time.time() - s.get("last_seen", 0)) < SLAVE_TIMEOUT)
        n = len(self.slaves)
        self._farm_lbl.setText(f"Farm: Online  {online}/{n} Nodes")
        col = "#3CBA54" if online else "#FF5555"
        self._farm_lbl.setStyleSheet(f"color:{col};font-size:11px;background:transparent;")

        # MGR-01: When a slave becomes idle/available, auto-dispatch any Pending jobs
        # that have this slave in their assigned_workers list (or have no preference).
        slave_status = msg.get("status", "")
        if slave_status == "Idle" or msg_type == "SLAVE_CONNECT":
            self._auto_dispatch_pending(host)

    def _auto_dispatch_pending(self, newly_idle_ip: str):
        """
        Called whenever a slave reports Idle or first connects.
        Finds the highest-priority Pending job assigned to (or compatible with)
        that slave and dispatches it automatically.
        """
        now = time.time()
        info = self.slaves.get(newly_idle_ip, {})
        if (now - info.get("last_seen", 0)) > SLAVE_TIMEOUT:
            return  # slave isn't actually alive

        # Collect all Pending jobs, sorted by priority descending then submitted time
        pending = [j for j in self.jobs.values() if j.status == JS.PENDING]
        if not pending:
            return
        pending.sort(key=lambda j: (-j.priority, j.submitted_epoch))

        for job in pending:
            # Check if this slave is eligible for this job
            if job.assigned_workers:
                if newly_idle_ip not in job.assigned_workers:
                    continue  # job is restricted to other slaves
            # Dispatch
            self._add_log(job,
                f"[AUTO-DISPATCH] Slave {self._ip_to_hostname(newly_idle_ip)} "
                f"became available — dispatching automatically")
            self._do_render_job(job, job.assigned_workers or [newly_idle_ip])
            return  # dispatch one job per idle event; next idle event handles next job

    def _on_frame_file_update(self, jid: str, count: int):
        job = self.jobs.get(jid)
        if not job or job.status not in (JS.RENDERING, JS.PAUSED): return
        if job.total_frames <= 0: return
        pct  = min(int(count / job.total_frames * 100), 99)
        last = job.start_frame + count - 1
        if last > job.current_frame:
            for f in range(job.current_frame, min(last + 1, job.end_frame + 1)):
                job.frame_status[f] = JS.COMPLETED
            job.current_frame = min(last, job.end_frame)
            job.progress = pct
            self._update_progress_bar(job)
        if self._sel_jid == jid: self._dirty = True

    def _on_progress(self, jid: str, frame: int, pct: int):
        job = self.jobs.get(jid)
        if not job: return
        job.current_frame = frame; job.progress = pct
        job.frame_status[frame] = JS.COMPLETED
        self._update_progress_bar(job)
        self.auto_debug_engine.update_progress(jid, frame)
        if self._sel_jid == jid: self._dirty = True

    def _on_status(self, jid: str, status: str):
        job = self.jobs.get(jid)
        if not job: return
        job.status = status
        if status in DONE_STATUSES:
            self.frame_watcher.unregister(jid)
            if status == JS.COMPLETED: job.progress = 100
        self._update_job_row(job); self._update_counts()
        self._dirty = True; save_history(self.jobs)

    def _on_log(self, jid: str, line: str):
        job = self.jobs.get(jid)
        if job: job.log_lines.append(line)

    def _on_frame_done(self, jid: str, frame: int):
        job = self.jobs.get(jid)
        if not job: return
        job.frame_status[frame] = JS.COMPLETED
        self.auto_debug_engine.update_progress(jid, frame)
        if self._sel_jid == jid: self._dirty = True

    def _on_auto_retry(self, jid: str, sf: int, ef: int, target: str):
        job = self.jobs.get(jid)
        if not job: return
        hn = self._ip_to_hostname(target)
        self._add_log(job, f"[AUTO-RETRY] frames {sf}-{ef} -> {hn}")
        # Kill current slave render if any
        if job.assigned_to and job.assigned_to != target:
            info = self.slaves.get(job.assigned_to, {})
            stop_slave_render(job.assigned_to,
                              reported_ip=info.get("reported_ip"))
        job.assigned_to = target
        job.status = JS.RENDERING; self._update_job_row(job)
        info = self.slaves.get(target, {})
        if not dispatch_to_slave(target, job, sf, ef,
                                 reported_ip=info.get("reported_ip")):
            self._add_log(job, f"[AUTO-RETRY] {hn} dispatch failed — marking FAILED")
            job.status = JS.FAILED; self._update_job_row(job); self._update_counts()

    # ── TABLE HELPERS ─────────────────────────────────────────────────────────
    def _update_progress_bar(self, job: RenderJob):
        r = job._table_row
        if r < 0 or r >= self.job_table.rowCount(): return
        bar = self.job_table.cellWidget(r, 7)
        if isinstance(bar, QProgressBar):
            bar.setValue(job.progress); bar.setFormat(f"{job.progress}%")

    def _insert_job_row_top(self, job: RenderJob):
        self.job_table.insertRow(0)
        job._table_row = 0
        for j in self.jobs.values():
            if j.id != job.id and j._table_row >= 0: j._table_row += 1
        bar = QProgressBar(); bar.setRange(0, 100)
        bar.setValue(job.progress); bar.setFormat(f"{job.progress}%")
        self.job_table.setCellWidget(0, 7, bar)
        self._update_job_row(job)

    def _add_job_row(self, job: RenderJob):
        r = self.job_table.rowCount()
        self.job_table.insertRow(r); job._table_row = r
        bar = QProgressBar(); bar.setRange(0, 100)
        bar.setValue(job.progress); bar.setFormat(f"{job.progress}%")
        self.job_table.setCellWidget(r, 7, bar)
        self._update_job_row(job)

    def _update_job_row(self, job: RenderJob):
        r = job._table_row
        if r < 0 or r >= self.job_table.rowCount(): return
        is_done = job.status in DONE_STATUSES
        is_fail = job.status == JS.FAILED
        row_fg  = GHOST_FG if is_done else ACTIVE_FG
        stat_col= QColor(STATUS_COLOR.get(job.status, "#E0E4EA"))
        if is_done: stat_col = stat_col.darker(160)

        def put(col, text, fg=None, bold=False, uid=None):
            item = self.job_table.item(r, col)
            if item is None:
                item = QTableWidgetItem(); self.job_table.setItem(r, col, item)
            item.setText(str(text)); item.setForeground(QBrush(fg if fg else row_fg))
            if uid is not None: item.setData(Qt.UserRole, uid)
            f2 = item.font(); f2.setBold(bold); item.setFont(f2)

        put(0, job.comp_name, uid=job.id)
        put(1, job.source_user)
        put(2, job.status, fg=stat_col, bold=True, uid=job.id)
        put(3, str(job.errors), fg=QColor("#FF5555") if job.errors else row_fg)
        put(4, self._ips_to_hostnames(job.assigned_workers),
            fg=QColor("#507090") if not is_done else GHOST_FG)
        put(5, str(job.priority))
        put(6, "ON" if job.auto_debug else "off",
            fg=QColor("#22AA55") if job.auto_debug else QColor("#404050"))
        put(8, job.submitted_at)
        self.job_table.setRowHeight(r, 26)

        bar = self.job_table.cellWidget(r, 7)
        if isinstance(bar, QProgressBar):
            bar.setValue(job.progress); bar.setFormat(f"{job.progress}%")
            bar.setProperty("done",   "true" if is_done else "false")
            bar.setProperty("failed", "true" if is_fail else "false")
            bar.style().unpolish(bar); bar.style().polish(bar)

        self._update_render_btn_state()

    def _update_render_btn_state(self):
        ids = self._get_selected_job_ids()
        if not ids: self._btn_render.setEnabled(False); return
        self._btn_render.setEnabled(
            any(self.jobs[j].status not in DONE_STATUSES
                for j in ids if j in self.jobs))

    def _on_job_selection_changed(self):
        rows = list({i.row() for i in self.job_table.selectedItems()})
        if not rows: self._update_render_btn_state(); return
        item = self.job_table.item(rows[0], 0)
        if not item: self._update_render_btn_state(); return
        jid = item.data(Qt.UserRole)
        if jid:
            self._sel_jid = jid
            job = self.jobs.get(jid)
            if job:
                self._tasks_hdr.setText(
                    f"TASKS  ({job.comp_name}  |  Frames {job.frame_range}  "
                    f"|  {job.total_frames} frames)")
                self._rebuild_task_pane(job)
        self._update_render_btn_state()

    # ── TASKS PANE ────────────────────────────────────────────────────────────
    def _rebuild_task_pane(self, job: RenderJob):
        total   = job.total_frames
        machine = self._ip_to_hostname(job.assigned_to) if job.assigned_to else LOCAL_HOSTNAME
        is_done = job.status in DONE_STATUSES
        self.task_table.setUpdatesEnabled(False)
        self.task_table.setRowCount(0); self.task_table.setRowCount(total)
        for idx in range(total):
            frame = job.start_frame + idx
            st, pct, mach = self._frame_state(job, frame, machine, is_done)
            retries = job.frame_retries.get(frame, 0)
            fi = QTableWidgetItem(str(frame)); fi.setData(Qt.UserRole, frame)
            self.task_table.setItem(idx, 0, fi)
            si = QTableWidgetItem(st)
            si.setForeground(QBrush(QColor(STATUS_COLOR.get(st, "#E0E4EA"))))
            f2 = si.font(); f2.setBold(True); si.setFont(f2)
            self.task_table.setItem(idx, 1, si)
            bar = QProgressBar(); bar.setRange(0, 100)
            bar.setValue(pct); bar.setFormat(f"{pct}%")
            self.task_table.setCellWidget(idx, 2, bar)
            self.task_table.setItem(idx, 3, QTableWidgetItem(mach))
            ri = QTableWidgetItem(str(retries) if retries else "")
            if retries: ri.setForeground(QBrush(QColor("#FFCC66")))
            self.task_table.setItem(idx, 4, ri)
            self.task_table.setRowHeight(idx, 22)
        self.task_table.setUpdatesEnabled(True)

    def _update_task_pane_live(self, job: RenderJob):
        if self.task_table.rowCount() != job.total_frames:
            self._rebuild_task_pane(job); return
        machine = self._ip_to_hostname(job.assigned_to) if job.assigned_to else LOCAL_HOSTNAME
        is_done = job.status in DONE_STATUSES
        self.task_table.setUpdatesEnabled(False)
        for idx in range(job.total_frames):
            frame = job.start_frame + idx
            st, pct, mach = self._frame_state(job, frame, machine, is_done)
            retries = job.frame_retries.get(frame, 0)
            si = self.task_table.item(idx, 1)
            bar = self.task_table.cellWidget(idx, 2)
            if si and si.text() == st:
                if isinstance(bar, QProgressBar): bar.setValue(pct)
                ri = self.task_table.item(idx, 4)
                if ri: ri.setText(str(retries) if retries else "")
                continue
            if si:
                si.setText(st)
                si.setForeground(QBrush(QColor(STATUS_COLOR.get(st, "#E0E4EA"))))
            if isinstance(bar, QProgressBar): bar.setValue(pct); bar.setFormat(f"{pct}%")
            mi = self.task_table.item(idx, 3)
            if mi: mi.setText(mach)
            ri = self.task_table.item(idx, 4)
            if ri: ri.setText(str(retries) if retries else "")
        self.task_table.setUpdatesEnabled(True)

    @staticmethod
    def _frame_state(job: RenderJob, frame: int, machine: str, is_done: bool):
        if job.frame_status.get(frame) == JS.COMPLETED or is_done:
            return JS.COMPLETED, 100, machine
        if job.status == JS.RENDERING:
            if frame < job.current_frame: return JS.COMPLETED, 100, machine
            if frame == job.current_frame: return JS.RENDERING, 50,  machine
        if job.status == JS.PAUSED:
            if frame < job.current_frame: return JS.COMPLETED, 100, machine
            return JS.PAUSED, 0, "--"
        if job.status == JS.FAILED:  return JS.FAILED, 0, "--"
        if job.status == JS.STOPPED: return JS.STOPPED, 0, "--"
        return "Waiting", 0, "--"

    # ── WORKERS TABLE ─────────────────────────────────────────────────────────
    def _refresh_workers(self):
        now = time.time()
        self.worker_table.setUpdatesEnabled(False)
        self.worker_table.setRowCount(0)
        total_cores = 0; total_ram = 0.0; online = 0
        for i, (host, info) in enumerate(self.slaves.items()):
            self.worker_table.insertRow(i)
            alive  = (now - info.get("last_seen", 0)) < SLAVE_TIMEOUT
            status = info.get("status", "Offline") if alive else "Offline"
            age    = f"{int(now - info.get('last_seen', now))}s"
            fg     = QColor(STATUS_COLOR.get(status, "#FF5555" if not alive else "#E0E4EA"))

            ni = QTableWidgetItem(f"NODE-{i+1:03d}")
            ni.setForeground(QBrush(QColor("#5A6478")))
            self.worker_table.setItem(i, 0, ni)
            self.worker_table.setItem(i, 1, QTableWidgetItem(info.get("hostname", host)))
            si = QTableWidgetItem(status)
            si.setForeground(QBrush(fg))
            f2 = si.font(); f2.setBold(True); si.setFont(f2)
            self.worker_table.setItem(i, 2, si)
            self.worker_table.setItem(i, 3, QTableWidgetItem(
                f"{info.get('cpu_pct','?')}% / {info.get('ram_gb','?')}GB"))
            self.worker_table.setItem(i, 4, QTableWidgetItem(
                info.get("ae_version", "--")[:14]))
            self.worker_table.setItem(i, 5, QTableWidgetItem(
                info.get("current_job", "--")))
            self.worker_table.setItem(i, 6, QTableWidgetItem(age))
            self.worker_table.setRowHeight(i, 24)
            if alive:
                online += 1
                try: total_cores += int(info.get("cpu_cores", 0))
                except: pass
                try: total_ram   += float(info.get("ram_total_gb", 0))
                except: pass
        self.worker_table.setUpdatesEnabled(True)
        self._worker_total_lbl.setText(
            f"Online: {online}/{len(self.slaves)}   "
            f"CPU: {total_cores} Cores   RAM: {total_ram:.0f} GB")

    # ── TICK ──────────────────────────────────────────────────────────────────
    def _tick(self):
        e = int(time.time() - self._start_t)
        d = e // 86400; h = (e % 86400) // 3600; m = (e % 3600) // 60; s = e % 60
        self._sb_up.setText(f"  Uptime: {d}d {h:02d}h {m:02d}m {s:02d}s")
        now = time.time()
        # Mark stale slaves offline
        newly_offline = []
        for ip, info in self.slaves.items():
            was_alive = info.get("status") != "Offline"
            if (now - info.get("last_seen", 0)) > SLAVE_TIMEOUT:
                if was_alive:
                    info["status"] = "Offline"
                    newly_offline.append(ip)
        # If a slave went offline while it owned a rendering job, handle it
        for ip in newly_offline:
            for job in self.jobs.values():
                if job.assigned_to == ip and job.status == JS.RENDERING:
                    self._add_log(job,
                        f"[WARN] Slave {self._ip_to_hostname(ip)} went offline mid-render")
                    if job.auto_debug:
                        # AutoDebugEngine will catch this on its next 15s cycle,
                        # but we can also emit immediately to reduce wait
                        pass  # let AutoDebugEngine handle it
                    else:
                        job.status = JS.FAILED
                        self._update_job_row(job)
        # Refresh worker table every 5 ticks (5 seconds) for age column
        self._tick_count = getattr(self, '_tick_count', 0) + 1
        if self._tick_count % 5 == 0:
            self._refresh_workers()
        if self._dirty and self._sel_jid:
            job = self.jobs.get(self._sel_jid)
            if job: self._update_task_pane_live(job)
        self._dirty = False

    def _update_counts(self):
        total     = len(self.jobs)
        rendering = sum(1 for j in self.jobs.values() if j.status == JS.RENDERING)
        pending   = sum(1 for j in self.jobs.values() if j.status == JS.PENDING)
        failed    = sum(1 for j in self.jobs.values() if j.status == JS.FAILED)
        self._count_bar.setText(
            f"  Total: {total}   Rendering: {rendering}   "
            f"Pending: {pending}   Failed: {failed}")

    def _filter_jobs(self):
        text     = self._filter_edit.text().lower()
        prio_idx = self._prio_filter.currentIndex()
        for row in range(self.job_table.rowCount()):
            item = self.job_table.item(row, 0)
            if not item: continue
            jid = item.data(Qt.UserRole)
            job = self.jobs.get(jid)
            if not job: self.job_table.setRowHidden(row, True); continue
            name_ok = text in job.comp_name.lower()
            if prio_idx == 1:   prio_ok = job.priority <= 3
            elif prio_idx == 2: prio_ok = 4 <= job.priority <= 6
            elif prio_idx == 3: prio_ok = job.priority >= 7
            else:               prio_ok = True
            self.job_table.setRowHidden(row, not (name_ok and prio_ok))

    # ── SORT ──────────────────────────────────────────────────────────────────
    def _sort_jobs(self, col: int, order):
        keys = {
            0: lambda j: j.comp_name.lower(),
            1: lambda j: j.source_user.lower(),
            2: lambda j: j.status.lower(),
            3: lambda j: j.errors,
            4: lambda j: ",".join(j.assigned_workers),
            5: lambda j: j.priority,
            6: lambda j: int(j.auto_debug),
            7: lambda j: j.progress,
            8: lambda j: j.submitted_epoch,
        }
        sorted_jobs = sorted(self.jobs.values(),
                             key=keys.get(col, lambda j: j.submitted_epoch),
                             reverse=(order == Qt.DescendingOrder))
        self.job_table.setUpdatesEnabled(False)
        self.job_table.setRowCount(0)
        for job in sorted_jobs:
            r = self.job_table.rowCount(); self.job_table.insertRow(r)
            job._table_row = r
            bar = QProgressBar(); bar.setRange(0, 100)
            bar.setValue(job.progress); bar.setFormat(f"{job.progress}%")
            self.job_table.setCellWidget(r, 7, bar)
            self._update_job_row(job)
        self.job_table.setUpdatesEnabled(True)

    # ── SELECTION ─────────────────────────────────────────────────────────────
    def _get_selected_job_ids(self) -> list:
        ids = set()
        for item in self.job_table.selectedItems():
            uid = item.data(Qt.UserRole)
            if uid: ids.add(uid)
        return list(ids)

    def _get_available_slaves(self) -> list:
        now = time.time()
        return [h for h, info in self.slaves.items()
                if (now - info.get("last_seen", 0)) < SLAVE_TIMEOUT]

    def _ip_to_hostname(self, ip: str) -> str:
        """Resolve a slave IP to its display hostname.
        Falls back to the IP itself if not found (should never happen in practice)."""
        if not ip or ip == "Local":
            return ip
        info = self.slaves.get(ip, {})
        return info.get("hostname", ip) or ip

    def _ips_to_hostnames(self, ips: list) -> str:
        """Convert a list of slave IPs to a comma-separated hostname string."""
        if not ips:
            return "Any"
        return ", ".join(self._ip_to_hostname(ip) for ip in ips)

    # ── PREFLIGHT (runs automatically before render) ───────────────────────────
    def _run_preflight_for_job(self, job: RenderJob) -> bool:
        """
        Run preflight on all assigned slaves.
        Returns True if all required plugins are present on all machines
        (or if there are no required effects, or no slaves assigned).
        Always shows a report if issues are found.
        """
        if not job.required_effects:
            return True

        # Use assigned_workers (IPs), fall back to all available slaves
        slave_ips = job.assigned_workers if job.assigned_workers else self._get_available_slaves()
        if not slave_ips:
            return True  # no slaves — skip preflight, job will fail at dispatch anyway

        report = {}
        effects_list = [
            e.get("matchName", e.get("displayName", "")) if isinstance(e, dict) else str(e)
            for e in job.required_effects
        ]
        self.statusBar().showMessage(f"Running preflight for {job.comp_name}...")
        QApplication.processEvents()

        missing_machines = []
        for ip in slave_ips:
            info     = self.slaves.get(ip, {})
            result   = check_slave_plugins(ip, info.get("port", SLAVE_PORT), effects_list)
            hostname = info.get("hostname", ip)
            report[hostname] = result
            if any(not ok for ok in result.values()):
                missing_machines.append(hostname)

        job.preflight_report = report
        self.statusBar().showMessage("Preflight complete.", 3000)

        if missing_machines:
            dlg = PreflightReportDialog(job, report, self)
            dlg.exec_()
            reply = QMessageBox.question(
                self, "Missing Plugins",
                f"These machines are missing required plugins:\n"
                f"{', '.join(missing_machines)}\n\n"
                f"Render anyway (job will likely fail on those machines)?",
                QMessageBox.Yes | QMessageBox.No)
            return reply == QMessageBox.Yes

        return True

    # ── RENDER ACTIONS ────────────────────────────────────────────────────────
    def _render_selected(self):
        ids = self._get_selected_job_ids()
        if not ids:
            QMessageBox.warning(self, "No Selection", "Select one or more jobs."); return
        actionable = [j for j in ids
                      if self.jobs.get(j) and self.jobs[j].status not in DONE_STATUSES]
        if not actionable:
            QMessageBox.information(self, "Nothing to Render",
                "All selected jobs are already completed or stopped."); return
        for jid in actionable:
            job = self.jobs[jid]
            dlg = AssignWorkersDialog(job, self.slaves, self)
            if dlg.exec_() != QDialog.Accepted: continue
            assigned_ips = dlg.get_assigned_ips()
            if not assigned_ips:
                QMessageBox.information(self, "No Slaves Selected",
                    "No slave machines were selected.\n"
                    "Start AE_RenderSlave.py on a render machine first."); continue
            job.assigned_workers = assigned_ips
            if not self._run_preflight_for_job(job): continue
            self._do_render_job(job, assigned_ips)

    def _do_render_job(self, job: RenderJob, assigned_ips: list):
        """
        Dispatch job to the first available slave in assigned_ips.
        Slaves only — no local rendering path.
        If a job is already rendering on a slave and workers are reassigned,
        the current slave is stopped and the job re-dispatched to the new target.
        """
        # Stop any existing render if reassigning a live job
        existing_worker = self.workers.pop(job.id, None)
        if existing_worker:
            try: existing_worker.stop()
            except: pass
        if job.assigned_to and job.assigned_to != "Local":
            stop_slave_render(job.assigned_to)

        if job.output_path:
            self.frame_watcher.register(job.id, job.output_path,
                                        job.start_frame, job.end_frame)

        # If no specific slaves assigned, use all currently idle slaves
        now = time.time()
        if not assigned_ips:
            assigned_ips = [
                h for h, info in self.slaves.items()
                if (now - info.get("last_seen", 0)) < SLAVE_TIMEOUT
                and info.get("status") == "Idle"
            ]
            if not assigned_ips:
                job.status = JS.PENDING
                self._add_log(job, "No idle slaves available — job queued as Pending")
                self._update_job_row(job); self._update_counts()
                return
        # Try each assigned slave in order until one accepts
        for ip in assigned_ips:
            info  = self.slaves.get(ip, {})
            alive = (now - info.get("last_seen", 0)) < SLAVE_TIMEOUT
            if not alive:
                self._add_log(job, f"Slave {info.get('hostname', ip)} is offline, skipping")
                continue
            self._add_log(job, f"Dispatching to {info.get('hostname', ip)}")
            if dispatch_to_slave(ip, job,
                                 reported_ip=info.get("reported_ip")):
                job.assigned_to = ip
                job.status      = JS.RENDERING
                self._update_job_row(job)
                self._update_counts()
                return
            else:
                self._add_log(job, f"Slave {ip} rejected dispatch, trying next")

        # All slaves failed
        job.status = JS.FAILED
        self._add_log(job, "All assigned slaves unreachable — job marked Failed")
        self._update_job_row(job)
        self._update_counts()

    def _pause_selected(self):
        for jid in self._get_selected_job_ids():
            job = self.jobs.get(jid)
            if not job or job.status != JS.RENDERING: continue
            if job.assigned_to:
                stop_slave_render(job.assigned_to)
                self._add_log(job, f"Pause sent to {self._ip_to_hostname(job.assigned_to)}")
            job.status = JS.PAUSED; self._update_job_row(job)
        self._update_counts()

    def _resume_selected(self):
        for jid in self._get_selected_job_ids():
            job = self.jobs.get(jid)
            if not job: continue
            if job.status == JS.PAUSED:
                # Re-dispatch from current frame to slave
                if job.assigned_to:
                    self._add_log(job, f"Resuming from frame {job.current_frame} on {self._ip_to_hostname(job.assigned_to)}")
                    if dispatch_to_slave(job.assigned_to, job,
                                         job.current_frame, job.end_frame):
                        job.status = JS.RENDERING; self._update_job_row(job)
                    else:
                        self._add_log(job, "Resume dispatch failed — slave unreachable")
                else:
                    self._add_log(job, "No slave assigned — use Render to reassign")
            elif job.status in (JS.PENDING, JS.FAILED, JS.STOPPED):
                dlg = AssignWorkersDialog(job, self.slaves, self)
                if dlg.exec_() == QDialog.Accepted:
                    assigned_ips = dlg.get_assigned_ips()
                    if assigned_ips:
                        job.assigned_workers = assigned_ips
                        if self._run_preflight_for_job(job):
                            self._do_render_job(job, assigned_ips)
        self._update_counts()

    def _stop_selected(self):
        ids = self._get_selected_job_ids()
        if not ids: return
        if QMessageBox.question(
                self, "Stop Jobs",
                f"Permanently stop {len(ids)} job(s)?\n\n"
                "The render process will be terminated. "
                "Job remains in history (edit the JSON file to remove).",
                QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        for jid in ids:
            job = self.jobs.get(jid)
            if not job: continue
            w = self.workers.pop(jid, None)
            if w:
                try: w.stop()
                except: pass
            if job.assigned_to and job.assigned_to != "Local":
                stop_slave_render(job.assigned_to)
            self.frame_watcher.unregister(jid)
            job.status = JS.STOPPED
            self._add_log(job, "Job stopped by user.")
            self._update_job_row(job)
        self._update_counts()
        save_history(self.jobs)

    def _stop_all(self):
        active = [jid for jid, j in self.jobs.items()
                  if j.status in ACTIVE_STATUSES]
        if not active: return
        if QMessageBox.question(
                self, "Stop All",
                f"Stop all {len(active)} active render(s)?",
                QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        # Stop each active job directly without relying on table selection
        for jid in active:
            job = self.jobs.get(jid)
            if not job: continue
            w = self.workers.pop(jid, None)
            if w:
                try: w.stop()
                except: pass
            if job.assigned_to and job.assigned_to != "Local":
                stop_slave_render(job.assigned_to)
            self.frame_watcher.unregister(jid)
            job.status = JS.STOPPED
            self._add_log(job, "Job stopped by Stop All.")
            self._update_job_row(job)
        self._update_counts()
        save_history(self.jobs)

    def _pause_all(self):
        for jid in list(self.jobs.keys()):
            job = self.jobs.get(jid)
            if job and job.status == JS.RENDERING:
                if job.assigned_to: stop_slave_render(job.assigned_to)
                job.status = JS.PAUSED; self._update_job_row(job)
        self._update_counts()

    def _resume_all(self):
        for jid in list(self.jobs.keys()):
            job = self.jobs.get(jid)
            if not job or job.status != JS.PAUSED: continue
            if job.assigned_to:
                if dispatch_to_slave(job.assigned_to, job,
                                     job.current_frame, job.end_frame):
                    job.status = JS.RENDERING; self._update_job_row(job)
        self._update_counts()

    def _approve_all_pending(self):
        for job in list(self.jobs.values()):
            if job.status == JS.PENDING:
                dlg = AssignWorkersDialog(job, self.slaves, self)
                if dlg.exec_() == QDialog.Accepted:
                    assigned_ips = dlg.get_assigned_ips()
                    if assigned_ips:
                        job.assigned_workers = assigned_ips
                        if self._run_preflight_for_job(job):
                            self._do_render_job(job, assigned_ips)

    def _retry_failed(self):
        for job in list(self.jobs.values()):
            if job.status in (JS.FAILED, JS.STOPPED):
                job.status       = JS.PENDING
                job.progress     = 0
                job.errors       = 0
                job.current_frame= job.start_frame
                job.frame_retries.clear()
                job.frame_status.clear()   # MGR-09: clear stale frame display
                job.started_at   = ""
                job.finished_at  = ""
                self._add_log(job, "[RETRY] Reset for re-render")
                self._update_job_row(job)
        self._update_counts()

    def _clear_done(self):
        """Remove Completed jobs from the live view and persist the change."""
        for jid in list(self.jobs.keys()):
            job = self.jobs.get(jid)
            if not job or job.status != JS.COMPLETED: continue
            if 0 <= job._table_row < self.job_table.rowCount():
                self.job_table.removeRow(job._table_row)
            del self.jobs[jid]
        # Re-index remaining rows
        for i in range(self.job_table.rowCount()):
            it = self.job_table.item(i, 0)
            if it:
                jid = it.data(Qt.UserRole)
                if jid and jid in self.jobs: self.jobs[jid]._table_row = i
        self._update_counts()
        save_history(self.jobs)  # MGR-05: persist so cleared jobs don't reappear

    def _set_priority_dialog(self):
        ids = self._get_selected_job_ids()
        if not ids: return
        job = self.jobs.get(ids[0])
        if not job: return
        dlg = PriorityDialog(job, self)
        if dlg.exec_() == QDialog.Accepted:
            for jid in ids:
                j = self.jobs.get(jid)
                if j:
                    j.priority = dlg.get_priority()
                    self._update_job_row(j)
            save_history(self.jobs)

    def _toggle_auto_debug(self, jid: str, state: bool):
        job = self.jobs.get(jid)
        if job: job.auto_debug = state; self._update_job_row(job)

    # ── PREFLIGHT (manual trigger from context menu) ──────────────────────────
    def _run_preflight_manual(self):
        ids = self._get_selected_job_ids()
        if not ids:
            QMessageBox.warning(self, "No Selection", "Select a job first."); return
        job = self.jobs.get(ids[0])
        if not job: return
        if not job.required_effects:
            QMessageBox.information(self, "Preflight",
                "No plugin requirements recorded for this job.\n"
                "(Submit from AE_Submit.jsx to capture them.)"); return
        slave_ips = self._get_available_slaves()
        if not slave_ips:
            QMessageBox.information(self, "Preflight",
                "No online slaves — connect render slaves first."); return
        effects_list = [
            e.get("matchName", e.get("displayName", "")) if isinstance(e, dict) else str(e)
            for e in job.required_effects
        ]
        report = {}
        for ip in slave_ips:
            info = self.slaves.get(ip, {})
            result = check_slave_plugins(ip, info.get("port", SLAVE_PORT), effects_list)
            report[info.get("hostname", ip)] = result
        job.preflight_report = report
        PreflightReportDialog(job, report, self).exec_()

    # ── CONTEXT MENUS ─────────────────────────────────────────────────────────
    def _job_context_menu(self, pos):
        ids = self._get_selected_job_ids()
        if not ids: return
        menu = QMenu(self)
        if len(ids) == 1:
            job = self.jobs.get(ids[0])
            if job:
                menu.addAction(
                    f"Auto-Debug: {'ON  [click to disable]' if job.auto_debug else 'OFF  [click to enable]'}",
                    lambda: self._toggle_auto_debug(job.id, not job.auto_debug))
                menu.addSeparator()
        menu.addAction("Render Selected",   self._render_selected)
        menu.addAction("Pause",             self._pause_selected)
        menu.addAction("Resume",            self._resume_selected)
        menu.addAction("Retry Failed",      self._retry_failed)
        menu.addAction("Set Priority...",   self._set_priority_dialog)
        menu.addSeparator()
        menu.addAction("Preflight Check",   self._run_preflight_manual)
        menu.addSeparator()
        menu.addAction("Open Output Folder",self._open_output_folder)
        menu.addAction("View Job Details",
                       lambda: self._show_job_detail_from_click(
                           self.job_table.currentIndex()))
        menu.addSeparator()
        menu.addAction("Stop Job",          self._stop_selected)
        menu.exec_(self.job_table.viewport().mapToGlobal(pos))

    def _task_context_menu(self, pos):
        if not self._sel_jid: return
        job = self.jobs.get(self._sel_jid)
        if not job: return
        frames = sorted({
            self.task_table.item(i.row(), 0).data(Qt.UserRole)
            for i in self.task_table.selectedItems()
            if self.task_table.item(i.row(), 0) is not None
        })
        if not frames: return
        menu = QMenu(self)
        first, last = frames[0], frames[-1]
        if len(frames) == 1:
            menu.addAction(f"Re-render frame {first}",
                           lambda f=first: self._rerender_frames(job, [f], None))
        else:
            menu.addAction(f"Re-render {len(frames)} frames  ({first}-{last})",
                           lambda fs=frames: self._rerender_frames(job, fs, None))
        menu.addSeparator()
        menu.addAction("Reassign frames to machine...",
                       lambda fs=frames: self._reassign_frames_dialog(job, fs))
        menu.exec_(self.task_table.viewport().mapToGlobal(pos))

    def _rerender_frames(self, job: RenderJob, frames: list, target_machine):
        if not frames: return
        runs = []
        run_s = run_e = frames[0]
        for f in frames[1:]:
            if f == run_e + 1: run_e = f
            else: runs.append((run_s, run_e)); run_s = run_e = f
        runs.append((run_s, run_e))
        self._add_log(job, f"Re-rendering {len(frames)} frames in {len(runs)} run(s)")
        for sf, ef in runs:
            if target_machine and target_machine != "Local":
                if not dispatch_to_slave(target_machine, job, sf, ef):
                    self._add_log(job, f"Slave {target_machine} unreachable, local fallback")
                    self._launch_local_rerender(job, sf, ef)
            else:
                self._launch_local_rerender(job, sf, ef)

    def _launch_local_rerender(self, job: RenderJob, sf: int, ef: int):
        """For re-render, dispatch to the job's currently assigned slave."""
        if job.assigned_to and job.assigned_to not in ("Local", ""):
            success = dispatch_to_slave(job.assigned_to, job, sf, ef)
            if success:
                self._add_log(job, f"Re-render dispatched to {self._ip_to_hostname(job.assigned_to)}: frames {sf}-{ef}")
                return
        # Fallback: try any available slave
        for ip in self._get_available_slaves():
            if dispatch_to_slave(ip, job, sf, ef):
                self._add_log(job, f"Re-render dispatched to {ip}: frames {sf}-{ef}")
                return
        QMessageBox.warning(self, "No Slave Available",
            f"Cannot re-render frames {sf}-{ef} — no slaves available.")

    def _reassign_frames_dialog(self, job: RenderJob, frames: list):
        dlg = FrameAssignDialog(job, frames, self.slaves, self)
        if dlg.exec_() == QDialog.Accepted:
            self._rerender_frames(job, frames, dlg.get_result())

    # ── MISC ──────────────────────────────────────────────────────────────────
    def _open_output_folder(self):
        ids = self._get_selected_job_ids()
        if not ids: return
        job = self.jobs.get(ids[0])
        if not job or not job.output_path:
            QMessageBox.information(self, "No Output Path",
                "No output path set for this job."); return
        path = os.path.dirname(job.output_path)
        if not os.path.exists(path):
            QMessageBox.warning(self, "Not Found",
                f"Directory does not exist:\n{path}"); return
        try:
            if platform.system() == "Windows": os.startfile(path)
            elif platform.system() == "Darwin": subprocess.Popen(["open", path])
            else: subprocess.Popen(["xdg-open", path])
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _show_job_detail_from_click(self, index):
        item = self.job_table.item(index.row(), 0)
        if item:
            job = self.jobs.get(item.data(Qt.UserRole))
            if job:
                # Resolve IPs to hostnames for display
                job._display_workers = self._ips_to_hostnames(job.assigned_workers)
                job._display_assigned_to = self._ip_to_hostname(job.assigned_to)
                JobDetailDialog(job, self).exec_()

    def _show_sysinfo(self):
        try: lip = socket.gethostbyname(LOCAL_HOSTNAME)
        except: lip = "unavailable"
        info = (
            f"Application  : {APP_NAME}  v{MANAGER_VERSION}\n"
            f"Hostname     : {LOCAL_HOSTNAME}\n"
            f"IP           : {lip}\n"
            f"OS           : {platform.system()} {platform.release()}\n"
            f"Python       : {sys.version}\n"
            f"Manager Port : {MANAGER_PORT}\n"
            f"Slave Port   : {SLAVE_PORT}\n"
            f"Watch Dir    : {JOB_WATCH_DIR}\n"
            f"aerender     : {self.aerender}\n"
            f"History File : {HISTORY_FILE}\n"
            f"Current User : {CURRENT_USER}\n\n"
            f"AEREN 2026 — Praveen Brijwal"
        )
        dlg = QDialog(self); dlg.setWindowTitle(f"{APP_NAME} — System Info")
        dlg.setMinimumSize(560, 340); dlg.setStyleSheet(SS)
        lay = QVBoxLayout(dlg)
        t = QTextEdit(); t.setReadOnly(True); t.setPlainText(info); lay.addWidget(t)
        b = QPushButton("Close"); b.clicked.connect(dlg.accept); lay.addWidget(b)
        dlg.exec_()

    # ── CLOSE ─────────────────────────────────────────────────────────────────
    def closeEvent(self, event):
        save_history(self.jobs)
        if _save_thread and _save_thread.is_alive():
            _save_thread.join(timeout=5.0)

        for svc in (self.watcher, self.http_thread,
                    self.frame_watcher, self.auto_debug_engine):
            try: svc.stop()
            except: pass
        # Send stop to all actively rendering slaves
        for job in self.jobs.values():
            if job.status in ACTIVE_STATUSES and job.assigned_to:
                try: stop_slave_render(job.assigned_to)
                except: pass
        event.accept()


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    apply_palette(app)
    win = AERenderManager()
    win.show()
    sys.exit(app.exec_())