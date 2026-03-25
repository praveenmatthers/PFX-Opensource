#!/usr/bin/env python3
"""
AEREN.py  —  AEREN Render Node  v6.10.0
=============================================
2026 - All rights reserved - Praveen Brijwal
"""

FARM_ROOT = r"\\DESKTOP-3BK9PQH\Projects\AE_RenderManager\AEREN_DATA_LOGS"
SLAVE_VERSION = "6.10.0"

import os, sys, re, json, time, socket, platform, shutil
import threading, subprocess, traceback
from datetime import datetime
from pathlib import Path

try:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QSplitter,
        QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
        QTableWidget, QTableWidgetItem, QHeaderView,
        QTextEdit, QProgressBar, QFrame, QSizePolicy,
        QAbstractItemView, QMessageBox, QMenu, QInputDialog
    )
    from PyQt5.QtCore    import Qt, QTimer, QThread, pyqtSignal
    from PyQt5.QtGui     import QColor, QBrush, QFont, QPalette, QPainter, QPixmap
except ImportError:
    print("CRITICAL ERROR: PyQt5 is not installed.")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS & UTILS
# ─────────────────────────────────────────────────────────────────────────────
JOBS_DIR    = os.path.join(FARM_ROOT, "jobs")
REFRESH_SEC = 5
TICK_MS     = 500

STATUS_PENDING   = "PENDING"
STATUS_RENDERING = "RENDERING"
STATUS_DONE      = "DONE"
STATUS_FAILED    = "FAILED"
STATUS_STOPPED   = "STOPPED"

STATUS_COLOR = {
    STATUS_PENDING:   "#555555",
    STATUS_RENDERING: "#22CC66",
    STATUS_DONE:      "#00D2DE",
    STATUS_FAILED:    "#CC2222",
    STATUS_STOPPED:   "#CC8822",
}

def find_aerender():
    for year in range(2030, 2018, -1):
        for suffix in ["", " (Beta)", " 2"]:
            p = Path(rf"C:\Program Files\Adobe\Adobe After Effects {year}{suffix}\Support Files\aerender.exe")
            if p.exists(): return str(p), str(year)
    return None, None

def get_hostname(): return socket.gethostname()

def format_size(bytes_val):
    if bytes_val < 1024: return f"{bytes_val} B"
    elif bytes_val < 1024*1024: return f"{bytes_val/1024:.1f} KB"
    else: return f"{bytes_val/(1024*1024):.1f} MB"

def fmt_time(seconds):
    seconds = max(0, int(seconds))
    h, m, s = seconds // 3600, (seconds % 3600) // 60, seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def get_frame_path(output_path, frame_num):
    if not output_path: return ""
    bracket = re.search(r'\[(#+)\]', output_path)
    if not bracket: return output_path
    pad = len(bracket.group(1))
    return re.sub(r'\[#+\]', str(frame_num).zfill(pad), output_path)

# Custom Widget for Status Circle
class StatusIndicator(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(24, 24)
        self._color = QColor("#555555")

    def setColor(self, hex_color):
        self._color = QColor(hex_color)
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setBrush(QBrush(self._color))
        painter.setPen(Qt.NoPen)
        painter.drawEllipse(2, 2, 20, 20)

# ─────────────────────────────────────────────────────────────────────────────
#  PURE BLACK PALETTE
# ─────────────────────────────────────────────────────────────────────────────
def apply_black_palette(app):
    app.setStyle("Fusion")
    p = QPalette()
    BG     = QColor("#000000")
    BG2    = QColor("#050505")
    BG3    = QColor("#0A0A0A")
    BORDER = QColor("#111111")
    FG     = QColor("#AAAAAA")
    SEL    = QColor("#1A1A1A")
    p.setColor(QPalette.Window,          BG)
    p.setColor(QPalette.WindowText,      FG)
    p.setColor(QPalette.Base,            BG2)
    p.setColor(QPalette.AlternateBase,   BG3)
    p.setColor(QPalette.Text,            FG)
    p.setColor(QPalette.Button,          BG3)
    p.setColor(QPalette.ButtonText,      FG)
    p.setColor(QPalette.Highlight,       SEL)
    p.setColor(QPalette.HighlightedText, QColor("#FFFFFF"))
    p.setColor(QPalette.Dark,            BG)
    app.setPalette(p)
    app.setStyleSheet("""
        QMainWindow, QWidget          { background:#000000; color:#CCCCCC; font-family: "Helvetica Neue", "Segoe UI", sans-serif; }
        QSplitter::handle             { background:#111111; }
        QTableWidget { 
            background:#050505; 
            gridline-color:#111111;
            border:1px solid #111111; 
            outline: 0; /* This removes the default dotted focus rect */
        }QTableWidget::item:selected {
            background: rgba(79, 115, 84, 0.15); /* #4F7354 at 15% opacity */
            border: 1px solid #4F7354;          /* Solid outline */
            color: #FFFFFF;                     /* Optional: make text bright white when selected */
        }
        QTableWidget::item            { padding:2px 4px; border:none; }
        QHeaderView::section          { background:#0A0A0A; color:#666666; border:none;
                                        padding:4px; font-size:11px; font-weight:600;
                                        border-bottom:1px solid #111111; border-right:1px solid #111111; }
        QPushButton                   { background:#0A0A0A; color:#A7D6C3; border:1px solid #222222;
                                        padding:4px 12px; font-size:11px; font-weight:600; }
        QPushButton:hover             { background:#111111; border:1px solid #444444; color:#FFFFFF; }
        QPushButton:pressed           { background:#000000; }
        QPushButton:disabled          { color:#444444; border-color:#111111; }
        QTextEdit                     { background:#050505; color:#888888; border:1px solid #111111;
                                        font-family:Consolas,monospace; font-size:11px; }
        QLabel#section_title          { color:#B0B0B0; font-size:13px; font-weight:600;
                                        letter-spacing:1px; padding:2px 0; }
        QLabel#val_label              { color:#BBBBBB; font-size:11px; }
        QFrame#divider                { background:#111111; max-height:1px; }
        QStatusBar                    { background:#000000; color:#555555; font-size:11px; border-top:1px solid #111111; }
        QScrollBar:vertical           { background:#000000; width:10px; }
        QScrollBar::handle:vertical   { background:#222222; min-height:20px; }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height:0; }
        QMenu                         { background:#0A0A0A; color:#CCCCCC; border:1px solid #222222; }
        QMenu::item:selected          { background:#1A1A1A; }
        QInputDialog                  { background:#0A0A0A; }
    """)

# ─────────────────────────────────────────────────────────────────────────────
#  JOB DATA
# ─────────────────────────────────────────────────────────────────────────────
def load_jobs():
    jobs = []
    if not os.path.isdir(JOBS_DIR): return jobs
    for fn in os.listdir(JOBS_DIR):
        if not fn.endswith(".json"): continue
        fp = os.path.join(JOBS_DIR, fn)
        try:
            with open(fp, encoding="utf-8") as f: data = json.load(f)
            data["_file"] = fp
            jobs.append(data)
        except: pass
    jobs.sort(key=lambda j: j.get("submitted_epoch", 0), reverse=True)
    return jobs

def update_job_field(job_data, key, value):
    fp = job_data.get("_file", "")
    if not fp or not os.path.exists(fp): return
    try:
        job_data[key] = value
        tmp = fp + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            d2 = {k: v for k, v in job_data.items() if k != "_file"}
            json.dump(d2, f, indent=2)
        os.replace(tmp, fp)
    except: pass

# ─────────────────────────────────────────────────────────────────────────────
#  RENDER THREAD
# ─────────────────────────────────────────────────────────────────────────────
class RenderThread(QThread):
    sig_log   = pyqtSignal(str)
    sig_frame = pyqtSignal(int)
    sig_done  = pyqtSignal(bool, str)

    def __init__(self, aerender, job, specific_frames=None, parent=None):
        super().__init__(parent)
        self.aerender   = aerender
        self.job        = job
        self.specific_frames = specific_frames
        self._stop      = threading.Event()
        self._proc      = None

    def stop(self):
        self._stop.set()
        if self._proc:
            try:
                if sys.platform == "win32":
                    subprocess.run(['taskkill', '/F', '/T', '/PID', str(self._proc.pid)], capture_output=True)
                else:
                    self._proc.kill()
            except Exception as e:
                pass

    def run(self):
        project  = self.job.get("project_path", "")
        comp     = self.job.get("comp_name", "")
        output   = self.job.get("output_path", "")
        sf       = self.job.get("start_frame", 0)
        ef       = self.job.get("end_frame", 0)
        rqi      = int(self.job.get("rq_index", 1))

        if not self.aerender:
            self.sig_done.emit(False, "aerender.exe not found.")
            return
        if not os.path.exists(project):
            self.sig_done.emit(False, f"Project not found: {project}")
            return

        if output:
            try: Path(get_frame_path(output, 0)).parent.mkdir(parents=True, exist_ok=True)
            except: pass

        rendered_by_us = set()
        frames_to_do = self.specific_frames if self.specific_frames else None

        if frames_to_do is not None:
            for f in frames_to_do:
                if self._stop.is_set(): break
                cmd = [self.aerender, "-project", project, "-comp", comp, "-rqindex", str(rqi), "-s", str(f), "-e", str(f)]
                if output: cmd += ["-output", output]
                self._run_cmd(cmd, sf, rendered_by_us, output)
        else:
            cmd = [self.aerender, "-project", project, "-comp", comp, "-rqindex", str(rqi), "-s", str(sf), "-e", str(ef)]
            if output: cmd += ["-output", output]
            self._run_cmd(cmd, sf, rendered_by_us, output)

        if self._stop.is_set():
            self.sig_done.emit(False, "Stopped by user.")
        else:
            self.sig_done.emit(True, "Render complete.")

    def _run_cmd(self, cmd, sf, rendered_set, output):
        flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        self.sig_log.emit("CMD: " + " ".join(cmd))
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, creationflags=flags)
            self._proc = proc

            for raw in iter(proc.stdout.readline, ""):
                if self._stop.is_set(): break
                line = raw.rstrip()
                if not line: continue
                self.sig_log.emit(line)

                fn = None
                m1 = re.search(r'PROGRESS:.*?\((\d+)\):', line)
                if m1: 
                    fn = int(m1.group(1))
                else:
                    m2 = re.search(r'(\d+)\s+of\s+\d+', line, re.IGNORECASE)
                    if m2: fn = int(m2.group(1)) + sf - 1

                if fn is not None:
                    if output:
                        fp = get_frame_path(output, fn)
                        if fp:
                            for _ in range(15):
                                try:
                                    if os.path.exists(fp) and os.path.getsize(fp) > 0:
                                        break
                                except: pass
                                time.sleep(0.1)

                    rendered_set.add(fn)
                    self.sig_frame.emit(fn)

            proc.wait()
        except Exception as e:
            if not self._stop.is_set():
                self.sig_log.emit(f"Process error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  UI MAIN
# ─────────────────────────────────────────────────────────────────────────────
class AERENSlave(QMainWindow):
    def __init__(self):
        super().__init__()
        self.aerender, self.ae_ver = find_aerender()
        self.hostname = get_hostname()
        self._jobs    = []
        self._sel_job = None
        self._render_thread = None
        self._frame_rows = {}
        self._render_start_t = 0.0

        # Batch Queue State
        self._render_queue = []
        self._is_rerender_failed_mode = False

        self._build_ui()
        self._refresh_jobs()

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._refresh_jobs)
        self._refresh_timer.start(REFRESH_SEC * 1000)

        self._tick_timer = QTimer(self)
        self._tick_timer.timeout.connect(self._tick)
        self._tick_timer.start(TICK_MS)

        self._size_refresh_timer = QTimer(self)
        self._size_refresh_timer.timeout.connect(self._refresh_frame_sizes)
        self._size_refresh_timer.start(5000)

    def _build_ui(self):
        self.setWindowTitle(f"AEREN v{SLAVE_VERSION}")
        self.resize(1200, 800)
        self.setMinimumSize(900, 600)

        central = QWidget(); self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        tb_widget = QWidget()
        tb_widget.setStyleSheet("background:#050505; border-bottom:1px solid #111111;")
        tb_lay = QHBoxLayout(tb_widget)
        tb_lay.setContentsMargins(30, 20, 15, 20)

        # REPLACED TEXT LOGO WITH IMAGE LOGO
        lbl_logo = QLabel()
        logo_path = "E:\AI Projects\AE_RenderManager\AEREN_LOGO.png"
        if os.path.exists(logo_path):
            pixmap = QPixmap(logo_path)
            # Scale down proportionally if it's too big, typical height for a top bar logo is around 25-30px
            lbl_logo.setPixmap(pixmap.scaledToHeight(25, Qt.SmoothTransformation))
        else:
            # Fallback text if the image isn't found at that exact path yet
            lbl_logo.setText("AEREN LOGO (Image not found at X/XXXX/XXXX.png)")
            lbl_logo.setStyleSheet("color:#666666; font-size:12px;")

        tb_lay.addWidget(lbl_logo)

        tb_lay.addStretch()

        lbl_copy = QLabel("2026 - All rights reserved - Praveen Brijwal")
        lbl_copy.setStyleSheet("color:#ADADAD; font-size:10px;")
        tb_lay.addWidget(lbl_copy)
        root_layout.addWidget(tb_widget)

        v_split = QSplitter(Qt.Vertical)

        top_split = QSplitter(Qt.Horizontal)

        left_panel = QWidget(); left_lay = QVBoxLayout(left_panel)
        left_lay.setContentsMargins(8, 8, 4, 8)

        hdr_row = QHBoxLayout()
        lbl = QLabel("JOBS"); lbl.setObjectName("section_title")
        hdr_row.addWidget(lbl)
        left_lay.addLayout(hdr_row)

        self._job_table = QTableWidget(0, 5)
        self._job_table.setHorizontalHeaderLabels(["Comp", "Frames", "Pri", "Status", "Date"])
        self._job_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._job_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)

        self._job_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._job_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._job_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._job_table.verticalHeader().setVisible(False)
        self._job_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self._job_table.customContextMenuRequested.connect(self._job_context_menu)
        self._job_table.itemSelectionChanged.connect(self._on_job_select)
        left_lay.addWidget(self._job_table)

        btn_row = QHBoxLayout()
        self._btn_render = QPushButton("RENDER")
        self._btn_render.setFixedHeight(30)
        self._btn_render.setEnabled(False)
        self._btn_render.clicked.connect(self._start_batch_render)

        self._btn_stop = QPushButton("STOP")
        self._btn_stop.setFixedHeight(30)
        self._btn_stop.setEnabled(False)
        self._btn_stop.clicked.connect(self._stop_render)

        self._btn_rerender_failed = QPushButton("Re-Render Failed Frames")
        self._btn_rerender_failed.setFixedHeight(30)
        self._btn_rerender_failed.setEnabled(False)
        self._btn_rerender_failed.clicked.connect(self._start_batch_rerender_failed)

        btn_row.addWidget(self._btn_render)
        btn_row.addWidget(self._btn_stop)
        btn_row.addWidget(self._btn_rerender_failed)
        btn_row.addStretch()
        left_lay.addLayout(btn_row)
        top_split.addWidget(left_panel)

        right_panel = QWidget(); right_lay = QVBoxLayout(right_panel)
        right_lay.setContentsMargins(8, 8, 8, 8)

        stat_box = QWidget()
        stat_box.setStyleSheet("border:1px solid #111111; background:#050505;")
        stat_lay = QHBoxLayout(stat_box)
        self._stat_dot = StatusIndicator()
        self._stat_lbl = QLabel("STATUS: IDLE")
        self._stat_lbl.setStyleSheet("font-size:24px; font-weight:700; color:#555555;")
        stat_lay.addWidget(self._stat_dot)
        stat_lay.addWidget(self._stat_lbl)
        stat_lay.addStretch()
        right_lay.addWidget(stat_box)

        right_lay.addSpacing(10)
        lbl2 = QLabel("DETAILS"); lbl2.setObjectName("section_title")
        right_lay.addWidget(lbl2)
        f = QFrame(); f.setObjectName("divider"); f.setFrameShape(QFrame.HLine)
        right_lay.addWidget(f)

        def d_row(txt):
            r = QHBoxLayout()
            l1 = QLabel(txt+":"); l1.setFixedWidth(70); l1.setStyleSheet("color:#666666;")
            l2 = QLabel("—"); l2.setObjectName("val_label"); l2.setWordWrap(True)
            r.addWidget(l1); r.addWidget(l2, 1)
            right_lay.addLayout(r)
            return l2

        self._d_comp = d_row("Comp")
        self._d_frms = d_row("Frames")
        self._d_proj = d_row("Project")
        self._d_out  = d_row("Output")
        self._d_plug = d_row("Plugins")
        self._d_user = d_row("User")
        right_lay.addStretch()
        top_split.addWidget(right_panel)
        top_split.setSizes([600, 400])

        bot_split = QSplitter(Qt.Horizontal)

        frames_panel = QWidget(); fr_lay = QVBoxLayout(frames_panel)
        fr_lay.setContentsMargins(8, 0, 4, 8)

        lbl_fr_hdr = QHBoxLayout()
        lbl4 = QLabel("RENDERED FRAMES"); lbl4.setObjectName("section_title")
        lbl_fr_hdr.addWidget(lbl4)
        lbl_fr_hdr.addStretch()

        self._sv_elapsed = QLabel("Elapsed: --:--")
        self._sv_elapsed.setStyleSheet("color:#888888; font-size:11px;")
        lbl_fr_hdr.addWidget(self._sv_elapsed)
        lbl_fr_hdr.addSpacing(15)

        self._prog_lbl = QLabel("")
        self._prog_lbl.setStyleSheet("color:#888888; font-size:11px;")
        lbl_fr_hdr.addWidget(self._prog_lbl)
        fr_lay.addLayout(lbl_fr_hdr)

        self._frame_table = QTableWidget(0, 2)
        self._frame_table.setHorizontalHeaderLabels(["Frame", "Size"])
        self._frame_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._frame_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._frame_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._frame_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._frame_table.verticalHeader().setVisible(False)
        self._frame_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self._frame_table.customContextMenuRequested.connect(self._frame_context_menu)
        fr_lay.addWidget(self._frame_table)
        bot_split.addWidget(frames_panel)

        log_panel = QWidget(); log_lay = QVBoxLayout(log_panel)
        log_lay.setContentsMargins(4, 0, 8, 8)

        lbl3 = QLabel("LOG"); lbl3.setObjectName("section_title")
        log_lay.addWidget(lbl3)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        log_lay.addWidget(self._log)
        bot_split.addWidget(log_panel)

        bot_split.setSizes([600, 400])

        v_split.addWidget(top_split)
        v_split.addWidget(bot_split)
        v_split.setSizes([350, 450])
        root_layout.addWidget(v_split)

    def _set_status(self, st):
        if st == STATUS_RENDERING:
            self._stat_dot.setColor(STATUS_COLOR[st])
            self._stat_lbl.setStyleSheet(f"font-size:24px; font-weight:700; color:{STATUS_COLOR[st]};")
            self._stat_lbl.setText("STATUS: RENDERING")
        elif st == STATUS_DONE:
            self._stat_dot.setColor("#444444")
            self._stat_lbl.setStyleSheet(f"font-size:24px; font-weight:700; color:#444444;")
            self._stat_lbl.setText("STATUS: COMPLETED")
        elif st == STATUS_FAILED:
            self._stat_dot.setColor(STATUS_COLOR[st])
            self._stat_lbl.setStyleSheet(f"font-size:24px; font-weight:700; color:{STATUS_COLOR[st]};")
            self._stat_lbl.setText("STATUS: FAILED")
        else:
            self._stat_dot.setColor("#555555")
            self._stat_lbl.setStyleSheet(f"font-size:24px; font-weight:700; color:#555555;")
            self._stat_lbl.setText("STATUS: IDLE")

    def _job_context_menu(self, pos):
        item = self._job_table.itemAt(pos)
        if not item: return

        # Determine target jobs (handle multiple selection)
        sel_rows = list(set([r.row() for r in self._job_table.selectedItems()]))
        clicked_row = item.row()

        if clicked_row not in sel_rows:
            target_indices = [self._job_table.item(clicked_row, 0).data(Qt.UserRole)]
        else:
            target_indices = [self._job_table.item(r, 0).data(Qt.UserRole) for r in sel_rows]

        jobs_to_edit = [self._jobs[i] for i in target_indices]

        menu = QMenu(self)
        a_pri  = menu.addAction("Change Priority")
        a_prop = menu.addAction("Job Properties JSON")
        a_out  = menu.addAction("Open Output Folder")
        action = menu.exec_(self._job_table.mapToGlobal(pos))

        if action == a_pri:
            curr_pri = int(jobs_to_edit[0].get("priority", 50))
            title = "Change Priority" if len(jobs_to_edit) == 1 else f"Change Priority ({len(jobs_to_edit)} jobs)"
            new_pri, ok = QInputDialog.getInt(self, title, "Enter new priority (0-10):", curr_pri, 0, 10, 1)
            if ok:
                for j in jobs_to_edit:
                    update_job_field(j, "priority", new_pri)
                self._refresh_jobs()
        elif action == a_out:
            d = jobs_to_edit[0].get("output_folder", "")
            if os.path.exists(d): os.startfile(d)
        elif action == a_prop:
            QMessageBox.information(self, "Properties", json.dumps(jobs_to_edit[0], indent=2))

    def _frame_context_menu(self, pos):
        item = self._frame_table.itemAt(pos)
        if not item: return
        fnum = self._frame_table.item(item.row(), 0).data(Qt.UserRole)

        menu = QMenu(self)
        a_rerender = menu.addAction(f"Delete & Re-render Frame {fnum}")
        action = menu.exec_(self._frame_table.mapToGlobal(pos))

        if action == a_rerender:
            self._re_render_single(fnum)

    def _re_render_single(self, fnum):
        if self._is_rendering(): return
        if not self._sel_job: return

        out = self._sel_job.get("output_path", "")
        fp = get_frame_path(out, fnum)
        try: os.remove(fp)
        except: pass

        self._log.append(f"\n--- MANUAL RE-RENDER FRAME {fnum} ---")
        self._run_job(self._sel_job, [fnum])

    def _refresh_jobs(self):
        if self._is_rendering(): 
            return

        sel_ids = []
        rows = self._job_table.selectedItems()
        if rows:
            unique_rows = set([r.row() for r in rows])
            for r in unique_rows:
                idx = self._job_table.item(r, 0).data(Qt.UserRole)
                sel_ids.append(self._jobs[idx].get("job_id"))

        self._jobs = load_jobs()

        scroll_val = self._job_table.verticalScrollBar().value()

        self._job_table.blockSignals(True)
        self._job_table.setRowCount(0)

        for i, j in enumerate(self._jobs):
            self._job_table.insertRow(i)
            sf, ef = j.get("start_frame",0), j.get("end_frame",0)
            pri = str(j.get("priority", 50))
            st = j.get("status", STATUS_PENDING)

            c_comp = QTableWidgetItem(j.get("comp_name",""))
            c_comp.setData(Qt.UserRole, i)
            c_frms = QTableWidgetItem(f"{sf}-{ef}")
            c_pri  = QTableWidgetItem(pri)
            c_stat = QTableWidgetItem(st)
            c_stat.setForeground(QBrush(QColor(STATUS_COLOR.get(st, "#AAAAAA"))))
            dt = j.get("submitted_at", "").split(" ")[0]
            c_date = QTableWidgetItem(dt)

            for col, it in enumerate([c_comp, c_frms, c_pri, c_stat, c_date]):
                self._job_table.setItem(i, col, it)

        if sel_ids:
            for i, j in enumerate(self._jobs):
                if j.get("job_id") in sel_ids:
                    for col in range(5):
                        self._job_table.item(i, col).setSelected(True)

        self._job_table.verticalScrollBar().setValue(scroll_val)
        self._job_table.blockSignals(False)

        if not self._is_rendering():
            self._on_job_select()

    def _on_job_select(self):
        rows = self._job_table.selectedItems()
        if not rows:
            self._sel_job = None
            for l in [self._d_comp, self._d_frms, self._d_proj, self._d_out, self._d_plug, self._d_user]: l.setText("—")
            self._btn_render.setEnabled(False)
            self._btn_rerender_failed.setEnabled(False)
            self._frame_table.setRowCount(0)
            self._frame_rows.clear()
            return

        unique_rows = list(set([r.row() for r in rows]))

        if len(unique_rows) > 1:
            self._sel_job = None
            self._d_comp.setText(f"[{len(unique_rows)} Jobs Selected for Batch]")
            for l in [self._d_frms, self._d_proj, self._d_out, self._d_plug, self._d_user]: l.setText("—")
            self._frame_table.setRowCount(0)
            self._frame_rows.clear()

            is_r = self._is_rendering()
            self._btn_render.setEnabled(not is_r)
            self._btn_rerender_failed.setEnabled(not is_r)

        else:
            idx = self._job_table.item(unique_rows[0], 0).data(Qt.UserRole)
            self._sel_job = self._jobs[idx]
            j = self._sel_job

            self._d_comp.setText(j.get("comp_name", ""))
            self._d_frms.setText(f"{j.get('start_frame')} - {j.get('end_frame')}")
            self._d_proj.setText(j.get("project_path", ""))
            self._d_out.setText(j.get("output_path", ""))

            pl = j.get("required_plugins", [])
            self._d_plug.setText(", ".join([p.get("displayName","") if isinstance(p,dict) else str(p) for p in pl]) if pl else "None")
            self._d_user.setText(f"{j.get('submitted_by')} @ {j.get('submitted_at')}")

            is_r = self._is_rendering()
            self._btn_render.setEnabled(not is_r)
            self._btn_rerender_failed.setEnabled(not is_r)

            self._scan_selected_job_frames()

    def _scan_selected_job_frames(self):
        if not self._sel_job: return
        j = self._sel_job
        sf, ef = j.get("start_frame",0), j.get("end_frame",0)
        out = j.get("output_path", "")
        if not out: return

        if not self._is_rendering():
            self._frame_table.setUpdatesEnabled(False)
            self._frame_table.setRowCount(0)
            self._frame_rows.clear()

            done_count = 0
            for f_idx in range(sf, ef + 1):
                fp = get_frame_path(out, f_idx)
                p = Path(fp)
                if p.exists():
                    sz = p.stat().st_size
                    if sz > 0: done_count += 1
                    r = self._frame_table.rowCount()
                    self._frame_table.insertRow(r)

                    i0 = QTableWidgetItem(str(f_idx)); i0.setData(Qt.UserRole, f_idx)
                    i1 = QTableWidgetItem(format_size(sz) if sz > 0 else "0 KB")
                    if sz == 0: i1.setForeground(QBrush(QColor("#CC2222")))

                    self._frame_table.setItem(r, 0, i0)
                    self._frame_table.setItem(r, 1, i1)
                    self._frame_rows[f_idx] = r

            self._frame_table.setUpdatesEnabled(True)
            self._frame_table.scrollToBottom()
            tot = max(ef - sf + 1, 1)
            pct = (done_count / tot) * 100
            self._prog_lbl.setText(f"{done_count}/{tot} ({pct:.1f}%)")

    def _refresh_frame_sizes(self):
        if not self._sel_job: return
        out = self._sel_job.get("output_path", "")
        if not out: return

        for f_idx, r in self._frame_rows.items():
            fp = get_frame_path(out, f_idx)
            try:
                sz = os.path.getsize(fp)
                sz_str = format_size(sz) if sz > 0 else "0 KB"
                self._frame_table.item(r, 1).setText(sz_str)
                if sz == 0:
                    self._frame_table.item(r, 1).setForeground(QBrush(QColor("#CC2222")))
                else:
                    self._frame_table.item(r, 1).setForeground(QBrush(QColor("#CCCCCC")))
            except:
                pass

        sf, ef = self._sel_job.get("start_frame",0), self._sel_job.get("end_frame",0)
        tot = max(ef - sf + 1, 1)
        done_count = sum(1 for row in range(self._frame_table.rowCount()) if self._frame_table.item(row, 1).text() != "0 KB")
        pct = (done_count / tot) * 100
        self._prog_lbl.setText(f"{done_count}/{tot} ({pct:.1f}%)")

    def _is_rendering(self):
        return self._render_thread and self._render_thread.isRunning()

    # ── BATCH LOGIC ──
    def _start_batch_render(self):
        self._init_batch_queue(is_rerender_failed=False)

    def _start_batch_rerender_failed(self):
        self._init_batch_queue(is_rerender_failed=True)

    def _init_batch_queue(self, is_rerender_failed):
        if self._is_rendering(): return

        rows = self._job_table.selectedItems()
        unique_rows = list(set([r.row() for r in rows]))
        if not unique_rows: return

        indices = [self._job_table.item(r, 0).data(Qt.UserRole) for r in unique_rows]
        jobs_to_render = [self._jobs[i] for i in indices]

        jobs_to_render.sort(key=lambda j: (int(j.get("priority", 50)), -int(j.get("submitted_epoch", 0))), reverse=True)

        all_plugins = {}
        for j in jobs_to_render:
            for p in j.get("required_plugins", []):
                pname = p.get("displayName","") if isinstance(p,dict) else str(p)
                if pname: all_plugins[pname] = True

        if all_plugins:
            msg = "This batch requires the following plugins:\n\n" + "\n".join("- " + n for n in all_plugins.keys())
            msg += "\n\nAre you sure these are installed on this render node?"
            reply = QMessageBox.question(self, "Plugin Pre-Flight Check", msg, QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply != QMessageBox.Yes: return

        self._log.clear()
        self._log.append(f"--- BATCH QUEUE INITIALIZED: {len(jobs_to_render)} JOBS ---\n")

        self._render_queue = jobs_to_render
        self._is_rerender_failed_mode = is_rerender_failed

        self._job_table.setEnabled(False)
        self._btn_render.setEnabled(False)
        self._btn_rerender_failed.setEnabled(False)
        self._btn_stop.setEnabled(True)

        self._process_render_queue()

    def _process_render_queue(self):
        if not self._render_queue:
            self._set_status(STATUS_IDLE)
            self._job_table.setEnabled(True)
            self._btn_render.setEnabled(True)
            self._btn_rerender_failed.setEnabled(True)
            self._btn_stop.setEnabled(False)
            self._log.append("\n--- BATCH QUEUE COMPLETE ---")
            self._refresh_jobs()
            return

        self._sel_job = self._render_queue.pop(0)
        j = self._sel_job

        self._d_comp.setText(j.get("comp_name", ""))
        self._d_frms.setText(f"{j.get('start_frame')} - {j.get('end_frame')}")
        self._d_proj.setText(j.get("project_path", ""))
        self._d_out.setText(j.get("output_path", ""))
        pl = j.get("required_plugins", [])
        self._d_plug.setText(", ".join([p.get("displayName","") if isinstance(p,dict) else str(p) for p in pl]) if pl else "None")
        self._d_user.setText(f"{j.get('submitted_by')} @ {j.get('submitted_at')}")

        self._log.append(f"\n=============================================")
        self._log.append(f"STARTING: {j.get('comp_name')} [Pri: {j.get('priority', 50)}]")
        self._log.append(f"=============================================")

        self._scan_selected_job_frames()

        if self._is_rerender_failed_mode:
            sf, ef = j.get("start_frame",0), j.get("end_frame",0)
            out = j.get("output_path", "")
            missing = []
            if out:
                for f_idx in range(sf, ef + 1):
                    fp = get_frame_path(out, f_idx)
                    p = Path(fp)
                    if p.exists() and p.stat().st_size == 0:
                        try: os.remove(fp); missing.append(f_idx)
                        except: pass
                    elif not p.exists():
                        missing.append(f_idx)

            if not missing:
                self._log.append(f"All frames complete. No 0KB or missing frames found.")
                QTimer.singleShot(100, self._process_render_queue)
                return

            self._log.append(f"Re-rendering {len(missing)} missing/failed frames...")
            self._run_job(j, missing)

        else:
            self._run_job(j)

    def _run_job(self, job_dict, specific_frames=None):
        self._render_start_t = time.time()
        self._sv_elapsed.setText("Elapsed: 00:00")
        self._set_status(STATUS_RENDERING)
        update_job_field(job_dict, "status", STATUS_RENDERING)

        self._render_thread = RenderThread(self.aerender, job_dict, specific_frames, self)
        self._render_thread.sig_log.connect(self._on_log)
        self._render_thread.sig_frame.connect(self._on_frame)
        self._render_thread.sig_done.connect(self._on_render_done)
        self._render_thread.start()

    def _stop_render(self):
        self._render_queue.clear()
        if self._is_rendering():
            self._render_thread.stop()
            self._btn_stop.setEnabled(False)
            self._set_status(STATUS_STOPPED)

    def _on_log(self, txt):
        self._log.append(txt)
        sb = self._log.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _on_frame(self, fn):
        out = self._sel_job.get("output_path", "")
        fp = get_frame_path(out, fn)
        sz = 0
        try: sz = os.path.getsize(fp)
        except: pass

        sz_str = format_size(sz) if sz > 0 else "0 KB"

        if fn in self._frame_rows:
            r = self._frame_rows[fn]
            self._frame_table.item(r, 1).setText(sz_str)
            if sz == 0: 
                self._frame_table.item(r, 1).setForeground(QBrush(QColor("#CC2222")))
            else:
                self._frame_table.item(r, 1).setForeground(QBrush(QColor("#CCCCCC")))
        else:
            r = self._frame_table.rowCount()
            self._frame_table.insertRow(r)
            i0 = QTableWidgetItem(str(fn)); i0.setData(Qt.UserRole, fn)
            self._frame_table.setItem(r, 0, i0)
            i1 = QTableWidgetItem(sz_str)
            if sz == 0: i1.setForeground(QBrush(QColor("#CC2222")))
            self._frame_table.setItem(r, 1, i1)
            self._frame_rows[fn] = r
            self._frame_table.scrollToBottom()

    def _on_render_done(self, ok, msg):
        if self._sel_job:
            st = STATUS_DONE if ok else STATUS_FAILED
            update_job_field(self._sel_job, "status", st)
            self._set_status(st)

        self._log.append(f"\n{'[OK]' if ok else '[FAIL]'} {msg}")
        self._render_thread = None

        self._refresh_frame_sizes()
        self._scan_selected_job_frames()

        if self._render_queue:
            QTimer.singleShot(500, self._process_render_queue)
        else:
            self._job_table.setEnabled(True)
            self._btn_render.setEnabled(True)
            self._btn_rerender_failed.setEnabled(True)
            self._btn_stop.setEnabled(False)
            self._log.append("\n--- BATCH QUEUE COMPLETE ---")
            self._refresh_jobs()

    def _tick(self):
        if not self._is_rendering(): return
        el = time.time() - self._render_start_t
        self._sv_elapsed.setText(f"Elapsed: {fmt_time(el)}")

    def closeEvent(self, ev):
        if self._is_rendering():
            if QMessageBox.question(self, "Quit", "Stop render and quit?", QMessageBox.Yes|QMessageBox.No) == QMessageBox.Yes:
                self._stop_render()
                time.sleep(0.5)
                ev.accept()
            else: ev.ignore()
        else: ev.accept()

def main():
    app = QApplication(sys.argv)
    apply_black_palette(app)
    win = AERENSlave()
    win.show()
    sys.exit(app.exec_())

if __name__ == "__main__": main()
