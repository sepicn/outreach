import json
import os
import random
import sqlite3
import sys
import time
import threading
from datetime import datetime
from typing import Optional, Tuple

from PySide6.QtCore import Qt, QThread, Signal, Slot
from PySide6.QtGui import QAction, QIcon
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QTextEdit, QSpinBox, QCheckBox, QGroupBox, QFormLayout,
    QFileDialog, QProgressBar, QMessageBox, QInputDialog
)

from instagrapi import Client

APP_TITLE = "IG DM Outreach — GUI"
DB_PATH = "dm_sent.sqlite3"
SESSION_PATH = "insta_session.json"
SETTINGS_PATH = "settings.json"

# ---------------- DB helpers ----------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_dm (
            username TEXT PRIMARY KEY,
            user_id TEXT,
            status TEXT,
            sent_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS known_thread_participants (
            username TEXT PRIMARY KEY,
            user_id TEXT,
            seen_at TEXT
        )
    """)
    conn.commit()
    return conn

def mark_known_thread(conn, username, user_id):
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO known_thread_participants (username, user_id, seen_at) VALUES (?, ?, ?)",
        (username, str(user_id), datetime.utcnow().isoformat())
    )
    conn.commit()

def already_has_thread(conn, username):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM known_thread_participants WHERE username = ?", (username,))
    return cur.fetchone() is not None

def already_sent(conn, username):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_dm WHERE username = ?", (username,))
    return cur.fetchone() is not None

def mark_sent(conn, username, user_id, status):
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO sent_dm (username, user_id, status, sent_at) VALUES (?, ?, ?, ?)",
        (username, str(user_id), status, datetime.utcnow().isoformat())
    )
    conn.commit()

# ---------------- Worker thread ----------------
class OutreachWorker(QThread):
    # UI signals
    log = Signal(str)
    progress = Signal(int, int)       # sent_today, cap
    status = Signal(str)
    finished_ok = Signal()
    popup_error = Signal(str)

    # Ask GUI for a verification code; GUI must answer via code_entered
    code_prompt = Signal(str)         # method text to show (e.g., "EMAIL" / "SMS")
    code_entered = Signal(str)        # GUI sends the code back

    def __init__(
        self,
        username: str,
        password: str,
        daily_cap: int = 100,
        sleep_range: Tuple[int, int] = (25, 90),
        login_sleep_range: Tuple[int, int] = (1, 3),
        prefill_amount: int = 1200,
        followers_batch: int = 0,
        test_mode: bool = False,
        no_delays: bool = False
    ):
        super().__init__()
        self.username = username
        self.password = password
        self.daily_cap = daily_cap
        self.sleep_range = sleep_range
        self.login_sleep_range = login_sleep_range
        self.prefill_amount = prefill_amount
        self.followers_batch = followers_batch
        self.test_mode = test_mode
        self.no_delays = no_delays
        self._stop = False

        self._code_event = threading.Event()
        self._challenge_code = ""

        # connect signal back to slot in this thread (thread-safe)
        self.code_entered.connect(self._on_code_entered)

        self.templates = [
            "Hi {name} thanks for following If you want a short guide reply START I am here for any questions",
            "Hello {name} glad to connect If you want our short guide just send START Happy to help",
            "Hey {name} appreciate the follow If you want the guide send START I am here to help"
        ]

    def stop(self):
        self._stop = True

    # ----- utils -----
    def build_message(self, username, full_name):
        name = full_name or username
        return random.choice(self.templates).format(name=name)

    def save_session(self, cl: Client):
        try:
            settings = cl.get_settings()
            with open(SESSION_PATH, "w", encoding="utf-8") as f:
                json.dump(settings, f)
        except Exception as e:
            self.log.emit(f"[WARN] Could not save session: {e}")

    def load_session(self) -> Optional[dict]:
        if os.path.exists(SESSION_PATH):
            try:
                with open(SESSION_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None
        return None

    def get_my_user_id(self, cl: Client) -> int:
        try:
            if getattr(cl, "user_id", None):
                return int(cl.user_id)
        except Exception:
            pass
        return cl.account_info().pk

    def prefill_threads_db(self, cl: Client, conn: sqlite3.Connection, amount: int):
        if amount <= 0:
            self.log.emit("[INFO] Prefill skipped")
            return
        self.log.emit(f"[INFO] Prefilling last {amount} DM threads...")
        try:
            threads = cl.direct_threads(amount=amount)
        except Exception as e:
            self.log.emit(f"[WARN] Could not fetch threads: {e}")
            return

        t_count = 0
        u_count = 0
        for thread in threads:
            t_count += 1
            for u in thread.users:
                try:
                    if hasattr(cl, "user_id") and int(getattr(cl, "user_id", 0)) == int(u.pk):
                        continue
                except Exception:
                    pass
                mark_known_thread(conn, u.username, u.pk)
                u_count += 1
        self.log.emit(f"[INFO] Prefill done threads={t_count} users_recorded={u_count}")

    # receive code from GUI (thread-safe via signal)
    @Slot(str)
    def _on_code_entered(self, code: str):
        self._challenge_code = code.strip()
        self._code_event.set()

    # ----- main run -----
    def run(self):
        try:
            conn = init_db()
            cl = Client()

            # login delays
            cl.delay_range = (0, 0) if self.no_delays else self.login_sleep_range

            # challenge handler: emit prompt to GUI and wait for code
            def ask_code_handler(user, method):
                self._challenge_code = ""
                self._code_event.clear()
                self.code_prompt.emit(str(method))
                self.status.emit(f"Waiting for verification code via {method}…")
                # block this worker thread until GUI supplies the code
                self._code_event.wait()
                return self._challenge_code

            cl.challenge_code_handler = ask_code_handler

            # login (reuse stored session if available)
            settings = self.load_session()
            if settings:
                try:
                    cl.set_settings(settings)
                    cl.login(self.username, self.password)
                    self.log.emit("[INFO] Login with existing session OK")
                except Exception as e:
                    self.log.emit(f"[WARN] Existing session login failed: {e}")
                    cl.login(self.username, self.password)
                    self.log.emit("[INFO] Classic login OK")
            else:
                cl.login(self.username, self.password)
                self.log.emit("[INFO] Classic login OK")

            # delays after login
            cl.delay_range = (0, 0) if self.no_delays else self.sleep_range

            me_id = self.get_my_user_id(cl)
            self.log.emit(f"[INFO] Logged in as @{self.username} (id={me_id})")

            self.prefill_threads_db(cl, conn, self.prefill_amount)

            self.log.emit("[INFO] Fetching followers...")
            try:
                followers_map = cl.user_followers(me_id, amount=self.followers_batch)  # dict {pk: UserShort}
            except Exception as e:
                self.popup_error.emit(f"Could not fetch followers: {e}")
                return

            followers = list(followers_map.values())
            random.shuffle(followers)
            self.log.emit(f"[INFO] Followers found: {len(followers)}")
            self.log.emit(f"[DEBUG] First 10: {[u.username for u in followers[:10]]}")

            if not followers:
                self.popup_error.emit("Followers list is empty.")
                return

            sent_today = 0
            self.progress.emit(sent_today, self.daily_cap)

            for u in followers:
                if self._stop:
                    self.status.emit("Stopped.")
                    break

                if sent_today >= self.daily_cap:
                    self.status.emit("Daily cap reached.")
                    break

                username = u.username
                user_id = u.pk
                full_name = u.full_name
                self.log.emit(f"\n[CHECK] @{username} (id={user_id})")

                if already_has_thread(conn, username):
                    self.log.emit("  -> skip (existing chat)")
                    continue
                if already_sent(conn, username):
                    self.log.emit("  -> skip (already messaged)")
                    continue

                msg = self.build_message(username, full_name)
                self.log.emit(f"  -> message: {msg}")

                try:
                    if self.test_mode:
                        self.log.emit(f"[TEST] Would message @{username}")
                        mark_sent(conn, username, user_id, "test")
                    else:
                        cl.direct_send(msg, [user_id])
                        mark_sent(conn, username, user_id, "sent")
                        sent_today += 1
                        self.log.emit(f"[SENT] @{username} (sent_today={sent_today})")
                        self.progress.emit(sent_today, self.daily_cap)

                    if not self.no_delays:
                        time.sleep(random.randint(*self.sleep_range))

                except Exception as e:
                    self.log.emit(f"[FAIL] @{username} -> {e}")
                    mark_sent(conn, username, user_id, f"failed {e}")

            try:
                self.save_session(cl)
            except Exception:
                pass

            conn.close()
            try:
                cl.logout()
            except Exception:
                pass

            self.finished_ok.emit()

        except Exception as e:
            self.popup_error.emit(str(e))

# ---------------- GUI ----------------
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.setMinimumSize(900, 640)
        try:
            self.setWindowIcon(QIcon.fromTheme("applications-internet"))
        except Exception:
            pass

        # Inputs
        self.username = QLineEdit()
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.Password)

        self.daily_cap = QSpinBox()
        self.daily_cap.setRange(1, 10000)
        self.daily_cap.setValue(100)

        self.prefill_amount = QSpinBox()
        self.prefill_amount.setRange(0, 100000)
        self.prefill_amount.setValue(1200)

        self.followers_batch = QSpinBox()
        self.followers_batch.setRange(0, 100000)
        self.followers_batch.setValue(0)   # 0 means all

        self.test_mode = QCheckBox("Test mode (do not send)")
        self.no_delays = QCheckBox("No delays (risky)")

        form = QFormLayout()
        form.addRow("Instagram username", self.username)
        form.addRow("Instagram password", self.password)
        form.addRow("Daily cap (messages)", self.daily_cap)
        form.addRow("Prefill DM threads", self.prefill_amount)
        form.addRow("Followers batch (0 = all)", self.followers_batch)
        form.addRow(self.test_mode)
        form.addRow(self.no_delays)

        box = QGroupBox("Settings")
        box.setLayout(form)

        # Buttons
        self.btn_start = QPushButton("Start")
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setEnabled(False)

        btns = QHBoxLayout()
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)

        # Progress + status + log
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.status = QLabel("Ready.")

        self.log = QTextEdit()
        self.log.setReadOnly(True)

        layout = QVBoxLayout()
        layout.addWidget(box)
        layout.addLayout(btns)
        layout.addWidget(self.progress)
        layout.addWidget(self.status)
        layout.addWidget(self.log)
        self.setLayout(layout)

        # Context menu on log to save text
        self.log.setContextMenuPolicy(Qt.ActionsContextMenu)
        act_save = QAction("Save log to file…", self)
        act_save.triggered.connect(self.save_log)
        self.log.addAction(act_save)

        # Import/export settings
        act_export = QAction("Export settings…", self)
        act_export.triggered.connect(self.export_settings)
        act_import = QAction("Import settings…", self)
        act_import.triggered.connect(self.import_settings)
        self.addAction(act_export)
        self.addAction(act_import)

        # Signals
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)

        self.worker: Optional[OutreachWorker] = None
        self.load_settings()

    # ---------- GUI helpers ----------
    def append_log(self, text: str):
        self.log.append(text)
        self.log.ensureCursorVisible()

    def set_status(self, text: str):
        self.status.setText(text)

    def on_progress(self, sent: int, cap: int):
        self.progress.setMaximum(cap)
        self.progress.setValue(sent)
        self.set_status(f"Sent today: {sent} / {cap}")

    def ask_verification_code(self, method_text: str):
        # Modal popup to ask for code
        code, ok = QInputDialog.getText(
            self,
            "Instagram Verification",
            f"Verification via {method_text}.\nEnter the code:",
        )
        if not ok:
            code = ""  # user canceled; send empty to fail quickly
        # Send back to worker thread
        if self.worker:
            self.worker.code_entered.emit(code)

    # ---------- start/stop ----------
    def on_start(self):
        if self.worker and self.worker.isRunning():
            QMessageBox.information(self, APP_TITLE, "Worker is already running.")
            return

        username = self.username.text().strip()
        password = self.password.text()
        if not username or not password:
            QMessageBox.warning(self, APP_TITLE, "Enter username and password.")
            return

        self.save_settings()

        self.worker = OutreachWorker(
            username=username,
            password=password,
            daily_cap=int(self.daily_cap.value()),
            sleep_range=(25, 90),
            login_sleep_range=(1, 3),
            prefill_amount=int(self.prefill_amount.value()),
            followers_batch=int(self.followers_batch.value()),
            test_mode=self.test_mode.isChecked(),
            no_delays=self.no_delays.isChecked()
        )
        # Connect signals
        self.worker.log.connect(self.append_log)
        self.worker.progress.connect(self.on_progress)
        self.worker.status.connect(self.set_status)
        self.worker.finished_ok.connect(lambda: self.set_status("Done."))
        self.worker.popup_error.connect(lambda msg: QMessageBox.critical(self, APP_TITLE, msg))
        self.worker.code_prompt.connect(self.ask_verification_code)

        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.set_status("Starting…")
        self.append_log("[INFO] Start…")
        self.worker.start()

    def on_stop(self):
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            self.append_log("[INFO] Stopping…")
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)

    def closeEvent(self, event):
        try:
            if self.worker and self.worker.isRunning():
                self.worker.stop()
                self.worker.wait(1500)
        except Exception:
            pass
        event.accept()

    # ---------- settings ----------
    def save_settings(self):
        data = {
            "username": self.username.text().strip(),
            "daily_cap": int(self.daily_cap.value()),
            "prefill_amount": int(self.prefill_amount.value()),
            "followers_batch": int(self.followers_batch.value()),
            "test_mode": self.test_mode.isChecked(),
            "no_delays": self.no_delays.isChecked()
        }
        try:
            with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.append_log(f"[WARN] Could not save settings: {e}")

    def load_settings(self):
        if not os.path.exists(SETTINGS_PATH):
            return
        try:
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.username.setText(data.get("username", ""))
            self.daily_cap.setValue(int(data.get("daily_cap", 100)))
            self.prefill_amount.setValue(int(data.get("prefill_amount", 1200)))
            self.followers_batch.setValue(int(data.get("followers_batch", 0)))
            self.test_mode.setChecked(bool(data.get("test_mode", False)))
            self.no_delays.setChecked(bool(data.get("no_delays", False)))
        except Exception as e:
            self.append_log(f"[WARN] Could not load settings: {e}")

    def export_settings(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export settings", "settings.json", "JSON (*.json)")
        if not path:
            return
        try:
            with open(SETTINGS_PATH, "r", encoding="utf-8") as src:
                data = json.load(src)
            with open(path, "w", encoding="utf-8") as dst:
                json.dump(data, dst, indent=2)
            QMessageBox.information(self, APP_TITLE, "Settings exported.")
        except Exception as e:
            QMessageBox.critical(self, APP_TITLE, f"Export failed: {e}")

    def import_settings(self):
        path, _ = QFileDialog.getOpenFileName(self, "Import settings", "", "JSON (*.json)")
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            with open(SETTINGS_PATH, "w", encoding="utf-8") as dst:
                json.dump(data, dst, indent=2)
            self.load_settings()
            QMessageBox.information(self, APP_TITLE, "Settings imported.")
        except Exception as e:
            QMessageBox.critical(self, APP_TITLE, f"Import failed: {e}")

    def save_log(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save log", "log.txt", "Text (*.txt)")
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.log.toPlainText())
            QMessageBox.information(self, APP_TITLE, "Log saved.")
        except Exception as e:
            QMessageBox.critical(self, APP_TITLE, f"Could not save log: {e}")

# ---------------- main ----------------
def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
