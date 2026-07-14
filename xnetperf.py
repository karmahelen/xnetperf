# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "flask",
#     "pywebview",
#     "qtpy",
#     "PyQt6",
#     "PyQt6-WebEngine",
# ]
# ///

"""
xnetperf — Hearth app for characterizing TCP throughput between the server
hosting this app and the client (browser) running it.

The client is a browser, so there are no raw sockets to play with the way
iperf does — this is a speed-test-style tool built on HTTP. Throughput is
only meaningful in serve mode with the client on a *different* device; in
local/loopback mode the numbers measure memory bandwidth, not the network,
and the UI flags that.

Two directions, measured sequentially:

  Download (server -> client): a single continuous GET stream of
    incompressible bytes. The client reads the stream, sums bytes against a
    monotonic clock, discards a slow-start warmup window, and aborts at the
    target duration. One continuous stream means no inter-request dead air,
    so download reads at/near link max.

  Upload (client -> server): the browser can't stream a time-bounded request
    body over HTTP/1.1, and materializing a duration's worth of bytes at
    gigabit is too much RAM, so upload is a loop of back-to-back POSTs of one
    reused ~16 MB incompressible block. The server drains request.stream and
    discards it (never touching disk). Slightly below link max by the small
    per-request turnaround; that's the method showing through, expected.

Framework dependency: this app uses Hearth's raw_ route primitive (pass-
through GET/POST at /raw/<name>) and opts into threaded serve mode.

Run:
    [uv run] xnetperf.py                 # native window
    [uv run] xnetperf.py --serve [port]  # LAN web access

Developer:  KarmaHelen
Contact:    xnetperf.puppet866@passinbox.com
Support:    https://buymeacoffee.com/karmahelen
"""

import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR.parent))

from hearth import run
from flask import Response, request as flask_request

# --- Methodology defaults -------------------------------------------------
# Centralized here (not scattered as JS constants) so the measurement is
# tunable from one place and the client fetches them at load via get_settings.
DEFAULTS = {
    "warmup_s": 1.0,        # discarded slow-start window per direction
    "measure_s": 6.0,       # steady-state measurement window per direction
    "gauge_interval_ms": 250,  # sample window: client (download) and server (upload)
    "up_probe_mb": 1,       # initial probe POST size during upload warmup
    "up_post_target_s": 0.5,  # measurement POSTs sized to land near this duration
    "up_chunk_mb": 32,      # prebuilt upload block = max measurement chunk
                            # (32 MiB so gigabit POSTs span several sample
                            # windows; built once client-side at page load)
    "down_cap_s": 120,      # ENFORCED server ceiling on a download stream's
                            # duration (was a dead fallback the client overrode;
                            # now the server clamps the client's cap_s to it).
                            # Sized to cover a long measure_s across parallel
                            # flows; the client still asks for only what it needs.
    # --- direction / concurrency (throughput tab) ---
    "parallel_streams": 1,  # concurrent flows per direction, 1-16. One TCP flow
                            # is bounded by its own window/RTT (the BDP); N flows
                            # fill a pipe a single flow can't — the iperf -P axis.
    "dir_down": True,       # run the download direction
    "dir_up": True,         # run the upload direction
    "bidirectional": False, # run both directions AT ONCE (full-duplex) instead
                            # of sequentially — exposes half- vs full-duplex
                            # (Wi-Fi shares airtime; a wired switch does not).
    "down_yield_kb": 256,   # server socket yield slice for download, 16-1024 KiB.
                            # Sweeps per-syscall granularity vs the Python/
                            # Werkzeug throughput ceiling.
    # --- stream safety (contention) test ---
    "idle_s": 3.0,          # phase A: latency baseline, nothing else running
    "stream_only_s": 6.0,   # phase B: paced stream alone (must be clean)
    "loaded_s": 9.0,        # phase C: paced stream + greedy bulk (the verdict)
    "probe_interval_ms": 200,   # latency probe cadence (runs through all phases)
    "margin_sample_ms": 250,    # buffer-margin / chart sample cadence
    "stream_cap_s": 45,     # server-side safety cap on a paced stream
    # --- iperf reference server (Iperf tab) ---
    # These become flags on the `iperf3 -s` subprocess, assembled at start time
    # by _iperf_build_cmd(). Applied on next Start (baked into the process).
    "iperf_port": 5201,             # -p: server listen port
    "iperf_interval": 1.0,          # -i: seconds between server reports (chart cadence)
    "iperf_verbose": False,         # -V: extra detail (CPU, retransmits) in the summary
    "iperf_forceflush": True,       # --forceflush: flush output every interval (real-time)
    "iperf_idle_timeout": 0,        # --idle-timeout seconds, restart if stuck (0 = off)
    "iperf_bind": "",               # -B address ("" = all interfaces)
    "iperf_affinity": "",           # -A core(s) ("" = none)
    # --- Multi Client tab ---
    "mc_duration_s": 6.0,           # how long a coordinated group test runs
    "mc_lat_good_ms": 30.0,         # latency band: at/below this reads as healthy
    "mc_lat_bad_ms": 100.0,         # latency band: at/above this reads as bad
}

# Validation bounds for every tunable setting: (min, max). The server is the
# authority — values are clamped to these on save, so a hand-edited JSON file
# or a misbehaving client can't push the methodology outside physically
# meaningful ranges. The client UI guides with these same bounds (served via
# get_settings) so there is a single source of truth for the ranges.
SETTING_BOUNDS = {
    "warmup_s":          (0.0, 5.0),
    "measure_s":         (1.0, 60.0),
    "gauge_interval_ms": (50, 1000),
    "up_probe_mb":       (1, 64),
    "up_post_target_s":  (0.1, 5.0),
    "up_chunk_mb":       (8, 256),
    "down_cap_s":        (10, 300),
    "parallel_streams":  (1, 16),
    "down_yield_kb":     (16, 1024),
    # stream-tab knobs (tunable too; bounds here for completeness)
    "idle_s":            (0.0, 30.0),
    "stream_only_s":     (1.0, 60.0),
    "loaded_s":          (1.0, 60.0),
    "probe_interval_ms": (50, 2000),
    "margin_sample_ms":  (50, 2000),
    "stream_cap_s":      (10, 300),
    # iperf server knobs
    "iperf_port":               (1024, 65535),
    "iperf_interval":           (0.5, 10.0),
    "iperf_idle_timeout":       (0, 86400),
    # Multi Client
    "mc_duration_s":            (1.0, 300.0),
    "mc_lat_good_ms":           (1.0, 10000.0),
    "mc_lat_bad_ms":            (1.0, 10000.0),
}
# Settings stored/validated as booleans rather than clamped numbers.
BOOL_SETTINGS = {"dir_down", "dir_up", "bidirectional",
                 "iperf_verbose", "iperf_forceflush"}
# Settings stored/validated as free strings (empty = unset/off): trimmed and
# length-capped. iperf itself is the backstop for a malformed value — a bad
# -B/-A just makes the server fail to start and print the error to the output box.
STR_SETTINGS = {"iperf_bind", "iperf_affinity"}
_STR_MAXLEN = 64

# Persisted user overrides live here as a DIFF from DEFAULTS (forward-compatible:
# knobs added to DEFAULTS later appear automatically; the file only ever holds
# the keys the user actually changed). Hearth excludes .json from static serving,
# so this never leaks over HTTP.
SETTINGS_FILENAME = "xnetperf.json"

# Stream-safety profiles: what each real flow needs. rate_mbps is the paced
# delivery rate; buffer_s models the player's prebuffer. Surfaced to the
# client via get_settings; "custom" is client-side (user-entered fields).
PROFILES = {
    "audio":      {"label": "Audio",      "rate_mbps": 2.0,  "buffer_s": 1.0},
    "video_low":  {"label": "Video Low",  "rate_mbps": 5.0,  "buffer_s": 3.0},
    "video_mid":  {"label": "Video Mid",  "rate_mbps": 20.0, "buffer_s": 4.0},
    "video_high": {"label": "Video High", "rate_mbps": 50.0, "buffer_s": 5.0},
}

# Size of the in-memory incompressible block streamed for download. Generated
# once at startup with os.urandom (incompressible -> defeats any compression),
# then cycled. Held in RAM, never read from disk (which would measure the ZFS
# pool instead of the wire).
_DOWNLOAD_BLOCK_BYTES = 4 * 1024 * 1024     # 4 MiB
_DOWNLOAD_YIELD_BYTES = 256 * 1024          # slice size yielded to the socket


class Xnetperf:
    def __init__(self):
        self.db_path = BASE_DIR / "xnetperf.db"
        self.settings_path = BASE_DIR / SETTINGS_FILENAME
        self._init_db()
        # Cached, sanitized user overrides (diff from DEFAULTS). Loaded once at
        # startup and refreshed on save/reset, so request handlers that consume
        # a setting (e.g. the download cap ceiling) read it without touching disk
        # per request.
        self._overrides = self._sanitize_settings(self._load_overrides())
        # One incompressible block, generated once and reused for every
        # download stream. Cheap to cycle; never regenerated per request.
        self._block = os.urandom(_DOWNLOAD_BLOCK_BYTES)
        # iperf3 reference server (Iperf tab): a long-running `iperf3 -s` whose
        # merged stdout/stderr is drained by a reader thread into a bounded
        # ring buffer of (id, text). Monotonic ids let any number of browser
        # viewers poll one shared server, each tracking its own cursor.
        self._iperf_proc = None
        self._iperf_reader = None
        self._iperf_lines = deque(maxlen=1000)
        self._iperf_next_id = 0
        self._iperf_lock = threading.Lock()
        self._iperf_active_port = None   # port/bind the running server was started
        self._iperf_active_bind = ""     # with (may differ from current settings)

        # --- Multi Client tab: cross-client coordination (serve mode) --------
        # The server is the single source of truth for who's connected, each
        # client's stream config, and the shared test state. Clients self-
        # register, then poll (heartbeat + roster + test state) on an interval.
        # All state is in-memory and ephemeral: a restart empties it and clients
        # re-register. Lock-guarded because Flask runs threaded.
        self._mc_lock = threading.Lock()
        self._mc_clients = {}            # id -> {ip,name,last_seen,dl{on,mbps},ul{on,mbps},lat}
        self._mc_next_id = 0
        # _mc_test gains a `plan` (config snapshot taken at Run, so mid-run roster
        # edits don't change what's running) and a richer lifecycle: idle →
        # running → complete (the last run's results are retained until the next
        # Run). _mc_results holds the per-client sample series for the current
        # run_id; clients measure at the edge and report batches into it.
        self._mc_test = {"state": "idle", "start_at": 0.0, "duration_s": 0.0,
                         "description": "", "run_id": 0, "plan": {}}
        self._mc_results = {}            # client_id -> {dl:[{t,v}], ul:[...], lat:[...]}
    _MC_PRUNE_S = 10.0                   # drop a client unseen this long (tab closed)
    _MC_START_LEAD_S = 2.0               # countdown lead so clients begin together
    _MC_MAX_SAMPLES = 5000              # per series, per client — safety cap

    # --- Database ---------------------------------------------------------
    def _connect(self):
        # Connection-per-request (Hearth rule) keeps threaded serve mode safe.
        # busy_timeout converts the brief writer-vs-writer overlaps that
        # threading makes possible from instant `database is locked` errors
        # into a short wait-then-succeed. Set per connection (it is not a
        # property of the file), so it touches only xnetperf's own DB.
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=5000")
        return con

    @contextmanager
    def _db(self):
        con = self._connect()
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as con:
            con.execute("PRAGMA busy_timeout=5000")
            con.execute("""
                CREATE TABLE IF NOT EXISTS results (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts             INTEGER NOT NULL,   -- epoch seconds (UTC)
                    down_mbps      REAL,
                    up_mbps        REAL,
                    down_peak_mbps REAL,
                    up_peak_mbps   REAL,
                    duration_s     REAL,               -- measure window per direction
                    client_ip      TEXT,
                    client_agent   TEXT,
                    mode           TEXT,               -- 'network' | 'loopback'
                    description    TEXT,               -- optional user label (NULL if blank)
                    streams        INTEGER,            -- parallel flows per direction
                    bidir          INTEGER,            -- 1 if down+up ran simultaneously
                    direction      TEXT                -- 'down' | 'up' | 'both'
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS stream_results (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts             INTEGER NOT NULL,
                    kind           TEXT,    -- 'contention' | 'stream' (solo, future)
                    profile        TEXT,    -- 'audio'|'video_low'|'video_mid'|'video_high'|'custom'
                    rate_mbps      REAL,    -- paced stream rate
                    buffer_s       REAL,    -- modeled player buffer
                    bulk_dir       TEXT,    -- 'down'|'up'  (NULL for solo / 'none' runs)
                    verdict        TEXT,    -- 'clean'|'tight'|'degraded'|'baseline'
                    idle_lat_p50   REAL,    -- ms, phase A
                    idle_lat_p95   REAL,
                    loaded_lat_p50 REAL,    -- ms, phase C  (NULL for solo runs)
                    loaded_lat_p95 REAL,
                    base_underruns INTEGER, -- phase B stalls (should be 0)
                    underruns      INTEGER, -- verdict-phase stalls
                    stall_ms       REAL,    -- verdict-phase total stalled time
                    min_margin_s   REAL,    -- verdict-phase minimum buffer margin
                    bulk_mbps      REAL,    -- greedy flow rate (NULL for solo runs)
                    stream_avg_mbps REAL,   -- avg stream throughput over verdict phase
                    timeline       TEXT,    -- JSON samples for chart replay
                    mode           TEXT,
                    client_ip      TEXT,
                    client_agent   TEXT,
                    description    TEXT     -- optional user label (NULL if blank)
                )
            """)
            # Forward migration: add columns introduced after the initial
            # release to databases that predate them (SQLite ADD COLUMN is
            # cheap and the new columns are nullable, so old rows read as NULL).
            have = {r[1] for r in con.execute("PRAGMA table_info(results)")}
            for name, decl in (("streams", "INTEGER"),
                               ("bidir", "INTEGER"),
                               ("direction", "TEXT")):
                if name not in have:
                    con.execute(f"ALTER TABLE results ADD COLUMN {name} {decl}")

    # --- Settings persistence --------------------------------------------
    def _load_overrides(self):
        """Read the saved override diff. Missing/corrupt file -> no overrides."""
        try:
            with open(self.settings_path) as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    def _atomic_write_json(self, path, obj):
        """Write via temp + os.replace so a crash mid-write can't leave a
        torn settings file (replace is atomic on the same filesystem)."""
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w") as f:
            json.dump(obj, f, indent=2)
        os.replace(tmp, path)

    def _sanitize_settings(self, incoming):
        """Clamp/coerce a settings dict against SETTING_BOUNDS / BOOL_SETTINGS.
        Unknown keys are dropped. Ints stay ints. This is the authority — the
        client UI guides with the same bounds but cannot bypass this."""
        out = {}
        for k, v in (incoming or {}).items():
            if k in BOOL_SETTINGS:
                out[k] = bool(v)
            elif k in STR_SETTINGS:
                out[k] = str(v).strip()[:_STR_MAXLEN]
            elif k in SETTING_BOUNDS:
                try:
                    num = float(v)
                except (TypeError, ValueError):
                    continue
                lo, hi = SETTING_BOUNDS[k]
                num = max(lo, min(num, hi))
                if isinstance(DEFAULTS.get(k), int):
                    num = int(round(num))
                out[k] = num
        return out

    def _setting(self, key):
        """Effective value of a setting: DEFAULTS with cached overrides applied."""
        if key in self._overrides:
            return self._overrides[key]
        return DEFAULTS.get(key)

    def _shutdown(self):
        # Kill the iperf3 server if we started one, so closing xnetperf doesn't
        # orphan a process holding port 5201. (The DB is connection-per-request,
        # so there is nothing else persistent to close.)
        self._iperf_terminate()

    # --- API methods (POST /api/<name>, JSON envelope) --------------------
    def get_settings(self):
        """Return the effective methodology settings (DEFAULTS + saved
        overrides), the stream profiles, and the validation bounds + boolean
        keys so the client can build the config UI from one source of truth."""
        resolved = dict(DEFAULTS)
        resolved.update(self._overrides)
        return {
            "defaults": resolved,
            "profiles": {k: dict(v) for k, v in PROFILES.items()},
            "bounds": {k: list(v) for k, v in SETTING_BOUNDS.items()},
            "bools": sorted(BOOL_SETTINGS),
            "strings": sorted(STR_SETTINGS),
        }

    def save_settings(self, settings):
        """Persist the user's settings as a sanitized DIFF from DEFAULTS.
        Returns the resolved effective settings so the client can adopt exactly
        what the server accepted (post-clamp)."""
        clean = self._sanitize_settings(settings)
        diff = {k: v for k, v in clean.items() if DEFAULTS.get(k) != v}
        self._atomic_write_json(self.settings_path, diff)
        self._overrides = clean
        resolved = dict(DEFAULTS)
        resolved.update(clean)
        return {"saved": True, "settings": resolved}

    def reset_settings(self):
        """Drop all overrides -> back to DEFAULTS."""
        try:
            if self.settings_path.exists():
                self.settings_path.unlink()
        except OSError:
            pass
        self._overrides = {}
        return {"reset": True, "settings": dict(DEFAULTS)}

    # --- iperf3 reference server (Iperf tab) -----------------------------
    # The Iperf tab runs `iperf3 -s` as the calibration reference the doc's
    # Calibration note calls for: a remote `iperf3 -c <host>` drives traffic,
    # the server prints per-second interval lines, and the client surfaces them
    # raw + parses the [SUM] bitrate into a live chart. One long-running server
    # handles many sequential tests. The command is assembled from settings as a
    # list argv (never a shell string), so there is no injection surface; the
    # only side effect is the iperf3 listener on the configured port.
    _ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

    def _iperf_running(self):
        p = self._iperf_proc
        return p is not None and p.poll() is None

    def _iperf_append(self, text):
        with self._iperf_lock:
            self._iperf_next_id += 1
            self._iperf_lines.append((self._iperf_next_id, text))

    def _iperf_read(self, proc):
        """Reader thread: drain the server's merged stdout/stderr line by line
        into the ring buffer until EOF (process exit)."""
        try:
            for line in proc.stdout:
                self._iperf_append(self._ANSI_RE.sub("", line.rstrip("\n")))
        except Exception:
            pass
        finally:
            self._iperf_append("----- iperf3 server stopped -----")

    def _iperf_terminate(self):
        """SIGTERM the server, escalating to SIGKILL if it lingers."""
        p = self._iperf_proc
        if p is not None and p.poll() is None:
            try:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except Exception:
                    p.kill()
            except Exception:
                pass

    @staticmethod
    def _fmt_num(v):
        # "1.0" -> "1", "0.5" -> "0.5": tidy numbers for the iperf argv.
        return "%g" % float(v)

    def _iperf_build_cmd(self):
        """Assemble the `iperf3 -s` argv from the saved iperf_* settings. Flags
        are appended only when meaningful (limits/timeouts > 0, strings set).
        --forceflush defaults on for real-time output; stdbuf is layered on in
        iperf_start as the belt to its suspenders."""
        s = self._setting
        cmd = ["iperf3", "-s", "-f", "m",
               "-i", self._fmt_num(s("iperf_interval")),
               "-p", str(int(s("iperf_port")))]
        if s("iperf_forceflush"):
            cmd.append("--forceflush")
        if s("iperf_verbose"):
            cmd.append("-V")
        idle = s("iperf_idle_timeout")
        if idle and int(idle) > 0:
            cmd += ["--idle-timeout", str(int(idle))]
        bind = (s("iperf_bind") or "").strip()
        if bind:
            cmd += ["-B", bind]
        aff = (s("iperf_affinity") or "").strip()
        if aff:
            cmd += ["-A", aff]
        return cmd

    def iperf_status(self):
        """Is iperf3 installed, which version, is our server running, and on
        which port/bind? When running these are the *active* server's; when
        stopped they're what the next start will use (for the state line + hint)."""
        path = shutil.which("iperf3")
        version = None
        if path:
            try:
                out = subprocess.run([path, "--version"], capture_output=True,
                                     text=True, timeout=5)
                first = (out.stdout or out.stderr or "").splitlines()
                version = first[0].strip() if first else None
            except Exception:
                version = None
        running = self._iperf_running()
        if running:
            port, bind = self._iperf_active_port, self._iperf_active_bind
        else:
            port = int(self._setting("iperf_port"))
            bind = (self._setting("iperf_bind") or "").strip()
        return {"installed": bool(path), "version": version,
                "running": running, "port": port, "bind": bind}

    def iperf_start(self):
        """Spawn the iperf3 server (idempotent), built from the saved iperf_*
        settings. stderr is merged into stdout so a bind error ('Address already
        in use') lands in the same stream the client shows. Launched under
        `stdbuf -oL` when available so libc line-buffers the pipe; --forceflush
        (default on) is iperf's own real-time flush layered on top."""
        if not shutil.which("iperf3"):
            return {"started": False, "running": False,
                    "error": "iperf3 not installed"}
        if self._iperf_running():
            return {"started": False, "running": True,
                    "port": self._iperf_active_port, "bind": self._iperf_active_bind}
        cmd = self._iperf_build_cmd()
        if shutil.which("stdbuf"):
            cmd = ["stdbuf", "-oL", "-eL"] + cmd
        try:
            self._iperf_proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1)
        except Exception as e:
            return {"started": False, "running": False, "error": str(e)}
        self._iperf_active_port = int(self._setting("iperf_port"))
        self._iperf_active_bind = (self._setting("iperf_bind") or "").strip()
        self._iperf_reader = threading.Thread(
            target=self._iperf_read, args=(self._iperf_proc,), daemon=True)
        self._iperf_reader.start()
        return {"started": True, "running": True,
                "port": self._iperf_active_port, "bind": self._iperf_active_bind}

    def iperf_stop(self):
        """Terminate the server. The reader thread sees EOF and appends the
        stopped marker line."""
        self._iperf_terminate()
        return {"stopped": True, "running": self._iperf_running()}

    def iperf_poll(self, after_id=0):
        """Ring-buffer lines newer than after_id, plus the high-water id and the
        running flag. Monotonic ids make this multi-viewer safe — each client
        tracks its own cursor against the one shared server."""
        after_id = int(after_id or 0)
        with self._iperf_lock:
            lines = [{"id": i, "text": t}
                     for (i, t) in self._iperf_lines if i > after_id]
            last_id = self._iperf_next_id
        return {"lines": lines, "last_id": last_id,
                "running": self._iperf_running()}

    def local_ip(self):
        """Best-guess LAN IP a remote iperf3 client should target, used to fill
        the Iperf-tab command hint when the UI is viewed on localhost. Reads the
        default-route source address via a UDP socket — connect() on UDP sends no
        packets, it just makes the kernel pick the outbound interface, so this
        returns the physical LAN IP and skips virbr0/docker bridges (not default
        routes). Returns {ip: None} if there's no route."""
        s = None
        ip = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = None
        finally:
            if s is not None:
                try:
                    s.close()
                except Exception:
                    pass
        return {"ip": ip}

    # --- Multi Client coordination (serve mode) --------------------------
    # In-memory, lock-guarded shared state: a client registry + one test state +
    # the current run's per-client sample series. Clients self-register, poll
    # (heartbeat + roster + state + results) ~1/s (faster during a run), and any
    # client may edit any row (shared source of truth). At Run the server
    # snapshots a plan and clients generate their configured paced streams at the
    # edge, measuring locally and reporting batches back; the server just stores
    # and serves them. All state is ephemeral (a restart empties it).
    @staticmethod
    def _mc_clamp_mbps(v):
        try:
            return max(0.0, min(float(v), 100000.0))
        except (TypeError, ValueError):
            return 0.0

    def _mc_prune(self, now):
        """Drop clients not seen within the prune window. Caller holds the lock."""
        for cid in [c for c, v in self._mc_clients.items()
                    if now - v["last_seen"] > self._MC_PRUNE_S]:
            del self._mc_clients[cid]

    def _mc_roster(self, now):
        """Roster snapshot for the wire. Caller holds the lock."""
        out = [{"id": cid, "ip": c["ip"], "name": c["name"],
                "dl": dict(c["dl"]), "ul": dict(c["ul"]), "lat": c["lat"],
                "seen_ago": round(now - c["last_seen"], 2)}
               for cid, c in self._mc_clients.items()]
        out.sort(key=lambda c: int(c["id"][1:]))
        return out

    def _mc_test_view(self, now):
        """Test state for the wire. A run whose window has elapsed lazily moves
        to 'complete' (NOT idle) so its results stay visible until the next Run.
        Caller holds the lock."""
        t = self._mc_test
        if t["state"] == "running" and now >= t["start_at"] + t["duration_s"]:
            t["state"] = "complete"
        return {"state": t["state"], "start_at": t["start_at"],
                "duration_s": t["duration_s"], "description": t["description"],
                "run_id": t["run_id"], "plan": t["plan"]}

    def mc_register(self, name=""):
        """Register the calling client for group tests; returns its assigned id,
        server time (for clock-offset correction), and the current roster/state."""
        ip = flask_request.remote_addr or "?"
        now = time.time()
        with self._mc_lock:
            self._mc_next_id += 1
            cid = "c%d" % self._mc_next_id
            self._mc_clients[cid] = {
                "ip": ip, "name": str(name or "").strip()[:40], "last_seen": now,
                "dl": {"on": False, "mbps": 0.0}, "ul": {"on": False, "mbps": 0.0},
                "lat": False}
            self._mc_prune(now)
            return {"client_id": cid, "server_now": now,
                    "clients": self._mc_roster(now), "test": self._mc_test_view(now)}

    def _mc_results_view(self):
        """Per-client sample series for the current run. Caller holds the lock."""
        return {cid: {k: list(v) for k, v in series.items()}
                for cid, series in self._mc_results.items()}

    def mc_poll(self, client_id=None):
        """Heartbeat + roster + test state (+ run plan/results) in one call.
        Refreshes the caller's last_seen (and IP), prunes the stale, returns
        everyone. `known` is False if the server has forgotten this client (e.g.
        after a restart) so it can re-register. `results` is included only while
        a run is running or complete."""
        now = time.time()
        with self._mc_lock:
            c = self._mc_clients.get(client_id) if client_id else None
            if c is not None:
                c["last_seen"] = now
                c["ip"] = flask_request.remote_addr or c["ip"]
            self._mc_prune(now)
            out = {"server_now": now, "known": c is not None,
                   "clients": self._mc_roster(now), "test": self._mc_test_view(now)}
            if out["test"]["state"] in ("running", "complete"):
                out["results"] = self._mc_results_view()
            return out

    def mc_update(self, client_id, name=None, dl_on=None, dl_mbps=None,
                  ul_on=None, ul_mbps=None, lat_on=None):
        """Edit any client's row (any client may edit any row). Only the fields
        provided change."""
        now = time.time()
        with self._mc_lock:
            c = self._mc_clients.get(client_id)
            if c is None:
                return {"ok": False, "clients": self._mc_roster(now)}
            if name is not None:
                c["name"] = str(name).strip()[:40]
            if dl_on is not None:
                c["dl"]["on"] = bool(dl_on)
            if dl_mbps is not None:
                c["dl"]["mbps"] = self._mc_clamp_mbps(dl_mbps)
            if ul_on is not None:
                c["ul"]["on"] = bool(ul_on)
            if ul_mbps is not None:
                c["ul"]["mbps"] = self._mc_clamp_mbps(ul_mbps)
            if lat_on is not None:
                c["lat"] = bool(lat_on)
            return {"ok": True, "clients": self._mc_roster(now)}

    def mc_unregister(self, client_id):
        """Remove a client from the roster (left the tab / closed the page)."""
        now = time.time()
        with self._mc_lock:
            self._mc_clients.pop(client_id, None)
            return {"ok": True, "clients": self._mc_roster(now)}

    def mc_start(self, description=""):
        """Arm a coordinated run: snapshot each client's config into a plan (so
        mid-run roster edits don't change what runs), clear last run's results,
        and flip to 'running' with a short lead so every client begins together
        at the same server time. Duration is from mc_duration_s (server-
        authoritative). No-op if a run is already running or fewer than two
        clients are connected. Clients read their plan entry, generate the
        configured paced streams at the edge, and report samples back."""
        now = time.time()
        with self._mc_lock:
            cur = self._mc_test_view(now)
            if cur["state"] == "running":
                return {"ok": False, "error": "a run is already in progress", "test": cur}
            if len(self._mc_clients) < 2:
                return {"ok": False, "error": "need at least 2 connected clients", "test": cur}
            plan = {cid: {"name": c["name"], "ip": c["ip"],
                          "dl": dict(c["dl"]), "ul": dict(c["ul"]), "lat": c["lat"]}
                    for cid, c in self._mc_clients.items()}
            self._mc_results = {}
            self._mc_test["run_id"] += 1
            self._mc_test["state"] = "running"
            self._mc_test["start_at"] = now + self._MC_START_LEAD_S
            self._mc_test["duration_s"] = float(self._setting("mc_duration_s"))
            self._mc_test["description"] = str(description or "").strip()[:200]
            self._mc_test["plan"] = plan
            return {"ok": True, "server_now": now, "test": self._mc_test_view(now)}

    def mc_stop(self):
        """End the current run early. Goes to 'complete' (not idle) so whatever
        samples arrived stay visible."""
        now = time.time()
        with self._mc_lock:
            if self._mc_test["state"] == "running":
                self._mc_test["state"] = "complete"
            return {"ok": True, "test": self._mc_test_view(now)}

    def mc_report(self, client_id, run_id, dl=None, ul=None, lat=None):
        """Append a batch of edge-measured samples for the current run. Each
        series is a list of {t, v} (t = seconds since start_at; v = Mbps for
        dl/ul, ms for lat). Ignored if run_id doesn't match the active run (a
        late report from a previous run) or the client isn't in the plan."""
        with self._mc_lock:
            if int(run_id) != self._mc_test["run_id"]:
                return {"ok": False, "stale": True}
            if client_id not in self._mc_test["plan"]:
                return {"ok": False}
            series = self._mc_results.setdefault(
                client_id, {"dl": [], "ul": [], "lat": []})
            for key, batch in (("dl", dl), ("ul", ul), ("lat", lat)):
                if not batch:
                    continue
                dst = series[key]
                for s in batch:
                    try:
                        dst.append({"t": round(float(s["t"]), 4),
                                    "v": round(float(s["v"]), 3)})
                    except (TypeError, ValueError, KeyError):
                        continue
                if len(dst) > self._MC_MAX_SAMPLES:
                    del dst[:len(dst) - self._MC_MAX_SAMPLES]
            return {"ok": True}

    def save_result(self, down_mbps, up_mbps, down_peak_mbps, up_peak_mbps,
                    duration_s, mode, client_agent="", description=None,
                    streams=None, bidir=None, direction=None):
        """Persist one completed test run. Returns the new row id.

        streams/bidir/direction record the test parameters so runs taken under
        different configurations stay comparable in history (a 4-stream
        full-duplex run reads very differently from a 1-stream sequential one).

        client_ip is read server-side from the request (the client can't know
        its own LAN address reliably); the save call comes from the same
        client that just ran the test, so remote_addr is correct."""
        client_ip = flask_request.remote_addr if flask_request else ""
        ts = int(time.time())
        description = (description or "").strip()[:200] or None
        streams = int(streams) if streams is not None else None
        bidir = (1 if bidir else 0) if bidir is not None else None
        with self._db() as con:
            cur = con.execute(
                """INSERT INTO results
                   (ts, down_mbps, up_mbps, down_peak_mbps, up_peak_mbps,
                    duration_s, client_ip, client_agent, mode, description,
                    streams, bidir, direction)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ts, down_mbps, up_mbps, down_peak_mbps, up_peak_mbps,
                 duration_s, client_ip, (client_agent or "")[:300], mode,
                 description, streams, bidir, direction),
            )
            new_id = cur.lastrowid
        return {"id": new_id, "ts": ts, "client_ip": client_ip}

    def get_results(self, limit=50):
        """Return the most recent test runs, newest first."""
        limit = max(1, min(int(limit), 500))
        with self._db() as con:
            rows = con.execute(
                "SELECT * FROM results ORDER BY ts DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_result(self, id):
        """Delete a single run by id."""
        with self._db() as con:
            con.execute("DELETE FROM results WHERE id=?", (int(id),))
        return {"deleted": int(id)}

    def clear_results(self):
        """Delete all stored runs."""
        with self._db() as con:
            con.execute("DELETE FROM results")
        return {"cleared": True}

    def save_stream_result(self, profile, rate_mbps, buffer_s, bulk_dir,
                           verdict, idle_lat_p50, idle_lat_p95,
                           loaded_lat_p50, loaded_lat_p95, base_underruns,
                           underruns, stall_ms, min_margin_s, bulk_mbps,
                           timeline, mode, kind="contention", client_agent="",
                           stream_avg_mbps=None, description=None):
        """Persist one stream-tab run. kind distinguishes a contention run
        (stream + bulk) from a solo stream run; solo runs pass NULL for the
        bulk_* and loaded_lat_* fields. timeline is a JSON string of chart
        samples (kept under control client-side: ~80 points per run)."""
        client_ip = flask_request.remote_addr if flask_request else ""
        ts = int(time.time())
        # Defensive cap: never let a malformed client bloat the DB.
        timeline = (timeline or "")[:60000]
        description = (description or "").strip()[:200] or None
        with self._db() as con:
            cur = con.execute(
                """INSERT INTO stream_results
                   (ts, kind, profile, rate_mbps, buffer_s, bulk_dir, verdict,
                    idle_lat_p50, idle_lat_p95, loaded_lat_p50, loaded_lat_p95,
                    base_underruns, underruns, stall_ms, min_margin_s,
                    bulk_mbps, stream_avg_mbps, timeline, mode, client_ip,
                    client_agent, description)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ts, kind, profile, rate_mbps, buffer_s, bulk_dir, verdict,
                 idle_lat_p50, idle_lat_p95, loaded_lat_p50, loaded_lat_p95,
                 base_underruns, underruns, stall_ms, min_margin_s,
                 bulk_mbps, stream_avg_mbps, timeline, mode, client_ip,
                 (client_agent or "")[:300], description),
            )
            new_id = cur.lastrowid
        return {"id": new_id, "ts": ts, "client_ip": client_ip}

    def get_stream_results(self, limit=50):
        """Return the most recent stream-tab runs, newest first."""
        limit = max(1, min(int(limit), 500))
        with self._db() as con:
            rows = con.execute(
                "SELECT * FROM stream_results ORDER BY ts DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_stream_result(self, id):
        """Delete a single stream-tab run by id."""
        with self._db() as con:
            con.execute("DELETE FROM stream_results WHERE id=?", (int(id),))
        return {"deleted": int(id)}

    def clear_stream_results(self):
        """Delete all stored stream-tab runs."""
        with self._db() as con:
            con.execute("DELETE FROM stream_results")
        return {"cleared": True}

    # --- Raw methods (pass-through GET/POST /raw/<name>) ------------------
    def raw_download(self, request):
        """GET /raw/download — one continuous stream of incompressible bytes.

        The client reads the body and aborts at its target duration; this
        generator just keeps cycling the pre-generated block until the client
        disconnects or the server-side safety cap (down_cap_s) trips, so an
        abandoned stream can't run forever. octet-stream + no-store defeat
        content sniffing and caching."""
        cap_s = float(request.args.get("cap_s", self._setting("down_cap_s")))
        # Enforced ceiling: never run longer than the effective down_cap_s no
        # matter what the client asks, so an abandoned/parallel stream can't run
        # away. (Previously the client's cap_s was honored unclamped.)
        cap_s = max(0.0, min(cap_s, float(self._setting("down_cap_s"))))
        yield_kb = int(request.args.get(
            "yield_kb", _DOWNLOAD_YIELD_BYTES // 1024))
        yield_kb = max(16, min(yield_kb, 1024))
        block = self._block
        n = len(block)
        step = yield_kb * 1024

        def gen():
            t0 = time.perf_counter()
            off = 0
            while True:
                if time.perf_counter() - t0 >= cap_s:
                    break
                end = off + step
                if end <= n:
                    yield block[off:end]
                    off = end if end < n else 0
                else:
                    # wrap around the block boundary
                    yield block[off:] + block[: end - n]
                    off = end - n

        return Response(
            gen(),
            mimetype="application/octet-stream",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Pragma": "no-cache",
                "X-Accel-Buffering": "no",  # discourage any proxy buffering
            },
        )

    def raw_echo(self, request):
        """GET /raw/echo — instant empty response for latency probing.

        The measured round trip includes ~1–2 ms of framework overhead on
        top of network RTT; that constant cancels out in the idle-vs-loaded
        *delta*, which is the diagnostic (queueing delay under load)."""
        return Response(b"", status=204,
                        headers={"Cache-Control": "no-store"})

    def raw_stream(self, request):
        """GET /raw/stream?rate_mbps=X — paced incompressible stream.

        Emulates a media flow: emits at a fixed rate instead of greedily.
        Pacing uses absolute deadlines (next += tick) rather than per-chunk
        sleeps, so scheduling error never accumulates into drift. If the
        socket backpressures (link congested), yields block and the schedule
        falls behind; on recovery the generator bursts to catch up — which
        is what a real HTTP media server does — but backlog older than 1 s
        is forgiven so a long stall can't trigger an unbounded burst.

        Chunks are sized to ~20 ms ticks, clamped [16 KiB, 256 KiB], so both
        a 2 Mbps audio flow and an 80 Mbps video flow pace smoothly. Sleeps
        release the GIL, so a paced stream costs concurrent flows nothing."""
        rate_mbps = float(request.args.get("rate_mbps", "20"))
        rate_mbps = max(0.1, min(rate_mbps, 2000.0))
        cap_s = float(request.args.get("cap_s", self._setting("stream_cap_s")))
        cap_s = max(0.0, min(cap_s, float(self._setting("stream_cap_s"))))
        bytes_per_s = rate_mbps * 1e6 / 8
        chunk_size = int(bytes_per_s * 0.02)
        chunk_size = max(16 * 1024, min(chunk_size, 256 * 1024))
        tick = chunk_size / bytes_per_s
        block = self._block
        n = len(block)

        def gen():
            t0 = time.perf_counter()
            next_t = t0
            off = 0
            while True:
                now = time.perf_counter()
                if now - t0 >= cap_s:
                    break
                if now < next_t:
                    time.sleep(next_t - now)
                elif now - next_t > 1.0:
                    next_t = now  # forgive backlog older than 1 s
                next_t += tick
                end = off + chunk_size
                if end <= n:
                    yield block[off:end]
                    off = 0 if end == n else end
                else:
                    yield block[off:] + block[:end - n]
                    off = end - n

        return Response(
            gen(),
            mimetype="application/octet-stream",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Pragma": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    def raw_upload(self, request):
        """POST /raw/upload — drain the request body, count bytes, and sample
        arrival pacing.

        Reads request.stream directly (never request.files / get_data), so
        Werkzeug neither buffers the body to a temp file nor parses it as a
        form — the bytes are read and discarded in memory.

        The server is the *receiving* end of an upload, so its read pacing is
        the only honest source for instantaneous upload rate: the client
        can't observe sub-POST pacing (fetch exposes no upload progress, and
        XHR's progress events count bytes buffered into the socket — not
        bytes arrived — which inflates rates at each POST start). The drain
        loop is sampled on ~interval_ms windows, with one correction: while
        Werkzeug parses the request headers, body bytes are already piling
        into the kernel receive buffer, so the drain's *first* window reads
        that backlog at memory speed and measures high. The first window is
        therefore discarded; only interior windows (genuinely arrival-paced —
        the loop blocks on the socket at steady state) become samples, which
        the client uses for peak. The client sizes interval_ms to ~1/3 of the
        expected POST duration so every measurement POST yields interior
        windows deterministically."""
        interval_ms = int(request.args.get(
            "interval_ms", DEFAULTS["gauge_interval_ms"]))
        interval = max(0.05, interval_ms / 1000.0)
        total = 0
        win_bytes = 0
        windows = []
        stream = request.stream
        t0 = time.perf_counter()
        win_start = t0
        while True:
            # 256 KiB reads: fine-grained enough that read timing tracks
            # arrival pacing (≈2 ms per read at gigabit) without per-call
            # overhead mattering.
            chunk = stream.read(256 * 1024)
            if not chunk:
                break
            n = len(chunk)
            total += n
            win_bytes += n
            now = time.perf_counter()
            if now - win_start >= interval:
                windows.append(
                    round((win_bytes * 8 / 1e6) / (now - win_start), 2))
                win_start = now
                win_bytes = 0
        end = time.perf_counter()
        # Trailing partial window: keep it only if it spans at least half an
        # interval — a rate over a tiny tail is noise, not a sample.
        if win_bytes and (end - win_start) >= interval / 2:
            windows.append(
                round((win_bytes * 8 / 1e6) / (end - win_start), 2))
        # Drop the first window (receive-buffer head start, reads high).
        samples = windows[1:]
        dt = end - t0
        return Response(
            json.dumps({"bytes": total, "server_seconds": round(dt, 6),
                        "samples_mbps": samples, "interval_ms": interval_ms}),
            mimetype="application/json",
            headers={"Cache-Control": "no-store"},
        )

    def raw_mc_upload(self, request):
        """POST /raw/mc_upload?client_id&run_id&rate_mbps — Multi Client paced
        upload, paced AND measured at the receiver. The exact inverse of
        raw_stream: there the server paces what it *writes* (download) and the
        client measures receipt; here the server throttles how fast it *reads*
        the request body, at the client's target rate, via absolute-deadline
        pacing. TCP backpressure then paces the client's send to match, so the
        on-wire upload rate is the target while the client just feeds bytes
        greedily. We measure the rate we actually read per ~250 ms window — the
        honest receiver-side throughput — and record it straight into this
        client's `ul` series in the run results (the receiver owns the upload
        truth, so this bypasses mc_report). When contention starves the read
        (bytes can't arrive at the target), the window reads low — which is the
        diagnosis. The first window is dropped: the kernel receive backlog plus
        the initial unthrottled read measure high (same correction raw_upload
        makes)."""
        client_id = request.args.get("client_id", "")
        try:
            run_id = int(request.args.get("run_id", "-1"))
        except (TypeError, ValueError):
            run_id = -1
        rate_mbps = max(0.1, self._mc_clamp_mbps(request.args.get("rate_mbps", 0)))
        bytes_per_s = rate_mbps * 1e6 / 8.0

        with self._mc_lock:
            t = self._mc_test
            current = (t["state"] == "running" and run_id == t["run_id"]
                       and client_id in t["plan"])
            start_at = t["start_at"]
            end_at = start_at + t["duration_s"] + 0.5
        if not current:
            return Response(json.dumps({"ok": False}), mimetype="application/json")

        stream = request.stream
        read_size = max(4096, min(int(bytes_per_s * 0.05), 64 * 1024))  # ~50 ms or 64 KiB
        window = 0.25
        t0 = time.perf_counter()
        read_total = 0
        win_bytes = 0
        win_start = t0
        win_index = 0
        try:
            while time.time() < end_at:
                # absolute-deadline read pacing: ahead of schedule → sleep; behind
                # (or starved, where the read below blocks) → read at arrival rate.
                ahead = read_total - (time.perf_counter() - t0) * bytes_per_s
                if ahead > 0:
                    time.sleep(min(ahead / bytes_per_s, window))
                    continue
                chunk = stream.read(read_size)
                if not chunk:
                    break
                read_total += len(chunk)
                win_bytes += len(chunk)
                now = time.perf_counter()
                if now - win_start >= window:
                    mbps = round((win_bytes * 8 / 1e6) / (now - win_start), 3)
                    win_index += 1
                    if win_index > 1:                       # drop the first window
                        with self._mc_lock:
                            tt = self._mc_test
                            if tt["run_id"] == run_id and tt["state"] in ("running", "complete"):
                                series = self._mc_results.setdefault(
                                    client_id, {"dl": [], "ul": [], "lat": []})
                                series["ul"].append(
                                    {"t": round(time.time() - start_at, 4), "v": mbps})
                                if len(series["ul"]) > self._MC_MAX_SAMPLES:
                                    del series["ul"][:len(series["ul"]) - self._MC_MAX_SAMPLES]
                    win_bytes = 0
                    win_start = now
        except (OSError, ValueError, IOError):
            pass   # client aborted / connection closed mid-read
        return Response(json.dumps({"ok": True}), mimetype="application/json")


if __name__ == "__main__":
    run(
        Xnetperf(),
        frontend=str(BASE_DIR / "xnetperf.html"),
        title="xnetperf",
        port=8086,
        threaded=True,   # opt-in concurrency: the only app that needs it
        window={
            "width": 880,
            "height": 720,
            "min_size": (420, 560),
            "background_color": "#0f1117",
            "text_select": True,
        },
    )
