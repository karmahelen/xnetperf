// xnetperf — frontend measurement engine, gauges, and stream suite.
//
// Throughput tab: greedy ceiling tests (download: continuous stream read via
// ReadableStream; upload: probe-sized POST loop with server-side interior-
// window arrival sampling). Stream tab: a three-phase contention test
// (idle latency baseline → paced stream alone → paced stream + greedy bulk)
// with a player buffer model and a margin/latency timeline chart. The client
// owns all wall-clock math; the server generates, paces, or drains bytes.

const FALLBACK_SETTINGS = {
    warmup_s: 1.0, measure_s: 6.0, gauge_interval_ms: 250,
    up_probe_mb: 1, up_post_target_s: 0.5, up_chunk_mb: 32, down_cap_s: 120,
    parallel_streams: 1, dir_down: true, dir_up: true, bidirectional: false,
    down_yield_kb: 256,
    idle_s: 3.0, stream_only_s: 6.0, loaded_s: 9.0,
    probe_interval_ms: 200, margin_sample_ms: 250, stream_cap_s: 45,
};
const FALLBACK_PROFILES = {
    audio:      { label: "Audio",      rate_mbps: 2,  buffer_s: 1 },
    video_low:  { label: "Video Low",  rate_mbps: 5,  buffer_s: 3 },
    video_mid:  { label: "Video Mid",  rate_mbps: 20, buffer_s: 4 },
    video_high: { label: "Video High", rate_mbps: 50, buffer_s: 5 },
};

const state = {
    settings: { ...FALLBACK_SETTINGS },
    profiles: { ...FALLBACK_PROFILES },
    bounds: {},
    lastRate: { down: null, up: null },   // most recent measured Mbps per direction (current mode), for the data estimate
    uploadBlob: null,
    running: false,          // a throughput run is active
    streamRunning: false,    // a stream-tab run is active
    cancelled: false,
    downControllers: [],
    mode: "network",
    activeTab: "throughput",
    profile: "video_mid",
    bulkDir: "none",
    iperf: {
        checked: false, installed: false, version: null, running: false,
        lastId: 0, polling: false, pollTimer: null,
        outLines: [],
        test: null,         // the test currently being parsed {num,dir,streams,clientIp,hasSum,samples,result}
        cards: [],          // per-test chart cards in display order {wrap,head,chart,test}
        active: null,       // the bottom slot card (live or "Waiting…")
        port: 5201,         // active server's port (or configured, when stopped)
        bind: "",           // active server's bind address ("" = all interfaces)
        localIp: "",        // cached backend local_ip() result for the hint
        outputShown: false, // output box revealed (stays visible after first start)
    },
    mc: {                   // Multi Client tab (cross-client coordination)
        clientId: null, connected: false,
        pollTimer: null, cdTimer: null,
        serverOffset: 0,    // server_now - client_now (s); add to client time for server time
        clients: [],
        test: { state: "idle", start_at: 0, duration_s: 0, run_id: 0, description: "", plan: {} },
        results: {},        // client_id -> {dl:[{t,v}], ul:[...], lat:[...]} (from poll)
        run: null,          // when this client is running traffic: see mcArmRun
    },
};

const MAX_LADDER = [100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000];

// Full stream-history rows keyed by id, for expanding a row into its verdict
// card + chart without a re-fetch. Rebuilt each loadStreamHistory.
let streamRowsCache = {};

// Last data rendered into the live stream chart, so a window resize can
// redraw it at the new width (the live sampler redraws during a run; this
// covers the static final chart after a run completes).
let lastLiveChart = null;

// ---- small helpers --------------------------------------------------------
const el = (id) => document.getElementById(id);
const setStatus = (t) => { el("statusText").textContent = t; };
const setInfo = (t) => { el("statusInfo").textContent = t; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitOrCancel(ms) {
    const end = performance.now() + ms;
    while (performance.now() < end) {
        if (state.cancelled) return false;
        await sleep(Math.min(100, end - performance.now()));
    }
    return !state.cancelled;
}

function bumpMax(cur, val) {
    let target = cur;
    for (const m of MAX_LADDER) {
        if (m >= val * 1.05) { target = m; break; }
        target = m;
    }
    return Math.max(cur, target);
}

function fmtMbps(v) {
    if (!isFinite(v) || v <= 0) return "0";
    if (v >= 1000) return Math.round(v).toString();
    if (v >= 100) return v.toFixed(0);
    if (v >= 10) return v.toFixed(1);
    return v.toFixed(2);
}

function fmtMs(v) {
    if (!isFinite(v)) return "—";
    if (v >= 100) return v.toFixed(0);
    if (v >= 10) return v.toFixed(1);
    return v.toFixed(2);
}

function fmtRate(v) {
    // A nominal/configured rate (a profile or custom value), not a measurement:
    // strip trailing zeros so "2.00" reads "2" and "20.0" reads "20". fmtMbps
    // stays for measured values, where the decimals carry real information.
    if (!isFinite(v) || v <= 0) return "0";
    return String(+v.toFixed(2));
}

const round2 = (v) => Math.round((v + Number.EPSILON) * 100) / 100;

function percentile(sorted, p) {
    if (!sorted.length) return NaN;
    const i = Math.min(sorted.length - 1,
        Math.max(0, Math.round((p / 100) * (sorted.length - 1))));
    return sorted[i];
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g,
        (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// ---- tabs ------------------------------------------------------------------
function switchTab(name) {
    state.activeTab = name;
    // Derive panels/buttons from the DOM rather than a hardcoded list, so adding
    // a tab in HTML can never fall out of sync with the JS (a stale list would
    // hide every panel and blank the view).
    document.querySelectorAll(".tab-panel").forEach((p) =>
        p.classList.toggle("hidden", p.id !== "tab-" + name));
    document.querySelectorAll(".tabs .tab").forEach((b) =>
        b.classList.toggle("active", b.id === "tabBtn-" + name));
    // The loopback note only concerns the throughput/stream self-tests; iperf
    // measures the real wire, and Multi Client is its own coordination flow.
    const lb = el("loopbackNote");
    if (lb) lb.classList.toggle("hidden",
        !(state.mode === "loopback" && name !== "iperf" && name !== "multiclient"));
    // Reseed the shared status bar to the entered tab's resting state so a
    // message from the tab you just left can't linger. A throughput/stream run
    // owns the bar while active (don't stomp its live progress); iperf seeds its
    // server state in iperfOnEnter and multiclient its connection state in
    // mcOnEnter, both of which run below and override this.
    if (!state.running && !state.streamRunning) { setStatus("Ready"); setInfo(""); }
    if (name === "iperf") iperfOnEnter(); else iperfStopPolling();
    if (name === "multiclient") mcOnEnter(); else mcOnLeave();
}

// ---- gauge (throughput tab) -------------------------------------------------
function buildGauge(wrapId) {
    const cx = 130, cy = 130, R = 100, W = 14;
    const startA = 225, sweep = 270, N = 90;
    const pts = [];
    for (let i = 0; i <= N; i++) {
        const a = (startA + (sweep * i) / N) * Math.PI / 180;
        pts.push([cx + R * Math.sin(a), cy - R * Math.cos(a)]);
    }
    const d = "M" + pts.map((p) => p[0].toFixed(2) + " " + p[1].toFixed(2)).join(" L");
    const leftX = cx + R * Math.sin(startA * Math.PI / 180);
    const leftY = cy - R * Math.cos(startA * Math.PI / 180);
    const rightX = cx + R * Math.sin((startA + sweep) * Math.PI / 180);
    const rightY = cy - R * Math.cos((startA + sweep) * Math.PI / 180);

    const wrap = el(wrapId);
    wrap.innerHTML = `
      <svg viewBox="0 0 260 200">
        <path class="gauge-track" d="${d}" stroke-width="${W}" stroke-linecap="round"/>
        <path class="gauge-value" d="${d}" stroke-width="${W}" stroke-linecap="round"/>
        <text class="gauge-num" x="130" y="126" text-anchor="middle" font-size="34" data-role="num">0</text>
        <text class="gauge-unit" x="130" y="150" text-anchor="middle" font-size="12">Mbps</text>
        <text class="gauge-scale" x="${leftX.toFixed(1)}" y="${(leftY + 16).toFixed(1)}" text-anchor="middle" font-size="10">0</text>
        <text class="gauge-scale" x="${rightX.toFixed(1)}" y="${(rightY + 16).toFixed(1)}" text-anchor="middle" font-size="10" data-role="max">—</text>
      </svg>`;
    const value = wrap.querySelector(".gauge-value");
    const len = value.getTotalLength();
    value.style.strokeDasharray = len;
    value.style.strokeDashoffset = len;
    value._len = len;
    wrap._max = 100;
}

function setGauge(which, mbps, max) {
    const wrap = el("gauge-" + which);
    if (max) wrap._max = max;
    const m = wrap._max || 100;
    const f = Math.max(0, Math.min(1, mbps / m));
    const value = wrap.querySelector(".gauge-value");
    value.style.strokeDashoffset = value._len * (1 - f);
    wrap.querySelector('[data-role="num"]').textContent = fmtMbps(mbps);
    wrap.querySelector('[data-role="max"]').textContent = m;
}

function setSub(which, avg, peak) {
    el("sub-" + which).textContent = `avg ${fmtMbps(avg)} · peak ${fmtMbps(peak)}`;
}

function setActive(which, on) {
    el("card-" + which).classList.toggle("active", on);
}

function resetGauges() {
    for (const w of ["down", "up"]) {
        const wrap = el("gauge-" + w);
        wrap._max = 100;
        setGauge(w, 0, 100);
        el("sub-" + w).textContent = "—";
        setActive(w, false);
    }
}

// ---- upload payload (one reused incompressible block) -----------------------
function makeUploadBlob(mb) {
    const size = mb * 1024 * 1024;
    const buf = new Uint8Array(size);
    const CHUNK = 65536; // crypto.getRandomValues caps at 64 KiB per call
    for (let off = 0; off < size; off += CHUNK) {
        crypto.getRandomValues(buf.subarray(off, Math.min(off + CHUNK, size)));
    }
    return new Blob([buf], { type: "application/octet-stream" });
}

// Shared raw-stream reader: fetch an incompressible byte stream and hand each
// chunk's length to onBytes until the stream ends. Owns the fetch options, the
// HTTP-status check, and reader cleanup; each caller owns its AbortController,
// its accumulator, and its error policy (rethrow / swallow / stop-sampler).
async function readRawStream(url, signal, onBytes) {
    const res = await fetch(url, { signal, cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const reader = res.body.getReader();
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            onBytes(value.byteLength);
        }
    } finally { try { reader.cancel(); } catch (_) {} }
}

// ---- download: continuous greedy stream -------------------------------------
// ---- download: N concurrent continuous streams ------------------------------
// One flow is bounded by its own window/RTT (the BDP); N flows fill a pipe a
// single flow can't (the iperf -P axis). All N readers pump into one shared
// byte counter; a single time-driven sampler owns the gauge and the warmup/
// measure boundaries, so aggregate throughput is summed in one place rather
// than inferred per-flow. avg = post-warmup aggregate bytes / window.
async function runDownload(S, streams) {
    const N = Math.max(1, Math.min((streams | 0) || 1, 16));
    setActive("down", true);
    let max = 100;
    const agg = { bytes: 0 };
    const ctrls = [];
    state.downControllers = ctrls;
    const cap = S.warmup_s + S.measure_s + 5;
    const ykb = S.down_yield_kb || 256;

    // N concurrent readers; one dying flow must not kill the run.
    const readers = [];
    for (let i = 0; i < N; i++) {
        const ctrl = new AbortController();
        ctrls.push(ctrl);
        const url = `/raw/download?cap_s=${cap}&yield_kb=${ykb}&cb=${Date.now()}_${i}`;
        readers.push((async () => {
            try {
                await readRawStream(url, ctrl.signal, (n) => { agg.bytes += n; });
            } catch (e) {
                if (e.name !== "AbortError") { /* swallow: flow ended/errored */ }
            }
        })());
    }

    // Single sampler: gauge cadence + warmup snapshot + measure-end decision.
    // After the warmup boundary it also records a (t, cumulative-bytes) timeline
    // so the average can be the best *sustained* window (same basis as upload).
    // On a continuous download the ramp is fast, so on a normal link this reads
    // the same as the whole-window mean — but on a high-BDP link (long slow-start)
    // or a dippy one it steps over a ramp/sag the fixed-warmup mean would bake in.
    const t0 = performance.now();
    let warmupBytes = null, warmupT = null, peak = 0;
    let lastT = t0, lastBytes = 0, endBytes = 0, endT = null;
    const tl = [];   // post-warmup cumulative-bytes timeline, anchored {0,0}
    while (true) {
        await sleep(S.gauge_interval_ms);
        const now = performance.now();
        const elapsed = (now - t0) / 1000;
        const bytes = agg.bytes;
        if (warmupBytes === null && elapsed >= S.warmup_s) {
            warmupBytes = bytes; warmupT = now;     // restart window at the boundary
            lastT = now; lastBytes = bytes;
        }
        const inst = ((bytes - lastBytes) * 8 / 1e6) / ((now - lastT) / 1000);
        if (isFinite(inst)) {
            max = bumpMax(max, inst);
            setGauge("down", inst, max);
            if (warmupBytes !== null) peak = Math.max(peak, inst);
        }
        lastT = now; lastBytes = bytes;
        if (warmupBytes !== null) tl.push({ t: (now - warmupT) / 1000, cum: bytes - warmupBytes });
        if (elapsed >= S.warmup_s + S.measure_s || state.cancelled) {
            endBytes = bytes; endT = now; break;
        }
    }
    for (const c of ctrls) { try { c.abort(); } catch (_) {} }
    state.downControllers = [];
    await Promise.allSettled(readers);

    // Best sustained window; fall back to the whole post-warmup mean if the run
    // was too short to form one (e.g. cancelled before the warmup boundary).
    let avg = sustainedMbps(tl, S.measure_s);
    if (!avg) {
        if (warmupBytes !== null && endT > warmupT) {
            avg = ((endBytes - warmupBytes) * 8 / 1e6) / ((endT - warmupT) / 1000);
        } else if (endBytes > 0) {
            avg = (endBytes * 8 / 1e6) / ((endT - t0) / 1000);
        }
    }
    max = bumpMax(max, avg);
    setGauge("down", avg, max);
    setSub("down", avg, peak);
    setActive("down", false);
    return { avg, peak };
}

// ---- upload: looped POSTs (depth 1) -----------------------------------------
// Phase 1 (= warmup): probe POSTs that ladder up in size (1 MiB doubling to
// blob/4) while they stay cheap — they establish keep-alive, carry slow-start,
// give the gauge a visible ramp, and yield the rate estimate. Phase 2
// (measurement): POSTs sized to land near up_post_target_s each, sampled
// server-side on windows of ~1/3 the expected POST duration so every POST
// yields interior arrival-paced windows deterministically. avg = best *sustained*
// window over the per-POST byte timeline (excludes the serial upload's slow TCP
// ramp — see sustainedMbps); peak = max server interior window (gap-free,
// head-start-corrected).
async function postChunk(blobSlice, intervalMs) {
    const ps = performance.now();
    const resp = await fetch(`/raw/upload?interval_ms=${intervalMs}&cb=${Date.now()}`, {
        method: "POST",
        body: blobSlice,
        headers: { "Content-Type": "application/octet-stream" },
        cache: "no-store",
    });
    const ack = await resp.json();
    const pe = performance.now();
    return { ps, pe, bytes: blobSlice.size, ack };
}

// Best *sustained* throughput from a cumulative-bytes timeline (samples are
// {t: seconds since measure-start, cum: cumulative bytes}, anchored at {0,0}).
// Returns the highest average over any contiguous window of at least the
// "sustained horizon" — which is what "max true achievable" means: it excludes
// the TCP slow-start ramp (early windows average lower) without having to guess
// the ramp's length, and the minimum width stops one lucky POST from dominating.
// Falls back to the whole-span average when the run is too short to form a window.
function sustainedMbps(tl, measureS) {
    if (!tl || tl.length < 2) return 0;
    const first = tl[0], last = tl[tl.length - 1];
    const span = last.t - first.t;
    const whole = span > 0 ? ((last.cum - first.cum) * 8 / 1e6) / span : 0;
    const minWin = Math.max(2, (measureS || span) * 0.4);
    if (span < minWin) return whole;
    let best = 0;
    for (let i = 0; i < tl.length - 1; i++) {
        for (let j = i + 1; j < tl.length; j++) {
            const dt = tl[j].t - tl[i].t;
            if (dt < minWin) continue;
            const r = ((tl[j].cum - tl[i].cum) * 8 / 1e6) / dt;
            if (r > best) best = r;
        }
    }
    return best || whole;
}

async function runUpload(S, streams) {
    const N = Math.max(1, Math.min((streams | 0) || 1, 16));
    setActive("up", true);
    let max = 100;
    const blob = state.uploadBlob;
    const MiB = 1024 * 1024;

    // Phase 1 — single-flow warmup probe with size laddering. One flow is
    // enough to estimate the per-flow rate and size the measurement chunk;
    // a single-flow estimate over-sizes the chunk under parallelism, which is
    // the safe direction (bigger POSTs = less turnaround, clamped to blob.size).
    let probeSize = Math.min(S.up_probe_mb * MiB, blob.size);
    const probeCap = Math.max(MiB, Math.floor(blob.size / 4 / MiB) * MiB);
    const w0 = performance.now();
    let probeBytes = 0;
    while (!state.cancelled && (performance.now() - w0) / 1000 < S.warmup_s) {
        const r = await postChunk(blob.slice(0, probeSize), S.gauge_interval_ms);
        probeBytes += r.bytes;
        const inst = (r.bytes * 8 / 1e6) / ((r.pe - r.ps) / 1000);
        max = bumpMax(max, inst);
        setGauge("up", inst, max);
        if (r.pe - r.ps < 100 && probeSize * 2 <= probeCap) probeSize *= 2;
    }
    if (state.cancelled) { setActive("up", false); return { avg: 0, peak: 0 }; }

    const probeRate = probeBytes / Math.max((performance.now() - w0) / 1000, 0.05);
    let chunkBytes = Math.round((probeRate * S.up_post_target_s) / MiB) * MiB;
    chunkBytes = Math.max(MiB, Math.min(chunkBytes, blob.size));
    const slice = blob.slice(0, chunkBytes);
    const expectedMs = (chunkBytes / Math.max(probeRate, 1)) * 1000;
    const intervalMs = Math.max(80, Math.min(250, Math.round(expectedMs / 3)));

    const m0 = performance.now();
    const deadline = m0 + S.measure_s * 1000;

    if (N === 1) {
        // Precise method: one flow, peak from the server's interior arrival-
        // paced windows (the client can't observe sub-POST pacing honestly).
        let total = 0, peak = 0;
        const tl = [{ t: 0, cum: 0 }];   // cumulative-bytes timeline for steady-state
        while (!state.cancelled && performance.now() < deadline) {
            const r = await postChunk(slice, intervalMs);
            total += r.bytes;
            tl.push({ t: (r.pe - m0) / 1000, cum: total });
            const samples = (r.ack && r.ack.samples_mbps) || [];
            for (const s of samples) peak = Math.max(peak, s);
            const shown = samples.length
                ? samples[samples.length - 1]
                : (r.bytes * 8 / 1e6) / ((r.pe - r.ps) / 1000);
            max = bumpMax(max, shown);
            setGauge("up", shown, max);
        }
        // Report the best *sustained* rate, not the whole-window mean: a serial
        // upload's TCP ramp is slow (each inter-POST idle slows cwnd growth), so
        // the mean is diluted by a long climb. The sustained window excludes it.
        const avg = sustainedMbps(tl, S.measure_s);
        peak = Math.max(peak, avg);
        max = bumpMax(max, avg);
        setGauge("up", avg, max);
        setSub("up", avg, peak);
        setActive("up", false);
        return { avg, peak };
    }

    // N>1 — client-aggregate. N concurrent POST loops feed a shared counter; a
    // client-side sampler reads it for the gauge + aggregate peak. Per-flow
    // server windows don't compose into an honest aggregate instantaneous rate,
    // so peak/gauge are client-observed (mildly optimistic at POST boundaries);
    // the average is the best sustained aggregate window (ramp-excluded).
    const agg = { bytes: 0 };
    const loops = [];
    for (let i = 0; i < N; i++) {
        loops.push((async () => {
            while (!state.cancelled && performance.now() < deadline) {
                const r = await postChunk(slice, intervalMs);
                agg.bytes += r.bytes;
            }
        })());
    }
    let peak = 0, lastT = m0, lastBytes = 0;
    const tl = [{ t: 0, cum: 0 }];   // aggregate cumulative-bytes timeline
    while (!state.cancelled && performance.now() < deadline) {
        await sleep(S.gauge_interval_ms);
        const now = performance.now();
        const bytes = agg.bytes;
        tl.push({ t: (now - m0) / 1000, cum: bytes });
        const inst = ((bytes - lastBytes) * 8 / 1e6) / ((now - lastT) / 1000);
        if (isFinite(inst)) {
            max = bumpMax(max, inst);
            setGauge("up", inst, max);
            peak = Math.max(peak, inst);
        }
        lastT = now; lastBytes = bytes;
    }
    await Promise.allSettled(loops);

    // Best sustained aggregate rate (ramp-excluded), same basis as N === 1.
    let avg = sustainedMbps(tl, S.measure_s);
    peak = Math.max(peak, avg);
    max = bumpMax(max, avg);
    setGauge("up", avg, max);
    setSub("up", avg, peak);
    setActive("up", false);
    return { avg, peak };
}

// ---- throughput orchestration ------------------------------------------------
function setStartButton(running) {
    const btn = el("startBtn");
    if (running) {
        btn.textContent = "Cancel";
        btn.classList.remove("success");
        btn.classList.add("danger");
    } else {
        btn.textContent = "Run Throughput Test";
        btn.classList.add("success");
        btn.classList.remove("danger");
    }
}

async function onStartClick() {
    if (state.running) {
        state.cancelled = true;
        for (const c of state.downControllers) { try { c.abort(); } catch (_) {} }
        setStatus("Cancelling…");
        return;
    }
    if (state.streamRunning) return;
    await startRun();
}

async function startRun() {
    const S = state.settings;
    const N = Math.max(1, Math.min((S.parallel_streams | 0) || 1, 16));
    const doDown = S.dir_down !== false;
    const doUp = S.dir_up !== false;
    if (!doDown && !doUp) {
        setStatus("Select at least one direction in configuration");
        return;
    }
    const bidir = !!S.bidirectional && doDown && doUp;
    const direction = (doDown && doUp) ? "both" : (doDown ? "down" : "up");
    const tag = (N > 1 ? `${N} streams` : "1 stream") +
        (bidir ? " · full-duplex" : "");

    state.running = true;
    state.cancelled = false;
    setStartButton(true);
    el("streamStartBtn").disabled = true;
    el("summary").classList.add("hidden");
    setInfo("");
    resetGauges();

    let down = null, up = null;
    try {
        if (bidir) {
            setStatus(`Measuring ↓↑ together · ${tag}…`);
            [down, up] = await Promise.all([runDownload(S, N), runUpload(S, N)]);
        } else {
            if (doDown) {
                setStatus(`Measuring download · ${tag}…`);
                down = await runDownload(S, N);
                if (state.cancelled) { setStatus("Cancelled"); return; }
            }
            if (doUp) {
                setStatus(`Measuring upload · ${tag}…`);
                up = await runUpload(S, N);
            }
        }
        if (state.cancelled) { setStatus("Cancelled"); return; }

        setStatus("Saving…");
        const descEl = el("testDesc");
        const description = descEl ? descEl.value.trim().slice(0, 200) : "";
        const r = await app.call("save_result", {
            down_mbps: down ? round2(down.avg) : null,
            up_mbps: up ? round2(up.avg) : null,
            down_peak_mbps: down ? round2(down.peak) : null,
            up_peak_mbps: up ? round2(up.peak) : null,
            duration_s: S.measure_s,
            mode: state.mode,
            client_agent: navigator.userAgent,
            description: description || null,
            streams: N,
            bidir: bidir ? 1 : 0,
            direction: direction,
        });
        showSummary(down, up, { N, bidir, direction });
        setStatus(state.mode === "loopback" ? "Done (loopback self-test)" : "Done");
        setInfo("saved #" + r.id);
        await loadHistory();
        updateEstimate();   // freshen the data estimate if the config panel is open
    } catch (e) {
        setStatus("Error: " + (e && e.message ? e.message : e));
    } finally {
        state.running = false;
        setStartButton(false);
        el("streamStartBtn").disabled = false;
        setActive("down", false);
        setActive("up", false);
    }
}

function showSummary(down, up, meta) {
    const s = el("summary");
    const dV = down ? fmtMbps(down.avg) : "—";
    const uV = up ? fmtMbps(up.avg) : "—";
    const dPk = down ? fmtMbps(down.peak) : "—";
    const uPk = up ? fmtMbps(up.peak) : "—";
    const bits = [`${state.settings.measure_s}s window`];
    if (meta) {
        if (meta.N > 1) bits.push(`${meta.N} streams`);
        if (meta.bidir) bits.push("full-duplex");
    }
    if (state.mode === "loopback") bits.push("loopback");
    s.innerHTML = `
      <div class="big-pair">
        <div class="metric down"><span class="v">${dV}</span><span class="k"><span class="arr">↓</span> download mbps</span></div>
        <div class="metric up"><span class="v">${uV}</span><span class="k"><span class="arr">↑</span> upload mbps</span></div>
      </div>
      <div class="peak-line">peak ${dPk} <span class="arr down">↓</span> · ${uPk} <span class="arr up">↑</span> &nbsp;·&nbsp; ${bits.join(" · ")}</div>`;
    s.classList.remove("hidden");
}

// ---- throughput test configuration panel ------------------------------------
// Tunable methodology, surfaced from the server (DEFAULTS + bounds). Every knob
// is exposed (engineer-grade); the server clamps on save, so the inputs guide
// but can't poison the methodology. Each row has an "i" that toggles inline
// help (no popover positioning — robust on desktop and the phone).
const I_SVG = '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">' +
    '<circle cx="8" cy="8" r="7" fill="none" stroke="currentColor" stroke-width="1.3"/>' +
    '<circle cx="8" cy="4.7" r="0.95" fill="currentColor"/>' +
    '<rect x="7.15" y="6.7" width="1.7" height="5" rx="0.6" fill="currentColor"/></svg>';

// Each entry is the three-beat framing: what is this, why does it matter, and
// — the operationally important one — what exactly it affects (the third beat
// consistently answers "does changing this move my number, or just the picture,
// or cost RAM, or nothing"). Rendered as labeled rows by glossaryHTML().
const SETTINGS_GLOSSARY = {
    warmup_s: {
        what: "Seconds skipped at the start of each direction before the measurement window opens.",
        why: "It lets the connection settle (keep-alive, and on upload the probe that sizes each POST) and gives the gauge a visible ramp. The TCP slow-start climb itself no longer has to be excluded here — the recorded number is the best sustained window, which steps over the ramp on its own.",
        affects: "Mostly the gauge's ramp and a little test time; only a marginal effect on the recorded number now. On a slow-ramping link, lengthen Measure rather than Warmup so the sustained window has steady run to land in.",
    },
    measure_s: {
        what: "Length of the steady-state measurement window, per direction.",
        why: "Longer runs give the sustained-window average more steady run to find, and start exposing variance — Wi-Fi airtime contention, retransmits — that a short window hides.",
        affects: "The number's stability (the sustained window needs roughly 40% of this as steady run), the test duration, and the data moved (rate × this, per direction).",
    },
    parallel_streams: {
        what: "Concurrent TCP flows per direction — the iperf -P axis.",
        why: "A single flow is capped by its own window and the path's bandwidth-delay product, so one flow often can't fill the link; running several shows whether your ceiling is per-flow or the link itself.",
        affects: "The aggregate rate measured (and the per-stream figure in history). Above 1, upload switches from the precise server-sampled method to a client-side aggregate — the average is the best sustained window either way, the peak goes slightly optimistic.",
    },
    dir_down: {
        what: "Whether to run the download direction (server → this device).",
        why: "Skipping a direction halves the test and lets you iterate on just the one you're tuning.",
        affects: "Whether a download result is produced this run; off shows \u201c\u2014\u201d for download in history.",
    },
    dir_up: {
        what: "Whether to run the upload direction (this device → server).",
        why: "Skipping a direction halves the test and lets you iterate on just the one you're tuning.",
        affects: "Whether an upload result is produced this run; off shows \u201c\u2014\u201d for upload in history.",
    },
    bidirectional: {
        what: "Run download and upload at the same time (full-duplex) instead of one then the other.",
        why: "If the two together only reach the one-way max, the path is half-duplex (Wi-Fi shares airtime); if each holds near its solo max, it's full-duplex (a wired switch).",
        affects: "How the run executes (one concurrent phase, not two) and the rates themselves — each direction now contends with the other, so both can read below their solo numbers.",
    },
    up_post_target_s: {
        what: "Target wall-clock duration of each upload POST.",
        why: "Bigger POSTs spend proportionally less time in per-request turnaround, so cwnd ramps faster and the flow reaches its steady rate sooner.",
        affects: "How quickly the upload reaches steady state (larger = sooner), not the recorded number directly — the sustained-window average already steps over the ramp. Capped by the upload block size.",
    },
    up_chunk_mb: {
        what: "Size of the incompressible block built once in browser RAM and reused for every upload POST — the maximum POST size.",
        why: "On a fast link the block must be large enough to meet the POST target (gigabit wants ~64 MiB for a 0.5 s target); too small caps the POST size and shortens each POST. Only matters above ~512 Mbps — slower links slice far less than this out of it.",
        affects: "The largest POST that \u201cPOST target\u201d can reach (relevant only on fast links), and resident browser RAM (this many MiB, held for the session).",
    },
    up_probe_mb: {
        what: "Starting size of the upload warmup probe, which doubles while POSTs stay cheap.",
        why: "It only sets how quickly warmup finds the rate — on a fast link a bigger start reaches steady probing sooner. Mostly inconsequential.",
        affects: "Warmup efficiency only — not the recorded result.",
    },
    gauge_interval_ms: {
        what: "How often the live needle resamples.",
        why: "A smaller window surfaces instantaneous jitter and micro-bursts; a larger one gives a calmer read.",
        affects: "The live display only — never the recorded average or peak.",
    },
    down_yield_kb: {
        what: "How many bytes the server hands to the socket per write on download.",
        why: "It sweeps per-syscall overhead against the Python/Werkzeug throughput ceiling — smaller means more syscalls, larger means fewer, coarser writes. A knob for the server's limit, not the network's.",
        affects: "The download rate only when the server (not the link) is the bottleneck — otherwise no visible effect.",
    },
    down_cap_s: {
        what: "Hard server-side ceiling on how long any single download stream may run, whatever the client asks.",
        why: "It's the runaway-stream backstop — an abandoned or buggy stream can't run forever.",
        affects: "Nothing in a normal run; it only ever truncates a stream that exceeds warmup + measure + margin.",
    },
    iperf_port: {
        what: "TCP port the iperf3 server listens on (iperf -p).",
        why: "Dodge a port already in use, run more than one server, or match a client that expects a specific port.",
        affects: "The listening port and the copy-command hint (which adds -p when it isn't the 5201 default). A client must target the same port.",
    },
    iperf_interval: {
        what: "Seconds between the server's throughput reports (iperf -i).",
        why: "A finer interval packs more points into each test's chart; a coarser one reads smoother with fewer.",
        affects: "Chart sample density and output cadence only — never the measured throughput.",
    },
    iperf_verbose: {
        what: "Add extra detail — CPU utilisation, retransmits, congestion info — to the server summary (-V).",
        why: "Deeper diagnostics when you want to see why a number landed where it did.",
        affects: "The text in the output box only — never the chart or the measured rate.",
    },
    iperf_forceflush: {
        what: "Flush the server's output at every interval (--forceflush).",
        why: "iperf's own guarantee that interval lines reach us in real time instead of buffered in lumps.",
        affects: "Output timing only, and lightly: stdbuf already line-buffers, so turning this off rarely changes what you see — it's the suspenders to that belt, plus a debugging lever.",
    },
    iperf_idle_timeout: {
        what: "Auto-restart the server if it sits idle or stuck for this many seconds (--idle-timeout; 0 = off).",
        why: "A long-lived reference server can wedge; this lets it self-heal without you noticing.",
        affects: "Server robustness only — nothing in a normal run.",
    },
    iperf_bind: {
        what: "Bind the listener to one local IP / interface (-B; empty = all interfaces).",
        why: "On a multi-homed box, listen only on the LAN you care about and skip the virbr0 / docker bridges.",
        affects: "Which interface accepts connections — and, when set, the IP in the copy-command hint, since a bound server only answers on that address (so it overrides the auto-detected one).",
    },
    iperf_affinity: {
        what: "Pin the server to a CPU core (-A; empty = none). Takes 'n' or 'n,m'.",
        why: "Cut scheduler jitter for steadier numbers in high-rate tests on a busy machine.",
        affects: "Measurement stability on a loaded host — niche, and no effect on a quiet one.",
    },
    idle_s: {
        what: "Seconds of the idle baseline phase — latency measured with no traffic running.",
        why: "Sets the reference the verdict compares against; a longer baseline gives steadier idle percentiles, a shorter one speeds the test up.",
        affects: "Total test length and baseline stability — not the loaded-phase verdict math itself.",
    },
    stream_only_s: {
        what: "Seconds the simulated stream runs alone, before the greedy bulk load joins it.",
        why: "Confirms the stream is clean without contention; longer builds confidence it's genuinely stable, shorter is quicker.",
        affects: "Total test length and how thoroughly the no-contention phase is sampled.",
    },
    loaded_s: {
        what: "Seconds the stream runs together with the greedy bulk transfer — the contention phase.",
        why: "This is the verdict: bufferbloat often needs sustained saturation to surface, and some links take longer than the default to reveal their true latency under load.",
        affects: "How long the link stays saturated, so how reliably bloat and stalls show — the most consequential duration here.",
    },
    probe_interval_ms: {
        what: "How often the latency probe pings the server, in milliseconds.",
        why: "Finer probing catches brief latency spikes the loaded phase induces; coarser reduces the probe's own overhead on the link.",
        affects: "Latency-curve resolution and a little probe traffic — not the stream or bulk rates.",
    },
    margin_sample_ms: {
        what: "How often the chart samples buffer margin and throughput, in milliseconds.",
        why: "Finer sampling draws a smoother, denser curve; coarser keeps fewer points.",
        affects: "Chart resolution only — purely how the run is drawn, not how it's measured.",
    },
    stream_cap_s: {
        what: "Server-side safety cap on how long a single paced stream may run.",
        why: "A runaway backstop so an abandoned stream can't run forever — and the ceiling for the test, since the stream spans the stream-only + loaded phases.",
        affects: "Nothing in a normal run; but lengthen the phases past this cap and the stream is cut short, so raise it when you extend them.",
    },
    mc_duration_s: {
        what: "How long a coordinated Multi Client run lasts, in seconds.",
        why: "Long enough for every client's streams to reach steady state and for contention to surface; short keeps the group test quick.",
        affects: "The shared run window — all clients start together and stop after this many seconds.",
    },
    mc_lat_good_ms: {
        what: "The upper edge of the healthy latency band on each client's latency chart.",
        why: "Round-trip at or below this reads as comfortable for real-time use (calls, gaming); it sets where the green band ends.",
        affects: "The green/amber boundary on the Multi Client latency charts — display only, not the measurement.",
    },
    mc_lat_bad_ms: {
        what: "The latency at which the chart band turns red.",
        why: "Round-trip at or above this is where interactive apps feel laggy; the amber band spans good→bad, red sits above.",
        affects: "The amber/red boundary on the Multi Client latency charts — display only, not the measurement.",
    },
};

// Render one glossary entry as three labeled beats.
function glossaryHTML(key) {
    const g = SETTINGS_GLOSSARY[key];
    if (!g) return "";
    const beat = (label, text) => text
        ? `<div class="cfg-help-row"><span class="cfg-help-label">${label}</span><span class="cfg-help-text">${text}</span></div>`
        : "";
    return beat("What", g.what) +
           beat("Why it matters", g.why) +
           beat("What it affects", g.affects);
}

// Ordered groups -> rows. type 'bool' = checkbox; otherwise a bounded number
// (min/max come from the server's bounds, step is a sensible local default).
const THROUGHPUT_GROUPS = [
    { title: "Measurement", fields: [
        { key: "warmup_s", label: "Warmup", unit: "s", step: 0.5 },
        { key: "measure_s", label: "Measure window", unit: "s", step: 1 },
        { key: "parallel_streams", label: "Parallel streams", unit: "", step: 1 },
    ]},
    { title: "Direction", fields: [
        { key: "dir_down", label: "Test download", type: "bool" },
        { key: "dir_up", label: "Test upload", type: "bool" },
        { key: "bidirectional", label: "Run simultaneously (full-duplex)", type: "bool" },
    ]},
    { title: "Upload", fields: [
        { key: "up_post_target_s", label: "POST target", unit: "s", step: 0.25 },
        { key: "up_chunk_mb", label: "Upload block", unit: "MiB", step: 8 },
        { key: "up_probe_mb", label: "Probe start", unit: "MiB", step: 1 },
    ]},
    { title: "Display", fields: [
        { key: "gauge_interval_ms", label: "Gauge interval", unit: "ms", step: 50 },
    ]},
    { title: "Advanced", collapsible: true, fields: [
        { key: "down_yield_kb", label: "Download yield slice", unit: "KiB", step: 16 },
        { key: "down_cap_s", label: "Download cap (ceiling)", unit: "s", step: 10 },
    ]},
];

// ---- shared config-panel rendering -----------------------------------------
// One source of truth for the cfg-* panel grammar, used by all four config
// builders (throughput, iperf, stream, multiclient). Each builder previously
// carried its own copy of the field/row/section/handler rendering; those copies
// had drifted (throughput inlined its inputs, iperf grew string fields, and two
// emitted a duplicate id="cfgsec-Advanced" that broke cross-panel toggling). The
// panels now differ only in their GROUPS data and their foot (estimate slot +
// reset/save handlers), which each builder passes in.

// One field's input — num | bool | str — with bounds pulled from the server.
function cfgFieldInput(f) {
    if (f.type === "bool")
        return `<input type="checkbox" class="cfg-check" data-key="${f.key}" ` +
            `${state.settings[f.key] ? "checked" : ""}>`;
    if (f.type === "str")
        return `<input type="text" class="cfg-num cfg-str" data-key="${f.key}" ` +
            `value="${escapeHtml(state.settings[f.key] || "")}" ` +
            `placeholder="${f.placeholder || ""}" maxlength="64" spellcheck="false">`;
    const b = state.bounds[f.key] || [];
    return `<input type="number" class="cfg-num" data-key="${f.key}" ` +
        `value="${state.settings[f.key]}" min="${b.length ? b[0] : ""}" ` +
        `max="${b.length ? b[1] : ""}" step="${f.step || 1}">` +
        `<span class="cfg-unit">${f.unit || ""}</span>`;
}

// One labeled row: name, ⓘ button, input, and the inline help. Deliberately
// carries NO element ids — the ⓘ handler finds its .cfg-help relative to the
// clicked button (see wireConfigPanel), so nothing collides across panels.
function cfgRowHTML(f) {
    return `<div class="cfg-row">
              <div class="cfg-row-main">
                <span class="cfg-name">${f.label}</span>
                <button class="info-btn" type="button" aria-label="About ${f.label}">${I_SVG}</button>
                <span class="cfg-spacer"></span>
                ${cfgFieldInput(f)}
              </div>
              <div class="cfg-help hidden">${glossaryHTML(f.key)}</div>
            </div>`;
}

// Groups -> sections. collapsible:true renders a toggle + a hidden body; the
// body has no id (wireConfigPanel resolves it relative to the toggle).
function cfgSectionsHTML(groups) {
    let html = "";
    for (const g of groups) {
        if (g.collapsible) {
            html += `<div class="cfg-section">
                <button class="cfg-section-toggle" type="button" aria-expanded="false">
                  <span class="chev">\u25B8</span><span class="cfg-section-title">${g.title}</span>
                </button>
                <div class="cfg-section-body hidden">${g.fields.map(cfgRowHTML).join("")}</div>
              </div>`;
        } else {
            html += `<div class="cfg-section"><div class="cfg-section-title">${g.title}</div>` +
                g.fields.map(cfgRowHTML).join("") + `</div>`;
        }
    }
    return html;
}

// Shared foot: an estimate slot (live → {id}; static → {text}) plus Reset/Save
// wired to the panel's own handlers (passed as global function names).
function cfgFootHTML(estimate, resetFn, saveFn) {
    const est = estimate.id
        ? `<span class="cfg-estimate" id="${estimate.id}"></span>`
        : `<span class="cfg-estimate">${estimate.text || ""}</span>`;
    return `<div class="cfg-foot">
        ${est}
        <span class="cfg-actions">
          <button class="btn small" type="button" onclick="${resetFn}()">Reset</button>
          <button class="btn primary small save-btn" type="button" onclick="${saveFn}(this)"><span class="btn-label">Save</span><span class="btn-saved"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12.5l4.5 4.5L19 7"/></svg>Saved</span><span class="btn-failed"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>Failed</span></button>
        </span>
      </div>`;
}

// Transient Save feedback, mirroring the iperf copy button: flash the clicked
// button green (✓ Saved) on success or red (✕ Failed) on error for ~1.6s, then
// revert. Pure class toggle — the markup carries all three states, CSS swaps
// them. clearTimeout guards rapid re-clicks. Pass ok=false from a save's catch.
function flashSaved(btn, ok) {
    if (!btn) return;
    btn.classList.remove("saved", "failed");
    btn.classList.add(ok === false ? "failed" : "saved");
    clearTimeout(btn._saveTimer);
    btn._saveTimer = setTimeout(() => {
        btn.classList.remove("saved", "failed");
        btn._saveTimer = null;
    }, 1600);
}

// The single delegated handler for every config panel: a section toggle
// expands/collapses its OWN body (resolved relative to the toggle — never via a
// global id), and the ⓘ button toggles its OWN inline help. opts.onInput, when
// given, fires on any [data-key] edit (panels with a live estimate use it).
function wireConfigPanel(panel, opts) {
    opts = opts || {};
    panel.addEventListener("click", (e) => {
        const sec = e.target.closest && e.target.closest(".cfg-section-toggle");
        if (sec) {
            const body = sec.parentElement.querySelector(".cfg-section-body");
            const open = body && !body.classList.toggle("hidden");
            sec.setAttribute("aria-expanded", String(open));
            sec.classList.toggle("open", open);
            return;
        }
        const ib = e.target.closest && e.target.closest(".info-btn");
        if (!ib) return;
        const row = ib.closest(".cfg-row");
        const help = row && row.querySelector(".cfg-help");
        if (!help) return;
        ib.classList.toggle("on", !help.classList.toggle("hidden"));
    });
    if (opts.onInput) {
        panel.addEventListener("input", (e) => {
            const inp = e.target.closest && e.target.closest("[data-key]");
            if (inp) opts.onInput(e, inp);
        });
    }
}

function buildConfigPanel() {
    const p = el("throughputConfig");
    p.innerHTML = cfgSectionsHTML(THROUGHPUT_GROUPS) +
        cfgFootHTML({ id: "cfgEstimate" }, "resetConfig", "saveConfig");
    wireConfigPanel(p, { onInput: (e, inp) => {
        if (inp.dataset.key === "dir_down" || inp.dataset.key === "dir_up") syncDuplexEnabled();
        updateEstimate();
    } });
    updateEstimate();
}

function readConfigPanel() {
    const out = {};
    el("throughputConfig").querySelectorAll("[data-key]").forEach((inp) => {
        const k = inp.dataset.key;
        out[k] = inp.type === "checkbox" ? inp.checked : parseFloat(inp.value);
    });
    return out;
}

function fillConfigInputs() {
    el("throughputConfig").querySelectorAll("[data-key]").forEach((inp) => {
        const k = inp.dataset.key;
        if (inp.type === "checkbox") inp.checked = !!state.settings[k];
        else inp.value = state.settings[k];
    });
    syncDuplexEnabled();
}

// Full-duplex only means anything when both directions run — the engine
// computes bidir = bidirectional && dir_down && dir_up, so a duplex run with
// one direction off silently degrades to a single-direction run. Mirror that
// in the panel: grey out and disable "Run simultaneously" unless both
// directions are on. The checkbox keeps its checked state while disabled, so
// the preference survives toggling a direction off and back on.
function syncDuplexEnabled() {
    const panel = el("throughputConfig");
    if (!panel) return;
    const down = panel.querySelector('[data-key="dir_down"]');
    const up = panel.querySelector('[data-key="dir_up"]');
    const bi = panel.querySelector('[data-key="bidirectional"]');
    if (!down || !up || !bi) return;
    const both = down.checked && up.checked;
    bi.disabled = !both;
    bi.title = both ? "" : "Full-duplex needs both directions enabled";
    const row = bi.closest(".cfg-row");
    if (row) row.classList.toggle("is-disabled", !both);
}

function updateEstimate() {
    const e = el("cfgEstimate");
    if (!e) return;
    const v = readConfigPanel();
    const want = [];
    if (v.dir_down) want.push("down");
    if (v.dir_up) want.push("up");
    if (!want.length) { e.textContent = "Select at least one direction."; return; }

    const secs = (isFinite(v.warmup_s) ? v.warmup_s : 0) +
                 (isFinite(v.measure_s) ? v.measure_s : 0);
    const ANCHOR = 1000;   // Mbps fallback for a direction with no measured rate yet
    const lr = state.lastRate || {};
    const toGB = (mbps) => (mbps * 1e6 / 8 * secs) / 1e9;
    const arrow = { down: '<span class="arr down">↓</span>',
                    up: '<span class="arr up">↑</span>' };

    let total = 0, anyFallback = false;
    const seg = [], anchorBits = [];
    for (const d of want) {
        const measured = lr[d] != null;
        const g = toGB(measured ? lr[d] : ANCHOR);
        total += g;
        seg.push(`${arrow[d]}${g.toFixed(2)} GB`);
        if (measured) anchorBits.push(`${d === "down" ? "↓" : "↑"}${Math.round(lr[d])}`);
        else anyFallback = true;
    }

    const head = want.length > 1
        ? `≈ ${seg.join(" · ")} · <b>${total.toFixed(2)} GB total</b>`
        : `≈ <b>${seg[0]}</b>`;
    let note;
    if (!anchorBits.length) {
        note = "at 1 Gbps";
    } else {
        note = `last run: ${anchorBits.join(" ")} Mbps` +
               (anyFallback ? " · others at 1 Gbps" : "");
    }
    e.innerHTML = `${head} <span class="cfg-anchor">(${note})</span>` +
        (v.parallel_streams > 1
            ? `<span class="cfg-anchor"> · streams fill the same pipe</span>` : "");
}

function toggleConfig() {
    const p = el("throughputConfig");
    const btn = el("cfgToggle");
    if (!p._built) { buildConfigPanel(); p._built = true; }
    const willOpen = p.classList.contains("hidden");
    p.classList.toggle("hidden", !willOpen);
    if (btn) {
        btn.classList.toggle("on", willOpen);
        btn.setAttribute("aria-expanded", String(willOpen));
    }
    if (willOpen) { fillConfigInputs(); updateEstimate(); }
}

async function saveConfig(btn) {
    const prevChunk = state.settings.up_chunk_mb;
    try {
        const r = await app.call("save_settings", { settings: readConfigPanel() });
        state.settings = { ...state.settings, ...r.settings };
        fillConfigInputs();   // reflect any server-side clamping back into the UI
        if (state.settings.up_chunk_mb !== prevChunk) {
            state.uploadBlob = makeUploadBlob(state.settings.up_chunk_mb);
        }
        updateEstimate();
        setStatus("Configuration saved");
        flashSaved(btn);
    } catch (e) {
        setStatus("Save failed: " + (e && e.message ? e.message : e));
        flashSaved(btn, false);
    }
}

async function resetConfig() {
    const prevChunk = state.settings.up_chunk_mb;
    try {
        const r = await app.call("reset_settings");
        state.settings = { ...state.settings, ...r.settings };
        fillConfigInputs();
        if (state.settings.up_chunk_mb !== prevChunk) {
            state.uploadBlob = makeUploadBlob(state.settings.up_chunk_mb);
        }
        updateEstimate();
        setStatus("Configuration reset to defaults");
    } catch (e) {
        setStatus("Reset failed: " + (e && e.message ? e.message : e));
    }
}

// ---- iperf server config panel ---------------------------------------------
// A parallel panel to the Throughput one: it shares the visual grammar (the
// cfg-* classes, the info "i" glossary, Reset/Save foot) but is built by its
// own functions rather than generalising the throughput builder — its needs
// diverge (string fields, a collapsible Advanced group, no live estimate, and
// applies-on-next-start semantics), and keeping it separate means the proven
// Throughput panel is untouched. The glossary entries live in the shared
// SETTINGS_GLOSSARY so glossaryHTML() renders both identically.
const IPERF_GROUPS = [
    { title: "Server", fields: [
        { key: "iperf_port", label: "Port", unit: "", step: 1 },
        { key: "iperf_interval", label: "Report interval", unit: "s", step: 0.5 },
    ]},
    { title: "Output", fields: [
        { key: "iperf_verbose", label: "Verbose summary", type: "bool" },
        { key: "iperf_forceflush", label: "Force flush (real-time)", type: "bool" },
    ]},
    { title: "Advanced", collapsible: true, fields: [
        { key: "iperf_idle_timeout", label: "Idle timeout", unit: "s", step: 10 },
        { key: "iperf_bind", label: "Bind address", type: "str", placeholder: "all interfaces" },
        { key: "iperf_affinity", label: "CPU affinity", type: "str", placeholder: "none" },
    ]},
];

function buildIperfConfig() {
    const p = el("iperfConfig");
    p.innerHTML = cfgSectionsHTML(IPERF_GROUPS) +
        cfgFootHTML({ text: "Applies on next start." }, "resetIperfConfig", "saveIperfConfig");
    wireConfigPanel(p);
}

function readIperfConfig() {
    const out = {};
    el("iperfConfig").querySelectorAll("[data-key]").forEach((inp) => {
        const k = inp.dataset.key;
        out[k] = inp.type === "checkbox" ? inp.checked
            : inp.type === "text" ? inp.value.trim()
            : parseFloat(inp.value);
    });
    return out;
}

function fillIperfConfig() {
    el("iperfConfig").querySelectorAll("[data-key]").forEach((inp) => {
        const k = inp.dataset.key;
        if (inp.type === "checkbox") inp.checked = !!state.settings[k];
        else if (inp.type === "text") inp.value = state.settings[k] || "";
        else inp.value = state.settings[k];
    });
}

function toggleIperfConfig() {
    const p = el("iperfConfig");
    const btn = el("iperfCfgToggle");
    if (!p._built) { buildIperfConfig(); p._built = true; }
    const willOpen = p.classList.contains("hidden");
    p.classList.toggle("hidden", !willOpen);
    if (btn) {
        btn.classList.toggle("on", willOpen);
        btn.setAttribute("aria-expanded", String(willOpen));
    }
    if (willOpen) fillIperfConfig();
}

async function saveIperfConfig(btn) {
    try {
        const r = await app.call("save_settings", { settings: readIperfConfig() });
        state.settings = { ...state.settings, ...r.settings };
        fillIperfConfig();   // reflect any server-side clamping back into the UI
        // The flags are baked into the process at launch, so an edit only lands
        // on the next start — say so plainly when a server is already running.
        setStatus(state.iperf.running
            ? "Saved \u2014 restart the iperf server to apply"
            : "Configuration saved");
        flashSaved(btn);
    } catch (e) {
        setStatus("Save failed: " + (e && e.message ? e.message : e));
        flashSaved(btn, false);
    }
}

async function resetIperfConfig() {
    // reset_settings is global (one settings file for both tabs), matching the
    // Throughput Reset — it drops every override, not just the iperf ones.
    try {
        const r = await app.call("reset_settings");
        state.settings = { ...state.settings, ...r.settings };
        fillIperfConfig();
        setStatus("Configuration reset to defaults");
    } catch (e) {
        setStatus("Reset failed: " + (e && e.message ? e.message : e));
    }
}

// ---- stream test config panel ----------------------------------------------
// Surfaces the six stream knobs that already live in DEFAULTS/SETTING_BOUNDS.
// Parallel to the iperf panel (shares cfg-* grammar, the glossary, collapsible
// Advanced), but its foot carries a live total-time estimate + a guard: the
// paced stream spans stream-only + loaded, so if that exceeds the safety cap the
// stream gets cut — the foot warns before you run into it. Applies on next run
// (runStream reads state.settings at start).
const STREAM_GROUPS = [
    { title: "Phases", fields: [
        { key: "idle_s", label: "Idle baseline", unit: "s", step: 0.5 },
        { key: "stream_only_s", label: "Stream alone", unit: "s", step: 0.5 },
        { key: "loaded_s", label: "Stream + bulk", unit: "s", step: 0.5 },
    ]},
    { title: "Sampling", fields: [
        { key: "probe_interval_ms", label: "Latency probe", unit: "ms", step: 10 },
        { key: "margin_sample_ms", label: "Chart sample", unit: "ms", step: 10 },
    ]},
    { title: "Advanced", collapsible: true, fields: [
        { key: "stream_cap_s", label: "Stream safety cap", unit: "s", step: 5 },
    ]},
];

function buildStreamConfig() {
    const p = el("streamConfig");
    p.innerHTML = cfgSectionsHTML(STREAM_GROUPS) +
        cfgFootHTML({ id: "streamCfgEstimate" }, "resetStreamConfig", "saveStreamConfig");
    wireConfigPanel(p, { onInput: () => updateStreamEstimate() });
    updateStreamEstimate();
}

// total = all three phases; the paced stream spans stream-only + loaded, so that
// sum is what the safety cap must clear.
function updateStreamEstimate() {
    const e = el("streamCfgEstimate");
    if (!e) return;
    const v = readStreamConfig();
    const n = (x) => (isFinite(x) ? x : 0);
    const fs = (x) => { const r = Math.round(x * 10) / 10; return (Number.isInteger(r) ? r : r.toFixed(1)) + "s"; };
    const total = n(v.idle_s) + n(v.stream_only_s) + n(v.loaded_s);
    const span = n(v.stream_only_s) + n(v.loaded_s);
    const cap = n(v.stream_cap_s);
    if (cap && span > cap) {
        e.innerHTML = `\u26A0 stream + loaded (${fs(span)}) exceeds the ${fs(cap)} cap \u2014 it'll be cut short; raise the cap.`;
        e.classList.add("warn");
    } else {
        e.innerHTML = `\u2248 total test time: ${fs(total)}`;
        e.classList.remove("warn");
    }
}

function readStreamConfig() {
    const out = {};
    el("streamConfig").querySelectorAll("[data-key]").forEach((inp) => {
        out[inp.dataset.key] = parseFloat(inp.value);
    });
    return out;
}

function fillStreamConfig() {
    el("streamConfig").querySelectorAll("[data-key]").forEach((inp) => {
        inp.value = state.settings[inp.dataset.key];
    });
}

function toggleStreamConfig() {
    const p = el("streamConfig");
    const btn = el("streamCfgToggle");
    if (!p._built) { buildStreamConfig(); p._built = true; }
    const willOpen = p.classList.contains("hidden");
    p.classList.toggle("hidden", !willOpen);
    if (btn) {
        btn.classList.toggle("on", willOpen);
        btn.setAttribute("aria-expanded", String(willOpen));
    }
    if (willOpen) { fillStreamConfig(); updateStreamEstimate(); }
}

async function saveStreamConfig(btn) {
    try {
        const r = await app.call("save_settings", { settings: readStreamConfig() });
        state.settings = { ...state.settings, ...r.settings };
        fillStreamConfig();          // reflect any server-side clamping
        updateStreamEstimate();
        setStatus("Configuration saved");   // applies on the next run
        flashSaved(btn);
    } catch (e) {
        setStatus("Save failed: " + (e && e.message ? e.message : e));
        flashSaved(btn, false);
    }
}

async function resetStreamConfig() {
    // reset_settings is global (one file for all tabs), matching the others.
    try {
        const r = await app.call("reset_settings");
        state.settings = { ...state.settings, ...r.settings };
        fillStreamConfig();
        updateStreamEstimate();
        setStatus("Configuration reset to defaults");
    } catch (e) {
        setStatus("Reset failed: " + (e && e.message ? e.message : e));
    }
}

// ============================================================================
//  STREAM SAFETY
// ============================================================================

// ---- latency probe -----------------------------------------------------------
// Round trips include ~1-2 ms of framework overhead; the idle-vs-loaded delta
// cancels that constant. The probe's keep-alive connection stays warm between
// hits, so each sample measures the path, not a fresh handshake.
function makeProbe(intervalMs) {
    const ctx = { stop: false, phase: "idle", samples: [], last: NaN };
    ctx.task = (async () => {
        while (!ctx.stop) {
            const t0 = performance.now();
            try {
                await fetch(`/raw/echo?cb=${Date.now()}`, { cache: "no-store" });
            } catch (e) {
                if (ctx.stop) break;
            }
            const ms = performance.now() - t0;
            ctx.samples.push({ t: t0, ms, phase: ctx.phase });
            ctx.last = ms;
            const wait = intervalMs - (performance.now() - t0);
            if (wait > 0) await sleep(wait);
        }
    })();
    return ctx;
}

function latStats(samples, phase) {
    const v = samples.filter((s) => s.phase === phase).map((s) => s.ms)
        .sort((a, b) => a - b);
    return { p50: percentile(v, 50), p95: percentile(v, 95), n: v.length };
}

// ---- player buffer model -------------------------------------------------------
// Mirrors a real player: prebuffer buffer_s seconds of content, then consume
// at the stream rate. On underrun, consumption PAUSES (a stall) and resumes
// once delivery rebuilds min(1s, buffer/2) of content — the rebuffer pattern
// real players use, so stall counts/durations read like real playback events.
class BufferModel {
    constructor(rateBps, bufferS) {
        this.rateBps = rateBps;
        this.prebufBytes = bufferS * rateBps;
        this.resumeBytes = Math.min(1.0, bufferS / 2) * rateBps;
        this.delivered = 0;
        this.consumed = 0;
        this.playing = false;
        this.stalled = false;
        this.underruns = 0;
        this.stallMs = 0;
        this.minMargin = Infinity;
        this.lastT = null;
    }
    addBytes(n) { this.delivered += n; }
    tick(t) {
        if (this.lastT === null) { this.lastT = t; return; }
        const dt = (t - this.lastT) / 1000;
        this.lastT = t;
        if (!this.playing) {
            if (this.delivered >= this.prebufBytes) this.playing = true;
            else return;
        }
        if (this.stalled) {
            this.stallMs += dt * 1000;
            if (this.delivered - this.consumed >= this.resumeBytes) this.stalled = false;
            this.minMargin = Math.min(this.minMargin, 0);
            return;
        }
        this.consumed += dt * this.rateBps;
        if (this.consumed >= this.delivered) {
            this.consumed = this.delivered;
            this.stalled = true;
            this.underruns += 1;
        }
        this.minMargin = Math.min(this.minMargin,
            (this.delivered - this.consumed) / this.rateBps);
    }
    margin() {
        return (this.delivered - this.consumed) / this.rateBps;
    }
    resetWindow() {  // start phase-scoped stats fresh
        this.underrunsBase = this.underruns;
        this.stallMsBase = this.stallMs;
        this.minMargin = this.margin();
    }
    windowStats() {
        return {
            underruns: this.underruns - (this.underrunsBase || 0),
            stallMs: this.stallMs - (this.stallMsBase || 0),
            minMargin: this.minMargin,
        };
    }
}

// ---- paced stream reader --------------------------------------------------------
function startPacedStream(rateMbps, capS, model) {
    const ctrl = new AbortController();
    const task = (async () => {
        try {
            await readRawStream(
                `/raw/stream?rate_mbps=${rateMbps}&cap_s=${capS}&cb=${Date.now()}`,
                ctrl.signal, (n) => model.addBytes(n));
        } catch (e) {
            if (e.name !== "AbortError") throw e;
        }
    })();
    return { ctrl, task };
}

// ---- greedy bulk load -------------------------------------------------------------
function startBulk(dir, capS) {
    const ctx = { stop: false, bytes: 0, t0: performance.now(), tEnd: null };
    if (dir === "down") {
        const ctrl = new AbortController();
        ctx.stopFn = () => { ctx.stop = true; ctrl.abort(); };
        ctx.task = (async () => {
            try {
                await readRawStream(
                    `/raw/download?cap_s=${capS}&cb=${Date.now()}`,
                    ctrl.signal, (n) => { ctx.bytes += n; });
            } catch (e) {
                if (e.name !== "AbortError") throw e;
            }
            ctx.tEnd = performance.now();
        })();
    } else {
        const ctrl = new AbortController();
        ctx.stopFn = () => { ctx.stop = true; ctrl.abort(); };
        ctx.task = (async () => {
            const blob = state.uploadBlob;
            // Post laddered slices, growing toward ~GROW_BELOW_MS per POST: start
            // small so slow links keep completing frequently (never 0, smooth),
            // and double on each fast completion up to the WHOLE blob. The cap is
            // the blob, NOT a few MB — capping low makes fast/loopback paths post
            // tiny chunks hundreds of times a second, which both collapses
            // throughput into per-request overhead and floods the dev server's
            // sockets/FDs until it wedges to 0. Big posts on a fast path amortize
            // the overhead and keep the request rate sane (this is what the
            // original whole-blob code did, minus the slow-link 0). Credit the
            // server-received byte count (raw_upload's ack) over wall-clock, so
            // the rate is measured at the receiver.
            const MIN_CHUNK = 64 * 1024;
            const MAX_CHUNK = blob.size;
            const GROW_BELOW_MS = 150;
            let chunk = Math.min(MIN_CHUNK, blob.size);
            while (!ctx.stop) {
                try {
                    const t0 = performance.now();
                    const resp = await fetch(`/raw/upload?interval_ms=250&cb=${Date.now()}`, {
                        method: "POST", body: blob.slice(0, chunk),
                        headers: { "Content-Type": "application/octet-stream" },
                        cache: "no-store", signal: ctrl.signal,
                    });
                    const ack = await resp.json();
                    ctx.bytes += (ack && Number.isFinite(ack.bytes)) ? ack.bytes : chunk;
                    if (performance.now() - t0 < GROW_BELOW_MS && chunk < MAX_CHUNK)
                        chunk = Math.min(chunk * 2, MAX_CHUNK);
                } catch (e) {
                    if (ctx.stop) break;
                    // Transient hiccup under load — back off and keep contending
                    // rather than killing the flow; one failed POST shouldn't
                    // freeze the line at 0 for the rest of the phase.
                    await sleep(50);
                }
            }
            ctx.tEnd = performance.now();
        })();
    }
    ctx.rate = () => {
        const end = ctx.tEnd || performance.now();
        const dt = (end - ctx.t0) / 1000;
        return dt > 0 ? (ctx.bytes * 8 / 1e6) / dt : 0;
    };
    return ctx;
}

// Live readout metric for the CURRENT box: big mono value + unit, small
// label beneath. cls tints the value to echo the chart (lat=violet line,
// mgn=green margin area); bulk stays neutral (not drawn on the chart).
function currentMetric(value, unit, label, cls) {
    return `<div class="cm"><span class="cv ${cls}">${value}<span class="cu">${unit}</span></span><span class="ck">${label}</span></div>`;
}

// ---- the chart -------------------------------------------------------------------
// Three stacked tracks across the shared test timeline: buffer margin (area,
// top), probe latency (line, middle), and throughput (bottom) — stream
// received rate in blue + bulk in white. The throughput track uses a DUAL
// y-axis: stream scaled to its own rate (left) and bulk to its own magnitude
// (right), because bulk can be 50× the stream — a shared scale would flatten
// the stream line and hide exactly the dips we want to see. History minis
// (compact) stay margin-only. One renderer serves live + replay.
//
// Responsive sizing (full mode): the viewBox width tracks the container's
// pixel width, floored at THR_W (760). Below THR_W the SVG scales down
// uniformly (height < 300); at/above it the viewBox width matches the render
// width 1:1, so the chart FILLS the width while height holds at 300 and the
// tracks just spread horizontally. Requires a re-render on resize (see
// reflowCharts) to keep the viewBox synced in the wide regime.
const CHART_THRESHOLD_W = 760;
function renderStreamChart(container, cfg, samples, compact) {
    const cw = (container && container.clientWidth) || CHART_THRESHOLD_W;
    const W = compact ? 110 : Math.max(CHART_THRESHOLD_W, Math.round(cw));
    // Full charts reserve a bottom gutter below the plot to hold the crosshair
    // time chip; the plot itself (tracks, bands, dotted line) ends at plotB so
    // nothing bleeds into the gutter. Compact minis have no gutter.
    const axisGutter = compact ? 0 : 24;
    const plotH = compact ? 26 : 300;     // height of the plot region
    const H = plotH + axisGutter;         // full svg height (the viewBox)
    // Rendered px per viewBox unit (1 at/above threshold; <1 when scaled down).
    // Used to counter-scale text to a constant rendered size, and to hold the
    // horizontal axis gutters constant in rendered px so the constant-size
    // y-axis labels always have room and never clip at the left/right edges.
    const renderScale = (cw / W) || 1;
    const padL = compact ? 0 : Math.round(54 / renderScale);
    const padR = compact ? 0 : Math.round(46 / renderScale);
    const padT = compact ? 1 : 14, padB = compact ? 1 : 14;
    const plotB = plotH - padB;           // bottom of the plot (bands + line end here)
    const totalS = cfg.idle_s + cfg.stream_only_s + cfg.loaded_s;
    const plotW = W - padL - padR;
    const x = (t) => padL + (Math.min(t, totalS) / totalS) * plotW;

    // Track layout. Compact = margin only (full height). Full = three tracks
    // with gaps: margin (headline, tallest), latency, throughput.
    const GAP = 16;
    const marginTop = padT;
    const marginH = compact ? (H - padT - padB) : 90;
    const latTop = marginTop + marginH + (compact ? 0 : GAP);
    const latH = compact ? 0 : 66;
    const thrTop = latTop + latH + (compact ? 0 : GAP);
    const thrH = compact ? 0 : 80;

    const yMaxMargin = Math.max(cfg.buffer_s * 1.25, 1);
    const ym = (v) => marginTop + marginH -
        (Math.max(0, Math.min(v, yMaxMargin)) / yMaxMargin) * marginH;

    const lats = samples.map((s) => s.lat).filter((v) => isFinite(v) && v > 0);
    const yMaxLat = Math.max(10, ...lats) * 1.15;
    const yl = (v) => latTop + latH -
        (Math.max(0, Math.min(v, yMaxLat)) / yMaxLat) * latH;

    // throughput dual scales
    const strVals = samples.map((s) => s.streamMbps).filter((v) => Number.isFinite(v));
    const blkVals = samples.map((s) => s.bulkMbps).filter((v) => Number.isFinite(v));
    const yMaxStr = Math.max(cfg.rate_mbps || 0, 1, ...strVals) * 1.2;
    const hasBulk = blkVals.length > 0;
    const yMaxBlk = hasBulk ? Math.max(1, ...blkVals) * 1.15 : 1;
    const ysStr = (v) => thrTop + thrH -
        (Math.max(0, Math.min(v, yMaxStr)) / yMaxStr) * thrH;
    const ysBlk = (v) => thrTop + thrH -
        (Math.max(0, Math.min(v, yMaxBlk)) / yMaxBlk) * thrH;

    let svg = `<svg viewBox="0 0 ${W} ${H}" style="--chart-scale:${renderScale.toFixed(3)}" xmlns="http://www.w3.org/2000/svg">`;

    // phase bands + boundaries. A solo run has no 'loaded' samples, so it
    // renders one stream band across the back instead of a misleading
    // amber "stream + bulk" band that never had a contender.
    const isSolo = !samples.some((s) => s && s.phase === "loaded");
    const bands = isSolo ? [
        ["idle", 0, cfg.idle_s, "idle"],
        ["stream", cfg.idle_s, totalS, "stream (no contention)"],
    ] : [
        ["idle", 0, cfg.idle_s, "idle"],
        ["stream", cfg.idle_s, cfg.idle_s + cfg.stream_only_s, "stream alone"],
        ["loaded", cfg.idle_s + cfg.stream_only_s, totalS, "stream + bulk"],
    ];
    for (const [cls, a, b, label] of bands) {
        svg += `<rect class="ch-band ${cls}" x="${x(a)}" y="${padT}" width="${x(b) - x(a)}" height="${plotB - padT}"/>`;
        if (!compact) svg += `<text class="ch-phase-label" x="${x(a) + 4}" y="${padT - 2}">${label}</text>`;
    }

    if (!compact) {
        // margin gridlines: buffer level + zero
        svg += `<line class="ch-grid" x1="${padL}" y1="${ym(cfg.buffer_s)}" x2="${W - padR}" y2="${ym(cfg.buffer_s)}"/>`;
        svg += `<text class="ch-axis mgn" x="${padL - 4}" y="${ym(cfg.buffer_s) + 3}" text-anchor="end">${cfg.buffer_s}s</text>`;
        svg += `<line class="ch-zero" x1="${padL}" y1="${ym(0)}" x2="${W - padR}" y2="${ym(0)}"/>`;
        svg += `<text class="ch-axis mgn" x="${padL - 4}" y="${ym(0) + 3}" text-anchor="end">0</text>`;
        svg += `<text class="ch-track-label" x="${padL}" y="${marginTop + 14}">buffer margin</text>`;
        // latency axis — zero line red-dotted to match the margin zero
        svg += `<line class="ch-zero" x1="${padL}" y1="${yl(0)}" x2="${W - padR}" y2="${yl(0)}"/>`;
        svg += `<text class="ch-axis lat" x="${padL - 4}" y="${latTop + 4}" text-anchor="end">${fmtMs(yMaxLat)}ms</text>`;
        svg += `<text class="ch-axis lat" x="${padL - 4}" y="${yl(0) + 3}" text-anchor="end">0</text>`;
        svg += `<text class="ch-track-label" x="${padL}" y="${latTop + 9}">latency</text>`;
        // throughput axes (dual): stream rate on the left (blue), bulk
        // magnitude on the right (white) — separate scales so a 20 Mbps
        // stream and a 900 Mbps bulk are both legible on one track.
        svg += `<line class="ch-grid" x1="${padL}" y1="${ysStr(0)}" x2="${W - padR}" y2="${ysStr(0)}"/>`;
        svg += `<text class="ch-axis str" x="${padL - 4}" y="${thrTop + 4}" text-anchor="end">${fmtMbps(yMaxStr)}</text>`;
        svg += `<text class="ch-axis str" x="${padL - 4}" y="${ysStr(0) + 3}" text-anchor="end">0</text>`;
        svg += `<text class="ch-track-label" x="${padL}" y="${thrTop + 9}">throughput (Mbps)</text>`;
        if (hasBulk) {
            svg += `<text class="ch-axis blk" x="${W - padR + 4}" y="${thrTop + 4}" text-anchor="start">${fmtMbps(yMaxBlk)}</text>`;
            svg += `<text class="ch-axis blk" x="${W - padR + 4}" y="${ysBlk(0) + 3}" text-anchor="start">0</text>`;
        }
        // bottom x-axis: absolute seconds, same style/cadence as the iperf chart.
        // The crosshair time chip paints over these on hover (its bg occludes).
        const xstep = Math.max(1, Math.round(totalS / 6));
        for (let ts = 0; ts <= totalS + 0.001; ts += xstep) {
            svg += `<text class="ch-axis" x="${x(ts).toFixed(1)}" y="${(plotB + 16).toFixed(1)}" text-anchor="middle">${ts}s</text>`;
        }
    }

    // stall shading: contiguous samples where the model was stalled
    let stallStart = null;
    for (let i = 0; i <= samples.length; i++) {
        const s = samples[i];
        const stalled = s && s.stalled;
        if (stalled && stallStart === null) stallStart = s.t;
        if (!stalled && stallStart !== null) {
            const end = s ? s.t : samples[samples.length - 1].t;
            svg += `<rect class="ch-stall" x="${x(stallStart)}" y="${marginTop}" width="${Math.max(1, x(end) - x(stallStart))}" height="${marginH}"/>`;
            stallStart = null;
        }
    }

    // margin area + line (only samples where the stream existed)
    const mPts = samples.filter((s) => isFinite(s.margin));
    if (mPts.length > 1) {
        const line = mPts.map((s) => `${x(s.t).toFixed(1)},${ym(s.margin).toFixed(1)}`).join(" ");
        const area = `${x(mPts[0].t).toFixed(1)},${ym(0).toFixed(1)} ${line} ${x(mPts[mPts.length - 1].t).toFixed(1)},${ym(0).toFixed(1)}`;
        svg += `<polygon class="ch-margin-area" points="${area}"/>`;
        svg += `<polyline class="ch-margin-line" points="${line}"/>`;
    }

    // latency line (full timeline)
    if (!compact) {
        const lPts = samples.filter((s) => isFinite(s.lat) && s.lat > 0);
        if (lPts.length > 1) {
            const line = lPts.map((s) => `${x(s.t).toFixed(1)},${yl(s.lat).toFixed(1)}`).join(" ");
            svg += `<polyline class="ch-lat-line" points="${line}"/>`;
        }
    }

    // throughput lines: stream received rate (blue, left scale) shows dips
    // when contention bites; bulk (white, right scale) shows what the greedy
    // flow grabbed in the same instant.
    if (!compact) {
        const sPts = samples.filter((s) => Number.isFinite(s.streamMbps));
        if (sPts.length > 1) {
            const line = sPts.map((s) => `${x(s.t).toFixed(1)},${ysStr(s.streamMbps).toFixed(1)}`).join(" ");
            svg += `<polyline class="ch-stream-line" points="${line}"/>`;
        }
        const bPts = samples.filter((s) => Number.isFinite(s.bulkMbps));
        if (bPts.length > 1) {
            const line = bPts.map((s) => `${x(s.t).toFixed(1)},${ysBlk(s.bulkMbps).toFixed(1)}`).join(" ");
            svg += `<polyline class="ch-bulk-line" points="${line}"/>`;
        }
    }

    svg += `</svg>`;
    container.innerHTML = svg;

    // Compact minis are not interactive; never wire a crosshair onto them.
    if (compact) { container._xh = null; return; }

    // Stash the geometry (scale closures + layout scalars) and the live sample
    // array on the persistent HOST element. renderStreamChart swaps the host's
    // innerHTML every tick, but the host div itself survives — so the pointer
    // listeners (wired once) and the pin state live here and outlast re-renders.
    wireCrosshair(container);
    const xh = container._xh || (container._xh = { pinT: null, hoverT: null });
    xh.geom = { W, H, plotB, padL, padR, padT, padB, plotW, totalS, renderScale, hasBulk,
                x, ym, yl, ysStr, ysBlk };
    xh.samples = samples;
    xh.cfg = cfg;
    xh.chips = (s, g, chip) => {
        let out = "";
        if (Number.isFinite(s.margin))
            out += chip(`${s.margin.toFixed(1)}s`, g.ym(s.margin), "--green", "right");
        if (Number.isFinite(s.lat) && s.lat > 0)
            out += chip(`${fmtMs(s.lat)}ms`, g.yl(s.lat), "--latency", "right");
        if (Number.isFinite(s.streamMbps))
            out += chip(`${fmtMbps(s.streamMbps)}`, g.ysStr(s.streamMbps), "--download", "right");
        if (g.hasBulk && Number.isFinite(s.bulkMbps))
            out += chip(`${fmtMbps(s.bulkMbps)}`, g.ysBlk(s.bulkMbps), "--text", "left");
        return out;
    };
    // Re-apply the crosshair into the freshly rendered SVG (no-op if nothing is
    // pinned or hovered). This is the "live re-apply": it runs after EVERY
    // render — live tick, final, resize reflow, history-row open — so a pin
    // survives the sampler's 4×/sec redraw automatically with no caller changes.
    drawCrosshair(container);
}

// ---- crosshair (hover-scrub on mouse, tap-to-pin on touch) -----------------
// One vertical dotted line across all three tracks, snapped to the nearest
// sample, with per-track value chips + a relative-time chip. The line and chips
// are appended as a single <g class="xh"> INTO the chart SVG (same coordinate
// space as the data — no px translation), rebuilt imperatively on each pointer
// event or re-render. Mouse: move scrubs (transient), leave clears, click pins.
// Touch: tap pins (persists); tap the × to dismiss. State lives on container._xh
// (the persistent host), so it outlives the innerHTML swaps of the SVG child.
const XH_NS = "http://www.w3.org/2000/svg";
const XH_TAP_PX = 10;   // pointerup within this many px of pointerdown = a tap

function wireCrosshair(container) {
    if (container._xhWired) return;
    container._xhWired = true;
    let down = null;

    container.addEventListener("pointerdown", (e) => {
        const onX = !!(e.target.closest && e.target.closest(".xh-dismiss"));
        down = { x: e.clientX, y: e.clientY, dismiss: onX };
    });
    container.addEventListener("pointerup", (e) => {
        if (!down) return;
        const moved = Math.hypot(e.clientX - down.x, e.clientY - down.y);
        const onX = down.dismiss ||
            !!(e.target.closest && e.target.closest(".xh-dismiss"));
        down = null;
        if (moved > XH_TAP_PX) return;        // a drag/scroll, not a tap/click
        if (onX) { clearCrosshairPin(container); return; }
        pinCrosshair(container, e);           // mouse click OR touch tap
    });
    // Hover-scrub is mouse-only; touch uses discrete tap (no drag-scrub in v1,
    // which also keeps page scroll working without a touch-action: none claim).
    container.addEventListener("pointermove", (e) => {
        if (e.pointerType !== "mouse") return;
        hoverCrosshair(container, e);
    });
    container.addEventListener("pointerleave", (e) => {
        if (e.pointerType && e.pointerType !== "mouse") return;
        if (!container._xh) return;
        container._xh.hoverT = null;          // pin (if any) remains
        drawCrosshair(container);
    });
    container.addEventListener("pointercancel", () => {
        down = null;
        if (container._xh) { container._xh.hoverT = null; drawCrosshair(container); }
    });
}

// Cursor → viewBox via the SVG's own CTM (handles the responsive scale for
// free), then invert x() to a test-relative time, clamped to the run length.
function xhTimeFromEvent(container, e) {
    const xh = container._xh;
    if (!xh || !xh.geom) return null;
    const svg = container.querySelector("svg");
    if (!svg || !svg.getScreenCTM) return null;
    const ctm = svg.getScreenCTM();
    if (!ctm) return null;
    const pt = svg.createSVGPoint();
    pt.x = e.clientX; pt.y = e.clientY;
    const loc = pt.matrixTransform(ctm.inverse());
    const g = xh.geom;
    const t = ((loc.x - g.padL) / g.plotW) * g.totalS;
    if (!isFinite(t)) return null;
    return Math.max(0, Math.min(t, g.totalS));
}

function xhNearest(samples, t) {
    let best = null, bestD = Infinity;
    for (const s of samples) {
        if (!s) continue;
        const d = Math.abs(s.t - t);
        if (d < bestD) { bestD = d; best = s; }
    }
    return best;
}

function hoverCrosshair(container, e) {
    const xh = container._xh;
    if (!xh || !xh.geom || !xh.samples || !xh.samples.length) return;
    const t = xhTimeFromEvent(container, e);
    if (t == null) return;
    const s = xhNearest(xh.samples, t);
    xh.hoverT = s ? s.t : null;
    drawCrosshair(container);
}

function pinCrosshair(container, e) {
    const xh = container._xh;
    if (!xh || !xh.geom || !xh.samples || !xh.samples.length) return;
    const t = xhTimeFromEvent(container, e);
    if (t == null) return;
    const s = xhNearest(xh.samples, t);
    if (!s) return;
    xh.pinT = s.t;       // store the snapped TIME, not the sample — stays valid
    xh.hoverT = null;    // as the array grows during a live run
    drawCrosshair(container);
}

function clearCrosshairPin(container) {
    const xh = container._xh;
    if (!xh) return;
    xh.pinT = null; xh.hoverT = null;
    drawCrosshair(container);
}

// Build + inject the <g class="xh">. hoverT (transient) wins over pinT so a
// desktop hover previews live while the pin is the sticky home it returns to on
// leave. Chips render only where the sample value is finite, so the reveal
// follows the phase: idle → latency only; stream → +margin +stream; loaded → all.
function drawCrosshair(container) {
    const xh = container._xh;
    if (!xh || !xh.geom) return;
    const svg = container.querySelector("svg");
    if (!svg) return;
    const old = svg.querySelector(".xh");
    if (old) old.remove();

    const t = (xh.hoverT != null) ? xh.hoverT : xh.pinT;
    if (t == null || !xh.samples || !xh.samples.length) return;
    const s = xhNearest(xh.samples, t);
    if (!s) return;

    const g = xh.geom;
    const inv = 1 / (g.renderScale || 1);   // viewBox units per rendered px
    const fs = 11 * inv;                     // → constant 11px rendered
    const px = g.x(s.t);
    const pinned = (xh.hoverT == null && xh.pinT != null);
    // Per-track preferred side: bulk goes LEFT of the line, everything else
    // RIGHT. margin/latency/stream live on separate tracks so they never collide
    // with each other; bulk shares the throughput track with stream (both
    // normalize near the top in the loaded phase), so opposite sides keep that
    // pair apart. A chip flips to its other side only if its preferred side
    // would run off the plot edge.
    const chip = (label, cy, colorVar, side) => {
        const padX = 5 * inv, padY = 3 * inv, gap = 7 * inv;
        const w = label.length * fs * 0.6 + padX * 2;
        const h = fs + padY * 2;
        let rx = side === "left" ? (px - gap - w) : (px + gap);
        if (side === "left" && rx < g.padL) rx = px + gap;                     // → right
        else if (side === "right" && rx + w > g.W - g.padR) rx = px - gap - w; // → left
        let ry = cy - h / 2;
        ry = Math.max(g.padT, Math.min(ry, g.plotB - h));   // full plot height usable now
        return (
            `<circle class="xh-dot" cx="${px.toFixed(1)}" cy="${cy.toFixed(1)}" r="${(2.5 * inv).toFixed(1)}" style="fill:var(${colorVar})"/>` +
            `<rect class="xh-chip" x="${rx.toFixed(1)}" y="${ry.toFixed(1)}" width="${w.toFixed(1)}" height="${h.toFixed(1)}" rx="${(3 * inv).toFixed(1)}" style="stroke:var(${colorVar})"/>` +
            `<text class="xh-chip-t" x="${(rx + padX).toFixed(1)}" y="${(ry + h / 2 + 0.34 * fs).toFixed(1)}" font-size="${fs.toFixed(1)}" style="fill:var(${colorVar})">${label}</text>`
        );
    };

    let inner =
        `<line class="xh-line" x1="${px.toFixed(1)}" y1="${g.padT}" x2="${px.toFixed(1)}" y2="${g.plotB.toFixed(1)}"/>`;

    // Per-chart value chips: the host supplies xh.chips(sample, geom, chipFn).
    // Everything else here (line, snapped dot via chip, time chip, dismiss) is
    // shared, so the Stream and Iperf charts scrub identically.
    if (xh.chips) inner += xh.chips(s, g, chip);

    // time chip (relative seconds) in the gutter BELOW the x-axis, centered on
    // the line, + dismiss × when pinned
    {
        const padX = 6 * inv, padY = 3 * inv;
        const label = `${s.t.toFixed(1)}s`;
        const w = label.length * fs * 0.6 + padX * 2;
        const h = fs + padY * 2;
        const dW = pinned ? 22 * inv : 0;
        const totalW = w + dW;
        let rx = px - totalW / 2;
        rx = Math.max(g.padL, Math.min(rx, g.W - g.padR - totalW));
        const ry = g.plotB + ((g.H - g.plotB) - h) / 2;   // vertically centered in the gutter
        inner +=
            `<rect class="xh-tchip" x="${rx.toFixed(1)}" y="${ry.toFixed(1)}" width="${totalW.toFixed(1)}" height="${h.toFixed(1)}" rx="${(3 * inv).toFixed(1)}"/>` +
            `<text class="xh-tchip-t" x="${(rx + padX).toFixed(1)}" y="${(ry + h / 2 + 0.34 * fs).toFixed(1)}" font-size="${fs.toFixed(1)}">${label}</text>`;
        if (pinned) {
            const dx = rx + w;   // generous transparent hit area for fingers
            inner +=
                `<g class="xh-dismiss">` +
                `<rect x="${dx.toFixed(1)}" y="${(ry - 3 * inv).toFixed(1)}" width="${dW.toFixed(1)}" height="${(h + 6 * inv).toFixed(1)}" rx="${(3 * inv).toFixed(1)}" fill="transparent"/>` +
                `<text class="xh-x" x="${(dx + dW / 2).toFixed(1)}" y="${(ry + h / 2 + 0.34 * fs).toFixed(1)}" font-size="${fs.toFixed(1)}" text-anchor="middle">×</text>` +
                `</g>`;
        }
    }

    const gEl = document.createElementNS(XH_NS, "g");
    gEl.setAttribute("class", "xh");
    gEl.innerHTML = inner;
    svg.appendChild(gEl);   // last child → painted on top of the data
}

// ---- stream orchestration -----------------------------------------------------------
function getStreamConfig() {
    const S = state.settings;
    let rate, buffer, profile = state.profile;
    if (profile === "custom") {
        rate = parseFloat(el("customRate").value) || 40;
        buffer = parseFloat(el("customBuffer").value) || 5;
        rate = Math.max(0.5, Math.min(rate, 2000));
        buffer = Math.max(1, Math.min(buffer, 30));
    } else {
        const p = state.profiles[profile] || FALLBACK_PROFILES.video_mid;
        rate = p.rate_mbps; buffer = p.buffer_s;
    }
    return {
        profile, rate_mbps: rate, buffer_s: buffer, bulk_dir: state.bulkDir,
        idle_s: S.idle_s, stream_only_s: S.stream_only_s, loaded_s: S.loaded_s,
    };
}

function setPhaseStep(name) {
    for (const p of ["idle", "stream", "loaded"]) {
        const e = el("ph-" + p);
        e.classList.remove("live");
        if (name === p) e.classList.add("live");
        else if (name && ["idle", "stream", "loaded"].indexOf(p) <
                 ["idle", "stream", "loaded"].indexOf(name)) e.classList.add("done");
    }
    if (name === null) {
        for (const p of ["idle", "stream", "loaded"])
            el("ph-" + p).classList.remove("live", "done");
    }
}

function setStreamButton(running) {
    const btn = el("streamStartBtn");
    if (running) {
        btn.textContent = "Cancel";
        btn.classList.remove("success");
        btn.classList.add("danger");
    } else {
        btn.textContent = "Run Stream Test";
        btn.classList.add("success");
        btn.classList.remove("danger");
    }
}

async function onStreamStartClick() {
    if (state.streamRunning) {
        state.cancelled = true;
        setStatus("Cancelling…");
        return;
    }
    if (state.running) return;
    await runStream();
}

async function runStream() {
    state.streamRunning = true;
    state.cancelled = false;
    setStreamButton(true);
    el("startBtn").disabled = true;
    el("streamVerdict").classList.add("hidden");
    el("phaseStrip").classList.remove("hidden");
    el("chartCard").classList.remove("hidden");
    setPhaseStep(null);
    setInfo("");

    const cfg = getStreamConfig();
    const S = state.settings;
    const totalS = cfg.idle_s + cfg.stream_only_s + cfg.loaded_s;
    const samples = [];   // {t, margin, lat, phase, stalled}
    const chartEl = el("streamChart");
    if (chartEl._xh) { chartEl._xh.pinT = null; chartEl._xh.hoverT = null; }
    lastLiveChart = { cfg, samples };   // for resize redraw of the final chart
    renderStreamChart(chartEl, cfg, samples, false);

    const probe = makeProbe(S.probe_interval_ms);
    const model = new BufferModel(cfg.rate_mbps * 1e6 / 8, cfg.buffer_s);
    let stream = null, bulk = null;
    const t0 = performance.now();

    // sampler: drives the model clock, collects chart points, paints live
    let phase = "idle";
    let lastSampleT = t0, lastStreamBytes = 0, lastBulkBytes = 0;
    const samplerCtx = { stop: false };
    const sampler = (async () => {
        while (!samplerCtx.stop) {
            const now = performance.now();
            const t = (now - t0) / 1000;
            if (stream) model.tick(now);
            // per-interval received throughput (Mbps) for the two flows
            const dtS = (now - lastSampleT) / 1000;
            let streamMbps = NaN, bulkMbps = NaN;
            if (stream && dtS > 0) {
                streamMbps = (model.delivered - lastStreamBytes) * 8 / 1e6 / dtS;
                lastStreamBytes = model.delivered;
            }
            if (bulk && dtS > 0) {
                bulkMbps = (bulk.bytes - lastBulkBytes) * 8 / 1e6 / dtS;
                lastBulkBytes = bulk.bytes;
            }
            lastSampleT = now;
            samples.push({
                t: round2(t),
                margin: stream ? round2(model.margin()) : NaN,
                lat: isFinite(probe.last) ? round2(probe.last) : NaN,
                streamMbps: isFinite(streamMbps) ? round2(streamMbps) : NaN,
                bulkMbps: isFinite(bulkMbps) ? round2(bulkMbps) : NaN,
                phase,
                stalled: stream ? model.stalled : false,
            });
            renderStreamChart(chartEl, cfg, samples, false);
            el("chartLive").innerHTML =
                currentMetric(fmtMs(probe.last), "ms", "latency", "lat") +
                (stream ? currentMetric(model.margin().toFixed(1), "s", "margin", "mgn") : "") +
                (stream ? currentMetric(fmtMbps(streamMbps), "Mbps", "stream", "str") : "") +
                (bulk ? currentMetric(fmtMbps(bulkMbps), "Mbps", "bulk", "blk") : "");
            await sleep(S.margin_sample_ms);
        }
    })();

    const solo = cfg.bulk_dir === "none";
    const nPhases = solo ? 2 : 3;
    let verdict, baseStats = null, loadedStats, idle, loaded = { p50: NaN, p95: NaN, n: 0 }, bulkMbps = null;

    try {
        // Phase A — idle latency baseline (run for both modes: the
        // idle-vs-stream perturbation is meaningful even with no bulk)
        setStatus(`Phase 1/${nPhases}: idle latency baseline…`);
        setPhaseStep("idle");
        if (!await waitOrCancel(cfg.idle_s * 1000)) throw new Error("cancelled");

        // Phase B — paced stream. Solo reuses the contention run's total
        // duration by streaming through both the stream_only and loaded
        // spans, so a solo run is a full measurement, not a stunted one.
        const streamDur = solo ? (cfg.stream_only_s + cfg.loaded_s) : cfg.stream_only_s;
        setStatus(`Phase 2/${nPhases}: stream${solo ? "" : " alone"}…`);
        setPhaseStep("stream");
        phase = "stream"; probe.phase = "stream";
        stream = startPacedStream(cfg.rate_mbps, streamDur + cfg.loaded_s + 10, model);

        if (solo) {
            // The stream phase IS the measurement. Don't resetWindow — that
            // seeds minMargin from the current (zero) margin before prebuffer
            // and would peg every solo run TIGHT. Cumulative-from-start stats
            // (underrunsBase defaults 0, minMargin tracks from Infinity once
            // playback begins) already scope the whole stream phase.
            if (!await waitOrCancel(streamDur * 1000)) throw new Error("cancelled");
            stream.ctrl.abort();
            probe.stop = true;
            samplerCtx.stop = true;
            await Promise.allSettled([stream.task, probe.task, sampler]);

            loadedStats = model.windowStats();   // whole stream-phase stats
            idle = latStats(probe.samples, "idle");
            // no contender → no 'loaded' latency, no bulk rate (stay NULL)

            // Verdict: no baseline to compare against — the run *is* the
            // baseline. If it never even prebuffered, that's the worst case.
            if (!model.playing || loadedStats.underruns > 0) verdict = "degraded";
            else if (loadedStats.minMargin < cfg.buffer_s * 0.5) verdict = "tight";
            else verdict = "clean";
        } else {
            if (!await waitOrCancel(cfg.stream_only_s * 1000)) throw new Error("cancelled");
            baseStats = (() => {
                const s = { underruns: model.underruns, stallMs: model.stallMs,
                            minMargin: model.minMargin };
                model.resetWindow();
                return s;
            })();

            // Phase C — stream + greedy bulk
            setStatus(`Phase 3/${nPhases}: stream + bulk load…`);
            setPhaseStep("loaded");
            phase = "loaded"; probe.phase = "loaded";
            bulk = startBulk(cfg.bulk_dir, cfg.loaded_s + 10);
            if (!await waitOrCancel(cfg.loaded_s * 1000)) throw new Error("cancelled");

            bulk.stopFn();
            stream.ctrl.abort();
            probe.stop = true;
            samplerCtx.stop = true;
            await Promise.allSettled([bulk.task, stream.task, probe.task, sampler]);

            loadedStats = model.windowStats();
            idle = latStats(probe.samples, "idle");
            loaded = latStats(probe.samples, "loaded");
            bulkMbps = bulk.rate();

            if (baseStats.underruns > 0 || (model.playing === false)) verdict = "baseline";
            else if (loadedStats.underruns > 0) verdict = "degraded";
            else if (loadedStats.minMargin < cfg.buffer_s * 0.5) verdict = "tight";
            else verdict = "clean";
        }

        // Average stream throughput over the verdict-relevant phase (loaded
        // for contention, the stream phase for solo) — parallels bulk_mbps's
        // window so "stream vs bulk" reads as a same-window comparison.
        const streamPhaseKey = solo ? "stream" : "loaded";
        const strSamples = samples.filter(
            (s) => s.phase === streamPhaseKey && Number.isFinite(s.streamMbps));
        const streamAvg = strSamples.length
            ? strSamples.reduce((a, s) => a + s.streamMbps, 0) / strSamples.length
            : NaN;

        renderStreamChart(chartEl, cfg, samples, false);
        showVerdict(verdict, cfg, baseStats, loadedStats, idle, loaded, bulkMbps, streamAvg);

        setStatus("Saving…");
        const sDescEl = el("streamTestDesc");
        const sDescription = sDescEl ? sDescEl.value.trim().slice(0, 200) : "";
        const r = await app.call("save_stream_result", {
            profile: cfg.profile,
            rate_mbps: round2(cfg.rate_mbps),
            buffer_s: round2(cfg.buffer_s),
            bulk_dir: solo ? null : cfg.bulk_dir,
            verdict,
            idle_lat_p50: round2(idle.p50), idle_lat_p95: round2(idle.p95),
            loaded_lat_p50: solo ? null : round2(loaded.p50),
            loaded_lat_p95: solo ? null : round2(loaded.p95),
            base_underruns: baseStats ? baseStats.underruns : null,
            underruns: loadedStats.underruns,
            stall_ms: Math.round(loadedStats.stallMs),
            min_margin_s: round2(Math.min(loadedStats.minMargin, cfg.buffer_s * 10)),
            bulk_mbps: solo ? null : round2(bulkMbps),
            stream_avg_mbps: Number.isFinite(streamAvg) ? round2(streamAvg) : null,
            timeline: JSON.stringify({ cfg: {
                idle_s: cfg.idle_s, stream_only_s: cfg.stream_only_s,
                loaded_s: cfg.loaded_s, buffer_s: cfg.buffer_s,
                rate_mbps: cfg.rate_mbps,
            }, samples }),
            mode: state.mode,
            kind: solo ? "stream" : "contention",
            client_agent: navigator.userAgent,
            description: sDescription || null,
        });
        setStatus(state.mode === "loopback"
            ? "Done (loopback — instrument noise floor, not a network verdict)"
            : "Done");
        setInfo("saved #" + r.id);
        await loadStreamHistory();
    } catch (e) {
        if (e.message === "cancelled" || state.cancelled) {
            setStatus("Cancelled");
        } else {
            setStatus("Error: " + (e && e.message ? e.message : e));
        }
    } finally {
        // make teardown idempotent for the cancel path
        try { if (bulk) bulk.stopFn(); } catch (_) {}
        try { if (stream) stream.ctrl.abort(); } catch (_) {}
        probe.stop = true;
        samplerCtx.stop = true;
        state.streamRunning = false;
        setStreamButton(false);
        el("startBtn").disabled = false;
        setPhaseStep(null);
        el("phaseStrip").classList.add("hidden");
    }
}

// Build the verdict-card body (badge + reconstructed sentence + stats grid)
// from a normalized data object. Shared by the live run and history-row
// expansion so the two can never drift. includeInfo adds the ⓘ explainer
// (live only — omitted in history rows to avoid duplicate-id collisions).
function verdictBody(d, includeInfo) {
    const solo = (d.bulkMbps == null);   // no contender ran
    const minM = Number.isFinite(d.minMargin) ? d.minMargin.toFixed(1) : "—";
    const labels = solo ? {
        clean: ["CLEAN", `The ${fmtMbps(d.rate)} Mbps stream held its full buffer on this link with no contention.`],
        tight: ["TIGHT", `No stalls, but the buffer dipped to ${minM}s of ${d.buffer}s on its own — little headroom before contention even enters.`],
        degraded: ["DEGRADED", `The stream stalled ${d.underruns}× (${(d.stallMs / 1000).toFixed(1)}s total) with no other load — the link or rate can't sustain it.`],
        baseline: ["DEGRADED", `The stream couldn't sustain itself on this link.`],
    } : {
        clean: ["CLEAN", `The ${fmtMbps(d.rate)} Mbps stream held its full buffer while bulk ran at ${fmtMbps(d.bulkMbps)} Mbps.`],
        tight: ["TIGHT", `No stalls, but the buffer dipped to ${minM}s of ${d.buffer}s under load — margin is thin.`],
        degraded: ["DEGRADED", `The stream stalled ${d.underruns}× (${(d.stallMs / 1000).toFixed(1)}s total) under bulk load.`],
        baseline: ["BASELINE ISSUE", `The stream couldn't run cleanly even without load — the path or rate is the problem, not contention.`],
    };
    const [badge, line] = labels[d.verdict] || labels.degraded;
    const dLat = (isFinite(d.loadedP95) && isFinite(d.idleP95))
        ? d.loadedP95 - d.idleP95 : NaN;
    const bulkStat = solo ? "—" : fmtMbps(d.bulkMbps);
    const bulkKey = solo ? "bulk mbps" : `bulk mbps (${d.bulkDir})`;
    const streamStat = Number.isFinite(d.streamAvg) ? fmtMbps(d.streamAvg) : "—";
    const latLoaded50 = solo ? "—" : fmtMs(d.loadedP50);
    const latLoaded95 = solo ? "—" : fmtMs(d.loadedP95);
    const info = includeInfo
        ? `<button class="v-info" id="verdictInfoBtn" onclick="toggleVerdictInfo()" aria-expanded="false" title="What do these mean?">ⓘ</button>`
        : "";
    const infoPanel = includeInfo
        ? `<div class="v-info-panel" id="verdictInfoPanel">${VERDICT_INFO_HTML}</div>`
        : "";
    return `
      ${info}
      <div class="v-head"><span class="v-badge">${badge}</span>
        <span class="v-line">${line}</span></div>
      <div class="v-stats">
        <div class="metric"><span class="v">${minM}s</span><span class="k">min margin</span></div>
        <div class="metric"><span class="v">${d.underruns}</span><span class="k">stalls</span></div>
        <div class="metric"><span class="v">${streamStat}</span><span class="k">stream mbps</span></div>
        <div class="metric"><span class="v">${bulkStat}</span><span class="k">${bulkKey}</span></div>
        <div class="metric"><span class="v">${fmtMs(d.idleP50)}→${latLoaded50}</span><span class="k">lat p50 ms</span></div>
        <div class="metric"><span class="v">${fmtMs(d.idleP95)}→${latLoaded95}</span><span class="k">lat p95 ms</span></div>
        <div class="metric"><span class="v">${isFinite(dLat) ? "+" + fmtMs(Math.max(0, dLat)) : "—"}</span><span class="k">bufferbloat Δp95</span></div>
      </div>
      ${infoPanel}`;
}

function showVerdict(verdict, cfg, baseStats, loadedStats, idle, loaded, bulkMbps, streamAvg) {
    const v = el("streamVerdict");
    v.className = "verdict " + verdict;
    v.innerHTML = verdictBody({
        verdict, rate: cfg.rate_mbps, buffer: cfg.buffer_s,
        minMargin: loadedStats.minMargin, underruns: loadedStats.underruns,
        stallMs: loadedStats.stallMs, streamAvg, bulkMbps, bulkDir: cfg.bulk_dir,
        idleP50: idle.p50, idleP95: idle.p95,
        loadedP50: loaded.p50, loadedP95: loaded.p95,
    }, true);
    v.classList.add("info-collapsed");
    v.classList.remove("hidden");
}

// Map a stored history row to the verdictBody data shape (nulls → NaN so the
// solo/contention "—" handling matches the live card exactly).
function rowToVerdictData(r) {
    return {
        verdict: r.verdict, rate: r.rate_mbps, buffer: r.buffer_s,
        minMargin: r.min_margin_s, underruns: r.underruns, stallMs: r.stall_ms,
        streamAvg: r.stream_avg_mbps == null ? NaN : r.stream_avg_mbps,
        bulkMbps: r.bulk_mbps,            // null → solo path
        bulkDir: r.bulk_dir,
        idleP50: r.idle_lat_p50, idleP95: r.idle_lat_p95,
        loadedP50: r.loaded_lat_p50 == null ? NaN : r.loaded_lat_p50,
        loadedP95: r.loaded_lat_p95 == null ? NaN : r.loaded_lat_p95,
    };
}

// Static legend for the verdict card's metrics (same every run).
const VERDICT_INFO_HTML = `
  <p><b>Latency probe</b> — a tiny message pinged to the server ~5×/sec throughout the test, timing the the TCP RTT over HTTP. Measures the responsiveness of a request.</p>
  <p><b>Lat p50</b> — typical (median) round-trip: half were faster, half slower. The calm baseline.</p>
  <p><b>Lat p95</b> — bad-moment round-trip: only the worst 5% were slower. Spikes, not averages, are what drain a stream's buffer.</p>
  <p>Shown as <b>idle → under load</b>, because the change matters more than the raw number (some of which is just overhead).</p>
  <p><b>Bufferbloat Δp95</b> — how much the worst-case latency grew once bulk load filled the link's queues. Small = the path keeps queues short and the stream stays smooth; Large = a greedy flow (big download) packs the queue and the stream's packets wait behind it, so it stutters from delay, not from running out of bandwidth.</p>
  <p class="v-info-foot">(MIN MARGIN = lowest buffer seen; STALLS = playback interruptions; BULK MBPS = throughput the contention flow achieved.)</p>`;

function toggleVerdictInfo() {
    const v = el("streamVerdict");
    const collapsed = v.classList.toggle("info-collapsed");
    const btn = el("verdictInfoBtn");
    if (btn) {
        btn.setAttribute("aria-expanded", String(!collapsed));
        btn.classList.toggle("on", !collapsed);
    }
}

// ---- stream config UI ----------------------------------------------------------
function buildProfileSeg() {
    const seg = el("profileSeg");
    const parts = [];
    // Order by rate so the ladder always reads low→high. Flask's jsonify
    // alphabetizes keys, so insertion order from the server isn't preserved;
    // sorting by rate makes the display order deterministic and meaningful.
    const ordered = Object.entries(state.profiles)
        .sort((a, b) => (a[1].rate_mbps || 0) - (b[1].rate_mbps || 0));
    for (const [key, p] of ordered) {
        parts.push(`<button class="seg-btn" data-prof="${key}" onclick="setProfile('${key}')">${escapeHtml(p.label)}<span class="seg-sub">${fmtRate(p.rate_mbps)} Mbps</span></button>`);
    }
    parts.push(`<button class="seg-btn" data-prof="custom" onclick="setProfile('custom')">Custom<span class="seg-sub">your rate</span></button>`);
    seg.innerHTML = parts.join("");
    setProfile(state.profile);
}

function setProfile(key) {
    state.profile = key;
    for (const b of el("profileSeg").querySelectorAll(".seg-btn"))
        b.classList.toggle("active", b.dataset.prof === key);
    el("customFields").classList.toggle("hidden", key !== "custom");
}

function setBulkDir(dir) {
    state.bulkDir = dir;
    for (const b of el("bulkSeg").querySelectorAll(".seg-btn"))
        b.classList.toggle("active", b.dataset.dir === dir);
}

// ---- histories ------------------------------------------------------------------
function fmtWhen(ts) {
    const d = new Date(ts * 1000);
    const now = new Date();
    const sameDay = d.toDateString() === now.toDateString();
    const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    if (sameDay) return "Today " + time;
    const date = d.toLocaleDateString([], { month: "short", day: "numeric" });
    return date + " " + time;
}

// Anchor for the data estimate: the most recent non-null measured rate per
// direction IN THE CURRENT MODE. A loopback rate is memory bandwidth, not the
// network, so it must never anchor a network-mode estimate (and vice versa).
// Recomputed on every history load, so it tracks new runs, deletes, and clears.
function deriveLastRate(rows) {
    const lr = { down: null, up: null };
    for (const r of rows || []) {           // rows are newest-first
        if (r.mode !== state.mode) continue;
        if (lr.down == null && r.down_mbps != null) lr.down = r.down_mbps;
        if (lr.up == null && r.up_mbps != null) lr.up = r.up_mbps;
        if (lr.down != null && lr.up != null) break;
    }
    state.lastRate = lr;
}

// Muted meta line under each throughput row: the config that produced the run
// (so rows stay comparable) plus the detail that doesn't fit the headline —
// peaks, the measurement window, and per-stream rate when parallel.
// ---- Past Runs: shared history-panel controller --------------------------------
// The Throughput and Stream tabs show a collapsible "Past Runs" panel with the
// same mechanics: toggle open/closed, a two-step arm/confirm Clear All, fetch-
// and-list, and per-row delete. Those live once here, parameterized by a per-tab
// descriptor. The per-tab differences are the endpoints, the empty message, the
// optional pre/post-render hooks, and rowHTML — the results box itself, which is
// unique to each test category.

const THROUGHPUT_HISTORY = {
    panelId: "history", toggleId: "historyToggle", listId: "historyList", clearBtnId: "clearBtn",
    endpoints: { get: "get_results", clear: "clear_results", del: "delete_result" },
    emptyMsg: "No runs yet — your tests will appear here.",
    onRows: (rows) => deriveLastRate(rows),   // seed the per-direction rate anchor
    afterRender: null,
    rowHTML: (r) => {
        const ip = r.client_ip ? `<span class="ip">${escapeHtml(r.client_ip)}</span>` : "";
        const loopTag = r.mode === "loopback" ? `<span class="loop-tag">loopback</span>` : "";
        const duplexTag = r.bidir ? `<span class="duplex-tag">full-duplex</span>` : "";
        const desc = (r.description || "").trim();
        const descTag = desc ? `<span class="desc">${escapeHtml(desc)}</span>` : "";
        // A skipped direction stored null; show "—" (not "0", which reads as a
        // measured zero). Which side is present also signals the direction.
        const dv = r.down_mbps != null ? fmtMbps(r.down_mbps) : "—";
        const uv = r.up_mbps != null ? fmtMbps(r.up_mbps) : "—";
        const dCls = "dir down" + (r.down_mbps == null ? " untested" : "");
        const uCls = "dir up" + (r.up_mbps == null ? " untested" : "");
        return `
          <div class="hrow" id="hrow-${r.id}">
            <div class="hrow-head" onclick="toggleResultRow(${r.id})">
              <span class="chev">▶</span>
              <div class="rmain">
                <span class="when">${fmtWhen(r.ts)} ${ip}${loopTag}${duplexTag}</span>
                ${descTag ? `<span class="rtags">${descTag}</span>` : ""}
              </div>
              <span class="${dCls}"><span class="arrow">↓</span><span class="val">${dv}</span></span>
              <span class="${uCls}"><span class="arrow">↑</span><span class="val">${uv}</span></span>
              <button class="del" title="Delete" onclick="event.stopPropagation(); onDeleteClick(${r.id})">×</button>
            </div>
            <div class="hrow-body"><span class="hrow-meta">${throughputMeta(r)}</span></div>
          </div>`;
    },
};

const STREAM_HISTORY = {
    panelId: "streamHistory", toggleId: "streamHistoryToggle", listId: "streamHistoryList", clearBtnId: "streamClearBtn",
    endpoints: { get: "get_stream_results", clear: "clear_stream_results", del: "delete_stream_result" },
    emptyMsg: "No stream runs yet.",
    // Cache full rows so an expanded row can rebuild its verdict + chart. Rebuilt
    // on every load → expansion is ephemeral (a new run collapses all).
    onRows: (rows) => { streamRowsCache = {}; for (const r of rows) streamRowsCache[r.id] = r; },
    afterRender: (rows) => {
        // Render the collapsed-row mini sparklines after DOM insertion.
        for (const r of rows) {
            try {
                const tl = JSON.parse(r.timeline || "{}");
                if (tl.cfg && tl.samples) {
                    renderStreamChart(el("mini-" + r.id),
                        { ...tl.cfg, buffer_s: tl.cfg.buffer_s }, tl.samples, true);
                }
            } catch (_) {}
        }
    },
    rowHTML: (r) => {
        const ip = r.client_ip ? `<span class="ip">${escapeHtml(r.client_ip)}</span>` : "";
        const loopTag = r.mode === "loopback" ? `<span class="loop-tag">loopback</span>` : "";
        const desc = (r.description || "").trim();
        const descTag = desc ? `<span class="desc">${escapeHtml(desc)}</span>` : "";
        const profLabel = (state.profiles[r.profile] || {}).label || r.profile;
        return `
          <div class="shrow" id="shrow-${r.id}">
            <div class="shrow-head" onclick="toggleStreamRow(${r.id})">
              <span class="chev">▶</span>
              <div class="rmain">
                <span class="when">${fmtWhen(r.ts)} ${ip}${loopTag}</span>
                <span class="rtags"><span class="prof">${escapeHtml(profLabel)} ${fmtRate(r.rate_mbps)}</span>${descTag}</span>
              </div>
              <span class="vbadge ${escapeHtml(r.verdict)}">${escapeHtml((r.verdict || "").toUpperCase())}</span>
              <span class="mini" id="mini-${r.id}"></span>
              <button class="del" title="Delete" onclick="event.stopPropagation(); onStreamDeleteClick(${r.id})">×</button>
            </div>
            <div class="shrow-body" id="shrow-body-${r.id}"></div>
          </div>`;
    },
};

async function loadHistoryPanel(h) {
    let rows;
    try {
        rows = await app.call(h.endpoints.get, { limit: 50 });
    } catch (e) {
        el(h.listId).innerHTML = `<div class="history-empty">Couldn't load history.</div>`;
        return;
    }
    if (h.onRows) h.onRows(rows);
    const list = el(h.listId);
    if (!rows.length) {
        list.innerHTML = `<div class="history-empty">${h.emptyMsg}</div>`;
        return;
    }
    list.innerHTML = rows.map(h.rowHTML).join("");
    if (h.afterRender) h.afterRender(rows);
}

function toggleHistoryPanel(h) {
    const panel = el(h.panelId);
    const collapsed = panel.classList.toggle("collapsed");
    el(h.toggleId).setAttribute("aria-expanded", String(!collapsed));
    if (!collapsed) loadHistoryPanel(h);
    else disarmClearPanel(h);
}

// Two-step arm/confirm Clear All: first click arms ("Confirm Clear", auto-disarms
// after 3s), a second click within the window clears. The armed flag and timer
// live on the descriptor (h._armed/h._timer), not in global state.
function clearHistoryPanel(h) {
    const btn = el(h.clearBtnId);
    if (!h._armed) {
        h._armed = true;
        btn.textContent = "Confirm Clear";
        h._timer = setTimeout(() => disarmClearPanel(h), 3000);
        return;
    }
    disarmClearPanel(h);
    app.call(h.endpoints.clear).then(() => loadHistoryPanel(h))
        .catch((e) => setStatus("Error: " + e.message));
}

function disarmClearPanel(h) {
    h._armed = false;
    if (h._timer) { clearTimeout(h._timer); h._timer = null; }
    const btn = el(h.clearBtnId);
    if (btn) btn.textContent = "Clear All";
}

async function deleteHistoryRow(h, id) {
    try {
        await app.call(h.endpoints.del, { id });
        await loadHistoryPanel(h);
    } catch (e) { setStatus("Error deleting: " + e.message); }
}

// Public names kept stable so the inline onclick handlers and per-row delete
// buttons don't change — each binds the shared controller to one tab.
async function loadHistory() { return loadHistoryPanel(THROUGHPUT_HISTORY); }
async function loadStreamHistory() { return loadHistoryPanel(STREAM_HISTORY); }
function toggleHistory() { toggleHistoryPanel(THROUGHPUT_HISTORY); }
function toggleStreamHistory() { toggleHistoryPanel(STREAM_HISTORY); }
function onClearClick() { clearHistoryPanel(THROUGHPUT_HISTORY); }
function onStreamClearClick() { clearHistoryPanel(STREAM_HISTORY); }
async function onDeleteClick(id) { return deleteHistoryRow(THROUGHPUT_HISTORY, id); }
async function onStreamDeleteClick(id) { return deleteHistoryRow(STREAM_HISTORY, id); }

function throughputMeta(r) {
    const streams = r.streams || 1;       // null = pre-feature row -> effectively 1
    const toks = [];
    toks.push(`${streams} stream${streams === 1 ? "" : "s"}`);
    if (r.duration_s != null) toks.push(`${+(+r.duration_s).toFixed(1)}s window`);
    const pk = [];
    if (r.down_peak_mbps != null) pk.push(`<span class="arr down">↓</span>${fmtMbps(r.down_peak_mbps)}`);
    if (r.up_peak_mbps != null) pk.push(`<span class="arr up">↑</span>${fmtMbps(r.up_peak_mbps)}`);
    if (pk.length) toks.push(`peak ${pk.join(" ")}`);
    if (streams > 1) {                    // per-flow rate illuminates per-flow vs link
        const ps = [];
        if (r.down_mbps != null) ps.push(`<span class="arr down">↓</span>${fmtMbps(r.down_mbps / streams)}`);
        if (r.up_mbps != null) ps.push(`<span class="arr up">↑</span>${fmtMbps(r.up_mbps / streams)}`);
        if (ps.length) toks.push(`${ps.join(" ")} /stream`);
    }
    return toks.join(" · ");
}

// Expand/collapse a throughput row. The meta body is pre-rendered (cheap, no
// chart), so this just flips a class — no lazy build or row cache like the
// Stream tab needs. Independent per row (not an accordion), matching .shrow.
function toggleResultRow(id) {
    const row = el("hrow-" + id);
    if (row) row.classList.toggle("expanded");
}

// Expand/collapse a history row. Body (verdict card) is built lazily on first
// expand. Multiple rows may be open at once.
function toggleStreamRow(id) {
    const row = el("shrow-" + id);
    if (!row) return;
    const expanding = !row.classList.contains("expanded");
    row.classList.toggle("expanded");
    if (expanding) {
        const body = el("shrow-body-" + id);
        if (body && !body.dataset.built) {
            const r = streamRowsCache[id];
            if (r) {
                body.innerHTML = `
                  <div class="verdict ${escapeHtml(r.verdict)} embedded">${verdictBody(rowToVerdictData(r), false)}</div>
                  <button class="row-chart-toggle" onclick="event.stopPropagation(); toggleRowChart(${id})" aria-expanded="false">
                    <span class="chev">▶</span><span>Show chart</span>
                  </button>
                  <div class="row-chart" id="row-chart-${id}"></div>`;
                body.dataset.built = "1";
            }
        }
    }
}

// Re-render the static charts at the current width on window resize. The live
// chart redraws itself every tick during a run, so skip it while running;
// otherwise redraw the final chart and every open history-row chart so the
// responsive viewBox re-syncs (heights hold, widths fill).
let chartResizeTimer = null;
function onChartResize() {
    clearTimeout(chartResizeTimer);
    chartResizeTimer = setTimeout(reflowCharts, 150);
}
function reflowCharts() {
    if (!state.streamRunning && lastLiveChart) {
        const c = el("streamChart");
        if (c && c.firstChild) {
            renderStreamChart(c, lastLiveChart.cfg, lastLiveChart.samples, false);
        }
    }
    document.querySelectorAll(".row-chart.open").forEach((cont) => {
        const id = cont.id.replace("row-chart-", "");
        const r = streamRowsCache[id];
        if (!r) return;
        try {
            const tl = JSON.parse(r.timeline || "{}");
            if (tl.cfg && tl.samples) renderStreamChart(cont, { ...tl.cfg }, tl.samples, false);
        } catch (_) {}
    });
    if (state.activeTab === "iperf") {
        for (const c of state.iperf.cards) {
            if (c.test && c.test.samples.length) drawIperfChart(c.chart, c.test);
        }
    }
    mcReflowCharts();
}

// Expand/collapse the full chart inside an expanded row. Re-rendered from the
// stored timeline JSON each time it opens (cheap; avoids stale DOM).
function toggleRowChart(id) {
    const cont = el("row-chart-" + id);
    if (!cont) return;
    const btn = cont.parentElement.querySelector(".row-chart-toggle");
    const showing = cont.classList.toggle("open");
    if (btn) {
        btn.setAttribute("aria-expanded", String(showing));
        btn.classList.toggle("open", showing);
        const lbl = btn.querySelector("span:last-child");
        if (lbl) lbl.textContent = showing ? "Hide chart" : "Show chart";
    }
    if (showing) {
        const r = streamRowsCache[id];
        try {
            const tl = JSON.parse((r && r.timeline) || "{}");
            if (tl.cfg && tl.samples) {
                renderStreamChart(cont, { ...tl.cfg }, tl.samples, false);
            } else {
                cont.innerHTML = `<div class="row-chart-empty">No chart data for this run.</div>`;
            }
        } catch (_) {
            cont.innerHTML = `<div class="row-chart-empty">No chart data.</div>`;
        }
    } else {
        cont.innerHTML = "";
    }
}

// ---- init ------------------------------------------------------------------------
// ======================= Iperf tab ========================================
// The host runs `iperf3 -s` as a calibration reference; a remote `iperf3 -c`
// drives the traffic. The client here is a passive viewer — it polls the
// server's ring buffer, prints raw lines, and parses the per-second interval
// lines into one direction-colored throughput series. One server handles many
// sequential tests; each "Server listening (test #N)" begins a fresh series.

const IPERF_POLL_MS = 300;
const IPERF_MAX_OUT_LINES = 1000;   // DOM scrollback cap (the server caps too)

function iperfSetState(label, on) {
    const s = el("iperfState");
    if (!s) return;
    s.textContent = label;
    s.className = "iperf-state" + (on ? " on" : "");
}

// First entry / refresh button: ask whether iperf3 is present and whether a
// server is already running, then lay out the tab accordingly.
async function iperfCheck() {
    let st;
    try {
        st = await app.call("iperf_status");
    } catch (e) {
        setStatus("iperf check failed: " + (e && e.message ? e.message : e));
        return;
    }
    state.iperf.checked = true;
    state.iperf.installed = !!st.installed;
    state.iperf.version = st.version || null;

    el("iperfMissing").classList.toggle("hidden", state.iperf.installed);
    el("iperfMain").classList.toggle("hidden", !state.iperf.installed);
    if (!state.iperf.installed) { iperfStopPolling(); return; }

    if (st.version) el("iperfMeta").textContent = st.version;
    state.iperf.port = st.port || 5201;
    state.iperf.bind = st.bind || "";
    iperfRenderCmd();
    iperfReflectRunning(!!st.running);
    if (st.running) iperfStartPolling();
}

// Resolve the IP the client command should target. A bind address wins — a
// bound server only answers there — so it overrides everything. Otherwise
// location.hostname is authoritative in serve mode (it's literally the address
// used to reach this page, so it reaches the server on the same machine); only
// when the UI is on localhost do we ask the backend for a best-guess LAN IP.
async function iperfResolveIp() {
    if (state.iperf.bind) return state.iperf.bind;
    const host = location.hostname;
    if (host && host !== "127.0.0.1" && host !== "localhost" && host !== "::1") return host;
    if (state.iperf.localIp) return state.iperf.localIp;
    try {
        const r = await app.call("local_ip");
        state.iperf.localIp = (r && r.ip) || "";
    } catch (_) { state.iperf.localIp = ""; }
    return state.iperf.localIp || "<SERVER_IP>";
}

// Rebuild the command hint with the resolved IP and the active port (-p is
// shown only when it isn't the 5201 default, to keep the common case clean).
async function iperfRenderCmd() {
    const cmd = el("iperfCmd");
    if (!cmd) return;
    const ip = await iperfResolveIp();
    const port = Number(state.iperf.port) || 5201;
    const portPart = port !== 5201 ? ` -p ${port}` : "";
    cmd.innerHTML = `iperf3 -c <span id="iperfServerIp">${escapeHtml(ip)}</span>` +
        `${portPart} -f m -i 1 -t 30 -P 6 -R`;
}

// Copy the (IP-resolved) client command, with brief icon feedback.
function iperfCopyCmd(btn) {
    const cmd = el("iperfCmd");
    if (!cmd) return;
    const ok = app.copyText(cmd.textContent.trim());
    if (!btn) return;
    btn.classList.remove("copied", "failed");
    btn.classList.add(ok ? "copied" : "failed");
    btn.title = ok ? "Copied" : "Copy failed";
    clearTimeout(btn._copyTimer);
    btn._copyTimer = setTimeout(() => {
        btn.classList.remove("copied", "failed");
        btn.title = "Copy command";
    }, 1200);
}

// Called whenever the Iperf tab becomes active.
function iperfOnEnter() {
    if (!state.iperf.checked) iperfCheck();
    else if (state.iperf.running) iperfStartPolling();
}

function iperfReflectRunning(running) {
    state.iperf.running = running;
    if (running) state.iperf.outputShown = true;   // reveal output on first start; keep it after
    const btn = el("iperfStartBtn");
    if (btn) {
        btn.textContent = running ? "Stop iperf Server" : "Start iperf Server";
        btn.classList.toggle("success", !running);
        btn.classList.toggle("danger", running);
    }
    iperfSetState(running ? `listening on ${Number(state.iperf.port) || 5201}` : "", running);
    // Mirror server up/down into the shared status bar — this also reseeds the
    // bar on tab entry, since iperfCheck routes through here. The pill stays as
    // the glanceable indicator; the bar narrates the per-test story below.
    setStatus(running ? `iperf server listening on ${Number(state.iperf.port) || 5201}` : "iperf server stopped");
    if (running) setInfo("");   // fresh session — a per-test result fills this in
    // The command hint is only actionable while the server is listening.
    el("iperfHint").classList.toggle("hidden", !running);
    // The output box stays once a session has started (holds the raw log for
    // post-stop analysis), but is hidden before the very first start.
    el("iperfOutputWrap").classList.toggle("hidden", !state.iperf.outputShown);
    if (!running) {
        // The server is down — drop a trailing "Waiting…" card (no test will
        // come), but keep every finished chart on screen for analysis.
        const a = state.iperf.active;
        if (a && (!a.test || !a.test.samples.length)) {
            const i = state.iperf.cards.indexOf(a);
            if (i >= 0) state.iperf.cards.splice(i, 1);
            if (a.wrap && a.wrap.parentNode) a.wrap.parentNode.removeChild(a.wrap);
            state.iperf.active = null;
        }
    }
    const haveData = state.iperf.cards.some((c) => c.test && c.test.samples.length);
    el("iperfCharts").classList.toggle("hidden", !running && !haveData);
}

async function onIperfToggle() {
    const btn = el("iperfStartBtn");
    if (btn) btn.disabled = true;
    try {
        if (state.iperf.running) {
            await app.call("iperf_stop");
            iperfReflectRunning(false);
            // poll loop keeps running until it drains the trailing stopped line
        } else {
            state.iperf.outLines = [];           // fresh view for a new session
            el("iperfOutput").textContent = "";
            iperfClearCards();
            state.iperf.test = null;
            const r = await app.call("iperf_start");
            if (r && r.error) setStatus("iperf: " + r.error);
            if (r) { state.iperf.port = r.port || 5201; state.iperf.bind = r.bind || ""; }
            iperfRenderCmd();
            iperfReflectRunning(!!(r && r.running));
            if (r && r.running) { iperfSpawnWaitingCard(); iperfStartPolling(); }
        }
    } catch (e) {
        setStatus("iperf error: " + (e && e.message ? e.message : e));
    } finally {
        if (btn) btn.disabled = false;
    }
}

function iperfStartPolling() {
    if (state.iperf.polling) return;
    state.iperf.polling = true;
    iperfPollOnce();
}
function iperfStopPolling() {
    state.iperf.polling = false;
    if (state.iperf.pollTimer) { clearTimeout(state.iperf.pollTimer); state.iperf.pollTimer = null; }
}
async function iperfPollOnce() {
    if (!state.iperf.polling) return;
    try {
        const r = await app.call("iperf_poll", { after_id: state.iperf.lastId });
        if (r && r.lines && r.lines.length) {
            for (const ln of r.lines) iperfIngest(ln.text);
            state.iperf.lastId = r.last_id;
            iperfRenderOutput();
        }
        if (r && typeof r.running === "boolean" && r.running !== state.iperf.running) {
            iperfReflectRunning(r.running);
        }
        // server down and nothing left to drain → stop the loop
        if (r && !r.running && (!r.lines || !r.lines.length)) {
            state.iperf.polling = false;
            return;
        }
    } catch (_) {
        // transient (e.g. brief network hiccup); keep trying
    }
    if (state.iperf.polling) {
        state.iperf.pollTimer = setTimeout(iperfPollOnce, IPERF_POLL_MS);
    }
}

function iperfIngest(text) {
    state.iperf.outLines.push(text);
    iperfParseLine(text);
}

function iperfRenderOutput() {
    const pre = el("iperfOutput");
    if (!pre) return;
    const lines = state.iperf.outLines;
    if (lines.length > IPERF_MAX_OUT_LINES) lines.splice(0, lines.length - IPERF_MAX_OUT_LINES);
    // stay pinned to the bottom only if already near it (don't yank a user who
    // has scrolled up to read).
    const atBottom = pre.scrollHeight - pre.scrollTop - pre.clientHeight < 30;
    pre.textContent = lines.join("\n");
    if (atBottom) pre.scrollTop = pre.scrollHeight;
}

// The whole-session firehose starts collapsed (per-test cards carry the useful
// per-run output now). The log keeps accumulating while hidden, so on expand we
// jump to the latest — a hidden <pre> can't be auto-scrolled live.
function toggleIperfOutput() {
    const head = el("iperfOutputToggle"), pre = el("iperfOutput");
    if (!head || !pre) return;
    const opening = pre.classList.contains("hidden");
    pre.classList.toggle("hidden", !opening);
    head.classList.toggle("open", opening);
    head.setAttribute("aria-expanded", String(opening));
    if (opening) pre.scrollTop = pre.scrollHeight;
}

// ---- iperf parse ----------------------------------------------------------
const IRE_LISTEN = /Server listening on \d+ \(test #(\d+)\)/;
const IRE_ACCEPT = /Accepted connection from (\S+?),/;
const IRE_CONNECTED = / connected to /;
// data line: [ id|SUM ]  a-b sec  <n> <unit>Bytes  <bitrate> Mbits/sec  [Retr/Cwnd]  (sender|receiver)?
const IRE_DATA = /^\[\s*(SUM|\d+)\]\s+([\d.]+)-([\d.]+)\s+sec\s+[\d.]+\s+[KMG]?Bytes\s+([\d.]+)\s+Mbits\/sec/;

// ---- client-side paste (overlay) ------------------------------------------
// The live server stream is pinned to `-f m`, but a pasted CLIENT log isn't
// under our control — its bitrate units depend on whatever `-f` the user ran
// (the copy-command hint suggests `-f m`, but we parse defensively). Same line
// grammar as IRE_DATA, but the bitrate unit prefix is captured and normalized.
const IRE_DATA_ANY = /^\[\s*(SUM|\d+)\]\s+([\d.]+)-([\d.]+)\s+sec\s+[\d.]+\s+[KMGT]?Bytes\s+([\d.]+)\s+([KMGT]?)bits\/sec/;
const IRE_ANSI = /\x1b\[[0-9;]*[A-Za-z]/g;

function bitsToMbps(n, prefix) {
    const f = { "": 1e-6, K: 1e-3, M: 1, G: 1e3, T: 1e6 }[prefix || ""];
    return n * (f || 1);
}

// Parse a pasted client-side iperf3 log into a chart overlay. One paste = one
// test. `dir` (from the card) decides which final total is the headline: a
// download/reverse test makes the client the receiver, an upload the sender.
// The per-interval [SUM] line is the series (single-stream falls back to the
// lone per-stream line), matching the live server parser's choice.
function parseIperfPaste(text, dir) {
    const lines = String(text || "").replace(IRE_ANSI, "").split(/\r?\n/);
    const sumPts = [];
    const perStream = {};        // id -> [{t,mbps}] (used only if no [SUM])
    const streamIds = new Set(); // distinct per-stream ids seen anywhere
    let senderSum = null, receiverSum = null, senderS = null, receiverS = null;
    for (const raw of lines) {
        const m = raw.match(IRE_DATA_ANY);
        if (!m) continue;
        const id = m[1], end = parseFloat(m[3]);
        const mbps = bitsToMbps(parseFloat(m[4]), m[5]);
        const isFinal = /\b(sender|receiver)\s*$/.test(raw);
        const isRecv = /receiver\s*$/.test(raw);
        if (id !== "SUM") streamIds.add(id);
        if (isFinal) {                       // whole-run total → headline, not a point
            if (id === "SUM") { if (isRecv) receiverSum = mbps; else senderSum = mbps; }
            else { if (isRecv) receiverS = mbps; else senderS = mbps; }
            continue;
        }
        if (id === "SUM") sumPts.push({ t: end, mbps });
        else (perStream[id] = perStream[id] || []).push({ t: end, mbps });
    }
    let samples = sumPts;
    const ids = Object.keys(perStream);
    if (!samples.length && ids.length) samples = perStream[ids[0]];   // single stream
    const streams = sumPts.length ? Math.max(streamIds.size, 1) : (streamIds.size || 1);
    let headline = (dir === "up")
        ? (senderSum || receiverSum || senderS || receiverS)
        : (receiverSum || senderSum || receiverS || senderS);
    if (!headline && samples.length) headline = samples[samples.length - 1].mbps;
    return { ok: samples.length > 0, samples, headline: headline || null, streams };
}

// ---- iperf per-test cards --------------------------------------------------
// Each test gets its own chart card; finished cards stay for analysis. The
// bottom card is the "active slot": it shows "Waiting for a client…" until a
// test produces data, then becomes that test's live chart. When iperf re-arms
// for the next test, the active card is locked in and a fresh waiting card is
// appended below it.
const IPERF_MAX_CARDS = 20;   // keep the last N tests; drop the oldest

function iperfClearCards() {
    const host = el("iperfCharts");
    if (host) host.innerHTML = "";
    state.iperf.cards = [];
    state.iperf.active = null;
}

function iperfSpawnWaitingCard() {
    const host = el("iperfCharts");
    if (!host) return null;
    const wrap = document.createElement("div");
    wrap.className = "chart-card iperf-test-card";
    const head = document.createElement("div");
    head.className = "iperf-chart-head";
    const chart = document.createElement("div");
    chart.className = "iperf-chart";
    chart.innerHTML = `<div class="iperf-chart-empty">Waiting for a client…</div>`;
    wrap.appendChild(head);
    wrap.appendChild(chart);
    host.appendChild(wrap);
    const card = { wrap, head, chart, test: null };
    state.iperf.cards.push(card);
    state.iperf.active = card;
    iperfTrimCards();
    return card;
}

function iperfTrimCards() {
    while (state.iperf.cards.length > IPERF_MAX_CARDS) {
        const old = state.iperf.cards.shift();
        if (old && old.wrap && old.wrap.parentNode) old.wrap.parentNode.removeChild(old.wrap);
    }
}

function iperfHeadHtml(t) {
    const dirLabel = t.dir === "down" ? "download" : t.dir === "up" ? "upload" : "—";
    const l1 = [];
    if (t.ts) l1.push(`<span class="ich-ts">${fmtWhen(t.ts)}</span>`);
    l1.push(`test #${t.num}`);
    l1.push(`<span class="ich-dir ${t.dir || ""}">${dirLabel}</span>`);
    if (t.streams) l1.push(`${t.streams} stream${t.streams === 1 ? "" : "s"}`);
    if (t.clientIp) l1.push(escapeHtml(t.clientIp));
    let html = `<div class="ich-head-l1">${l1.join(" · ")}</div>`;

    // Second line: averages, only once there's something to show (the run's
    // final summary lands after streaming; a client paste may add its own).
    const hasClient = t.client && t.client.headline != null;
    const l2 = [];
    if (t.result) l2.push(`${hasClient ? "server " : ""}avg <span class="ich-avg ${t.dir || ""}">${fmtMbps(t.result.mbps)} Mbps</span>`);
    if (hasClient) l2.push(`client avg <span class="ich-avg-client">${fmtMbps(t.client.headline)} Mbps</span>`);
    if (l2.length) html += `<div class="ich-head-l2">${l2.join(" · ")}</div>`;
    return html;
}

// Per-card "Add Client-side Results": a paste box that overlays the client's
// view of the same test on the chart (shared Mbps axis, white line). Built once
// per card the first time it has data; wired with closures over the card (DOM
// cards aren't re-rendered from a template, so no global handlers/indices).
function iperfBuildCardFoot(card) {
    if (card._footBuilt || !card.wrap) return;
    const foot = document.createElement("div");
    foot.className = "iperf-card-foot";
    foot.innerHTML =
        `<div class="iperf-client-bar">` +
        `<button class="btn small iperf-server-toggle" type="button">See Server-side Results</button>` +
        `<button class="btn small iperf-client-toggle" type="button">Add Client-side Results</button>` +
        `</div>` +
        `<div class="iperf-server-entry hidden">` +
        `<textarea class="iperf-client-paste iperf-readonly" rows="8" spellcheck="false" readonly></textarea>` +
        `<div class="iperf-client-row"><span class="cfg-spacer"></span>` +
        `<button class="btn small iperf-server-copy" type="button">Copy</button></div>` +
        `</div>` +
        `<div class="iperf-client-entry hidden">` +
        `<textarea class="iperf-client-paste" rows="6" spellcheck="false" ` +
        `placeholder="Paste the client-side iperf3 output here\u2026"></textarea>` +
        `<div class="iperf-client-row">` +
        `<span class="iperf-client-msg"></span><span class="cfg-spacer"></span>` +
        `<button class="btn small iperf-client-cancel" type="button">Cancel</button>` +
        `<button class="btn primary small iperf-client-add" type="button">Add Results</button>` +
        `</div></div>`;
    card.wrap.appendChild(foot);
    card._foot = foot;

    const serverToggle = foot.querySelector(".iperf-server-toggle");
    const serverEntry = foot.querySelector(".iperf-server-entry");
    const serverTa = foot.querySelector(".iperf-readonly");
    const clientToggle = foot.querySelector(".iperf-client-toggle");
    const entry = foot.querySelector(".iperf-client-entry");
    const ta = foot.querySelector(".iperf-client-paste:not(.iperf-readonly)");
    const msg = foot.querySelector(".iperf-client-msg");
    const setMsg = (text, isErr) => { msg.textContent = text || ""; msg.className = "iperf-client-msg" + (isErr ? " err" : ""); };
    const closeServer = () => { serverEntry.classList.add("hidden"); serverToggle.textContent = "See Server-side Results"; };

    // Server-side view: read-only snapshot of THIS run's raw lines, refilled on
    // each open. Accordion — opening it collapses the client paste box.
    serverToggle.addEventListener("click", () => {
        const opening = serverEntry.classList.contains("hidden");
        if (opening) {
            entry.classList.add("hidden"); setMsg("");           // accordion
            const raw = (card.test && card.test.raw && card.test.raw.length)
                ? card.test.raw.join("\n") : "(no server output captured for this run)";
            serverTa.value = raw;
        }
        serverEntry.classList.toggle("hidden", !opening);
        serverToggle.textContent = opening ? "Hide Server-side Results" : "See Server-side Results";
    });
    foot.querySelector(".iperf-server-copy").addEventListener("click", (e) => {
        const ok = app.copyText(serverTa.value);
        const b = e.currentTarget;
        b.textContent = ok ? "Copied" : "Copy failed";
        clearTimeout(b._t);
        b._t = setTimeout(() => { b.textContent = "Copy"; }, 1200);
    });

    clientToggle.addEventListener("click", () => {
        const opening = entry.classList.contains("hidden");
        if (opening) { closeServer(); setMsg(""); }              // accordion
        entry.classList.toggle("hidden", !opening);
        if (opening) ta.focus();
    });
    foot.querySelector(".iperf-client-cancel").addEventListener("click", () => {
        entry.classList.add("hidden"); setMsg("");
    });
    foot.querySelector(".iperf-client-add").addEventListener("click", () => {
        if (!card.test) return;
        const parsed = parseIperfPaste(ta.value, card.test.dir);
        if (!parsed.ok) { setMsg("No interval data found \u2014 paste the full client output.", true); return; }
        card.test.client = parsed;
        drawIperfChart(card.chart, card.test);
        if (card.head) card.head.innerHTML = iperfHeadHtml(card.test);
        entry.classList.add("hidden"); setMsg("");
        iperfRefreshFootState(card);
    });

    card._footBuilt = true;
    iperfRefreshFootState(card);
}

// Reflect whether an overlay is present: relabel the toggle and add/remove the
// "Remove" button next to it.
function iperfRefreshFootState(card) {
    if (!card._foot) return;
    const toggle = card._foot.querySelector(".iperf-client-toggle");
    const has = !!(card.test && card.test.client);
    toggle.textContent = has ? "Replace Client-side Results" : "Add Client-side Results";
    let rm = card._foot.querySelector(".iperf-client-remove");
    if (has && !rm) {
        rm = document.createElement("button");
        rm.className = "btn small iperf-client-remove";
        rm.type = "button";
        rm.textContent = "Remove";
        rm.addEventListener("click", () => {
            if (card.test) card.test.client = null;
            drawIperfChart(card.chart, card.test);
            if (card.head) card.head.innerHTML = iperfHeadHtml(card.test);
            iperfRefreshFootState(card);
        });
        toggle.after(rm);
    } else if (!has && rm) {
        rm.remove();
    }
}

// Render the current test into the active (bottom) card. A test with no samples
// keeps the card's "Waiting…" message rather than blanking to an empty chart.
function iperfRenderActive() {
    const a = state.iperf.active, t = state.iperf.test;
    if (!a || !t || !t.samples.length) return;
    a.test = t;
    if (a.head) a.head.innerHTML = iperfHeadHtml(t);
    drawIperfChart(a.chart, t);
    iperfBuildCardFoot(a);   // a real chart now exists → offer the client paste
}

function iperfParseLine(text) {
    let m = text.match(IRE_LISTEN);
    if (m) {                                 // a test is announced (server (re)arms)
        const a = state.iperf.active;
        // If the active slot already holds a finished test (has data), lock it
        // in and open a fresh waiting card below; otherwise reuse the waiting
        // slot we already have (or create the first one).
        if (a && a.test && a.test.samples.length) iperfSpawnWaitingCard();
        else if (!a) iperfSpawnWaitingCard();
        const t = {
            num: +m[1], dir: null, streams: 0, clientIp: null, ts: null,
            hasSum: false, samples: [], result: null,
            raw: [text],   // this run's verbatim server lines (for "See Server-side Results")
        };
        state.iperf.test = t;
        if (state.iperf.active) state.iperf.active.test = t;
        return;
    }
    const t = state.iperf.test;
    if (!t) return;                          // data before any 'listening' line
    t.raw.push(text);                        // every line of this run, verbatim

    m = text.match(IRE_ACCEPT);
    if (m) {                                 // metadata only; head appears with 1st sample
        t.clientIp = m[1];
        if (!t.ts) t.ts = Date.now() / 1000; // connect time = when this run actually ran
        // A new client is the cue to retire the previous run's headline.
        setStatus(`iperf: ${t.clientIp} connected…`);
        setInfo("");
        return;
    }

    if (IRE_CONNECTED.test(text)) { t.streams++; return; }

    // The header sets direction: 'Retr'/'Cwnd' columns ⇒ the server is sending
    // (remote ran -R ⇒ download); their absence ⇒ server receiving (upload).
    if (/^\[\s*ID\]/.test(text)) {
        t.dir = /Retr/.test(text) ? "down" : "up";
        // Server's view: 'down' = it's sending to the client (remote used -R);
        // 'up' = it's receiving the client's upload.
        const who = t.clientIp || "client";
        setStatus(t.dir === "down"
            ? `iperf: sending ↓ to ${who}…`
            : `iperf: receiving ↑ from ${who}…`);
        return;
    }

    m = text.match(IRE_DATA);
    if (!m) return;
    const id = m[1], end = parseFloat(m[3]), mbps = parseFloat(m[4]);
    const isFinal = /\b(sender|receiver)\s*$/.test(text);
    if (isFinal) {                           // whole-run summary → headline avg, not a point
        if (id === "SUM" || t.streams <= 1) {
            t.result = { mbps, end };
            setStatus(`iperf: ${t.clientIp || "client"} — done`);
            setInfo(fmtMbps(t.result.mbps) + " Mbps");
        }
        iperfRenderActive();
        return;
    }
    // interval sample: prefer [SUM] (multi-stream); single-stream has no SUM, so
    // use the lone per-stream line instead.
    if (!t.ts) t.ts = Date.now() / 1000;     // fallback connect stamp (no ACCEPT line seen)
    if (id === "SUM") { t.hasSum = true; t.samples.push({ t: end, mbps }); iperfRenderActive(); }
    else if (t.streams <= 1 && !t.hasSum) { t.samples.push({ t: end, mbps }); iperfRenderActive(); }
}

// ---- iperf chart ----------------------------------------------------------
function drawIperfChart(container, t) {
    if (!t || !t.samples.length) {
        container.innerHTML = `<div class="iperf-chart-empty">Waiting for a client…</div>`;
        container._xh = null;
        return;
    }
    const samples = t.samples;
    const clientS = (t.client && t.client.samples && t.client.samples.length)
        ? t.client.samples : null;
    const cw = container.clientWidth || CHART_THRESHOLD_W;
    const W = Math.max(CHART_THRESHOLD_W, Math.round(cw));
    const H = 220;
    const renderScale = (cw / W) || 1;
    const padL = Math.round(52 / renderScale);
    const padR = Math.round(18 / renderScale);
    const padT = 16, padB = 26;
    const plotB = H - padB;
    const plotW = W - padL - padR;
    // Shared Mbps axis: range spans BOTH series so the white client line sits at
    // its true distance from the server line (the gap is the diagnostic).
    const tMax = Math.max(samples[samples.length - 1].t,
                          clientS ? clientS[clientS.length - 1].t : 0, 1);
    const yMax = Math.max(1, ...samples.map((s) => s.mbps),
                          ...(clientS ? clientS.map((s) => s.mbps) : [])) * 1.2;
    const x = (v) => padL + (Math.min(v, tMax) / tMax) * plotW;
    const y = (v) => plotB - (Math.max(0, Math.min(v, yMax)) / yMax) * (plotB - padT);
    const colorVar = t.dir === "down" ? "--download" : t.dir === "up" ? "--upload" : "--accent";

    let svg = `<svg viewBox="0 0 ${W} ${H}" style="--chart-scale:${renderScale.toFixed(3)};--series:var(${colorVar})" xmlns="http://www.w3.org/2000/svg">`;
    svg += `<line class="ch-grid" x1="${padL}" y1="${y(yMax).toFixed(1)}" x2="${W - padR}" y2="${y(yMax).toFixed(1)}"/>`;
    svg += `<line class="ch-grid" x1="${padL}" y1="${y(0).toFixed(1)}" x2="${W - padR}" y2="${y(0).toFixed(1)}"/>`;
    // y-axis numbers carry the direction color (--series), set on the SVG root
    svg += `<text class="ich-yaxis" x="${padL - 4}" y="${(y(yMax) + 4).toFixed(1)}" text-anchor="end">${fmtMbps(yMax)}</text>`;
    svg += `<text class="ich-yaxis" x="${padL - 4}" y="${(y(0) + 3).toFixed(1)}" text-anchor="end">0</text>`;
    svg += `<text class="ch-track-label" x="${padL}" y="${padT - 3}">sum throughput (Mbps)</text>`;
    const xstep = Math.max(1, Math.round(tMax / 6));
    for (let s = 0; s <= tMax + 0.001; s += xstep) {
        svg += `<text class="ch-axis" x="${x(s).toFixed(1)}" y="${(plotB + 16).toFixed(1)}" text-anchor="middle">${s}s</text>`;
    }
    const pts = samples.map((s) => `${x(s.t).toFixed(1)},${y(s.mbps).toFixed(1)}`).join(" ");
    const f = samples[0], l = samples[samples.length - 1];
    svg += `<polygon class="ich-area" points="${x(f.t).toFixed(1)},${y(0).toFixed(1)} ${pts} ${x(l.t).toFixed(1)},${y(0).toFixed(1)}"/>`;
    svg += `<polyline class="ich-line" points="${pts}"/>`;
    // client overlay (white), same shared axis, drawn on top — no area fill so it
    // reads as an overlay rather than a second filled series.
    if (clientS) {
        const cpts = clientS.map((s) => `${x(s.t).toFixed(1)},${y(s.mbps).toFixed(1)}`).join(" ");
        svg += `<polyline class="ich-line-client" points="${cpts}"/>`;
    }
    svg += `</svg>`;
    container.innerHTML = svg;

    // crosshair: same hover-scrub / tap-to-pin mechanism as the stream chart.
    // One value chip (sum throughput, direction-colored) + the shared time chip.
    wireCrosshair(container);
    const xh = container._xh || (container._xh = { pinT: null, hoverT: null });
    xh.geom = { W, H, plotB, padL, padR, padT, plotW, totalS: tMax, renderScale, x, y, colorVar };
    xh.samples = samples;
    xh.clientSamples = clientS;
    xh.chips = (s, g, chip) => {
        let out = chip(`${fmtMbps(s.mbps)}`, g.y(s.mbps), g.colorVar, "right");
        if (xh.clientSamples && xh.clientSamples.length) {
            const c = xhNearest(xh.clientSamples, s.t);
            if (c) out += chip(`${fmtMbps(c.mbps)}`, g.y(c.mbps), "--client-series", "left");
        }
        return out;
    };
    drawCrosshair(container);
}

// ============================================================================
//  MULTI CLIENT  (cross-client coordination; phase 1 — presence + start signal)
// ============================================================================
const MC_POLL_MS = 1000;

function mcServerNow() { return Date.now() / 1000 + state.mc.serverOffset; }

function mcSyncOffset(serverNow) {
    if (typeof serverNow === "number") state.mc.serverOffset = serverNow - Date.now() / 1000;
}

// ---- connect / disconnect --------------------------------------------------
async function mcRegister() {
    const r = await app.call("mc_register", {});
    state.mc.clientId = r.client_id;
    mcSyncOffset(r.server_now);
    state.mc.clients = r.clients || [];
    if (r.test) state.mc.test = r.test;
}

async function mcToggleConnect() {
    if (state.mc.connected) { mcDisconnect(); return; }
    try {
        await mcRegister();
        state.mc.connected = true;
        mcStartPolling();
        mcRenderRoster();
        mcRenderState();
        setStatus("Connected to the group");
    } catch (e) {
        setStatus("Connect failed: " + (e && e.message ? e.message : e));
    }
}

async function mcDisconnect() {
    const id = state.mc.clientId;
    mcAbortRun();
    state.mc.connected = false;
    mcStopPolling();
    state.mc.clientId = null;
    state.mc.clients = [];
    mcRenderRoster();
    mcRenderState();
    setStatus("Disconnected");
    if (id) { try { await app.call("mc_unregister", { client_id: id }); } catch (_) {} }
}

async function mcFillIntroUrl() {
    const span = el("mcIntroUrl");
    if (!span) return;
    let host = location.hostname;
    if (host === "127.0.0.1" || host === "localhost" || host === "::1") {
        try { const r = await app.call("local_ip"); if (r && r.ip) host = r.ip; } catch (_) {}
    }
    span.textContent = `http://${host}${location.port ? ":" + location.port : ""}`;
}

// Mode-aware intro note. Loopback can't host a multi-device run (local mode
// binds 127.0.0.1; a serve-mode box opened at localhost should be reopened at
// its LAN address), so the tab collapses to a single note and mcRenderState
// hides the rest. Network mode shows the coordination prompt with the CTA
// highlighted in --text against the muted --text2 note body.
function mcRenderIntro() {
    const intro = el("mcIntro");
    if (!intro) return;
    if (state.mode === "loopback") {
        intro.textContent =
            "Not supported in loopback. Run from another device in serve mode.";
        return;
    }
    intro.innerHTML =
        "Coordinate a test across multiple connected devices. Open " +
        '<span class="mc-url" id="mcIntroUrl">this app\'s address</span> and ' +
        '<span class="mc-cta">Connect Client</span>.';
    mcFillIntroUrl();
}

// ---- polling (heartbeat + roster + test state) -----------------------------
function mcStartPolling() {
    if (state.mc.pollTimer) return;
    mcPollOnce();
}
function mcStopPolling() {
    if (state.mc.pollTimer) { clearTimeout(state.mc.pollTimer); state.mc.pollTimer = null; }
}
async function mcPollOnce() {
    if (!state.mc.connected) return;
    try {
        const r = await app.call("mc_poll", { client_id: state.mc.clientId });
        if (r) {
            mcSyncOffset(r.server_now);
            if (r.known === false) {
                await mcRegister();          // server forgot us (restart) → re-register
            } else {
                state.mc.clients = r.clients || [];
                if (r.test) state.mc.test = r.test;
                state.mc.results = r.results || {};
            }
            mcRenderRoster();
            mcRenderState();
            mcMaybeStartRun();   // arm/launch this client's traffic if a new run began
            mcRenderResults();
        }
    } catch (_) { /* transient; keep polling */ }
    if (state.mc.connected) state.mc.pollTimer = setTimeout(mcPollOnce, MC_POLL_MS);
}

// ---- roster (incremental DOM so live re-render never clobbers an edit) ------
async function mcUpdate(id, fields) {
    try {
        const r = await app.call("mc_update", { client_id: id, ...fields });
        if (r && r.clients) { state.mc.clients = r.clients; mcRenderRoster(); }
    } catch (_) {}
}

function mcCreateRow(c) {
    const row = document.createElement("div");
    row.className = "mc-row";
    row.dataset.id = c.id;
    row.innerHTML =
        `<span class="mc-light" title="connected"></span>` +
        `<span class="mc-ip"></span>` +
        `<input class="mc-name" type="text" maxlength="40" placeholder="name" spellcheck="false">` +
        `<label class="mc-stream"><input class="mc-dl-on" type="checkbox"><span>down</span>` +
        `<input class="mc-mbps mc-dl-mbps" type="number" min="0" step="5" placeholder="Mbps"></label>` +
        `<label class="mc-stream"><input class="mc-ul-on" type="checkbox"><span>up</span>` +
        `<input class="mc-mbps mc-ul-mbps" type="number" min="0" step="5" placeholder="Mbps"></label>` +
        `<label class="mc-stream"><input class="mc-lat-on" type="checkbox"><span>latency</span></label>`;
    const id = c.id;
    const q = (s) => row.querySelector(s);
    let nT, dT, uT;
    q(".mc-name").addEventListener("input", (e) => { clearTimeout(nT); nT = setTimeout(() => mcUpdate(id, { name: e.target.value }), 400); });
    q(".mc-dl-on").addEventListener("change", (e) => mcUpdate(id, { dl_on: e.target.checked }));
    q(".mc-dl-mbps").addEventListener("input", (e) => { clearTimeout(dT); dT = setTimeout(() => mcUpdate(id, { dl_mbps: parseFloat(e.target.value) || 0 }), 400); });
    q(".mc-ul-on").addEventListener("change", (e) => mcUpdate(id, { ul_on: e.target.checked }));
    q(".mc-ul-mbps").addEventListener("input", (e) => { clearTimeout(uT); uT = setTimeout(() => mcUpdate(id, { ul_mbps: parseFloat(e.target.value) || 0 }), 400); });
    q(".mc-lat-on").addEventListener("change", (e) => mcUpdate(id, { lat_on: e.target.checked }));
    return row;
}

function mcUpdateRow(row, c) {
    const isSelf = c.id === state.mc.clientId;
    row.classList.toggle("self", isSelf);
    const light = row.querySelector(".mc-light");
    light.classList.toggle("on", c.seen_ago < 3);          // green if heartbeat fresh
    row.querySelector(".mc-ip").textContent = c.ip + (isSelf ? " (you)" : "");
    // Only write a field the user isn't actively editing, so a synced re-render
    // never yanks an in-progress edit out from under them.
    const setVal = (sel, v) => { const e = row.querySelector(sel); if (e && e !== document.activeElement) e.value = (v || v === 0) ? v : ""; };
    const setChk = (sel, v) => { const e = row.querySelector(sel); if (e && e !== document.activeElement) e.checked = !!v; };
    setVal(".mc-name", c.name || "");
    setChk(".mc-dl-on", c.dl.on);
    setVal(".mc-dl-mbps", c.dl.mbps || "");
    setChk(".mc-ul-on", c.ul.on);
    setVal(".mc-ul-mbps", c.ul.mbps || "");
    setChk(".mc-lat-on", c.lat);
}

function mcRenderRoster() {
    const wrap = el("mcRoster");
    if (!wrap) return;
    const clients = state.mc.clients || [];
    const seen = new Set();
    for (const c of clients) {
        seen.add(c.id);
        let row = wrap.querySelector('.mc-row[data-id="' + c.id + '"]');
        if (!row) { row = mcCreateRow(c); wrap.appendChild(row); }
        mcUpdateRow(row, c);
    }
    wrap.querySelectorAll(".mc-row").forEach((row) => {
        if (!seen.has(row.dataset.id)) row.remove();
    });
}

// ---- overall tab state (visibility, gating, connect button) ----------------
function mcRenderState() {
    const mc = state.mc;
    // Loopback can't host a multi-device run (see mcRenderIntro): collapse the
    // tab to the note alone — hide the connect row, roster, run controls,
    // countdown, and any results.
    if (state.mode === "loopback") {
        el("mcConnectControls").classList.add("hidden");
        el("mcRosterWrap").classList.add("hidden");
        el("mcRunWrap").classList.add("hidden");
        el("mcCountdown").classList.add("hidden");
        el("mcResults").classList.add("hidden");
        return;
    }
    const n = (mc.clients || []).length;
    el("mcRosterWrap").classList.toggle("hidden", !mc.connected);
    el("mcRunWrap").classList.toggle("hidden", !(mc.connected && n >= 2));
    el("mcRosterMeta").textContent = mc.connected ? `${n} connected` : "";
    const btn = el("mcConnectBtn");
    btn.textContent = mc.connected ? "Disconnect" : "Connect Client";
    btn.classList.toggle("danger", mc.connected);
    btn.classList.toggle("success", !mc.connected);
}

// ---- coordinated run + synchronized countdown ------------------------------
async function mcRun() {
    try {
        const r = await app.call("mc_start", { description: el("mcTestDesc").value || "" });
        if (r && r.ok) {
            mcSyncOffset(r.server_now);
            state.mc.test = r.test;
            state.mc.results = {};
            mcTickCountdown();
            mcMaybeStartRun();
            setStatus("Test starting…");
        } else {
            setStatus("Can't start: " + ((r && r.error) || "unknown"));
        }
    } catch (e) {
        setStatus("Start failed: " + (e && e.message ? e.message : e));
    }
}

// Driven by a 100 ms display ticker while the tab is open; all clients share
// server time (via the offset) so the countdown is identical on every device.
function mcTickCountdown() {
    const cd = el("mcCountdown");
    if (!cd) return;
    const t = state.mc.test;
    const running = !!(t && t.state === "running");
    cd.classList.toggle("hidden", !running);
    const rb = el("mcRunBtn");
    if (rb) rb.disabled = running;
    if (!running) return;
    const now = mcServerNow();
    const startIn = t.start_at - now;
    const remain = (t.start_at + t.duration_s) - now;
    if (startIn > 0.05) {
        cd.innerHTML = `<span class="mc-cd-label">Starting in</span> <span class="mc-cd-num">${Math.ceil(startIn)}</span>`;
    } else {
        cd.innerHTML = `<span class="mc-cd-label">Running</span> ` +
            `<span class="mc-cd-num">${Math.max(0, Math.ceil(remain))}</span> ` +
            `<span class="mc-cd-label">s left</span>` +
            (t.description ? ` <span class="mc-cd-desc">${escapeHtml(t.description)}</span>` : "");
    }
}

// ---- tab lifecycle ---------------------------------------------------------
// Polling runs from connect to disconnect regardless of the active tab (so a
// client set aside on another tab stays in the pool). The 100 ms countdown
// ticker is cosmetic, so it only runs while the Multi Client tab is showing.
function mcOnEnter() {
    if (!state.mc.cdTimer) state.mc.cdTimer = setInterval(mcTickCountdown, 100);
    // Reflect this tab's connection state (switchTab reset the bar to "Ready" on
    // the way in; refine it to "connected" when we already are).
    if (state.mc.connected) setStatus("Connected to the group");
    mcRenderIntro();
    mcRenderRoster();
    mcRenderState();
    mcTickCountdown();
    mcRenderResults();
    mcReflowCharts();
}
function mcOnLeave() {
    if (state.mc.cdTimer) { clearInterval(state.mc.cdTimer); state.mc.cdTimer = null; }
}

// ============================================================================
//  MULTI CLIENT — run execution (edge-measured paced streams + batched report)
// ============================================================================
// Each client reads its plan entry and generates the configured paced streams,
// measuring at the edge exactly like the single-client tabs: download = received
// rate off /raw/stream (server-paced), upload = server-returned arrival rate off
// /raw/upload (the honest consumer-side number), latency = /raw/echo RTT. Samples
// are timestamped relative to start_at (shared axis) and reported in ~1 s batches,
// so report lag never corrupts accuracy — only liveness.
const MC_SAMPLE_MS = 250;    // download sampling cadence
const MC_PROBE_MS = 200;     // latency probe cadence
const MC_REPORT_MS = 1000;   // batch report cadence (download + latency)

function mcMaybeStartRun() {
    const t = state.mc.test;
    if (!t) return;
    if (state.mc.run && state.mc.run.runId !== t.run_id) mcAbortRun();   // new run → drop old
    if (t.state !== "running") return;
    const plan = t.plan && t.plan[state.mc.clientId];
    if (!plan) return;                                          // not in this run
    if (state.mc.run && state.mc.run.runId === t.run_id) return; // already armed
    const hasWork = (plan.dl && plan.dl.on) || (plan.ul && plan.ul.on) || plan.lat;
    if (!hasWork) return;                                       // passive — just watch
    mcArmRun(t.run_id, t.start_at, t.duration_s, plan);
}

function mcArmRun(runId, startAt, durationS, plan) {
    mcAbortRun();
    // Only download + latency are client-measured/reported; upload is measured
    // and recorded at the server (raw_mc_upload), so it isn't in samples/report.
    const run = { runId, startAt, durationS, plan, _flushing: false,
        samples: { dl: [], lat: [] }, reported: { dl: 0, lat: 0 },
        streams: [], reportTimer: null, stopTimer: null, launchTimer: null };
    state.mc.run = run;
    const delay = Math.max(0, (startAt - mcServerNow()) * 1000);
    run.launchTimer = setTimeout(() => mcLaunchRun(run), delay);
}

function mcLaunchRun(run) {
    if (state.mc.run !== run) return;                  // superseded
    const capS = run.durationS + 3;                    // server backstop beyond our own stop
    const p = run.plan;
    if (p.dl && p.dl.on)
        run.streams.push(mcStreamDownload(p.dl.mbps, capS, run.startAt, (s) => run.samples.dl.push(s)));
    if (p.ul && p.ul.on)
        run.streams.push(mcFeedUpload(p.ul.mbps, run.runId, run.startAt, run.durationS));
    if (p.lat)
        run.streams.push(mcStreamLatency(MC_PROBE_MS, run.startAt, (s) => run.samples.lat.push(s)));
    run.reportTimer = setInterval(() => mcFlushReport(run), MC_REPORT_MS);
    run.stopTimer = setTimeout(() => mcFinishRun(run), run.durationS * 1000 + 150);
}

function mcStopStreams(run) {
    for (const s of run.streams) { try { s.stop(); } catch (_) {} }
    run.streams = [];
}
function mcFinishRun(run) {
    if (state.mc.run !== run) return;
    mcStopStreams(run);
    if (run.reportTimer) { clearInterval(run.reportTimer); run.reportTimer = null; }
    mcFlushReport(run);                                // final batch
}
function mcAbortRun() {
    const run = state.mc.run;
    if (!run) return;
    if (run.launchTimer) clearTimeout(run.launchTimer);
    if (run.stopTimer) clearTimeout(run.stopTimer);
    if (run.reportTimer) clearInterval(run.reportTimer);
    mcStopStreams(run);
    state.mc.run = null;
}

async function mcFlushReport(run) {
    if (run._flushing) return;
    const payload = { client_id: state.mc.clientId, run_id: run.runId };
    const next = {};
    let any = false;
    for (const k of ["dl", "lat"]) {
        const pending = run.samples[k].slice(run.reported[k]);
        if (pending.length) { payload[k] = pending; next[k] = run.reported[k] + pending.length; any = true; }
    }
    if (!any) return;
    run._flushing = true;
    try {
        await app.call("mc_report", payload);
        for (const k in next) run.reported[k] = next[k];   // advance cursor only on success
    } catch (_) { /* keep cursor; retry next tick */ }
    finally { run._flushing = false; }
}

// ---- per-metric samplers ---------------------------------------------------
function mcStreamDownload(rateMbps, capS, startAt, onSample) {
    const ctrl = new AbortController();
    let bytes = 0, lastBytes = 0, lastT = mcServerNow();
    let timer = null;
    const stopSampler = () => { if (timer !== null) { clearInterval(timer); timer = null; } };
    (async () => {
        try {
            await readRawStream(
                `/raw/stream?rate_mbps=${rateMbps}&cap_s=${capS}&cb=${Date.now()}`,
                ctrl.signal, (n) => { bytes += n; });
        } catch (e) {
            // AbortError = intentional stop (stop() below). A genuine failure
            // (fetch reject, non-200, mid-stream drop) must NOT leave the sampler
            // emitting 0-Mbps points — that manufactures a fake "0 Mbps" result
            // indistinguishable from a slow link. Stop sampling so this client's
            // download falls to "—" (no samples) or the partial average so far.
            if (e.name !== "AbortError") stopSampler();
        }
    })();
    timer = setInterval(() => {
        const now = mcServerNow(), dt = now - lastT;
        if (dt > 0) {
            onSample({ t: round2(now - startAt), v: round2((bytes - lastBytes) * 8 / 1e6 / dt) });
            lastBytes = bytes; lastT = now;
        }
    }, MC_SAMPLE_MS);
    return { stop: () => { stopSampler(); try { ctrl.abort(); } catch (_) {} } };
}

// Upload is paced and measured at the RECEIVER: raw_mc_upload throttles its
// reads to the target rate and records the arrival rate straight into results.
// The client's only job is to keep the pipe fed — greedy POSTs of the upload
// blob; TCP backpressure from the server's throttled reads paces the on-wire
// rate to the target. Re-posts if the blob drains before the run ends; stops
// itself at the duration and aborts any in-flight POST.
function mcFeedUpload(rateMbps, runId, startAt, durationS) {
    const ctrl = new AbortController();
    let stop = false;
    (async () => {
        while (!stop && state.uploadBlob && (mcServerNow() - startAt) < durationS + 0.3) {
            try {
                const resp = await fetch(
                    `/raw/mc_upload?client_id=${encodeURIComponent(state.mc.clientId)}` +
                    `&run_id=${runId}&rate_mbps=${rateMbps}&cb=${Date.now()}`,
                    { method: "POST", body: state.uploadBlob,
                      headers: { "Content-Type": "application/octet-stream" },
                      cache: "no-store", signal: ctrl.signal });
                await resp.arrayBuffer();
            } catch (e) { if (stop) break; }
        }
    })();
    return { stop: () => { stop = true; try { ctrl.abort(); } catch (_) {} } };
}

function mcStreamLatency(intervalMs, startAt, onSample) {
    let stop = false;
    (async () => {
        while (!stop) {
            const t0 = performance.now();
            try { await fetch(`/raw/echo?cb=${Date.now()}`, { cache: "no-store" }); }
            catch (e) { if (stop) break; }
            onSample({ t: round2(mcServerNow() - startAt), v: round2(performance.now() - t0) });
            const wait = intervalMs - (performance.now() - t0);
            if (wait > 0) await sleep(wait);
        }
    })();
    return { stop: () => { stop = true; } };
}

// ============================================================================
//  MULTI CLIENT — results rendering (stacked per-client charts + crosshair)
// ============================================================================
function mcRenderResults() {
    const wrap = el("mcResults");
    if (!wrap) return;
    const t = state.mc.test;
    if (!t || (t.state !== "running" && t.state !== "complete")) {
        wrap.classList.add("hidden"); wrap.innerHTML = ""; wrap._skelRun = null; wrap._doneRun = null;
        return;
    }
    wrap.classList.remove("hidden");
    const plan = t.plan || {};
    const ids = Object.keys(plan)
        .filter((id) => { const p = plan[id]; return (p.dl && p.dl.on) || (p.ul && p.ul.on) || p.lat; })
        .sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
    if (wrap._skelRun !== t.run_id) {
        mcBuildResultsSkeleton(wrap, t, plan, ids);
        wrap._skelRun = t.run_id; wrap._doneRun = null;
    }
    const complete = t.state === "complete";
    if (complete && wrap._doneRun === t.run_id) return;   // finalized; leave static for crosshair
    for (const id of ids) {
        const p = plan[id];
        const res = (state.mc.results && state.mc.results[id]) || { dl: [], ul: [], lat: [] };
        mcRenderReadout(id, p, res, complete);
        mcDrawChart(id, p, res);
    }
    if (complete) wrap._doneRun = t.run_id;
}

function mcBuildResultsSkeleton(wrap, t, plan, ids) {
    const when = (typeof fmtWhen === "function") ? fmtWhen(t.start_at) : "";
    let html = `<div class="mc-res-head"><span class="mc-res-when">${escapeHtml(when)}</span>` +
        (t.description ? ` <span class="mc-sep">·</span> <span class="mc-res-desc">${escapeHtml(t.description)}</span>` : "") +
        `</div>`;
    for (const id of ids) {
        const p = plan[id];
        const label = p.name || p.ip || id;
        const tgt = [];
        if (p.dl && p.dl.on) tgt.push(`down ${fmtRate(p.dl.mbps)} Mbps`);
        if (p.ul && p.ul.on) tgt.push(`up ${fmtRate(p.ul.mbps)} Mbps`);
        html += `<div class="mc-client" data-id="${id}">` +
            `<div class="mc-client-head"><span class="mc-client-name">${escapeHtml(label)}</span>` +
            (tgt.length ? ` <span class="mc-sep">·</span> <span class="mc-client-tgt">${tgt.join(" · ")}</span>` : "") +
            `</div>` +
            `<div class="mc-readout" id="mc-readout-${id}"></div>` +
            `<div class="mc-chart" id="mc-chart-${id}"></div>` +
            `</div>`;
    }
    wrap.innerHTML = html;
}

function mcFiniteOr(v, fmt) { return isFinite(v) ? fmt(v) : "—"; }
function mcLast(arr) { return (arr && arr.length) ? arr[arr.length - 1].v : NaN; }
function mcAvg(arr) {
    if (!arr || !arr.length) return NaN;
    const dur = state.mc.test.duration_s || 0;
    const warm = Math.min(1.0, dur * 0.2);                 // trim ramp: ≤1 s, or 20% of a short run
    let v = arr.filter((s) => s.t >= warm).map((s) => s.v);
    if (!v.length) v = arr.map((s) => s.v);
    return v.reduce((a, b) => a + b, 0) / v.length;
}

function mcRenderReadout(id, plan, res, complete) {
    const box = el("mc-readout-" + id);
    if (!box) return;
    const stat = complete ? mcAvg : mcLast;
    const col = (txt, v) => `<span style="color:var(${v})">${txt}</span>`;
    const parts = [];
    if (plan.dl && plan.dl.on) parts.push(`down = ${col(`${mcFiniteOr(stat(res.dl), fmtMbps)} Mbps`, "--download")}`);
    if (plan.ul && plan.ul.on) parts.push(`up = ${col(`${mcFiniteOr(stat(res.ul), fmtMbps)} Mbps`, "--upload")}`);
    if (plan.lat) parts.push(`lat = ${col(`${mcFiniteOr(stat(res.lat), (v) => Math.round(v))} ms`, "--latency")}`);
    const prefix = complete ? `<span class="mc-ro-label">(Avg)</span> ` : "";
    box.innerHTML = prefix + parts.join(' <span class="mc-sep">·</span> ');
}

// ---- charts ----------------------------------------------------------------
// One stacked SVG per client, built to match the Stream tab exactly: latency
// track on top, throughput below, a red-dotted ch-zero divider between them,
// shared ch-* classes (so fonts/scaling/colors are identical), x-axis time
// ticks, and the SAME crosshair system (wireCrosshair/drawCrosshair) for
// hover-scrub, click/tap-to-pin, the time chip, and per-track value chips.
// Throughput uses one shared Mbps axis for down (azure) + up (orange); each
// direction's target is a dashed reference line. No latency threshold bands.
function mcSpine(res, has) {
    // Merge the three independently-timed series into one timeline the crosshair
    // can snap to: each spine entry carries every active series' nearest value at
    // that instant, so a single hover reads down/up/latency together.
    const ts = new Set();
    if (has.dl) for (const p of res.dl) ts.add(p.t);
    if (has.ul) for (const p of res.ul) ts.add(p.t);
    if (has.lat) for (const p of res.lat) ts.add(p.t);
    const near = (arr, t) => { const s = xhNearest(arr, t); return s ? s.v : NaN; };
    return [...ts].sort((a, b) => a - b).map((t) => ({
        t,
        dl: has.dl ? near(res.dl, t) : NaN,
        ul: has.ul ? near(res.ul, t) : NaN,
        lat: has.lat ? near(res.lat, t) : NaN,
    }));
}

function mcDrawChart(id, plan, res) {
    const container = el("mc-chart-" + id);
    if (!container) return;
    const has = { dl: !!(plan.dl && plan.dl.on), ul: !!(plan.ul && plan.ul.on), lat: !!plan.lat };
    const totalS = state.mc.test.duration_s || 1;

    // responsive sizing — identical dynamics to renderStreamChart
    const cw = container.clientWidth || CHART_THRESHOLD_W;
    const W = Math.max(CHART_THRESHOLD_W, Math.round(cw));
    const renderScale = (cw / W) || 1;
    const padL = Math.round(54 / renderScale);
    const padR = Math.round(22 / renderScale);
    const padT = 14, padB = 14, GAP = 16, axisGutter = 24;

    // tracks: latency (top), throughput (bottom), no gap beyond the standard GAP
    const latTop = padT;
    const latH = has.lat ? 70 : 0;
    const thrTop = latTop + latH + (has.lat && (has.dl || has.ul) ? GAP : 0);
    const thrH = (has.dl || has.ul) ? 96 : 0;
    const plotB = thrTop + thrH;   // bottom of the lowest track
    const H = plotB + axisGutter;
    const plotW = W - padL - padR;
    const x = (t) => padL + (Math.min(t, totalS) / totalS) * plotW;

    const tgts = [];
    if (has.dl) tgts.push(plan.dl.mbps);
    if (has.ul) tgts.push(plan.ul.mbps);
    const dlV = has.dl ? res.dl.map((p) => p.v) : [];
    const ulV = has.ul ? res.ul.map((p) => p.v) : [];
    const yMaxThr = Math.max(1, ...tgts, ...dlV, ...ulV) * 1.2;
    const ysThr = (v) => thrTop + thrH - (Math.max(0, Math.min(v, yMaxThr)) / yMaxThr) * thrH;
    const latV = has.lat ? res.lat.map((p) => p.v).filter((v) => isFinite(v) && v > 0) : [];
    const yMaxLat = Math.max(10, ...latV) * 1.15;
    const yLat = (v) => latTop + latH - (Math.max(0, Math.min(v, yMaxLat)) / yMaxLat) * latH;

    let svg = `<svg viewBox="0 0 ${W} ${H}" style="--chart-scale:${renderScale.toFixed(3)}" class="mc-svg" xmlns="http://www.w3.org/2000/svg">`;

    // latency track (top): its baseline is the red-dotted ch-zero ONLY when a
    // throughput track sits below it (then it reads as the divider between the
    // two charts); a latency-only chart gets the plain gray ch-grid baseline.
    if (has.lat) {
        const latBase = (has.dl || has.ul) ? "ch-zero" : "ch-grid";
        svg += `<line class="${latBase}" x1="${padL}" y1="${yLat(0).toFixed(1)}" x2="${(W - padR).toFixed(1)}" y2="${yLat(0).toFixed(1)}"/>`;
        svg += `<text class="ch-axis lat" x="${padL - 4}" y="${(latTop + 4).toFixed(1)}" text-anchor="end">${fmtMs(yMaxLat)}ms</text>`;
        svg += `<text class="ch-axis lat" x="${padL - 4}" y="${(yLat(0) + 3).toFixed(1)}" text-anchor="end">0</text>`;
        svg += `<text class="ch-track-label" x="${padL}" y="${(latTop + 9).toFixed(1)}">latency</text>`;
    }
    // throughput track (bottom): gray ch-grid baseline + dashed per-direction
    // target lines. The red-dotted divider (if any) is the latency baseline above.
    if (has.dl || has.ul) {
        svg += `<line class="ch-grid" x1="${padL}" y1="${ysThr(0).toFixed(1)}" x2="${(W - padR).toFixed(1)}" y2="${ysThr(0).toFixed(1)}"/>`;
        svg += `<text class="ch-axis str" x="${padL - 4}" y="${(thrTop + 4).toFixed(1)}" text-anchor="end">${fmtMbps(yMaxThr)}</text>`;
        svg += `<text class="ch-axis str" x="${padL - 4}" y="${(ysThr(0) + 3).toFixed(1)}" text-anchor="end">0</text>`;
        svg += `<text class="ch-track-label" x="${padL}" y="${(thrTop + 9).toFixed(1)}">throughput (Mbps)</text>`;
        if (has.dl) svg += `<line class="mc-target" style="stroke:var(--download)" x1="${padL}" y1="${ysThr(plan.dl.mbps).toFixed(1)}" x2="${(W - padR).toFixed(1)}" y2="${ysThr(plan.dl.mbps).toFixed(1)}"/>`;
        if (has.ul) svg += `<line class="mc-target" style="stroke:var(--upload)" x1="${padL}" y1="${ysThr(plan.ul.mbps).toFixed(1)}" x2="${(W - padR).toFixed(1)}" y2="${ysThr(plan.ul.mbps).toFixed(1)}"/>`;
    }
    // x-axis time ticks (seconds), same cadence/style as Stream + iperf
    const xstep = Math.max(1, Math.round(totalS / 6));
    for (let ts = 0; ts <= totalS + 0.001; ts += xstep) {
        svg += `<text class="ch-axis" x="${x(ts).toFixed(1)}" y="${(plotB + 16).toFixed(1)}" text-anchor="middle">${ts}s</text>`;
    }
    // lines
    if (has.lat) {
        const pts = res.lat.filter((p) => isFinite(p.v) && p.v > 0);
        if (pts.length > 1) svg += `<polyline class="ch-lat-line" points="${pts.map((p) => `${x(p.t).toFixed(1)},${yLat(p.v).toFixed(1)}`).join(" ")}"/>`;
    }
    if (has.dl) {
        const pts = res.dl.filter((p) => Number.isFinite(p.v));
        if (pts.length > 1) svg += `<polyline class="ch-stream-line" points="${pts.map((p) => `${x(p.t).toFixed(1)},${ysThr(p.v).toFixed(1)}`).join(" ")}"/>`;
    }
    if (has.ul) {
        const pts = res.ul.filter((p) => Number.isFinite(p.v));
        if (pts.length > 1) svg += `<polyline class="ch-up-line" points="${pts.map((p) => `${x(p.t).toFixed(1)},${ysThr(p.v).toFixed(1)}`).join(" ")}"/>`;
    }
    svg += `</svg>`;
    container.innerHTML = svg;

    // shared crosshair: same geometry contract the Stream/Iperf charts use.
    wireCrosshair(container);
    const xh = container._xh || (container._xh = { pinT: null, hoverT: null });
    xh.geom = { W, H, plotB, padL, padR, padT, padB, plotW, totalS, renderScale, x, ysThr, yLat,
                hasDl: has.dl, hasUl: has.ul, hasLat: has.lat };
    xh.samples = mcSpine(res, has);
    // chips match the Stream tab; when both down+up are present the download chip
    // sits LEFT of the line and upload RIGHT, so the pair never overlaps.
    xh.chips = (s, g, chip) => {
        let out = "";
        if (g.hasDl && Number.isFinite(s.dl))
            out += chip(`${fmtMbps(s.dl)}`, g.ysThr(s.dl), "--download", g.hasUl ? "left" : "right");
        if (g.hasUl && Number.isFinite(s.ul))
            out += chip(`${fmtMbps(s.ul)}`, g.ysThr(s.ul), "--upload", "right");
        if (g.hasLat && Number.isFinite(s.lat))
            out += chip(`${fmtMs(s.lat)}ms`, g.yLat(s.lat), "--latency", "right");
        return out;
    };
    drawCrosshair(container);   // re-apply any pin into the freshly rendered SVG
}

// Re-render every results chart at the current container width — on resize and
// on tab-enter, so a chart first drawn while the panel was hidden (clientWidth
// 0 → placeholder width) re-syncs once it's actually on screen.
function mcReflowCharts() {
    const t = state.mc.test;
    if (!t || (t.state !== "running" && t.state !== "complete")) return;
    const wrap = el("mcResults");
    if (!wrap || wrap.classList.contains("hidden")) return;
    const plan = t.plan || {};
    for (const id of Object.keys(plan)) {
        const p = plan[id];
        if (!((p.dl && p.dl.on) || (p.ul && p.ul.on) || p.lat)) continue;
        const res = (state.mc.results && state.mc.results[id]) || { dl: [], ul: [], lat: [] };
        mcDrawChart(id, p, res);
    }
}

// ---- config panel (cogwheel) -----------------------------------------------
const MC_GROUPS = [
    { title: "Test", fields: [
        { key: "mc_duration_s", label: "Duration", unit: "s", step: 1 },
    ]},
];

function buildMcConfig() {
    const p = el("mcConfig");
    p.innerHTML = cfgSectionsHTML(MC_GROUPS) +
        cfgFootHTML({ text: "Applies to the next run." }, "resetMcConfig", "saveMcConfig");
    wireConfigPanel(p);
}

function readMcConfig() {
    const out = {};
    el("mcConfig").querySelectorAll("[data-key]").forEach((inp) => { out[inp.dataset.key] = parseFloat(inp.value); });
    return out;
}
function fillMcConfig() {
    el("mcConfig").querySelectorAll("[data-key]").forEach((inp) => { inp.value = state.settings[inp.dataset.key]; });
}
function toggleMcConfig() {
    const p = el("mcConfig"), btn = el("mcCfgToggle");
    if (!p._built) { buildMcConfig(); p._built = true; }
    const willOpen = p.classList.contains("hidden");
    p.classList.toggle("hidden", !willOpen);
    if (btn) { btn.classList.toggle("on", willOpen); btn.setAttribute("aria-expanded", String(willOpen)); }
    if (willOpen) fillMcConfig();
}
async function saveMcConfig(btn) {
    try {
        const r = await app.call("save_settings", { settings: readMcConfig() });
        state.settings = { ...state.settings, ...r.settings };
        fillMcConfig();
        setStatus("Configuration saved");
        flashSaved(btn);
    } catch (e) {
        setStatus("Save failed: " + (e && e.message ? e.message : e));
        flashSaved(btn, false);
    }
}
async function resetMcConfig() {
    try {
        const r = await app.call("reset_settings");
        state.settings = { ...state.settings, ...r.settings };
        fillMcConfig();
        setStatus("Configuration reset to defaults");
    } catch (e) { setStatus("Reset failed: " + (e && e.message ? e.message : e)); }
}

async function init() {
    buildGauge("gauge-down");
    buildGauge("gauge-up");
    resetGauges();

    const host = location.hostname;
    state.mode = (host === "127.0.0.1" || host === "localhost" || host === "::1")
        ? "loopback" : "network";
    if (state.mode === "loopback") el("loopbackNote").classList.remove("hidden");
    setInfo(host);

    try {
        const s = await app.call("get_settings");
        if (s && s.defaults) {
            state.settings = { ...FALLBACK_SETTINGS, ...s.defaults };
            state.profiles = { ...FALLBACK_PROFILES, ...(s.profiles || {}) };
            state.bounds = s.bounds || {};
        } else {
            state.settings = { ...FALLBACK_SETTINGS, ...s };
        }
    } catch (_) {
        state.settings = { ...FALLBACK_SETTINGS };
    }

    buildProfileSeg();
    setBulkDir(state.bulkDir);
    state.uploadBlob = makeUploadBlob(state.settings.up_chunk_mb);
    window.addEventListener("resize", onChartResize);
    loadHistory();   // seed the per-direction rate anchor from prior runs (and preload Past Runs)
    setStatus("Ready");
}

document.addEventListener("DOMContentLoaded", init);
