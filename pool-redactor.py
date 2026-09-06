#!/usr/bin/env python3
"""Privacy redactor + miner lookup for the ZKas pool's public stats.

Computes ALL pool stats (pool + per-worker hashrate, shares, blocks) directly
from the stratum bridge's per-instance Prometheus metrics, so the dashboard works
independent of the bridge's /api/stats aggregator. Every wallet address is masked
on the public feed; a miner sees their own full stats via /api/miner.

  GET /api/stats          -> pool-wide stats, every address MASKED.
  GET /api/miner?address= -> stats for ONE address (supply the full address).

Bind loopback; nginx proxies it.
"""
import collections
import json
import os
import re
import subprocess
import threading
import time
import urllib.request
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INSTANCE_PORTS = [2114, 2115, 2116, 2117, 2118]
LISTEN = ("127.0.0.1", 3034)
# The subsidy is a live consensus value; it decays continuously, so a launch
# constant (57 from the old 60-ZKAS subsidy) must never be used for the public
# dashboard or miner totals.  Keep a legacy fallback only for a temporary
# explorer outage; it is not used while the live endpoint is reachable.
REWARD_API_URL = os.environ.get(
    "ZKAS_REWARD_API_URL", "https://explorer.zkas.info/api/info/blockreward"
)
LEGACY_REWARD_FC = 57.0
_reward_cache = {"miner": LEGACY_REWARD_FC, "ts": 0.0}
_reward_lock = threading.Lock()
REWARD_CACHE_SECS = 60.0
TWO32 = 2 ** 32
SAMPLE_SECS = 5         # scrape cadence (server-side refresh; client polls ~5s too)
WINDOW_SECS = 600       # hashrate = Δ(share-diff) · 2^32 / Δt over a 10-min rolling window
START = time.time()
# Workers can be moved to the isolated VPS3 listener by miner/MRR failover.
# Import only its public bridge stats; this does not make VPS3 part of the
# payout/accounting authority and a timeout simply leaves the prior snapshot.
REMOTE_POOL_STATS_URLS = [u.strip() for u in os.environ.get(
    "ZKAS_REMOTE_POOL_STATS_URLS",
    "http://204.10.194.28:3036/api/stats,http://204.10.194.28:3035/api/stats",
).split(",") if u.strip()]


def _rx(metric):
    return re.compile(r'^' + metric + r'\{([^}]*)\}\s+([0-9.eE+-]+)', re.M)


RX_DIFF = _rx("ks_valid_share_diff_counter")   # cumulative Σ share difficulty per worker
RX_SHARES = _rx("ks_valid_share_counter")       # cumulative valid shares per worker
RX_FOUND = _rx("ks_blocks_accepted_by_node")    # blocks found per worker
RX_MINED = _rx("ks_blocks_mined")               # confirmed/paid per worker
RX_PENDING = _rx("ks_blocks_not_confirmed_blue")  # maturing per worker
RX_KAS = _rx("ks_merged_parent_submit_total")   # merged parents Kaspa ACCEPTED, per worker
RX_KAS_SET = _rx("ks_worker_kas_payout_set")    # 1 = worker supplied a usable kaspa: address

# Who the KAS coinbase actually paid. A miner that never set a `kaspa:` address,
# or that was in its pool-fee minute, earned nothing from the block -- crediting
# it on the dashboard reads as "you were paid" and generates support questions.
KAS_TO_MINER = "miner"
KAS_TO_POOL = "pool"
# Only this outcome is a Kaspa block that landed. The bridge never emits the
# counter for parents that failed to clear Kaspa's target (see
# `record_merged_parent_submit`), so "accepted" is the whole story; `zkas_claim`
# is summed across first/duplicate because a duplicate ZKas claim is still a
# distinct, reward-bearing Kaspa block.
KAS_ACCEPTED = "accepted"
RX_NETHR = re.compile(r'^ks_estimated_network_hashrate_gauge\s+([0-9.eE+-]+)', re.M)
RX_NETBLK = re.compile(r'^ks_network_block_count\s+([0-9.eE+-]+)', re.M)
RX_NETDIFF = re.compile(r'^ks_network_difficulty_gauge\s+([0-9.eE+-]+)', re.M)

# Authoritative network hashrate + difficulty straight from the node. The bridge's
# ks_estimated_network_hashrate_gauge was observed WRONG (256 GH/s while the node
# measured ~68 TH/s), producing the impossible "pool hashrate > network hashrate".
# The node's EstimateNetworkHashesPerSecond is the real measured total-network rate
# (Σ blueWork / Δt over the window) — the same figure Kaspa explorers report.
NODE_RPC = "127.0.0.1:16110"
PROTO_DIR = "/root/work/rusty-kaspa/rpc/grpc/core/proto"
HR_WINDOW = 1000       # blocks; node's blueWork/time averaging window for the estimate
_NODE = {"hr": 0.0, "diff": 0.0, "blocks": 0, "ts": 0.0}
NODE_TTL = 20.0        # seconds; refresh at roughly the scrape cadence
_HEIGHT = {"value": 0, "ts": 0.0}
_HEIGHT_LOCK = threading.Lock()
HEIGHT_TTL = 1.0       # UI polls every 2s; avoid the 20s hashrate snapshot delay

def _node_rpc(payload):
    return subprocess.run(
        ["grpcurl", "-plaintext", "-import-path", PROTO_DIR, "-proto", "messages.proto",
         "-d", payload, NODE_RPC, "protowire.RPC/MessageStream"],
        capture_output=True, text=True, timeout=8).stdout

def node_stats():
    """(network_hashrate_H/s, network_difficulty, block_count) measured by the node, cached for
    NODE_TTL. Both come straight from the node so the dashboard matches reality
    regardless of the bridge's gauges. Returns last-good (or zeros) on failure."""
    now = time.time()
    if now - _NODE["ts"] < NODE_TTL and _NODE["hr"] > 0:
        return _NODE["hr"], _NODE["diff"], _NODE["blocks"]
    try:
        out = _node_rpc('{"estimateNetworkHashesPerSecondRequest":{"windowSize":%d}}' % HR_WINDOW)
        m = re.search(r'"networkHashesPerSecond":\s*"?([0-9.eE+]+)"?', out)
        dag = _node_rpc('{"getBlockDagInfoRequest":{}}')
        md = re.search(r'"difficulty":\s*([0-9.eE+]+)', dag)
        mb = re.search(r'"blockCount":\s*"?([0-9]+)"?', dag)
        if m:
            _NODE["hr"] = float(m.group(1))
            if md:
                _NODE["diff"] = float(md.group(1))
            if mb:
                _NODE["blocks"] = int(mb.group(1))
            _NODE["ts"] = now
    except Exception:
        pass  # keep last good values
    return _NODE["hr"], _NODE["diff"], _NODE["blocks"]


def live_block_height(fallback=0):
    """Return the current virtual DAA score from the local node API.

    Height is cheap and time-sensitive, unlike the network-hashrate estimate.
    Cache it for one second so many dashboard viewers do not fan out into
    duplicate node requests.
    """
    now = time.time()
    with _HEIGHT_LOCK:
        if now - _HEIGHT["ts"] < HEIGHT_TTL and _HEIGHT["value"] > 0:
            return _HEIGHT["value"]
        try:
            raw = urllib.request.urlopen(
                "http://127.0.0.1:8500/info/blockdag", timeout=1
            ).read().decode()
            dag = json.loads(raw)
            height = int(dag.get("virtualDaaScore") or dag.get("blockCount") or 0)
            if height > 0:
                _HEIGHT.update(value=height, ts=now)
                return height
        except Exception:
            pass
        return _HEIGHT["value"] or fallback


def _labels(s):
    return dict(re.findall(r'(\w+)="([^"]*)"', s))


def _agg_sessions(rx, text, out):
    """Aggregate a per-worker counter keyed by (wallet, worker).

    katpool emits duplicate series for one connection (with and without the
    `miner` label) AND a brand-new series per (re)connect — the `ip` label
    carries the source ip:port, which changes every session. So: take the MAX
    within one (wallet, worker, ip) session (dedupes the label variants) and
    SUM across sessions. The old max-only aggregation froze a miner's shares/
    blocks at the previous session's value after a stop/resume (new series
    restarts at 0 and never exceeds the old max) — the live "numbers never
    change again" bug."""
    per_session = {}
    for labels, val in rx.findall(text):
        lb = _labels(labels)
        w, wk = lb.get("wallet"), lb.get("worker")
        if not w:
            continue
        try:
            v = float(val)
        except ValueError:
            continue
        sess = (w, wk, lb.get("ip") or "")
        if v > per_session.get(sess, 0.0):
            per_session[sess] = v
    for (w, wk, _ip), v in per_session.items():
        k = (w, wk)
        out[k] = out.get(k, 0.0) + v


def _agg_sessions_where(rx, text, out, **want):
    """`_agg_sessions`, restricted to series whose labels match `want`.

    Needed for `ks_merged_parent_submit_total`, which carries an `outcome` label
    — summing every outcome would count rejected submissions as found blocks."""
    per_session = {}
    for labels, val in rx.findall(text):
        lb = _labels(labels)
        if any(lb.get(k) != v for k, v in want.items()):
            continue
        w, wk = lb.get("wallet"), lb.get("worker")
        if not w:
            continue
        try:
            v = float(val)
        except ValueError:
            continue
        # Same session key as _agg_sessions, plus zkas_claim: first/duplicate are
        # separate series for one connection and must ADD, not max out.
        sess = (w, wk, lb.get("ip") or "", lb.get("zkas_claim") or "", lb.get("kas_payout") or "")
        if v > per_session.get(sess, 0.0):
            per_session[sess] = v
    for (w, wk, _ip, _claim, _payout), v in per_session.items():
        k = (w, wk)
        out[k] = out.get(k, 0.0) + v


# ---- shared state, refreshed by a background sampler --------------------------
_lock = threading.Lock()
_state = {
    "networkHashrate": 0.0, "networkBlockCount": 0, "networkDifficulty": 0.0,
    "activeWorkers": 0, "totalShares": 0, "totalBlocks": 0, "kasBlocksFound": 0,
    "bridgeUptime": 0, "workers": [], "blocks": [],
}
_hist = {}   # (wallet, worker) -> deque[(ts, cumulative_diff)] over WINDOW_SECS
# Rolling (ts, pool_blocks_cumulative, network_blocks_cumulative) for the pool's
# block-find share. Pool hashrate = network_hashrate · (Δpool_blocks / Δnet_blocks):
# the pool's real fraction of the network, so it is always ≤ network and the rest
# is the other miners. This replaces summing the bridge's (inflated) per-worker rates.
_blockshare = collections.deque()
BLOCKSHARE_WINDOW = 900   # 10-15 min of blocks for a stable share estimate
BLOCKSHARE_MIN_NET = 30   # need at least this many network blocks before trusting it

# ---- per-wallet payout history (solo model: one accepted block pays the
# consensus miner share to that wallet). Fed from the bridge's recent-blocks list and
# persisted to disk so it survives redactor AND bridge restarts. ------------
PAYOUT_HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "redactor-payout-history.json")
PAYOUT_RECOVERY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "payout-history-recovery.json")
PAYOUT_HISTORY_CAP = 200        # per wallet
_payouts_lock = threading.Lock()
_payouts = {}                   # wallet -> [{"ts","worker","hash"}] newest LAST
_payouts_seen = set()           # block hashes already recorded
_payouts_dirty = False


def current_miner_reward():
    """Return the current 95%-of-subsidy miner reward in ZKAS.

    `blockreward` is emitted by the consensus-backed explorer API.  Cache it
    briefly so a dashboard refresh cannot fan out requests, and retain the
    last good value during a short API outage.  This replaces the old fixed
    57-ZKAS value, which became wrong as the emission curve decayed.
    """
    now = time.time()
    with _reward_lock:
        if now - _reward_cache["ts"] < REWARD_CACHE_SECS and _reward_cache["miner"] > 0:
            return _reward_cache["miner"]
    try:
        req = urllib.request.Request(REWARD_API_URL, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            payload = json.loads(resp.read().decode())
        gross = float(payload.get("blockreward"))
        miner = gross * 0.95
        if not (miner > 0 and miner < 1_000_000):
            raise ValueError("invalid block reward")
        with _reward_lock:
            _reward_cache.update(miner=miner, ts=now)
            return miner
    except Exception:
        with _reward_lock:
            return _reward_cache["miner"] or LEGACY_REWARD_FC

# Lifetime headline counters must survive a bridge restart. Prometheus worker
# counters are process-local, so publishing their raw sum made the dashboard
# jump from thousands of blocks back to zero whenever Stratum was restarted.
# Store the last raw bridge counters and add only their positive delta. If the
# raw value regresses, a new bridge epoch began and its full current value is
# the delta. The file is tiny and is written atomically.
LIFETIME_TOTALS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "redactor-lifetime-totals.json")
# `totalKas` is the pool's lifetime merge-mined KAS block count. It is tracked the
# same monotonic way as shares/blocks because a bridge restart zeroes every
# Prometheus counter, and a headline that falls from 21,715 to 0 reads as lost
# blocks. `rawKas` is the last raw counter value seen, used to detect that reset.
_lifetime = {"totalShares": 0, "totalBlocks": 0, "totalKas": 0,
             "rawShares": 0, "rawBlocks": 0, "rawKas": 0, "savedAt": 0}
_lifetime_last_save = 0.0


def _lifetime_load():
    global _lifetime
    try:
        with open(LIFETIME_TOTALS_FILE, encoding="utf-8") as f:
            loaded = json.load(f)
        for key in _lifetime:
            _lifetime[key] = int(loaded.get(key, _lifetime[key]))
    except Exception:
        pass


def _lifetime_update(raw_shares, raw_blocks, raw_kas=0):
    """Return monotonic (shares, blocks, kas) across bridge and redactor restarts."""
    global _lifetime_last_save
    raw_shares, raw_blocks = max(0, int(raw_shares)), max(0, int(raw_blocks))
    raw_kas = max(0, int(raw_kas))
    previous_shares, previous_blocks = _lifetime["rawShares"], _lifetime["rawBlocks"]
    previous_kas = _lifetime["rawKas"]
    share_delta = raw_shares if raw_shares < previous_shares else raw_shares - previous_shares
    block_delta = raw_blocks if raw_blocks < previous_blocks else raw_blocks - previous_blocks
    kas_delta = raw_kas if raw_kas < previous_kas else raw_kas - previous_kas
    _lifetime["totalShares"] += share_delta
    _lifetime["totalBlocks"] += block_delta
    _lifetime["totalKas"] += kas_delta
    _lifetime["rawShares"], _lifetime["rawBlocks"] = raw_shares, raw_blocks
    _lifetime["rawKas"] = raw_kas

    now = time.time()
    if block_delta or kas_delta or now - _lifetime_last_save >= 30:
        _lifetime["savedAt"] = int(now)
        tmp = LIFETIME_TOTALS_FILE + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(_lifetime, f)
            os.replace(tmp, LIFETIME_TOTALS_FILE)
            _lifetime_last_save = now
        except Exception:
            pass
    return _lifetime["totalShares"], _lifetime["totalBlocks"], _lifetime["totalKas"]


def _payouts_load():
    global _payouts, _payouts_seen
    try:
        with open(PAYOUT_HISTORY_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _payouts = {w: list(v)[-PAYOUT_HISTORY_CAP:] for w, v in data.items()
                        if isinstance(v, list)}
            _payouts_seen = {e.get("hash") for v in _payouts.values() for e in v
                             if isinstance(e, dict) and e.get("hash")}
        # Merge records previously evicted by the old unsorted retention logic.
        # This is idempotent and uses the same timestamp ordering as new data.
        try:
            with open(PAYOUT_RECOVERY_FILE, encoding="utf-8") as f:
                recovery = json.load(f)
            for wallet, entries in (recovery.items() if isinstance(recovery, dict) else []):
                target = _payouts.setdefault(wallet, [])
                for entry in entries if isinstance(entries, list) else []:
                    h = entry.get("hash") if isinstance(entry, dict) else None
                    if h and h not in _payouts_seen:
                        target.append(entry)
                        _payouts_seen.add(h)
                target.sort(key=lambda e: int(e.get("ts", 0)))
                del target[:-PAYOUT_HISTORY_CAP]
        except FileNotFoundError:
            pass
    except Exception:
        _payouts, _payouts_seen = {}, set()


def _payouts_record(blocks):
    """Append new bridge blocks (wallet, worker, hash, timestamp) to history."""
    global _payouts_dirty
    reward = current_miner_reward()
    with _payouts_lock:
        for b in blocks or []:
            h, w = b.get("hash"), b.get("wallet")
            if not h or not w or h in _payouts_seen:
                continue
            ts = b.get("timestamp")
            try:
                ts = int(float(ts))
            except (TypeError, ValueError):
                ts = int(time.time())
            lst = _payouts.setdefault(w, [])
            # Persist the value observed when this block was recorded.  Payout
            # history must not be re-priced every time the emission curve moves.
            lst.append({"ts": ts, "worker": b.get("worker") or "—", "hash": h,
                        "amountFc": reward})
            # Blocks can be delivered by the bridge out of timestamp order.
            # Retain the newest records by timestamp, otherwise a late-arriving
            # old block can evict a genuinely newer payout from the history.
            lst.sort(key=lambda e: int(e.get("ts", 0)))
            del lst[:-PAYOUT_HISTORY_CAP]
            _payouts_seen.add(h)
            _payouts_dirty = True


def _payouts_save():
    global _payouts_dirty
    with _payouts_lock:
        if not _payouts_dirty:
            return
        snap = {w: list(v) for w, v in _payouts.items()}
        _payouts_dirty = False
    tmp = PAYOUT_HISTORY_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        os.replace(tmp, PAYOUT_HISTORY_FILE)
    except Exception:
        pass


def payout_history(wallet, limit=50):
    with _payouts_lock:
        # Bridge block gauges are not guaranteed to arrive chronologically.
        # Sort before applying the retention window; slicing the raw insertion
        # order can hide newer payouts (for example July 30 entries behind
        # later-arriving July 28 records).
        lst = sorted(_payouts.get(wallet, []), key=lambda e: int(e.get("ts", 0)))
    # Old records predate amount persistence.  Re-price only those legacy rows
    # with the live consensus value; new rows retain the amount observed at
    # block discovery.  This removes the stale 57-ZKAS display immediately
    # without changing rows that already carry an exact amount.
    live_reward = current_miner_reward()
    return [{"ts": e["ts"], "worker": e["worker"], "hash": e["hash"],
             "amountFc": float(e.get("amountFc", live_reward))}
            for e in reversed(lst[-limit:])]


def remote_pool_workers():
    """Return live workers on the failover pool, or [] if it is unavailable.

    The remote endpoint is treated as an untrusted telemetry source: only
    workers explicitly marked online are imported, and no remote totals are
    used for payouts or block accounting.
    """
    result = []
    for stats_url in REMOTE_POOL_STATS_URLS:
      try:
        req = urllib.request.Request(stats_url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            payload = json.loads(resp.read().decode())
        for rw in payload.get("workers") or []:
            if (rw.get("status") or "online") != "online":
                continue
            wallet = (rw.get("wallet") or "").strip()
            if not wallet:
                continue
            result.append({
                "worker": rw.get("worker") or "—",
                "wallet": wallet,
                "hashrate": max(0.0, float(rw.get("hashrate") or 0.0)),
                "shares": int(rw.get("shares") or 0),
                "blocks": int(rw.get("blocks") or 0),
                "difficulty": rw.get("currentDifficulty"),
                "warmingUp": float(rw.get("hashrate") or 0.0) <= 0,
                "online": True,
                "status": "redirected-vps3",
                "source": "VPS3 new 5555" if ":3036/" in stats_url else "VPS3 old 5577",
                "lastSeen": rw.get("lastSeen"),
                "sessionUptime": rw.get("sessionUptime"),
            })
      except Exception:
        continue
    return result


def sample():
    text = ""
    for p in INSTANCE_PORTS:
        try:
            # Large long-running pools can expose tens of megabytes of
            # per-session metrics. Keep the timeout above observed generation
            # time so a healthy bridge is not rendered as an empty pool.
            text += urllib.request.urlopen(f"http://127.0.0.1:{p}/metrics", timeout=15).read().decode() + "\n"
        except Exception:
            continue
    if not text:
        return

    diff, shares, found, mined, pending, kas, kas_pool = {}, {}, {}, {}, {}, {}, {}
    _agg_sessions(RX_DIFF, text, diff)
    _agg_sessions(RX_SHARES, text, shares)
    _agg_sessions(RX_FOUND, text, found)
    _agg_sessions(RX_MINED, text, mined)
    _agg_sessions(RX_PENDING, text, pending)
    # `kas` is only what the MINER was paid. Pool-paid parents are counted
    # separately: they are the pool's revenue, not the miner's. A series with no
    # `kas_payout` label at all is from a pre-upgrade bridge; it is left out of
    # the miner's total rather than guessed at, because over-crediting is the
    # failure mode that misleads.
    _agg_sessions_where(RX_KAS, text, kas, outcome=KAS_ACCEPTED, kas_payout=KAS_TO_MINER)
    _agg_sessions_where(RX_KAS, text, kas_pool, outcome=KAS_ACCEPTED, kas_payout=KAS_TO_POOL)

    # Which workers currently have a usable kaspa: payout address.
    kas_set = {}
    for labels, val in RX_KAS_SET.findall(text):
        lb = _labels(labels)
        w, wk = lb.get("wallet"), lb.get("worker")
        if not w:
            continue
        try:
            v = float(val)
        except ValueError:
            continue
        key = (w, wk)
        kas_set[key] = kas_set.get(key, 0.0) or v

    def _gmax(rx):
        vals = [float(v) for v in rx.findall(text)]
        return max(vals) if vals else 0.0

    # The stratum bridge already computes a session-aware per-worker hashrate,
    # share count and current difficulty. Our own Δ(share-diff)/Δt is unreliable
    # for miners that reconnect often (each reconnect adds a new ip-labelled
    # Prometheus series, so the max-aggregated counter is non-monotonic and the
    # rate flaps to 0). So we PREFER the bridge's value and only fall back to our
    # computed rate when the bridge has none.
    bridge_by_key = {}
    bridge_total_blocks = None   # bridge's central cumulative pool-block count
    try:
        braw = urllib.request.urlopen("http://127.0.0.1:3033/api/stats", timeout=15).read().decode()
        bjson = json.loads(braw)
        # Central pool-block total from the bridge — monotonic within a bridge
        # session (unlike the sum of per-worker Prometheus series, which drops when
        # a worker disconnects). Used for the block-find share so worker churn
        # doesn't reset the window.
        bt = bjson.get("totalBlocks")
        if bt is not None:
            bridge_total_blocks = int(bt)
        # Record newly found blocks into the persistent per-wallet payout
        # history using the live consensus subsidy.
        _payouts_record(bjson.get("blocks") or [])
        _payouts_save()
        for bw in (bjson.get("workers") or []):
            # The bridge retains recently disconnected sessions as `idle`.
            # They are historical records, not live workers, and must not keep
            # zero-hash MRR probes visible or inflate the active-worker count.
            if bw.get("status") != "online":
                continue
            w = bw.get("wallet")
            wk = bw.get("worker") or "—"
            if not w:
                continue
            bridge_by_key[(w, wk)] = {
                "hr": float(bw.get("hashrate") or 0.0),          # GH/s
                "shares": int(bw.get("shares") or 0),
                "diff": bw.get("currentDifficulty"),
                # Keep lifecycle metadata separate from the rate.  A connected
                # miner can legitimately have 0 H/s before its first share.
                "status": bw.get("status") or "online",
                "lastSeen": bw.get("lastSeen"),
                "sessionUptime": bw.get("sessionUptime"),
            }
    except Exception:
        pass

    now = time.time()
    workers = []
    for k, cur in diff.items():
        wallet, worker = k
        dq = _hist.setdefault(k, collections.deque())
        # counter reset (bridge restart) → the cumulative counter dropped: start fresh.
        if dq and cur < dq[-1][1]:
            dq.clear()
        dq.append((now, cur))
        while len(dq) > 1 and now - dq[0][0] > WINDOW_SECS:
            dq.popleft()
        # Fallback hashrate = Δ(share-difficulty) · 2^32 / Δt over the rolling window.
        hr_ghs = 0.0
        if len(dq) >= 2:
            old_time, old_diff = dq[0]
            dt = now - old_time
            if dt > 0 and cur >= old_diff:
                hr_ghs = (cur - old_diff) * TWO32 / dt / 1e9
        b = bridge_by_key.get(k)
        hr_final = b["hr"] if (b and b["hr"] > 0) else hr_ghs   # prefer bridge, session-aware
        workers.append({
            "worker": worker or "—",
            "wallet": wallet,
            "hashrate": hr_final if b else 0.0,  # a dead session has no live rate
            "shares": int(shares.get(k, 0)) or (b["shares"] if b else 0),
            "blocks": int(found.get(k, 0)),
            # Kaspa blocks this worker's merged parents landed AND was paid for.
            # Separate from `blocks` (ZKas): different chains, different rewards.
            "kasBlocks": int(kas.get(k, 0)),
            # False => this worker set no usable kaspa: address, so its KAS mints
            # to the pool. The dashboard shows "not set" instead of a count.
            "kasPayoutSet": bool(kas_set.get(k, 0.0)),
            "difficulty": b["diff"] if b else None,
            "status": b["status"] if b else "offline",
            "lastSeen": b.get("lastSeen") if b else None,
            "sessionUptime": b.get("sessionUptime") if b else None,
            # Prometheus counters persist for every session since bridge start;
            # only workers the bridge currently tracks are actually connected.
            "online": b is not None,
        })
    # Also surface workers that are CONNECTED at the bridge but have not landed a
    # valid share yet (e.g. a small rig stuck on too-high difficulty, or one that
    # just connected). Prometheus only emits a series once a worker shares, so
    # without this they connect but never appear in the dashboard / miner lookup.
    seen = {(w["wallet"], w["worker"]) for w in workers}
    for (wallet, worker), b in bridge_by_key.items():
        if (wallet, worker) in seen:
            continue
        workers.append({
            "worker": worker,
            "wallet": wallet,
            "hashrate": b["hr"],                       # bridge rate (may be 0 = warming up)
            "shares": b["shares"],
            "blocks": int(found.get((wallet, worker), 0)),
            "kasBlocks": int(kas.get((wallet, worker), 0)),
            "kasPayoutSet": bool(kas_set.get((wallet, worker), 0.0)),
            "difficulty": b["diff"],
            "warmingUp": b["hr"] <= 0,
            "online": True,
            "status": b["status"],
            "lastSeen": b.get("lastSeen"),
            "sessionUptime": b.get("sessionUptime"),
        })

    # A miner that fails over to VPS3 is no longer visible to the primary
    # bridge. Merge its live telemetry so the public dashboard follows the
    # individual worker instead of displaying a stale 0 H/s row.
    workers.extend(remote_pool_workers())

    # drop workers gone since last scrape
    live = set(diff.keys())
    for k in list(_hist.keys()):
        if k not in live:
            _hist.pop(k, None)

    raw_shares = int(sum(shares.values()))
    raw_blocks_found = int(sum(found.values()))
    raw_kas_total = int(sum(kas.values()) + sum(kas_pool.values()))
    total_shares, blocks_found, kas_lifetime = _lifetime_update(raw_shares, raw_blocks_found, raw_kas_total)

    # ---- Network + pool hashrate, both grounded in the node ----------------
    # Network = the node's measured EstimateNetworkHashesPerSecond (authoritative;
    # includes every miner, not just ours). Difficulty likewise from the node.
    net_hs, net_diff, node_blocks = node_stats()
    net_blocks = node_blocks or int(_gmax(RX_NETBLK))
    # Refresh the display height in the single background sampler. Never make
    # this explorer request from an HTTP handler: if :8500 slows down, callers
    # otherwise serialize behind _HEIGHT_LOCK and exhaust the dashboard timeout.
    display_height = live_block_height(net_blocks)
    if net_diff <= 0:                           # node unreachable → fall back to bridge gauge
        net_diff = _gmax(RX_NETDIFF)

    # Pool hashrate headline comes from accepted share difficulty, not short-term
    # block luck. A block-ratio estimate over 10–15 minutes swings violently on
    # this fast chain (including dropping to zero while shares and jobs continue),
    # which made a healthy pool look stalled. Keep the block-share window below
    # as a diagnostic, but use the live share-derived worker sum for the UI and
    # cap it at the node's authoritative network estimate.
    raw_sum_hs = sum((w.get("hashrate") or 0) for w in workers) * 1e9  # GH/s -> H/s
    # Use the bridge's central monotonic block total; only fall back to the
    # (churn-sensitive) per-worker sum if the bridge total is unavailable.
    pool_blocks = bridge_total_blocks if bridge_total_blocks is not None else blocks_found
    bs = _blockshare
    if bs and (pool_blocks < bs[-1][1] or net_blocks < bs[-1][2]):
        bs.clear()                              # counter reset (bridge/node restart)
    bs.append((now, pool_blocks, net_blocks))
    while len(bs) > 1 and now - bs[0][0] > BLOCKSHARE_WINDOW:
        bs.popleft()
    block_share_hs = None
    dpool = dnet = -1
    if net_hs > 0 and len(bs) >= 2:
        dpool = pool_blocks - bs[0][1]
        dnet = net_blocks - bs[0][2]
        if dnet >= BLOCKSHARE_MIN_NET and dpool >= 0:
            block_share_hs = net_hs * min(1.0, dpool / dnet)
    pool_hs = min(raw_sum_hs, net_hs) if net_hs > 0 else raw_sum_hs
    # When the node is unreachable the network hashrate is simply UNKNOWN. It was
    # previously filled in with `max(raw_sum_hs, pool_hs)` — the pool's own worker
    # sum wearing the network's label, which reads as "this pool is 100% of the
    # network" on every sample and silently understated the real figure by ~130x
    # on a box where grpcurl was missing. A gap is honest; a fabricated number is
    # not. 0 renders as "—" in the dashboard.
    if net_hs <= 0:
        net_hs = 0.0

    # Rescale the per-worker rates so they sum to the true pool hashrate — keeps
    # relative rig sizes but makes the workers add up to the real pool total.
    scale = (pool_hs / raw_sum_hs) if raw_sum_hs > 0 else 0.0
    for w in workers:
        if w.get("hashrate"):
            w["hashrate"] = w["hashrate"] * scale

    with _lock:
        _state.update({
            "networkHashrate": net_hs,
            "poolHashrate": pool_hs,
            "networkBlockCount": display_height,
            "networkDifficulty": net_diff,
            "blockShareHashrate": block_share_hs,
            # A worker is "active" if the bridge currently tracks its connection.
            # Prom counter series persist for every session since bridge start, so
            # counting raw series keys inflates this with long-disconnected rigs.
            "activeWorkers": sum(1 for w in workers if w.get("online")),
            "totalShares": total_shares,
            "totalBlocks": blocks_found,
            # Pool-wide: every KAS block the pool's hashrate landed, however it was
            # paid. Split out so the two audiences do not get one confusing number --
            # `toMiners` is what miners were paid directly, `toPool` is pool revenue
            # (unset payout addresses plus the one fee minute per lane per hour).
            "kasBlocksFound": int(kas_lifetime),
            "kasBlocksToMiners": int(sum(kas.values())),
            "kasBlocksToPool": int(sum(kas_pool.values())),
            "bridgeUptime": int(now - START),
            "workers": workers,
            "blocks": [],
        })
        bbw = {}
        keys = set(list(found) + list(mined) + list(pending) + list(kas) + list(kas_set))
        for (w, wk) in keys:
            d = bbw.setdefault(w, {"found": 0, "confirmed": 0, "pending": 0, "kas": 0, "kasPayoutSet": False})
            d["found"] += int(found.get((w, wk), 0))
            d["confirmed"] += int(mined.get((w, wk), 0))
            d["pending"] += int(pending.get((w, wk), 0))
            # Miner-paid only: this is the miner's own page.
            d["kas"] += int(kas.get((w, wk), 0))
            if kas_set.get((w, wk), 0.0):
                d["kasPayoutSet"] = True
        _state["_bbw"] = bbw


def sampler_loop():
    while True:
        try:
            sample()
        except Exception:
            pass
        time.sleep(SAMPLE_SECS)


def mask_addr(a):
    if not isinstance(a, str) or ":" not in a:
        return "—"
    hrp, _, body = a.partition(":")
    if len(body) <= 12:
        return f"{hrp}:{body}"
    return f"{hrp}:{body[:4]}…{body[-4:]}"


def blocks_by_wallet():
    """{wallet: {found, confirmed, pending}} aggregated across that wallet's workers."""
    with _lock:
        return {w: dict(v) for w, v in _state.get("_bbw", {}).items()}


def snapshot():
    with _lock:
        s = dict(_state)
    s["workers"] = list(_state.get("workers", []))
    return s


def redact(stats, bbw):
    workers = stats.get("workers") or []
    # poolHashrate is computed at scrape time as the block-find share of the node's
    # network hashrate (workers are already rescaled to sum to it); fall back to the
    # worker sum only if an older snapshot lacks the field.
    pool_hashrate_hs = stats.get("poolHashrate")
    if pool_hashrate_hs is None:
        pool_hashrate_hs = sum((w.get("hashrate") or 0) for w in workers) * 1e9
    return {
        "networkHashrate": stats.get("networkHashrate"),
        "networkBlockCount": stats.get("networkBlockCount"),
        "networkDifficulty": stats.get("networkDifficulty"),
        "activeWorkers": stats.get("activeWorkers") or len(workers),
        "poolHashrate": pool_hashrate_hs,
        "totalBlocks": stats.get("totalBlocks"),
        "blocksAccepted": sum(v["found"] for v in bbw.values()),
        # Kaspa blocks found by merge-mining. A separate chain and a separate
        # reward: for miners who set a kaspa: address in the stratum password
        # the KAS mints to them, otherwise to the pool's own payout address.
        "kasBlocksFound": stats.get("kasBlocksFound") or 0,
        "kasBlocksToMiners": stats.get("kasBlocksToMiners") or 0,
        "kasBlocksToPool": stats.get("kasBlocksToPool") or 0,
        "totalShares": stats.get("totalShares"),
        "bridgeUptime": stats.get("bridgeUptime"),
        "workers": [{
            "worker": w.get("worker") or "—",
            "wallet": mask_addr(w.get("wallet")),
            "hashrate": w.get("hashrate"),
            "shares": w.get("shares"),
            "blocks": w.get("blocks") or 0,
            "kasBlocks": w.get("kasBlocks") or 0,
            "kasPayoutSet": bool(w.get("kasPayoutSet")),
            "difficulty": w.get("difficulty"),
            "warmingUp": bool(w.get("warmingUp")),
            "status": w.get("status") or ("online" if w.get("online") else "offline"),
            "lastSeen": w.get("lastSeen"),
            "sessionUptime": w.get("sessionUptime"),
            "source": w.get("source") or "primary",
        } for w in sorted(workers, key=lambda x: -(x.get("hashrate") or 0))
          if w.get("online")],
        "blocks": [],
    }


def miner(address, stats, bbw):
    address = (address or "").strip()
    workers = [w for w in (stats.get("workers") or []) if w.get("wallet") == address]
    blk = bbw.get(address, {"found": 0, "confirmed": 0, "pending": 0, "kas": 0, "kasPayoutSet": False})
    history = payout_history(address)
    confirmed = blk["confirmed"]
    pending = blk.get("pending") or max(0, blk["found"] - confirmed)
    return {
        "address": address,
        # A bridge restart can clear the in-memory counters while the
        # persisted payout history still proves this address has pool activity.
        "found": bool(workers) or address in bbw or bool(history),
        "workers": [{
            "worker": w.get("worker") or "—",
            "hashrate": w.get("hashrate"),   # GH/s (0 for offline sessions)
            "shares": w.get("shares") or 0,
            "blocks": w.get("blocks") or 0,
            "kasBlocks": w.get("kasBlocks") or 0,
            "kasPayoutSet": bool(w.get("kasPayoutSet")),
            "difficulty": w.get("difficulty"),
            "warmingUp": bool(w.get("warmingUp")),
            "online": bool(w.get("online")),
            "status": w.get("status") or ("online" if w.get("online") else "offline"),
            "lastSeen": w.get("lastSeen"),
            "sessionUptime": w.get("sessionUptime"),
            "source": w.get("source") or "primary",
        } for w in workers],
        "totalHashrate": sum((w.get("hashrate") or 0) for w in workers if w.get("online")),  # GH/s
        "totalShares": sum((w.get("shares") or 0) for w in workers),
        "blocksFound": blk["found"],
        # Kaspa blocks this wallet's workers landed via merged mining AND were paid
        # for. Blocks whose KAS minted to the pool (no `kaspa:` address set, or the
        # lane's fee minute) are deliberately not counted here -- showing them reads
        # as "you were paid this" when the miner received nothing.
        "kasBlocksFound": blk.get("kas", 0),
        # False => no worker on this wallet has a usable kaspa: payout address, so
        # the UI must say "not set" rather than print a count.
        "kasPayoutSet": bool(blk.get("kasPayoutSet")) or any(w.get("kasPayoutSet") for w in workers),
        "blocksConfirmed": confirmed,
        "blocksPending": pending,
        # Use the current consensus value for aggregate balances. Per-block
        # history retains the exact value observed when each block was found.
        "paidFc": confirmed * current_miner_reward(),
        "pendingFc": pending * current_miner_reward(),
        # Per-block payout history, newest first.
        "payouts": history,
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body):
        data = body.encode() if isinstance(body, str) else body
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            # The browser/proxy abandoned an expired request. Do not attempt a
            # second response or emit a traceback for a dead socket.
            return

    def do_GET(self):
        u = urlparse(self.path)
        path = u.path.rstrip("/")
        try:
            if path in ("/api/stats", "/pubstats"):
                public = redact(snapshot(), blocks_by_wallet())
                self._send(200, json.dumps(public))
            elif path == "/api/miner":
                addr = (parse_qs(u.query).get("address") or [""])[0]
                if not addr:
                    self._send(400, json.dumps({"error": "address required"}))
                    return
                self._send(200, json.dumps(miner(addr, snapshot(), blocks_by_wallet())))
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}))


if __name__ == "__main__":
    _lifetime_load()
    _payouts_load()  # payout history survives redactor + bridge restarts
    # Bind immediately. A synchronous initial scrape can take several seconds
    # when a node/explorer dependency is slow, producing a 502 window on every
    # service restart. The sampler fills the initially empty snapshot shortly.
    threading.Thread(target=sampler_loop, daemon=True).start()
    ThreadingHTTPServer(LISTEN, Handler).serve_forever()
