Content is user-generated and unverified.
#!/usr/bin/env python3
"""
LMTS staking positions tracker -> Google Sheets.

Contract: Synthetix-style StakingRewards with min lock time.
  events:  Staked(address indexed user, uint256 amount)
           Withdrawn(address indexed user, uint256 amount)
           RewardPaid(address indexed user, uint256 amount)
  views:   balanceOf(address), earned(address), totalSupply(),
           lastStakedAt(address), timeToUnlock(address), availableRewards()

Reads from the spreadsheet:
  - Wallets   (our addresses + labels; columns: wallet / wallet_address / address, label / labeled / name)
  - _State    (service sheet, created automatically)

Writes:
  - Staking          (full snapshot, sorted by position desc)
  - Staking_Summary  (totals, our vs retail, self-verification)
  - Movements        (append-only log of large stakes/unstakes)
  - _State           (deploy_block, last_movement_block, last_run_utc)

Required env:
  BASESCAN_API_KEY              Alchemy API key (name kept for compatibility)
  GOOGLE_SHEET_ID / GSHEET_ID
  GOOGLE_SERVICE_ACCOUNT_JSON

Optional env:
  STAKING_ADDRESS       default 0x843c68de2c36c6abbe4a3c28c949ea2f8ba6c195
  ALCHEMY_BASE_URL      default https://base-mainnet.g.alchemy.com/v2
  MOVE_THRESHOLD        default 10000        (in LMTS, min size to log a movement)
  DUST_THRESHOLD        default 0            (hide positions below this from Staking sheet)
  MOVEMENTS_BACKFILL    default true         (on first run, log whole history)
  MOVEMENTS_MAX_ROWS    default 5000         (trim oldest rows above this)
  LOG_CHUNK             default 100000       (initial eth_getLogs block window)
  CALL_BATCH            default 100          (eth_call per JSON-RPC batch)
  RATE_LIMIT_RPS        default 4
  CONFIRMATIONS         default 5            (blocks to stay behind head)
"""

import os
import json
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Any

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

getcontext().prec = 60

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("staking")


# =========================
# CONFIG
# =========================

ALCHEMY_API_KEY = os.getenv("BASESCAN_API_KEY", "").strip()
ALCHEMY_BASE_URL = os.getenv("ALCHEMY_BASE_URL", "https://base-mainnet.g.alchemy.com/v2").strip()

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip() or os.getenv("GSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

STAKING_ADDRESS = os.getenv(
    "STAKING_ADDRESS", "0x843c68de2c36c6abbe4a3c28c949ea2f8ba6c195"
).strip().lower()

MOVE_THRESHOLD = Decimal(os.getenv("MOVE_THRESHOLD", "10000"))
DUST_THRESHOLD = Decimal(os.getenv("DUST_THRESHOLD", "0"))
MOVEMENTS_BACKFILL = os.getenv("MOVEMENTS_BACKFILL", "true").lower() in {"1", "true", "yes"}
MOVEMENTS_MAX_ROWS = int(os.getenv("MOVEMENTS_MAX_ROWS", "5000"))
LOG_CHUNK = int(os.getenv("LOG_CHUNK", "100000"))
CALL_BATCH = int(os.getenv("CALL_BATCH", "100"))
RATE_LIMIT_RPS = int(os.getenv("RATE_LIMIT_RPS", "4"))
CONFIRMATIONS = int(os.getenv("CONFIRMATIONS", "5"))

BASESCAN_TX = "https://basescan.org/tx/"
BASESCAN_ADDR = "https://basescan.org/address/"

# --- selectors (keccak-verified) ---
SEL_BALANCE_OF = "0x70a08231"      # balanceOf(address)
SEL_EARNED = "0x008cc262"          # earned(address)
SEL_LAST_STAKED_AT = "0x77a46edd"  # lastStakedAt(address)
SEL_TIME_TO_UNLOCK = "0x3345d3d0"  # timeToUnlock(address)
SEL_TOTAL_SUPPLY = "0x18160ddd"    # totalSupply()
SEL_AVAILABLE_REWARDS = "0x879d9090"  # availableRewards()
SEL_STAKING_TOKEN = "0x72f702f3"   # stakingToken()
SEL_REWARDS_TOKEN = "0xd1af0c7d"   # rewardsToken()
SEL_REWARD_RATE = "0x7b0a47ee"     # rewardRate()
SEL_FINISH_AT = "0x67d3b488"       # finishAt()
SEL_MIN_LOCK = "0xa60ff766"        # minLockTime()
SEL_DECIMALS = "0x313ce567"        # decimals()

# --- event topics (keccak-verified) ---
TOPIC_STAKED = "0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d"
TOPIC_WITHDRAWN = "0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5"
TOPIC_REWARD_PAID = "0xe2403640ba68fed3a2f88b7557551d1993f84b99bb10ff833f0cf8db0c5e0486"

STATE_SHEET = "_State"
SHEET_POSITIONS = "Staking"
SHEET_SUMMARY = "Staking_Summary"
SHEET_MOVEMENTS = "Movements"


# =========================
# MODELS
# =========================

@dataclass
class StakeEvent:
    block: int
    ts: int
    tx_hash: str
    log_index: int
    user: str
    kind: str          # STAKE | UNSTAKE | REWARD
    amount: Decimal


@dataclass
class Position:
    address: str
    label: str = ""
    is_ours: bool = False
    staked: Decimal = Decimal(0)          # on-chain balanceOf
    staked_from_events: Decimal = Decimal(0)
    pending_rewards: Decimal = Decimal(0)
    claimed_rewards: Decimal = Decimal(0)
    last_staked_ts: int = 0
    unlock_in_sec: int = 0
    first_seen_ts: int = 0
    events: list[StakeEvent] = field(default_factory=list)


# =========================
# HELPERS
# =========================

def norm_addr(a: Any) -> str:
    return str(a or "").strip().lower()


def hex_to_int(v: Any) -> int:
    if v is None or v == "":
        return 0
    if isinstance(v, int):
        return v
    s = str(v).strip()
    return int(s, 16) if s.lower().startswith("0x") else int(s)


def to_hex(n: int) -> str:
    return hex(int(n))


def ts_to_utc(ts: int) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def now_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def dec_str(v: Any, places: int = 6) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        q = Decimal(1).scaleb(-places)
        s = format(v.quantize(q), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s and s != "-0" else "0"
    return str(v)


def topic_to_addr(topic: str) -> str:
    return "0x" + str(topic)[-40:].lower()


def encode_addr_call(selector: str, address: str) -> str:
    return selector + norm_addr(address)[2:].rjust(64, "0")


def human_duration(seconds: int) -> str:
    if seconds <= 0:
        return "unlocked"
    d, rem = divmod(int(seconds), 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


# =========================
# RPC
# =========================

class RateLimiter:
    def __init__(self, rps: int):
        self.min_interval = 1.0 / max(int(rps), 1)
        self.last = 0.0

    def wait(self):
        delta = time.monotonic() - self.last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self.last = time.monotonic()


_limiter = RateLimiter(RATE_LIMIT_RPS)
_req_id = 0


def _next_id() -> int:
    global _req_id
    _req_id += 1
    return _req_id


def _rpc_url() -> str:
    if not ALCHEMY_API_KEY:
        raise RuntimeError("BASESCAN_API_KEY is missing (should contain the Alchemy key)")
    return f"{ALCHEMY_BASE_URL.rstrip('/')}/{ALCHEMY_API_KEY}"


def _post(payload: Any, max_retries: int = 6) -> Any:
    url = _rpc_url()
    backoff = 1.0

    for attempt in range(max_retries):
        _limiter.wait()
        try:
            r = requests.post(url, json=payload, timeout=90,
                              headers={"Content-Type": "application/json"})
        except requests.RequestException as e:
            log.warning("network error: %s (attempt %s)", e, attempt + 1)
            time.sleep(backoff)
            backoff *= 2
            continue

        if r.status_code == 429 or r.status_code >= 500:
            log.warning("HTTP %s, sleeping %.1fs", r.status_code, backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        r.raise_for_status()
        try:
            return r.json()
        except ValueError as e:
            log.warning("bad JSON: %s", e)
            time.sleep(backoff)
            backoff *= 2

    raise RuntimeError("RPC failed after retries")


def rpc(method: str, params: list) -> Any:
    data = _post({"jsonrpc": "2.0", "id": _next_id(), "method": method, "params": params})
    if isinstance(data, dict) and data.get("error"):
        raise RpcError(str(data["error"]))
    return data.get("result") if isinstance(data, dict) else None


class RpcError(RuntimeError):
    pass


def rpc_batch(calls: list[tuple[str, list]]) -> list[Any]:
    """Send many RPC calls in one HTTP request. Returns results in the same order."""
    if not calls:
        return []

    payload = []
    order: dict[int, int] = {}
    for i, (method, params) in enumerate(calls):
        rid = _next_id()
        order[rid] = i
        payload.append({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})

    data = _post(payload)

    if isinstance(data, dict):  # some providers collapse single-item batches
        data = [data]

    out: list[Any] = [None] * len(calls)
    for item in data:
        idx = order.get(item.get("id"))
        if idx is None:
            continue
        if item.get("error"):
            raise RpcError(f"batch item error: {item['error']}")
        out[idx] = item.get("result")
    return out


def eth_call(to: str, data: str, block: str = "latest") -> str:
    return rpc("eth_call", [{"to": to, "data": data}, block]) or "0x"


def call_uint(to: str, data: str, block: str = "latest") -> int:
    return hex_to_int(eth_call(to, data, block))


def latest_block() -> int:
    return hex_to_int(rpc("eth_blockNumber", []))


def block_timestamp(n: int) -> int:
    b = rpc("eth_getBlockByNumber", [to_hex(n), False]) or {}
    return hex_to_int(b.get("timestamp", "0x0"))


# =========================
# DEPLOY BLOCK
# =========================

def find_deploy_block(address: str, hi: int) -> int:
    """Binary search for the first block where the contract has code."""
    lo = 0
    log.info("searching deploy block for %s ...", address)
    while lo < hi:
        mid = (lo + hi) // 2
        code = rpc("eth_getCode", [address, to_hex(mid)]) or "0x"
        if len(code) > 2:
            hi = mid
        else:
            lo = mid + 1
    log.info("deploy block = %s", lo)
    return lo


# =========================
# LOGS
# =========================

def get_logs_chunked(address: str, topics: list, from_block: int, to_block: int) -> list[dict]:
    """eth_getLogs with adaptive window: halves the range whenever the node complains."""
    out: list[dict] = []
    start = from_block
    window = max(LOG_CHUNK, 1)

    while start <= to_block:
        end = min(start + window - 1, to_block)
        params = {
            "address": address,
            "fromBlock": to_hex(start),
            "toBlock": to_hex(end),
            "topics": topics,
        }
        try:
            res = rpc("eth_getLogs", [params]) or []
        except RpcError as e:
            msg = str(e).lower()
            too_big = any(k in msg for k in
                          ("more than", "limit", "range", "too large", "exceed", "timeout"))
            if too_big and window > 1:
                window = max(window // 4, 1)
                log.warning("shrinking log window to %s blocks", window)
                continue
            raise

        out.extend(res)
        log.info("logs %s-%s: +%s (total %s)", start, end, len(res), len(out))
        start = end + 1

        if len(res) < 2000 and window < LOG_CHUNK:
            window = min(window * 2, LOG_CHUNK)

    return out


def parse_events(raw_logs: list[dict], decimals: int) -> list[StakeEvent]:
    kinds = {
        TOPIC_STAKED: "STAKE",
        TOPIC_WITHDRAWN: "UNSTAKE",
        TOPIC_REWARD_PAID: "REWARD",
    }
    scale = Decimal(10) ** decimals
    events: list[StakeEvent] = []

    for lg in raw_logs:
        topics = lg.get("topics") or []
        if not topics:
            continue
        kind = kinds.get(str(topics[0]).lower())
        if not kind or len(topics) < 2:
            continue

        events.append(StakeEvent(
            block=hex_to_int(lg.get("blockNumber")),
            ts=0,
            tx_hash=str(lg.get("transactionHash", "")).lower(),
            log_index=hex_to_int(lg.get("logIndex")),
            user=topic_to_addr(topics[1]),
            kind=kind,
            amount=Decimal(hex_to_int(lg.get("data"))) / scale,
        ))

    events.sort(key=lambda e: (e.block, e.log_index))
    return events


def attach_timestamps(events: list[StakeEvent]) -> None:
    """Fetch block timestamps in batches, one request per unique block."""
    blocks = sorted({e.block for e in events})
    if not blocks:
        return

    ts_map: dict[int, int] = {}
    for i in range(0, len(blocks), CALL_BATCH):
        chunk = blocks[i:i + CALL_BATCH]
        results = rpc_batch([("eth_getBlockByNumber", [to_hex(b), False]) for b in chunk])
        for b, res in zip(chunk, results):
            ts_map[b] = hex_to_int((res or {}).get("timestamp", "0x0"))
        log.info("block timestamps %s/%s", min(i + CALL_BATCH, len(blocks)), len(blocks))

    for e in events:
        e.ts = ts_map.get(e.block, 0)


# =========================
# POSITIONS
# =========================

def read_positions(addresses: list[str], block: int, decimals: int) -> dict[str, dict]:
    """Batch-read balanceOf / earned / lastStakedAt / timeToUnlock for every address."""
    scale = Decimal(10) ** decimals
    blk = to_hex(block)
    out: dict[str, dict] = {}

    per_addr = 4
    per_batch = max(CALL_BATCH // per_addr, 1)

    for i in range(0, len(addresses), per_batch):
        chunk = addresses[i:i + per_batch]
        calls: list[tuple[str, list]] = []
        for addr in chunk:
            for sel in (SEL_BALANCE_OF, SEL_EARNED, SEL_LAST_STAKED_AT, SEL_TIME_TO_UNLOCK):
                calls.append(("eth_call", [
                    {"to": STAKING_ADDRESS, "data": encode_addr_call(sel, addr)}, blk
                ]))

        results = rpc_batch(calls)

        for j, addr in enumerate(chunk):
            base = j * per_addr
            out[addr] = {
                "staked": Decimal(hex_to_int(results[base])) / scale,
                "earned": Decimal(hex_to_int(results[base + 1])) / scale,
                "last_staked_at": hex_to_int(results[base + 2]),
                "time_to_unlock": hex_to_int(results[base + 3]),
            }

        log.info("positions read %s/%s", min(i + per_batch, len(addresses)), len(addresses))

    return out


# =========================
# GOOGLE SHEETS
# =========================

def open_sheet():
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("GOOGLE_SHEET_ID / GSHEET_ID is missing")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is missing")

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)


def ensure_ws(ss, title: str, rows: int = 1000, cols: int = 26):
    try:
        return ss.worksheet(title)
    except WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def write_sheet(ss, title: str, header: list[str], rows: list[list], chunk: int = 2000):
    need_rows = max(len(rows) + 10, 100)
    need_cols = max(len(header), 10)
    ws = ensure_ws(ss, title, rows=need_rows, cols=need_cols)

    if ws.row_count < need_rows or ws.col_count < need_cols:
        ws.resize(rows=max(ws.row_count, need_rows), cols=max(ws.col_count, need_cols))

    ws.clear()
    matrix = [header] + rows

    for start in range(0, len(matrix), chunk):
        part = matrix[start:start + chunk]
        for attempt in range(5):
            try:
                ws.update(values=part, range_name=f"A{start + 1}",
                          value_input_option="USER_ENTERED")
                break
            except APIError as e:
                if attempt == 4:
                    raise
                wait = 2 ** attempt
                log.warning("sheets write retry in %ss: %s", wait, e)
                time.sleep(wait)
        time.sleep(0.3)

    log.info("wrote %s rows -> %s", len(rows), title)


def read_state(ss) -> dict[str, str]:
    ws = ensure_ws(ss, STATE_SHEET, rows=50, cols=4)
    values = ws.get_all_values()
    state: dict[str, str] = {}
    for row in values:
        if len(row) >= 2 and row[0].strip():
            state[row[0].strip().lower()] = row[1].strip()
    return state


def write_state(ss, state: dict[str, str]):
    rows = [[k, v] for k, v in sorted(state.items())]
    write_sheet(ss, STATE_SHEET, ["key", "value"], rows)


def read_our_wallets(ss) -> dict[str, str]:
    """Returns {address: label}. Tolerant to column naming."""
    ws = None
    for name in ("Wallets", "wallets", "Labels", "labels"):
        try:
            ws = ss.worksheet(name)
            break
        except WorksheetNotFound:
            continue

    if ws is None:
        log.warning("no Wallets sheet found - everything will be treated as RETAIL")
        return {}

    values = ws.get_all_values()
    if not values:
        return {}

    headers = [h.strip().lower() for h in values[0]]

    def find_col(*names) -> int:
        for n in names:
            if n in headers:
                return headers.index(n)
        return -1

    addr_col = find_col("wallet", "wallet_address", "address", "adress")
    label_col = find_col("label", "labeled", "name", "title")

    out: dict[str, str] = {}

    if addr_col == -1:
        # fall back: scan every cell for something that looks like an address
        for row in values:
            for cell in row:
                c = norm_addr(cell)
                if c.startswith("0x") and len(c) == 42:
                    out.setdefault(c, "")
        log.warning("no address column header found, scanned %s addresses heuristically", len(out))
        return out

    for row in values[1:]:
        addr = norm_addr(row[addr_col]) if addr_col < len(row) else ""
        if not (addr.startswith("0x") and len(addr) == 42):
            continue
        label = ""
        if 0 <= label_col < len(row):
            label = str(row[label_col]).strip()
        out[addr] = label or addr[:10]

    return out


def append_movements(ss, header: list[str], new_rows: list[list]):
    ws = ensure_ws(ss, SHEET_MOVEMENTS, rows=max(len(new_rows) + 100, 1000), cols=len(header) + 2)
    existing = ws.get_all_values()

    if not existing or not existing[0] or existing[0][0].strip().lower() != header[0].lower():
        write_sheet(ss, SHEET_MOVEMENTS, header, new_rows)
        return

    body = existing[1:]
    combined = body + new_rows

    if MOVEMENTS_MAX_ROWS and len(combined) > MOVEMENTS_MAX_ROWS:
        combined = combined[-MOVEMENTS_MAX_ROWS:]
        write_sheet(ss, SHEET_MOVEMENTS, header, combined)
        return

    if not new_rows:
        return

    needed = len(combined) + 10
    if ws.row_count < needed:
        ws.resize(rows=needed, cols=max(ws.col_count, len(header)))

    for attempt in range(5):
        try:
            ws.append_rows(new_rows, value_input_option="USER_ENTERED",
                           table_range=f"A{len(existing)}")
            break
        except APIError as e:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)
            log.warning("movements append retry: %s", e)

    log.info("appended %s movements", len(new_rows))


# =========================
# MAIN
# =========================

def main():
    ss = open_sheet()
    state = read_state(ss)

    head = latest_block()
    target_block = max(head - CONFIRMATIONS, 0)
    log.info("head=%s target_block=%s", head, target_block)

    # --- contract sanity ---
    staking_token = "0x" + eth_call(STAKING_ADDRESS, SEL_STAKING_TOKEN)[-40:]
    rewards_token = "0x" + eth_call(STAKING_ADDRESS, SEL_REWARDS_TOKEN)[-40:]
    decimals = call_uint(staking_token, SEL_DECIMALS) or 18

    log.info("stakingToken=%s rewardsToken=%s decimals=%s",
             staking_token, rewards_token, decimals)

    # --- deploy block (cached) ---
    deploy_block = int(state.get("deploy_block") or 0)
    if not deploy_block:
        deploy_block = find_deploy_block(STAKING_ADDRESS, target_block)
        state["deploy_block"] = str(deploy_block)

    # --- full event history ---
    raw = get_logs_chunked(
        STAKING_ADDRESS,
        [[TOPIC_STAKED, TOPIC_WITHDRAWN, TOPIC_REWARD_PAID]],
        deploy_block,
        target_block,
    )
    events = parse_events(raw, decimals)
    log.info("parsed %s events", len(events))

    if not events:
        log.warning("no events found - nothing to do")
        return

    attach_timestamps(events)

    # --- our addresses ---
    our = read_our_wallets(ss)
    log.info("our wallets loaded: %s", len(our))

    # --- build positions from events (independent of on-chain read) ---
    positions: dict[str, Position] = {}
    for e in events:
        p = positions.get(e.user)
        if p is None:
            p = Position(
                address=e.user,
                label=our.get(e.user, ""),
                is_ours=e.user in our,
                first_seen_ts=e.ts,
            )
            positions[e.user] = p

        p.events.append(e)
        if e.kind == "STAKE":
            p.staked_from_events += e.amount
        elif e.kind == "UNSTAKE":
            p.staked_from_events -= e.amount
        elif e.kind == "REWARD":
            p.claimed_rewards += e.amount

    addresses = sorted(positions.keys())
    log.info("unique addresses seen: %s", len(addresses))

    # --- on-chain truth ---
    onchain = read_positions(addresses, target_block, decimals)
    for addr, data in onchain.items():
        p = positions[addr]
        p.staked = data["staked"]
        p.pending_rewards = data["earned"]
        p.last_staked_ts = data["last_staked_at"]
        p.unlock_in_sec = data["time_to_unlock"]

    scale = Decimal(10) ** decimals
    total_supply = Decimal(call_uint(STAKING_ADDRESS, SEL_TOTAL_SUPPLY, to_hex(target_block))) / scale
    available_rewards = Decimal(call_uint(STAKING_ADDRESS, SEL_AVAILABLE_REWARDS, to_hex(target_block))) / scale
    contract_balance = Decimal(
        call_uint(staking_token, encode_addr_call(SEL_BALANCE_OF, STAKING_ADDRESS), to_hex(target_block))
    ) / scale

    # =====================
    # POSITIONS SHEET
    # =====================
    active = [p for p in positions.values() if p.staked > DUST_THRESHOLD]
    active.sort(key=lambda p: p.staked, reverse=True)

    sum_positions = sum((p.staked for p in positions.values()), Decimal(0))
    our_total = sum((p.staked for p in active if p.is_ours), Decimal(0))
    retail_total = sum((p.staked for p in active if not p.is_ours), Decimal(0))

    now_ts = int(time.time())

    pos_header = [
        "rank", "group", "label", "address", "staked_lmts", "share_pct",
        "pending_rewards", "claimed_rewards", "last_staked_utc", "unlock_in",
        "stakes_count", "unstakes_count", "first_seen_utc", "events_check_diff", "link",
    ]

    pos_rows = []
    our_rank = 0
    retail_rank = 0

    for i, p in enumerate(active, start=1):
        if p.is_ours:
            our_rank += 1
        else:
            retail_rank += 1

        share = (p.staked / sum_positions * 100) if sum_positions else Decimal(0)
        diff = p.staked - p.staked_from_events

        pos_rows.append([
            i,
            "OUR" if p.is_ours else "RETAIL",
            p.label or ("" if not p.is_ours else p.address[:10]),
            p.address,
            dec_str(p.staked),
            dec_str(share, 4),
            dec_str(p.pending_rewards),
            dec_str(p.claimed_rewards),
            ts_to_utc(p.last_staked_ts),
            human_duration(p.unlock_in_sec),
            sum(1 for e in p.events if e.kind == "STAKE"),
            sum(1 for e in p.events if e.kind == "UNSTAKE"),
            ts_to_utc(p.first_seen_ts),
            dec_str(diff),
            f"{BASESCAN_ADDR}{p.address}",
        ])

    write_sheet(ss, SHEET_POSITIONS, pos_header, pos_rows)

    # =====================
    # SUMMARY SHEET
    # =====================
    exited = [p for p in positions.values() if p.staked <= DUST_THRESHOLD]
    max_drift = max((abs(p.staked - p.staked_from_events) for p in positions.values()),
                    default=Decimal(0))

    check_supply = abs(sum_positions - total_supply)
    check_balance = contract_balance - total_supply - available_rewards

    summary = [
        ["generated_at_utc", now_utc()],
        ["block", target_block],
        ["block_time_utc", ts_to_utc(block_timestamp(target_block))],
        ["staking_contract", STAKING_ADDRESS],
        ["staking_token", staking_token],
        ["rewards_token", rewards_token],
        ["", ""],
        ["total_staked (totalSupply)", dec_str(total_supply)],
        ["our_staked", dec_str(our_total)],
        ["retail_staked", dec_str(retail_total)],
        ["our_share_pct", dec_str((our_total / total_supply * 100) if total_supply else Decimal(0), 4)],
        ["retail_share_pct", dec_str((retail_total / total_supply * 100) if total_supply else Decimal(0), 4)],
        ["", ""],
        ["stakers_active", len(active)],
        ["stakers_our_active", sum(1 for p in active if p.is_ours)],
        ["stakers_retail_active", sum(1 for p in active if not p.is_ours)],
        ["addresses_ever_staked", len(positions)],
        ["addresses_fully_exited", len(exited)],
        ["", ""],
        ["pending_rewards_total", dec_str(sum((p.pending_rewards for p in positions.values()), Decimal(0)))],
        ["claimed_rewards_total", dec_str(sum((p.claimed_rewards for p in positions.values()), Decimal(0)))],
        ["available_rewards (contract)", dec_str(available_rewards)],
        ["", ""],
        ["--- SELF-CHECK ---", ""],
        ["sum(balanceOf) vs totalSupply", dec_str(check_supply)],
        ["check_1_positions_match_supply", "OK" if check_supply < Decimal("0.000001") else "MISMATCH"],
        ["contract_LMTS_balance", dec_str(contract_balance)],
        ["balance - totalSupply - availableRewards", dec_str(check_balance)],
        ["check_2_balance_reconciles", "OK" if abs(check_balance) < Decimal("1") else "REVIEW"],
        ["max_drift_events_vs_onchain", dec_str(max_drift)],
        ["check_3_events_match_onchain", "OK" if max_drift < Decimal("0.000001") else "REVIEW"],
        ["events_scanned", len(events)],
        ["scanned_from_block", deploy_block],
    ]

    write_sheet(ss, SHEET_SUMMARY, ["metric", "value"], summary)

    # =====================
    # MOVEMENTS
    # =====================
    last_move_block = int(state.get("last_movement_block") or 0)
    first_run = last_move_block == 0

    if first_run and not MOVEMENTS_BACKFILL:
        move_from = target_block
    else:
        move_from = last_move_block

    # running balance so we can report position before/after each move
    running: dict[str, Decimal] = {}
    move_rows: list[list] = []

    move_header = [
        "detected_at_utc", "block_time_utc", "block", "action", "group", "label",
        "address", "amount_lmts", "position_before", "position_after",
        "exited_fully", "tx_hash", "link",
    ]

    for e in events:
        before = running.get(e.user, Decimal(0))

        if e.kind == "STAKE":
            after = before + e.amount
        elif e.kind == "UNSTAKE":
            after = before - e.amount
        else:
            continue  # reward claims are not position moves

        running[e.user] = after

        if e.block <= move_from:
            continue
        if e.amount < MOVE_THRESHOLD:
            continue

        p = positions[e.user]
        move_rows.append([
            now_utc(),
            ts_to_utc(e.ts),
            e.block,
            "STAKE" if e.kind == "STAKE" else "UNSTAKE",
            "OUR" if p.is_ours else "RETAIL",
            p.label or "",
            e.user,
            dec_str(e.amount),
            dec_str(before),
            dec_str(after),
            "YES" if (e.kind == "UNSTAKE" and after <= Decimal("0.000001")) else "",
            e.tx_hash,
            f"{BASESCAN_TX}{e.tx_hash}",
        ])

    if move_rows:
        append_movements(ss, move_header, move_rows)
    else:
        ensure_ws(ss, SHEET_MOVEMENTS, rows=1000, cols=len(move_header))
        existing = ss.worksheet(SHEET_MOVEMENTS).get_all_values()
        if not existing:
            write_sheet(ss, SHEET_MOVEMENTS, move_header, [])
        log.info("no movements above threshold %s", MOVE_THRESHOLD)

    # --- persist state ---
    state["last_movement_block"] = str(target_block)
    state["last_run_utc"] = now_utc()
    state["last_total_staked"] = dec_str(total_supply)
    state["last_stakers_count"] = str(len(active))
    write_state(ss, state)

    log.info(
        "DONE | total=%s our=%s retail=%s stakers=%s moves=%s",
        dec_str(total_supply), dec_str(our_total), dec_str(retail_total),
        len(active), len(move_rows),
    )


if __name__ == "__main__":
    main()
