import argparse
import json
import math
import os
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from futu import OpenQuoteContext, OptionType, RET_OK


OPTION_CODE_RE = re.compile(r"^[A-Z]{1,8}\d{6}[CP]\d+$")
DEFAULT_REPORT_SETTINGS = {
    "focus_symbols": ["CRCL", "HOOD", "RKLB", "CRWV", "CMPS"],
    "secondary_symbols": ["ASPI", "GOSS"],
    "option_dte_max": 90,
    "covered_call_min_dte": 7,
    "covered_call_target_delta": 0.20,
}


def find_latest_positions(data_dir: Path) -> Path:
    files = sorted([p for p in data_dir.glob("*.json") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            return path
    raise FileNotFoundError(f"No positions json with rows found under {data_dir}")


def load_report_settings(project_dir: Path) -> Dict:
    settings = dict(DEFAULT_REPORT_SETTINGS)
    settings_path = project_dir / "report_settings.json"
    if settings_path.exists():
        loaded = json.loads(settings_path.read_text(encoding="utf-8-sig"))
        if isinstance(loaded, dict):
            settings.update(loaded)
    settings["focus_symbols"] = [str(x).upper() for x in settings.get("focus_symbols", [])]
    settings["secondary_symbols"] = [str(x).upper() for x in settings.get("secondary_symbols", [])]
    settings["option_dte_max"] = int(settings.get("option_dte_max", 90))
    settings["covered_call_min_dte"] = int(settings.get("covered_call_min_dte", 7))
    settings["covered_call_target_delta"] = float(settings.get("covered_call_target_delta", 0.20))
    return settings


def normalize_code(code: str) -> str:
    value = (code or "").upper()
    if "." in value:
        value = value.split(".", 1)[1]
    return value


def to_underlying(code: str) -> str:
    normalized = normalize_code(code)
    if OPTION_CODE_RE.match(normalized):
        for idx, ch in enumerate(normalized):
            if ch.isdigit():
                return normalized[:idx]
    return normalized


def load_strategy_map(strategy_dir: Path) -> Dict[str, Dict[str, str]]:
    strategy_map: Dict[str, Dict[str, str]] = {}
    for path in sorted(strategy_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8-sig")
        ticker = path.stem.upper().split("_")[0].split("-")[0].split(" ")[0]
        summary_lines: List[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            summary_lines.append(stripped)
            if len(summary_lines) >= 2:
                break
        strategy_map[ticker] = {
            "file": path.name,
            "summary": " ".join(summary_lines) if summary_lines else "(no summary)",
        }
    return strategy_map


def parse_expiry(code: str) -> Optional[datetime]:
    normalized = normalize_code(code)
    match = re.match(r"^[A-Z]{1,8}(\d{6})[CP]\d+$", normalized)
    if not match:
        return None
    yymmdd = match.group(1)
    yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
    return datetime(year=2000 + yy, month=mm, day=dd)


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def call_delta_estimate(spot: float, strike: float, t_years: float, sigma: float, rate: float = 0.04) -> float:
    if spot <= 0 or strike <= 0 or t_years <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * t_years) / (sigma * math.sqrt(t_years))
    return max(0.0, min(1.0, norm_cdf(d1)))


def chunked(items: List[str], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def connect_quote_context(host: str, port: int) -> Optional[OpenQuoteContext]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2.0)
        if sock.connect_ex((host, port)) != 0:
            return None
    return OpenQuoteContext(host=host, port=port)


def get_market_states(quote_ctx: Optional[OpenQuoteContext]) -> Dict[str, str]:
    if quote_ctx is None:
        return {"market_us": "UNKNOWN", "market_hk": "UNKNOWN", "program_status_type": "UNKNOWN"}
    ret, data = quote_ctx.get_global_state()
    if ret != RET_OK or not isinstance(data, dict):
        return {"market_us": "UNKNOWN", "market_hk": "UNKNOWN", "program_status_type": "UNKNOWN"}
    return {
        "market_us": str(data.get("market_us", "UNKNOWN")),
        "market_hk": str(data.get("market_hk", "UNKNOWN")),
        "program_status_type": str(data.get("program_status_type", "UNKNOWN")),
    }


def format_market_state(state: str) -> str:
    mapping = {
        "MORNING": "open",
        "AFTERNOON": "open",
        "REST": "midday break",
        "PRE_MARKET_BEGIN": "pre-market",
        "PRE_MARKET_END": "pre-market",
        "AFTER_HOURS_BEGIN": "after-hours",
        "AFTER_HOURS_END": "after-hours closed",
        "CLOSED": "closed",
        "NIGHT_OPEN": "night open",
        "NIGHT_END": "night closed",
        "FUTURE_OPEN": "futures open",
        "FUTURE_CLOSE": "futures closed",
        "UNKNOWN": "unknown",
    }
    return mapping.get(state, state.lower())


def get_underlying_snapshot(quote_ctx: OpenQuoteContext, code: str) -> Tuple[float, float, bool]:
    ret, snap = quote_ctx.get_market_snapshot([code])
    if ret != RET_OK or snap is None or len(snap) == 0:
        return 0.0, 0.0, False
    row = snap.iloc[0]
    last = float(row.get("last_price", 0) or 0)
    prev = float(row.get("prev_close_price", 0) or 0)
    pct = ((last - prev) / prev * 100.0) if prev > 0 else 0.0
    return last, pct, True


def fetch_call_chain(quote_ctx: OpenQuoteContext, owner_code: str, min_dte: int, max_dte: int) -> List[Dict]:
    today = datetime.now().date()
    chain_frames = []
    for start_day in range(min_dte, max_dte + 1, 30):
        start = (today.fromordinal(today.toordinal() + start_day)).strftime("%Y-%m-%d")
        end_day = min(start_day + 29, max_dte)
        end = (today.fromordinal(today.toordinal() + end_day)).strftime("%Y-%m-%d")
        ret, frame = quote_ctx.get_option_chain(code=owner_code, start=start, end=end, option_type=OptionType.CALL)
        if ret == RET_OK and frame is not None and len(frame) > 0:
            chain_frames.append(frame)
    if not chain_frames:
        return []
    return pd.concat(chain_frames, ignore_index=True).drop_duplicates(subset=["code"]).to_dict(orient="records")


def choose_call_candidate(
    quote_ctx: Optional[OpenQuoteContext],
    owner_code: str,
    fallback_spot: float,
    min_dte: int,
    max_dte: int,
    target_delta: float,
) -> Tuple[Optional[Dict], float, bool]:
    if quote_ctx is None:
        return None, 0.0, False

    calls = fetch_call_chain(quote_ctx, owner_code, min_dte=min_dte, max_dte=max_dte)
    if not calls:
        return None, 0.0, False

    spot, day_pct, day_known = get_underlying_snapshot(quote_ctx, owner_code)
    if spot <= 0 and fallback_spot > 0:
        spot = fallback_spot
    if spot <= 0:
        return None, day_pct, day_known

    filtered = []
    now = datetime.now().date()
    for row in calls:
        strike = float(row.get("strike_price", 0) or 0)
        strike_time = str(row.get("strike_time", ""))
        if strike <= 0 or not strike_time:
            continue
        expiry = datetime.strptime(strike_time, "%Y-%m-%d").date()
        dte = (expiry - now).days
        if not (min_dte <= dte <= max_dte):
            continue
        if not (spot * 1.02 <= strike <= spot * 1.40):
            continue
        filtered.append((dte, abs(strike / spot - 1.15), row))
    filtered.sort(key=lambda item: (item[0], item[1]))
    narrowed = [row for _, _, row in filtered[:120]]
    if not narrowed:
        return None, day_pct, day_known

    call_codes = [str(row.get("code", "")) for row in narrowed if row.get("code")]
    snapshots: List[Dict] = []
    for group in chunked(call_codes, 180):
        ret, snap = quote_ctx.get_market_snapshot(group)
        if ret == RET_OK and snap is not None and len(snap) > 0:
            snapshots.extend(snap.to_dict(orient="records"))
    if not snapshots:
        return None, day_pct, day_known

    snapshot_map = {str(row.get("code", "")): row for row in snapshots}
    best = None
    for chain_row in narrowed:
        code = str(chain_row.get("code", ""))
        snap_row = snapshot_map.get(code)
        if not snap_row:
            continue
        strike = float(chain_row.get("strike_price", 0) or 0)
        strike_time = str(chain_row.get("strike_time", ""))
        expiry = datetime.strptime(strike_time, "%Y-%m-%d").date()
        dte = (expiry - now).days

        bid = float(snap_row.get("bid_price", 0) or 0)
        ask = float(snap_row.get("ask_price", 0) or 0)
        oi = float(snap_row.get("option_open_interest", 0) or 0)
        iv = float(snap_row.get("option_implied_volatility", 0) or 0)
        delta_quote = float(snap_row.get("option_delta", 0) or 0)

        if oi < 10 or bid < 0 or ask < 0:
            continue
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else max(bid, ask)
        if mid <= 0.03:
            continue

        sigma = iv if iv > 0.01 else 0.60
        delta_est = call_delta_estimate(spot, strike, max(dte, 1) / 365.0, sigma)
        delta = delta_quote if 0.01 <= abs(delta_quote) <= 0.99 else delta_est
        if not (0.10 <= delta <= 0.35):
            continue

        score = abs(delta - target_delta) + (0.002 * dte)
        candidate = {
            "expiry": strike_time,
            "dte": dte,
            "strike": strike,
            "delta": delta,
            "mid": mid,
            "oi": oi,
            "score": score,
        }
        if best is None or candidate["score"] < best["score"]:
            best = candidate
    return best, (day_pct if day_known else 0.0), day_known


def format_strike(value: float) -> str:
    if value >= 20:
        return f"{value:.0f}"
    if value >= 2:
        return f"{value:.1f}".rstrip("0").rstrip(".")
    return f"{value:.2f}".rstrip("0").rstrip(".")


def estimate_usd_to_base(rows: List[Dict], account_info: Dict, base_ccy: str) -> float:
    if base_ccy != "HKD":
        return 1.0
    long_mv_base = float(account_info.get("long_mv", 0) or 0)
    hk_long_local = sum(
        float(row.get("market_val", 0) or 0)
        for row in rows
        if str(row.get("position_side", "")).upper() == "LONG" and str(row.get("position_market", "")).upper() == "HK"
    )
    us_long_local = sum(
        float(row.get("market_val", 0) or 0)
        for row in rows
        if str(row.get("position_side", "")).upper() == "LONG" and str(row.get("position_market", "")).upper() == "US"
    )
    if us_long_local <= 0:
        return 7.8
    est = (long_mv_base - hk_long_local) / us_long_local
    return est if est > 0 else 7.8


def to_base_value(row: Dict, usd_to_base: float, base_ccy: str) -> float:
    market_value = float(row.get("market_val", 0) or 0)
    currency = str(row.get("currency", "")).upper()
    if base_ccy == "HKD":
        if currency == "HKD":
            return market_value
        if currency == "USD":
            return market_value * usd_to_base
    if base_ccy == "USD":
        if currency == "USD":
            return market_value
        if currency == "HKD":
            return market_value / max(usd_to_base, 1e-6)
    return market_value


def today_change_pct(row: Dict) -> float:
    market_value = float(row.get("market_val", 0) or 0)
    today_pl = float(row.get("today_pl_val", 0) or 0)
    prev_market_value = market_value - today_pl
    if abs(prev_market_value) < 1e-9:
        return 0.0
    return today_pl / prev_market_value * 100.0


def is_focus_row(row: Dict, focus_symbols: List[str]) -> bool:
    return to_underlying(str(row.get("code", ""))) in set(focus_symbols)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build concise portfolio report with action suggestions")
    parser.add_argument("--project-dir", default=".")
    parser.add_argument("--positions-json", default="")
    parser.add_argument("--strategy-dir", default="strategies")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()

    project_dir = Path(args.project_dir).resolve()
    strategy_dir = (project_dir / args.strategy_dir).resolve()
    report_dir = (project_dir / args.report_dir).resolve()
    data_dir = project_dir / "data"

    report_settings = load_report_settings(project_dir)
    positions_path = Path(args.positions_json).resolve() if args.positions_json else find_latest_positions(data_dir)
    payload = json.loads(positions_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    account_info = payload.get("account_info", {}) or {}
    strategies = load_strategy_map(strategy_dir)
    now = datetime.now()

    equity_longs = []
    for row in rows:
        code = normalize_code(str(row.get("code", "")))
        market_value = float(row.get("market_val", 0) or 0)
        if not code or OPTION_CODE_RE.match(code) or market_value <= 0:
            continue
        equity_longs.append(row)
    equity_longs.sort(key=lambda row: float(row.get("market_val", 0) or 0), reverse=True)

    base_ccy = str(account_info.get("currency", "HKD") or "HKD").upper()
    usd_to_base = estimate_usd_to_base(rows, account_info, base_ccy)
    total_assets_base = float(account_info.get("total_assets", 0) or 0)
    if total_assets_base <= 0:
        total_assets_base = sum(to_base_value(row, usd_to_base, base_ccy) for row in equity_longs) or 1.0

    focus_symbols = report_settings["focus_symbols"]
    focus_equity_longs = [row for row in equity_longs if is_focus_row(row, focus_symbols)]
    if not focus_equity_longs:
        focus_equity_longs = equity_longs[: min(5, len(equity_longs))]

    option_rows = []
    for row in rows:
        expiry = parse_expiry(str(row.get("code", "")))
        if expiry is None:
            continue
        dte = (expiry.date() - now.date()).days
        if dte <= report_settings["option_dte_max"] and (float(row.get("qty", 0) or 0) < 0 or is_focus_row(row, focus_symbols)):
            option_rows.append((dte, row))
    option_rows.sort(key=lambda item: item[0])

    load_dotenv(project_dir / ".env")
    host = os.getenv("FUTU_HOST", "127.0.0.1")
    port = int(os.getenv("FUTU_PORT", "11111"))
    quote_ctx = connect_quote_context(host, port)
    market_states = get_market_states(quote_ctx)

    candidates = []
    for row in focus_equity_longs:
        ticker = to_underlying(str(row.get("code", "")))
        market = str(row.get("position_market", "")).upper()
        qty = float(row.get("qty", 0) or 0)
        pl_ratio = float(row.get("pl_ratio", 0) or 0)
        weight = to_base_value(row, usd_to_base, base_ccy) / total_assets_base * 100.0
        score = weight + max(0.0, -pl_ratio * 0.35)
        candidates.append((score, ticker, market, qty, row))
    candidates.sort(reverse=True, key=lambda item: item[0])

    action_lines: List[str] = []
    used = set()
    for _, ticker, market, qty, row in candidates:
        if ticker in used or market != "US" or qty < 100:
            continue
        owner_code = str(row.get("code", ""))
        owner_code = owner_code if "." in owner_code else f"US.{ticker}"
        rec, day_pct, day_known = choose_call_candidate(
            quote_ctx=quote_ctx,
            owner_code=owner_code,
            fallback_spot=float(row.get("nominal_price", 0) or 0),
            min_dte=report_settings["covered_call_min_dte"],
            max_dte=report_settings["option_dte_max"],
            target_delta=report_settings["covered_call_target_delta"],
        )
        if rec:
            stock_name = str(row.get("stock_name", "")).strip() or ticker
            currency = str(row.get("currency", "")).strip() or "USD"
            day_text = f"{day_pct:+.2f}%" if day_known else "N/A(no live quote entitlement)"
            action_lines.append(
                f"- {ticker} ({stock_name}, {currency}): if day gain reaches +3%, consider selling {rec['expiry']} {format_strike(rec['strike'])}C, est delta={rec['delta']:.2f}, mid premium about {rec['mid']:.2f}. Current day move={day_text}."
            )
            used.add(ticker)
        if len(action_lines) >= 4:
            break

    if quote_ctx is not None:
        quote_ctx.close()

    nonzero_today_rows = sum(1 for row in rows if abs(float(row.get("today_pl_val", 0) or 0)) > 1e-9)

    lines: List[str] = []
    lines.append(f"# Portfolio Risk Briefing - {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- Positions source: `{positions_path.name}`")
    lines.append(f"- Position rows: {len(rows)}")
    lines.append(f"- Strategy files loaded: {len(strategies)}")
    lines.append(f"- Account base: {base_ccy} | total_assets={total_assets_base:.2f} | usd_to_{base_ccy}={usd_to_base:.4f}")
    lines.append(
        f"- Market state: US={format_market_state(market_states['market_us'])} | HK={format_market_state(market_states['market_hk'])} | OpenD={market_states['program_status_type']}"
    )
    lines.append(f"- Focus layer: {', '.join(focus_symbols)}")
    lines.append(f"- Today fields updated on {nonzero_today_rows} rows.")
    lines.append("")

    lines.append("## Focus Holdings")
    for row in focus_equity_longs[:8]:
        code = normalize_code(str(row.get("code", "")))
        name = str(row.get("stock_name", "")).strip() or code
        currency = str(row.get("currency", "")).strip() or "N/A"
        weight = to_base_value(row, usd_to_base, base_ccy) / total_assets_base * 100.0
        total_pl = float(row.get("pl_ratio", 0) or 0)
        today_pl = float(row.get("today_pl_val", 0) or 0)
        day_pct = today_change_pct(row)
        lines.append(
            f"- {code} {name} | {currency} | weight={weight:.2f}% | today={today_pl:+.2f} ({day_pct:+.2f}%) | total_pl={total_pl:.2f}%"
        )
    lines.append("")

    lines.append("## Today Actions")
    if action_lines:
        lines.extend(action_lines)
    else:
        lines.append("- No covered-call setup passed the liquidity and delta filters. Prioritize expiry management and risk reduction today.")
    lines.append("")

    lines.append(f"## Option Calendar (<= {report_settings['option_dte_max']}D)")
    if not option_rows:
        lines.append("- None.")
    else:
        for dte, row in option_rows[:14]:
            code = normalize_code(str(row.get("code", "")))
            qty = float(row.get("qty", 0) or 0)
            pl_ratio = float(row.get("pl_ratio", 0) or 0)
            currency = str(row.get("currency", "")).strip() or "N/A"
            lines.append(f"- {code} | qty={qty:g} | dte={dte} | pl={pl_ratio:.2f}% | {currency}")
    lines.append("")

    missing = sorted({to_underlying(str(row.get("code", ""))) for row in focus_equity_longs} - set(strategies.keys()))
    lines.append("## Strategy Coverage")
    lines.append("- Missing strategy files: " + (", ".join(missing) if missing else "none"))

    report_dir.mkdir(parents=True, exist_ok=True)
    output_path = report_dir / f"portfolio_risk_briefing_{now.strftime('%Y%m%d_%H%M%S')}.md"
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    print(f"[OK] report={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

