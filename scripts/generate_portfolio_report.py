import argparse
import json
import math
import os
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from futu import OpenQuoteContext, OptionType, RET_OK
import pandas as pd


OPTION_CODE_RE = re.compile(r"^[A-Z]{1,8}\d{6}[CP]\d+$")


def find_latest_positions(data_dir: Path) -> Path:
    files = sorted([p for p in data_dir.glob("*.json") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files:
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, dict) and isinstance(obj.get("rows"), list):
                return p
        except Exception:
            continue
    raise FileNotFoundError(f"No positions json with rows found under {data_dir}")


def normalize_code(code: str) -> str:
    c = (code or "").upper()
    if "." in c:
        c = c.split(".", 1)[1]
    return c


def to_underlying(code: str) -> str:
    c = normalize_code(code)
    if OPTION_CODE_RE.match(c):
        for i, ch in enumerate(c):
            if ch.isdigit():
                return c[:i]
    return c


def load_strategy_map(strategy_dir: Path) -> Dict[str, Dict[str, str]]:
    strategy_map: Dict[str, Dict[str, str]] = {}
    for p in sorted(strategy_dir.glob("*.md")):
        text = p.read_text(encoding="utf-8-sig")
        ticker = p.stem.upper().split("_")[0].split("-")[0].split(" ")[0]
        summary_lines: List[str] = []
        for line in text.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            summary_lines.append(s)
            if len(summary_lines) >= 2:
                break
        strategy_map[ticker] = {"file": p.name, "summary": " ".join(summary_lines) if summary_lines else "(no summary)"}
    return strategy_map


def parse_expiry(code: str) -> Optional[datetime]:
    c = normalize_code(code)
    m = re.match(r"^[A-Z]{1,8}(\d{6})[CP]\d+$", c)
    if not m:
        return None
    yymmdd = m.group(1)
    yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
    return datetime(year=2000 + yy, month=mm, day=dd)


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def call_delta_estimate(s: float, k: float, t_years: float, sigma: float, r: float = 0.04) -> float:
    if s <= 0 or k <= 0 or t_years <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t_years) / (sigma * math.sqrt(t_years))
    return max(0.0, min(1.0, norm_cdf(d1)))


def chunked(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def get_underlying_snapshot(quote_ctx: OpenQuoteContext, code: str) -> Tuple[float, float, bool]:
    ret, snap = quote_ctx.get_market_snapshot([code])
    if ret != RET_OK or snap is None or len(snap) == 0:
        return 0.0, 0.0, False
    row = snap.iloc[0]
    last = float(row.get("last_price", 0) or 0)
    prev = float(row.get("prev_close_price", 0) or 0)
    pct = ((last - prev) / prev * 100.0) if prev > 0 else 0.0
    return last, pct, True


def fetch_call_chain_90d(quote_ctx: OpenQuoteContext, owner_code: str, min_dte: int = 7, max_dte: int = 90):
    now = datetime.now().date()
    chain_parts = []
    for start_day in range(min_dte, max_dte + 1, 30):
        start = (now.fromordinal(now.toordinal() + start_day)).strftime("%Y-%m-%d")
        end_day = min(start_day + 29, max_dte)
        end = (now.fromordinal(now.toordinal() + end_day)).strftime("%Y-%m-%d")
        ret, df = quote_ctx.get_option_chain(code=owner_code, start=start, end=end, option_type=OptionType.CALL)
        if ret == RET_OK and df is not None and len(df) > 0:
            chain_parts.append(df)
    if not chain_parts:
        return []
    chain_df = pd.concat(chain_parts, ignore_index=True)
    chain_df = chain_df.drop_duplicates(subset=["code"])
    return chain_df.to_dict(orient="records")


def choose_call_candidate_futu(
    quote_ctx: OpenQuoteContext,
    owner_code: str,
    fallback_spot: float = 0.0,
    min_dte: int = 7,
    max_dte: int = 90,
    target_delta: float = 0.20,
):
    calls = fetch_call_chain_90d(quote_ctx, owner_code, min_dte=min_dte, max_dte=max_dte)
    if not calls:
        return None, 0.0

    spot, day_pct, day_known = get_underlying_snapshot(quote_ctx, owner_code)
    if spot <= 0 and fallback_spot > 0:
        spot = fallback_spot
    if spot <= 0:
        return None, day_pct, day_known

    # Keep only practical strike zone and limit code volume to avoid slow snapshot calls.
    filtered_calls = []
    for x in calls:
        strike = float(x.get("strike_price", 0) or 0)
        exp_str = str(x.get("strike_time", ""))
        if strike <= 0 or not exp_str:
            continue
        exp = datetime.strptime(exp_str, "%Y-%m-%d")
        dte = (exp.date() - datetime.now().date()).days
        if not (min_dte <= dte <= max_dte):
            continue
        if not (spot * 1.02 <= strike <= spot * 1.40):
            continue
        filtered_calls.append((dte, abs(strike / spot - 1.15), x))
    filtered_calls.sort(key=lambda t: (t[0], t[1]))
    narrowed = [x for _, _, x in filtered_calls[:120]]
    if not narrowed:
        return None, (day_pct if day_known else 0.0), day_known

    call_codes = [str(x.get("code", "")) for x in narrowed if x.get("code")]
    snap_rows = []
    for group in chunked(call_codes, 180):
        ret, snap = quote_ctx.get_market_snapshot(group)
        if ret == RET_OK and snap is not None and len(snap) > 0:
            snap_rows.extend(snap.to_dict(orient="records"))
    if not snap_rows:
        return None, (day_pct if day_known else 0.0), day_known

    snap_map = {str(x.get("code", "")): x for x in snap_rows}
    now = datetime.now()

    best = None
    for c in narrowed:
        code = str(c.get("code", ""))
        snap = snap_map.get(code)
        if not snap:
            continue
        strike = float(c.get("strike_price", 0) or 0)
        exp_str = str(c.get("strike_time", ""))
        if not exp_str or strike <= 0:
            continue
        exp = datetime.strptime(exp_str, "%Y-%m-%d")
        dte = (exp.date() - now.date()).days
        if dte < min_dte or dte > max_dte:
            continue

        bid = float(snap.get("bid_price", 0) or 0)
        ask = float(snap.get("ask_price", 0) or 0)
        oi = float(snap.get("option_open_interest", 0) or 0)
        iv = float(snap.get("option_implied_volatility", 0) or 0)
        delta_quote = float(snap.get("option_delta", 0) or 0)

        if oi < 10:
            continue
        if bid < 0 or ask < 0:
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
        rec = {
            "expiry": exp_str,
            "dte": dte,
            "strike": strike,
            "delta": delta,
            "mid": mid,
            "oi": oi,
            "score": score,
        }
        if best is None or rec["score"] < best["score"]:
            best = rec
    return best, (day_pct if day_known else 0.0), day_known


def format_strike(v: float) -> str:
    if v >= 20:
        return f"{v:.0f}"
    if v >= 2:
        return f"{v:.1f}".rstrip("0").rstrip(".")
    return f"{v:.2f}".rstrip("0").rstrip(".")


def estimate_usd_to_base(rows: List[Dict], account_info: Dict, base_ccy: str) -> float:
    if base_ccy != "HKD":
        return 1.0
    long_mv_base = float(account_info.get("long_mv", 0) or 0)
    hk_long_local = sum(
        float(r.get("market_val", 0) or 0)
        for r in rows
        if str(r.get("position_side", "")).upper() == "LONG" and str(r.get("position_market", "")).upper() == "HK"
    )
    us_long_local = sum(
        float(r.get("market_val", 0) or 0)
        for r in rows
        if str(r.get("position_side", "")).upper() == "LONG" and str(r.get("position_market", "")).upper() == "US"
    )
    if us_long_local <= 0:
        return 7.8
    est = (long_mv_base - hk_long_local) / us_long_local
    if est <= 0:
        return 7.8
    return est


def to_base_value(row: Dict, usd_to_base: float, base_ccy: str) -> float:
    mv = float(row.get("market_val", 0) or 0)
    ccy = str(row.get("currency", "")).upper()
    if base_ccy == "HKD":
        if ccy == "HKD":
            return mv
        if ccy == "USD":
            return mv * usd_to_base
    if base_ccy == "USD":
        if ccy == "USD":
            return mv
        if ccy == "HKD":
            return mv / max(usd_to_base, 1e-6)
    return mv


def today_change_pct(row: Dict) -> float:
    mv = float(row.get("market_val", 0) or 0)
    today_pl = float(row.get("today_pl_val", 0) or 0)
    prev_mv = mv - today_pl
    if abs(prev_mv) < 1e-9:
        return 0.0
    return today_pl / prev_mv * 100.0


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
    data_dir = (project_dir / "data").resolve()

    positions_path = Path(args.positions_json).resolve() if args.positions_json else find_latest_positions(data_dir)
    payload = json.loads(positions_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    account_info = payload.get("account_info", {}) or {}
    strategies = load_strategy_map(strategy_dir)
    now = datetime.now()

    # Keep only equity long legs for concise exposure view.
    equity_longs = []
    for r in rows:
        code = normalize_code(str(r.get("code", "")))
        if not code or OPTION_CODE_RE.match(code):
            continue
        mv = float(r.get("market_val", 0) or 0)
        if mv <= 0:
            continue
        equity_longs.append(r)
    equity_longs.sort(key=lambda x: float(x.get("market_val", 0) or 0), reverse=True)
    base_ccy = str(account_info.get("currency", "HKD") or "HKD").upper()
    total_assets_base = float(account_info.get("total_assets", 0) or 0)
    usd_to_base = estimate_usd_to_base(rows, account_info, base_ccy)
    if total_assets_base <= 0:
        total_assets_base = sum(to_base_value(r, usd_to_base, base_ccy) for r in equity_longs) or 1.0

    # Option positions within 90 days.
    option_90d = []
    for r in rows:
        code = str(r.get("code", ""))
        exp = parse_expiry(code)
        if exp is None:
            continue
        dte = (exp.date() - now.date()).days
        if dte <= 90:
            option_90d.append((dte, r))
    option_90d.sort(key=lambda x: x[0])

    # Build "today action" candidates from risk + concentration.
    candidates = []
    for r in equity_longs:
        ticker = to_underlying(str(r.get("code", "")))
        market = str(r.get("position_market", "")).upper()
        qty = float(r.get("qty", 0) or 0)
        pl = float(r.get("pl_ratio", 0) or 0)
        mv = float(r.get("market_val", 0) or 0)
        weight = to_base_value(r, usd_to_base, base_ccy) / total_assets_base * 100.0
        score = weight + max(0.0, -pl * 0.35)
        candidates.append((score, ticker, market, qty, r))
    candidates.sort(reverse=True, key=lambda x: x[0])

    action_lines: List[str] = []
    used = set()
    load_dotenv(project_dir / ".env")
    host = os.getenv("FUTU_HOST", "127.0.0.1")
    port = int(os.getenv("FUTU_PORT", "11111"))
    quote_ctx = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2.0)
        if sock.connect_ex((host, port)) == 0:
            quote_ctx = OpenQuoteContext(host=host, port=port)

    for _, ticker, market, qty, r in candidates:
        if ticker in used:
            continue
        if market != "US" or qty < 100:
            continue
        rec = None
        day_pct = 0.0
        day_known = False
        if quote_ctx is not None:
            owner_code = str(r.get("code", ""))
            owner_code = owner_code if "." in owner_code else f"US.{ticker}"
            try:
                rec, day_pct, day_known = choose_call_candidate_futu(
                    quote_ctx=quote_ctx,
                    owner_code=owner_code,
                    fallback_spot=float(r.get("nominal_price", 0) or 0),
                    min_dte=7,
                    max_dte=90,
                    target_delta=0.20,
                )
            except Exception:
                rec = None
                day_pct = 0.0
                day_known = False
        if rec:
            stock_name = str(r.get("stock_name", "")).strip() or ticker
            ccy = str(r.get("currency", "")).strip() or "USD"
            day_txt = f"{day_pct:+.2f}%" if day_known else "N/A(无行情权限)"
            action_lines.append(
                f"- {ticker} ({stock_name}, {ccy}): 若当日涨幅>=+3%，可考虑卖出 {rec['expiry']} {format_strike(rec['strike'])}C，估算delta={rec['delta']:.2f}，中间价约={rec['mid']:.2f}。当前日内涨跌={day_txt}。"
            )
            used.add(ticker)
        if len(action_lines) >= 4:
            break
    if quote_ctx is not None:
        quote_ctx.close()

    lines: List[str] = []
    lines.append(f"# Portfolio Risk Briefing - {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- Positions source: `{positions_path.name}`")
    lines.append(f"- Position rows: {len(rows)}")
    lines.append(f"- Strategy files loaded: {len(strategies)}")
    lines.append(
        f"- Account base: {base_ccy} | total_assets={total_assets_base:.2f} | usd_to_{base_ccy}={usd_to_base:.4f}"
    )
    lines.append("")

    lines.append("## Key Holdings (Concise)")
    for r in equity_longs[:8]:
        code_raw = str(r.get("code", ""))
        code = normalize_code(code_raw)
        name = str(r.get("stock_name", "")).strip() or str(r.get("stock_name", "")).strip()
        ccy = str(r.get("currency", "")).strip() or "N/A"
        w = to_base_value(r, usd_to_base, base_ccy) / total_assets_base * 100.0
        pl = float(r.get("pl_ratio", 0) or 0)
        day_pl = float(r.get("today_pl_val", 0) or 0)
        day_pct = today_change_pct(r)
        lines.append(
            f"- {code} {name} | {ccy} | weight={w:.2f}% | today={day_pl:+.2f} ({day_pct:+.2f}%) | total_pl={pl:.2f}%"
        )
    lines.append("")

    lines.append("## 今日操作建议")
    if action_lines:
        lines.extend(action_lines)
    else:
        lines.append("- 未找到满足流动性与delta范围的建议call；今日优先做风险减仓与到期管理。")
    lines.append("")

    lines.append("## 期权到期日历 (<= 90天)")
    if not option_90d:
        lines.append("- 无。")
    else:
        for dte, r in option_90d[:14]:
            code = normalize_code(str(r.get("code", "")))
            qty = float(r.get("qty", 0) or 0)
            pl = float(r.get("pl_ratio", 0) or 0)
            ccy = str(r.get("currency", "")).strip() or "N/A"
            lines.append(f"- {code} | qty={qty:g} | dte={dte} | pl={pl:.2f}% | {ccy}")
    lines.append("")

    missing = sorted({to_underlying(str(r.get("code", ""))) for r in equity_longs} - set(strategies.keys()))
    lines.append("## Strategy Coverage")
    lines.append("- Missing strategy files: " + (", ".join(missing) if missing else "none"))

    report_dir.mkdir(parents=True, exist_ok=True)
    out = report_dir / f"portfolio_risk_briefing_{now.strftime('%Y%m%d_%H%M%S')}.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    print(f"[OK] report={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
