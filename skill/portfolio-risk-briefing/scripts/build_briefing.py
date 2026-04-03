import argparse
import json
import os
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from futu import OpenSecTradeContext, RET_OK, TrdEnv, TrdMarket


OPTION_RE = re.compile(r"^[A-Z]{1,8}\d{6}[CP]\d+$")


def parse_market(value: str):
    v = (value or "US").strip().upper()
    mapping = {
        "US": TrdMarket.US,
        "HK": TrdMarket.HK,
        "HKCC": TrdMarket.HKCC,
        "CN": TrdMarket.CN,
        "SG": TrdMarket.SG,
        "JP": TrdMarket.JP,
    }
    if v not in mapping:
        raise ValueError(f"Unsupported FUTU_MARKET={value}")
    return mapping[v]


def parse_env(value: str):
    v = (value or "REAL").strip().upper()
    if v == "REAL":
        return TrdEnv.REAL
    if v in {"SIM", "SIMULATE"}:
        return TrdEnv.SIMULATE
    raise ValueError(f"Unsupported FUTU_TRD_ENV={value}")


def normalize_code(code: str) -> str:
    c = (code or "").upper()
    if "." in c:
        c = c.split(".", 1)[1]
    return c


def to_underlying(code: str) -> str:
    c = normalize_code(code)
    if OPTION_RE.match(c):
        for i, ch in enumerate(c):
            if ch.isdigit():
                return c[:i]
    return c


def parse_expiry(code: str):
    c = normalize_code(code)
    m = re.match(r"^[A-Z]{1,8}(\d{6})[CP]\d+$", c)
    if not m:
        return None
    yymmdd = m.group(1)
    yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
    return datetime(year=2000 + yy, month=mm, day=dd)


def load_strategies(strategy_dir: Path) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not strategy_dir.exists():
        return out
    for fp in sorted(strategy_dir.glob("*.md")):
        text = fp.read_text(encoding="utf-8-sig")
        ticker = fp.stem.upper().split("_")[0].split("-")[0].split(" ")[0]
        summary: List[str] = []
        for line in text.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            summary.append(s)
            if len(summary) >= 2:
                break
        out[ticker] = {"file": fp.name, "summary": " ".join(summary) if summary else "(empty strategy)"}
    return out


def fetch_positions(project_dir: Path, snapshot_dir: Path, refresh_cache: bool) -> Path:
    load_dotenv(project_dir / ".env")
    host = os.getenv("FUTU_HOST", "127.0.0.1")
    port = int(os.getenv("FUTU_PORT", "11111"))
    market = parse_market(os.getenv("FUTU_MARKET", "US"))
    trd_env = parse_env(os.getenv("FUTU_TRD_ENV", "REAL"))
    acc_id_raw = (os.getenv("FUTU_ACC_ID", "") or "").strip()
    acc_id = int(acc_id_raw) if acc_id_raw else 0

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2.0)
        if sock.connect_ex((host, port)) != 0:
            raise RuntimeError(f"Cannot connect to OpenD {host}:{port}")

    ctx = OpenSecTradeContext(filter_trdmarket=market, host=host, port=port)
    try:
        ret, pos_df = ctx.position_list_query(code="", trd_env=trd_env, acc_id=acc_id, refresh_cache=refresh_cache)
        if ret != RET_OK:
            raise RuntimeError(f"position_list_query failed: {pos_df}")
    finally:
        ctx.close()

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = snapshot_dir / f"positions_snapshot_{ts}.json"
    payload = {
        "source": "futu_api",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "market": str(market),
        "trd_env": str(trd_env),
        "acc_id": acc_id,
        "rows": json.loads(pos_df.to_json(orient="records", force_ascii=False)) if pos_df is not None else [],
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def build_report(snapshot_path: Path, strategy_dir: Path, report_dir: Path) -> Path:
    data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    rows = data.get("rows", [])
    strategies = load_strategies(strategy_dir)
    now = datetime.now()

    long_rows = [r for r in rows if float(r.get("market_val", 0) or 0) > 0]
    long_rows.sort(key=lambda x: float(x.get("market_val", 0) or 0), reverse=True)
    total_mv = sum(float(r.get("market_val", 0) or 0) for r in long_rows) or 1.0

    dd_rows = [r for r in long_rows if float(r.get("pl_ratio", 0) or 0) <= -20]
    dd_rows.sort(key=lambda x: float(x.get("pl_ratio", 0) or 0))

    exp_rows = []
    for r in rows:
        exp = parse_expiry(str(r.get("code", "")))
        if exp is None:
            continue
        dte = (exp.date() - now.date()).days
        if dte <= 21:
            exp_rows.append((dte, r))
    exp_rows.sort(key=lambda x: x[0])

    lines: List[str] = []
    lines.append(f"# Portfolio Risk Briefing - {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append("")
    lines.append("## Input")
    lines.append(f"- Snapshot: `{snapshot_path.name}`")
    lines.append(f"- Positions: {len(rows)}")
    lines.append(f"- Strategy files: {len(strategies)}")
    lines.append("")
    lines.append("## Top Exposure")
    for r in long_rows[:10]:
        code = normalize_code(str(r.get("code", "")))
        ticker = to_underlying(code)
        mv = float(r.get("market_val", 0) or 0)
        pl = float(r.get("pl_ratio", 0) or 0)
        w = mv / total_mv * 100
        lines.append(f"- {ticker} | value={mv:.2f} | weight={w:.2f}% | pl={pl:.2f}%")
    lines.append("")
    lines.append("## Drawdown Focus")
    if not dd_rows:
        lines.append("- None.")
    else:
        for r in dd_rows[:10]:
            ticker = to_underlying(str(r.get("code", "")))
            pl = float(r.get("pl_ratio", 0) or 0)
            hint = strategies.get(ticker, {}).get("summary", "(strategy file missing)")
            lines.append(f"- {ticker} | pl={pl:.2f}% | action_hint={hint}")
    lines.append("")
    lines.append("## Option Expiry <= 21D")
    if not exp_rows:
        lines.append("- None.")
    else:
        for dte, r in exp_rows:
            lines.append(
                f"- {normalize_code(str(r.get('code', '')))} | qty={float(r.get('qty', 0) or 0):g} | dte={dte} | pl={float(r.get('pl_ratio', 0) or 0):.2f}%"
            )
    lines.append("")
    lines.append("## Coverage Check")
    covered = set(strategies.keys())
    long_underlyings = {to_underlying(str(r.get('code', ''))) for r in long_rows if to_underlying(str(r.get('code', '')))}
    missing = sorted(long_underlyings - covered)
    lines.append("- Missing strategy files: " + (", ".join(missing) if missing else "none"))

    report_dir.mkdir(parents=True, exist_ok=True)
    out = report_dir / f"risk_briefing_{now.strftime('%Y%m%d_%H%M%S')}.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build portfolio briefing from Futu positions and strategy folder")
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--strategy-dir", default="strategies")
    parser.add_argument("--snapshot-dir", default="data")
    parser.add_argument("--report-dir", default="reports")
    parser.add_argument("--refresh-cache", action="store_true")
    args = parser.parse_args()

    project_dir = Path(args.project_dir).resolve()
    strategy_dir = (project_dir / args.strategy_dir).resolve()
    snapshot_dir = (project_dir / args.snapshot_dir).resolve()
    report_dir = (project_dir / args.report_dir).resolve()

    snapshot = fetch_positions(project_dir=project_dir, snapshot_dir=snapshot_dir, refresh_cache=args.refresh_cache)
    report = build_report(snapshot_path=snapshot, strategy_dir=strategy_dir, report_dir=report_dir)

    print(f"[OK] snapshot={snapshot}")
    print(f"[OK] report={report}")
    print("[SAFE] Read-only. No trading endpoint is called.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

