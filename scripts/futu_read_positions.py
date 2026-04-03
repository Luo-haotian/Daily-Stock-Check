import argparse
import json
import os
import socket
from pathlib import Path

from dotenv import load_dotenv
from futu import OpenSecTradeContext, RET_OK, TrdEnv, TrdMarket


def parse_market(value: str) -> TrdMarket:
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
        raise ValueError(f"Unsupported FUTU_MARKET: {value}")
    return mapping[v]


def parse_env(value: str) -> TrdEnv:
    v = (value or "REAL").strip().upper()
    if v == "REAL":
        return TrdEnv.REAL
    if v in {"SIMULATE", "SIM"}:
        return TrdEnv.SIMULATE
    raise ValueError(f"Unsupported FUTU_TRD_ENV: {value}")


def dump_json(data, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Futu positions fetcher")
    parser.add_argument("--output-json", default="data/positions.latest.json")
    parser.add_argument("--output-csv", default="")
    parser.add_argument("--project-dir", default=".")
    parser.add_argument("--refresh-cache", action="store_true")
    args = parser.parse_args()

    project_dir = Path(args.project_dir).resolve()
    load_dotenv(project_dir / ".env")

    host = os.getenv("FUTU_HOST", "127.0.0.1")
    port = int(os.getenv("FUTU_PORT", "11111"))
    market = parse_market(os.getenv("FUTU_MARKET", "US"))
    trd_env = parse_env(os.getenv("FUTU_TRD_ENV", "REAL"))
    acc_id_raw = (os.getenv("FUTU_ACC_ID", "") or "").strip()
    acc_id = int(acc_id_raw) if acc_id_raw else 0

    print(f"[INFO] Connecting OpenD host={host} port={port} market={market} env={trd_env}")

    # Fast fail when OpenD is not listening.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2.0)
        if sock.connect_ex((host, port)) != 0:
            print(f"[ERROR] Cannot connect to OpenD at {host}:{port}.")
            print("[HINT] Start OpenD, enable API access, and keep the configured port open.")
            return 1

    ctx = OpenSecTradeContext(filter_trdmarket=market, host=host, port=port)
    try:
        ret, acc_df = ctx.get_acc_list()
        if ret != RET_OK:
            print(f"[ERROR] get_acc_list failed: {acc_df}")
            return 2

        if acc_df is not None and len(acc_df) > 0:
            print("[INFO] Accounts visible:")
            for _, row in acc_df.iterrows():
                print(
                    f"  - acc_id={row.get('acc_id')} trd_env={row.get('trd_env')} "
                    f"trd_market={row.get('trd_market')} sim={row.get('sim_acc_type')}"
                )

        ret, pos_df = ctx.position_list_query(
            code="",
            trd_env=trd_env,
            acc_id=acc_id,
            refresh_cache=bool(args.refresh_cache),
        )
        if ret != RET_OK:
            print(f"[ERROR] position_list_query failed: {pos_df}")
            return 3

        ret, info_df = ctx.accinfo_query(
            trd_env=trd_env,
            acc_id=acc_id,
            refresh_cache=bool(args.refresh_cache),
        )
        account_info = {}
        if ret == RET_OK and info_df is not None and len(info_df) > 0:
            row = info_df.iloc[0].to_dict()
            account_info = {
                "currency": row.get("currency"),
                "total_assets": row.get("total_assets"),
                "securities_assets": row.get("securities_assets"),
                "market_val": row.get("market_val"),
                "long_mv": row.get("long_mv"),
                "short_mv": row.get("short_mv"),
                "cash": row.get("cash"),
                "available_funds": row.get("available_funds"),
                "risk_status": row.get("risk_status"),
            }

        out_json = project_dir / args.output_json
        payload = {
            "source": "futu_api",
            "host": host,
            "port": port,
            "market": str(market),
            "trd_env": str(trd_env),
            "acc_id": acc_id,
            "account_info": account_info,
            "rows": [],
        }
        if pos_df is not None and len(pos_df) > 0:
            payload["rows"] = json.loads(pos_df.to_json(orient="records", force_ascii=False))
        dump_json(payload, out_json)
        print(f"[OK] Wrote JSON: {out_json}")
        print(f"[OK] Position rows: {len(payload['rows'])}")

        if args.output_csv:
            out_csv = project_dir / args.output_csv
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            if pos_df is None:
                out_csv.write_text("", encoding="utf-8-sig")
            else:
                pos_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"[OK] Wrote CSV: {out_csv}")

        print("[SAFE] This script is read-only and does not place orders.")
        return 0
    finally:
        ctx.close()


if __name__ == "__main__":
    raise SystemExit(main())
