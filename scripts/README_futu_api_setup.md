# Futu API Read-Only Setup

This setup only reads account positions. It does not place orders.

## 1) Start OpenD

1. Open Futu OpenD on your computer.
2. In OpenD settings:
   - Enable API access.
   - Confirm the listening host/port (default `127.0.0.1:11111`).
3. Log in to your Futu account in the linked client.

## 2) Configure env

1. Copy `.env.example` to `.env`.
2. Edit values if needed:
   - `FUTU_HOST`
   - `FUTU_PORT`
   - `FUTU_MARKET` (for example: `US`, `HK`)
   - `FUTU_TRD_ENV` (`REAL` or `SIMULATE`)
   - optional `FUTU_ACC_ID`

## 3) Fetch positions (read-only)

```powershell
python ".\futu_read_positions.py" --project-dir "."
```

Optional CSV export:

```powershell
python ".\futu_read_positions.py" --project-dir "." --output-csv "positions.latest.csv"
```

## 4) Output files

- `positions.latest.json`
- optional `positions.latest.csv`

## Notes

- If OpenD is not running or API access is disabled, the script will fail with connection errors.
- No trading endpoint is used in this script.

