# Daily-Stock-Check

Read-only portfolio risk workflow:

- Pull positions from Futu OpenAPI
- Load per-symbol strategy notes
- Generate concise actionable report

## Run

```powershell
powershell -ExecutionPolicy Bypass -File .\run_briefing.ps1
```

## Safety

This project is read-only by design. No trading endpoints are called.

## V0.11

- Added project-level `report_settings.json` for focus-layer symbols and option scan settings.
- Added US/HK market-state summary from OpenD global state.
- Shrunk the report around the focus layer instead of the full book.
- Kept daily move in `Focus Holdings` based on Futu position fields.
