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

