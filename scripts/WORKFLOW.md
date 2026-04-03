# Workflow

Run the full read-only briefing workflow:

```powershell
powershell -ExecutionPolicy Bypass -File ".\run_briefing.ps1"
```

What it does:

1. Pull positions from Futu OpenAPI (read-only)
2. Save the latest snapshot under `data/`
3. Read strategy notes under `strategies/`
4. Generate a risk briefing report under `reports/`

