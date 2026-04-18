---
name: portfolio-risk-briefing
description: Build a read-only portfolio risk briefing from Futu OpenAPI positions and per-symbol strategy notes stored in a project folder. Use when the user wants a daily/periodic risk report, option-expiry focus, drawdown prioritization, and strategy coverage checks without any automated trading.
---

# Portfolio Risk Briefing

Run this skill when the user wants a low-frequency, action-focused report from live positions.

## Workflow

1. Confirm the project directory that contains:
   - environment variables for OpenD access
   - a strategy-notes folder (one file per symbol)
   - a project report-settings file for focus symbols and scan parameters
   - output folders for snapshots and reports
2. Run the bundled script:
   - `python <skill_dir>/scripts/build_briefing.py --project-dir "<project_dir>"`
3. Review the generated report and highlight:
   - top exposure concentration
   - drawdown names that need action
   - option legs with near expiry
   - missing strategy coverage

## Rules

- Keep execution strictly read-only.
- Never call trading endpoints (no place/modify/cancel order).
- Prefer short-cycle, risk-first interpretation when macro or headline risk is elevated.
- Treat strategy files as project-owned assets; do not hardcode symbol lists in this skill.
- Honor the project-level report-settings file when deciding which symbols belong to the focus layer.

## Outputs

- One fresh positions snapshot in the project snapshot directory.
- One markdown risk briefing in the project report directory.
