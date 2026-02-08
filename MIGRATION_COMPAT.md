# Rename Migration Compatibility (3 days)

## New canonical names
1. Project folder: `/Users/yuhan/workspace/quant/data/quantclass-sync`
2. Main script: `quantclass_sync.py`

## Temporary compatibility bridge
1. Old folder path `/Users/yuhan/workspace/quant/data/scripts` is a symlink to `quantclass-sync`.
2. Old script name `quantclass_daily_sync.py` forwards to `quantclass_sync.py`.

## Compatibility retention
1. Keep bridge until: `2026-02-11 23:59` (local time).
2. After that, remove old entry points and use only canonical names.

## Planned cleanup commands
```bash
rm -f /Users/yuhan/workspace/quant/data/scripts
rm -f /Users/yuhan/workspace/quant/data/quantclass-sync/quantclass_daily_sync.py
```
