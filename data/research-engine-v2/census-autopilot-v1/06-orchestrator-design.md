# Orchestrator Design

**Entry:** `lib/research-engine-v2/census-autopilot-v1/orchestrator.js`  
**CLI:** `scripts/census-autopilot-v1.mjs` → `npm run census:autopilot-v1`

## Options

| Flag | Purpose |
|------|---------|
| `--mode` | discovery | reconstruction | full_record | freshness | reconciliation | activation | image_integrity | escalation | unified_benchmark |
| `--group` | Parent families (e.g. IHG,Hilton,Choice) |
| `--brand` | Single brand filter |
| `--country` | Country (default Mexico for benchmark) |
| `--region` | Reserved |
| `--priority` | P0,P1 filter |
| `--max-records` | Cap |
| `--dry-run` | Default true; V1 never writes |
| `--resume` | Run ID |

## Modes

Modes are thin routers over existing RE2 surfaces — see `02-mode-registry.json`.

## Stopping rules

Per field: stop on High/Exact authoritative evidence.  
Per hotel: max field attempts / Lane B attempts / escalations — then escalate remainder.
