# Marriott Webhound Source Pattern Learning v1

**Status:** `production_census_marriott_webhound_source_pattern_learning_v1_partial_adapter_backlog_remaining`
**Objective:** `marriott-webhound-source-pattern-learning-v1`
**Census mode:** `field-completion-only` (no inserts)
**Webhound as Census SoT:** false
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Verdict

DAM factsheet adapter is **working end-to-end** on official Marriott PDFs (revalidated, not Webhound-as-SoT). Production writes remain **0** because:

1. Overview/HQV path still Akamai-blocked for Autopilot.
2. Confirmed DAM seeds (**SJULU**, **GYECY**) are **not present** in Hotel Property Census (1224 scanned).
3. Asset-id discovery for in-Census MARSHAs is still the unlock (DDG HTML probe returned no PDF URLs for 10 Census MARSHAs).

## Adapter learning (this pass)

| Module | Role |
| --- | --- |
| `marriott-dam-factsheet-discovery.js` | MARSHA → DAM URL index, PDF fetch (GET+UA), `pdf-parse` text, High patch builder |
| `marriott-factsheet-adapter.js` | DAM layout address/phone/rooms extractors |
| Seed index | SJULU (104 rooms + address + phone), GYECY (144 rooms) |

### Live dry-run (official PDF revalidation)

- **SJULU:** rooms=104, address=`110 Seaside Drive, Luquillo, PR 00773`, phone=`787.657.0000`
- **GYECY:** rooms=144
- `webhound_as_sot`: false on all paths

### Production pass (2026-08-07T17-48)

- DAM index size: 2
- Extracted: 40 (all overview blocked)
- Proposals: 0 / updates: 0 / inserts: 0
- Status: `…_partial_adapter_backlog_remaining`

## Next backlog (ordered)

1. **Expand DAM URL index for in-Census MARSHAs** (Webhound targeted session on BOGJW / CUNJW / SDQJW / etc., or official link harvest) — expected High rooms+address/phone writes once URLs known
2. Press-release room NLP for sparse openings
3. Property-specific `modules.marriott.com` extraction (reject aggregates)
4. Optional growth insert for SJULU/GYECY only if founder opens insert mode (currently field-completion-only)

## Command to continue

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
MARRIOTT_LEARN_EXTRACT_LIMIT=80 \
npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \
  --objective marriott-webhound-source-pattern-learning-v1 \
  --census-mode field-completion-only \
  --parent-company "Marriott International" \
  --strategy highest-yield-safe --batch-size 100 \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --enable-production-writes
```

After expanding `marriott-dam-factsheet-url-index.json` / seed map with in-Census MARSHAs, re-run the same command.
