# GDI Airtable Presentation Persistence V1

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Mode:** APPLY  
**Approved fingerprint (unchanged):** `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66`

## Verdict

| Check | Result |
|-------|--------|
| Airtable-only reconstruction vs V2 | **67/67 PASS** |
| Publication package mutated | **NO** |
| Feedback mutated | **NO** |
| Package fallback in reconstruction | **NO** |
| Compaction preserves salesAction/datePill/drawers | **PASS** |
| Airtable field-completeness gate | **READY_TO_CERTIFY** |
| Full E2E template auto-certified | **NO** (other product gates may remain) |

## What was fixed

1. **Canonical presentation persistence contract** — required customer fields must survive Airtable writes.
2. **Compaction re-attach** — emergency payload floors can no longer strip `salesAction*`, `datePillDisplayLabel`, drawers, need-date, contacts.
3. **Upsert merge** — blank research updates cannot wipe recovered presentation; sales/decision ownership preserved.
4. **Bethesda backfill** — approved V2 presentation merged into Airtable with provenance `RECOVERED_FROM_APPROVED_PUBLICATION_V2` (not claimed as independent research verification).
5. **opportunityType read fidelity** — payload presentation type preferred over Airtable select allowlist when present.
6. **Need-date** — 39 frozen matches persisted; 28 stamped `EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION` (no invention despite extra match rows).

## Acceptance map

- 67/67 reconstruct approved customer-facing presentation from Airtable → **met**
- Commercial-action fields retained → **met**
- Date-pill labels retained → **met**
- Titles, summaries, contacts, drawers retained → **met**
- Need-Date Match reconciled → **met**
- No hidden publication fallback → **met**
- V2 fingerprint unchanged → **met**
- Feedback unchanged → **met**
- Other hotels not written → **met**

## Artifacts

`01`–`13` in this directory.
