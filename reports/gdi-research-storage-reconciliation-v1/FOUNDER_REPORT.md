# GDI Research Storage Reconciliation V1

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Mode:** Controlled CREATE-only research registry migration (authorized)  
**Customer publication:** **unchanged** — fingerprint `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66`

## Verdict

| Check | Result |
|-------|--------|
| All 67 published IDs in Airtable research registry | **PASS** (0 missing) |
| Airtable count | 59 → **86** (+27 CREATE) |
| FS mirror count | 59 → **86** |
| Duplicates introduced | **0** |
| 19 Airtable-only rows preserved + dispositioned | **PASS** (none promoted) |
| Publication fingerprint | **unchanged** `df8da65a…` |
| Feedback SHA | **unchanged** |
| Bundled V2 package SHA | **unchanged** |
| Stale `data/…/CURRENT` (`441773…`) | **quarantined** |
| Template research-storage gate | **PASS** |

Full E2E hotel template research-storage integrity is no longer blocked by the 27 missing registry IDs. Template certification still requires the broader product gates; this pack clears the **research registry completeness** blocker.

## What was done

1. **Immutable snapshots** under `pre-migration-snapshots/` (Airtable dump, FS opportunities, reconstructed bag, feedback, both CURRENT pointers, V2 package).
2. **86-ID reconciliation** across publication ∪ Airtable (see `02-86_ID_RECONCILIATION.json`).
3. **Validated all 27** publication-only IDs against `reconstructed-opportunities-v1.json` — 0 quarantined, 0 invented fields; provenance stamped `gdi-research-registry-historical-upsert-v1`.
4. **Dispositioned all 19** Airtable-only rows (archive / research-stage / market intelligence / junk / excluded) — preserve, **do not publish**.
5. **Dry-run then CREATE-only upserts** via `upsertOpportunity` into `appa2cE7FTRmIbB32` / `Group Demand Opportunities` (`tblRuReslJMwsfRQj`). Existing rows were never updated.
6. **FS mirror append** of the same 27 candidates (fuller research retained on disk).
7. **Quarantined stale data CURRENT** → `CURRENT.quarantined-stale-441773211494-2026-11-13.json` (+ staged). Bundled CURRENT remains V2.
8. **Research refresh dry-run** (no paid discovery) — registry coverage, no dupes, feedback/publication intact.
9. **Reusable contract** `lib/group-demand-intelligence/research/gdi-research-persistence-contract-v1.js` with published⊆registry certification gate.

## Storage responsibilities (locked)

| Master | Owns |
|--------|------|
| **Airtable** | Canonical mutable opportunity registry, stable IDs, classifications |
| **Durable FS research** | Full evidence, runs, historical recovery, mirrors |
| **Customer publication package** | Immutable approved presentation + fingerprint |
| **Feedback** | Mutable validation/outcomes (outside fingerprint) |

Research sync must **never** overwrite approved customer publication content.

## Acceptance criteria map

- 67 published IDs in reconciled registry → **met**
- 19 preserved + classified → **met**
- No duplicates → **met**
- Historical evidence retained (reconstructed SHA unchanged; sources on candidates) → **met**
- Fingerprint `df8da65a…` → **met**
- Customer report unchanged → **met**
- Feedback intact → **met**
- Future refresh cannot silently lose recovered IDs (gate + registry membership) → **met**

## Artifacts

See `01`–`12` JSON in this directory.

**GitHub:** [PR #272](https://github.com/joandejarden-create/my-operators-backend/pull/272) **MERGED** (`3bf3e6b87…`) · customer V2 package not modified · no other-hotel publication deploy.
