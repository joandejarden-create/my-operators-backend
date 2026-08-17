# Brand Explorer Openings Display Parity Audit v31K

Compares **Openings / Examples / Properties** (`footprint.openings`) images and label text across two brands.

**Command**

```bash
npm run brand-explorer-openings-display-parity-audit -- --left radisson --right radisson-individuals-by-choice --dry-run
```

**Reports**

- `reports/brand-explorer-openings-display-parity-audit.md`
- `reports/brand-explorer-openings-display-parity-audit.json`

## Scope

| Side | Brand | Slug | Record ID |
|------|-------|------|-----------|
| Left | Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` |
| Right | Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` |

Audit only — no Airtable writes, no image approvals, no materialization, no Company Validated changes.

## v31K finding (2026-07-10)

| Metric | Radisson | Radisson Individuals |
|--------|----------|----------------------|
| Airtable `footprint.openings` rows | 4 | 8 |
| Quarantined (Do Not Display) | 0 | **8/8** |
| Visible in API | **4** | **0** |
| Images in API | **4** | **0** |
| Internal-language hits | 0 | **41** |
| Frontend mode | property-example-card grid | **propertyShell() × 3 empty shells** |

**Primary root cause:** All 8 Individuals openings rows are quarantined (`External Display Status = Do Not Display`). The API excludes them from `blocks[]`, so the UI falls back to three empty `property-example-card` shells with blank labels and no images. Radisson renders four full cards with images and parsed label text (chips, location, meta, scenario, teaser).

**Labels paradox:** Individuals rows have structurally complete label fields in Airtable (title, location, meta, scenario, teaser, tags all parse correctly), but none reach the UI because of quarantine. Six rows also contain internal/census/source-capture language (v31C trigger).

**Not the cause:** Brand-specific frontend logic, API mapping difference, or expansion slug mismatch.

## Recommended sequence

1. **P1 — v31L openings rebuild:** owner-facing copy rewrite + approved images + registry linkage.
2. **P1 — Repair internal language** in body and case-summary fields before clearing quarantine.
3. **P2 — Registry + approval** per property row, then clear Do Not Display.
4. **Optional UI:** hide openings section when zero API blocks (uniform, not brand-specific).
