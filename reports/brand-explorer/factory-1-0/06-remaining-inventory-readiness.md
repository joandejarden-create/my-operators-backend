# Factory 1.0 — Remaining Brand Inventory Readiness

After Autograph completes approved writeback + Current Saved verification:

## Queue principles

1. One brand at a time through `brandId → audit → research → candidate → QA → founder approval → writeback`.
2. Never mutate Curio (`receQkxgjlezsc1xg`) or Ascend (`reclkgOzvAcBheUSo`) during other-brand runs unless explicitly authorized.
3. Prefer brands with Active Brand Status, existing Presentation, and accessible public sources.
4. Paid research only with explicit spend authorization; default free authoritative sources first.
5. No mass writeback / customer publication without per-brand exact-candidate approval.

## Suggested order (from selection report)

| Priority | Brand | ID | Notes |
|---|---|---|---|
| 1 (in flight) | Autograph Collection | `recEJCTDj1zrsjPM6` | Awaiting exact-candidate approval |
| 2 | AC Hotels by Marriott | `rec9aZp7GHtzUEg0c` | Same parent; different hard-brand economics |
| 3 | Moxy Hotels | `recahVIW4aCx0Ao84` | Lifestyle; tests shorter Presentation |
| 4 | Vignette Collection | `recDwzv86TWnz2gGB` | IHG soft-collection generality |
| 5 | Canopy by Hilton | `recsggfbKlJbjeRP9` | Hilton peer after Curio |
| 6 | Even Hotels | `recvvmiyReHhiKdoK` | IHG select-service contrast |

## Factory gaps to close before mass runs

- Generic research pack generator (today Autograph-specific gap-fill script).
- Structured Airtable persistence beyond Presentation (attributes, loyalty metrics, fee exhibits).
- Per-brand product-QA script template (today Autograph 8080 verify).
- Brand-specific founder tokens minted at approval time.
