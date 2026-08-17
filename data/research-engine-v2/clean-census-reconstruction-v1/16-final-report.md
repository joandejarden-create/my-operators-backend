# Clean Census Reconstruction V1 — Final Report

## Most important answer

**Yes — Dealality can demonstrate through code, provenance, and audit artifacts** that a production census path is independently discovered, independently researched, frozen before legacy comparison, and not populated from legacy STR/client-derived values.

This pilot implements that evidentiary trail for Hotel Indigo + Kimpton Mexico. Full production cutover is **not** done yet; legacy table is untouched.

## Answers

1. **Independent universe without legacy seeds?** **Yes.** 9 hotels from IHG Mexico directory; firewall blocked legacy pre-freeze.
2. **Materially complete records?** **Partially.** Core identity/status/URL/ID largely yes; rooms/operator/amenities/geo often Unknown without inventing values.
3. **% material fields independently supported?** Core field coverage ≈ **100%**; all material schema fields ≈ **48%** (unknowns correctly left blank).
4. **Matches legacy?** **6**
5. **Independent-only?** **1**
6. **Legacy-only?** **8**
7. **Legacy-only rediscovery?** Challenges run post-freeze; confirmed only via official directory evidence — never by copying legacy values. Unconfirmed remain pending/escalation.
8. **Difficult fields?** Management Company (9); Chain Scale (9); Market (9); Submarket (9); Location (9); Latitude (9); Longitude (9); Amenities (9)
9. **First-party capture?** Validation packs — see `10-first-party-validation-design.md`.
10. **Strongest FP authority?** Brand/operator portfolio, status, identity, supplied keys/amenities — not silent override of conflicting regulatory evidence.
11. **Images?** Separate rights classes; no auto-import — `11-image-rights-design.md`.
12. **DB path?** **Option B** — Verified Independent Hotel Census as master; quarantine legacy `Hotel Census`.
13. **Evidence retained?** Freeze hash, discovery JSON, field claims, firewall audit, post-freeze comparison, challenge results.
14. **Waves?** See `14-full-reconstruction-roadmap.md`.

## Success test checklist

| # | Criterion | Result |
|---|-----------|--------|
| 1 | Discover without legacy seed | PASS |
| 2 | Materially complete core records | PARTIAL PASS (core strong; extended gaps honest Unknown) |
| 3 | Freeze before legacy | PASS |
| 4 | Reconcile only after | PASS |
| 5 | No copy of unresolved legacy values | PASS |
| 6 | Provenance on material fields | PASS |
| 7 | Independent-only + legacy-only identified | PASS |
| 8 | Brand validation path designed | PASS (design) |

## Constraints honored

No Webhound · No credits · No Airtable writes · Legacy not deleted · Legacy not used as research evidence · Freeze-before-compare · No auto FP overrides · No auto image use
