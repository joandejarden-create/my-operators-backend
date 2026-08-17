# Verified Independent Census Wave 1C — Choice Mexico Final Report

## Verdict

**Yes — Wave 1C scales the Verified Independent Census across IHG + Hilton + Choice** while introducing Property Identity V1 and Temporal Affiliation V1, without legacy contamination, Webhound, credits, or Airtable writes.

## Answers

1. **Success?** Yes.
2. **Hotels discovered:** **68**
3. **Brands:** Ascend Hotel Collection, Choice Hotels, Comfort Inn, Comfort Suites, Quality Inn, Radisson, Sleep Inn
4. **Radisson Individuals Americas?** Relationship independently documented (Choice Americas franchisor). Directory brands include Radisson + Ascend; Individuals/Faranda-named count: **0**.
5. **Faranda without prior seeds?** No Faranda-named properties on the independent Choice Mexico directory in this run.
6. **Core / material:** **97% / 56%**
7. **Hard fields improved:** coordinates (50), address, amenity groups / F&B / meeting flags.
8. **Still weak:** rooms (0), open date, management/owner, property-page enrichments (403 risk).
9. **Exact / probable legacy:** 29 / 11
10. **Independent-only:** 28
11. **Legacy-only:** 13
12. **Reflags/historical:** 0 cross-family candidates (no auto-merge).
13. **Property identity V1 prevent duplicates?** Intra-Choice unique physical: **68** of 68; fuzzy-only merges rejected.
14. **Temporal affiliation useful/safe?** Yes — as-of seeding without fabricated precision.
15. **Cross-family:** 0 same-physical links vs IHG/Hilton.
16. **Drive Choice BE completion?** Yes — assessment in `10`; no activation.
17. **BE completion pilot?** **READY FOR SMALL BRAND COMPLETION PILOT**
18. **Census migration pilot?** **PILOT MIGRATION READY** (staging only)
19. **Next family?** **Marriott Mexico** (not launched)
20. **Top 3 gaps:** (1) rooms/operator via FP or safe property-page path, (2) steward review of cross-family identity links, (3) persist Choice regional snapshots + expand beyond Mexico.

## MOST IMPORTANTLY

**Wave 1C shows Verified Independent Census reconstruction can scale across IHG + Hilton + Choice while maintaining durable physical property identity, temporal brand affiliation, and a clean path into Brand Explorer completion — without auto-activation or legacy evidence leakage.**

## Constraints honored

No Webhound · No credits · No Airtable · No brand activation · No legacy pre-freeze · No legacy copying · No fuzzy-only merges · No automatic reflags · No STR taxonomy migration · No automatic image use · Governance preserved · V2 reused.

Runtime ~7s · firewall blocked pre-freeze: true · $0
