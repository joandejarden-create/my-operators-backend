# Verified Independent Census Program V1 — Final Report

## Most important answer

**Yes — we now have a scalable program** to build a Verified Independent Hotel Census, quarantine legacy from production use over time, complete Brand Explorer on that foundation, and maintain via shadow ops — **without** writing Airtable or spending Webhound in this phase.

## Benchmark: IHG Mexico — ALL brands

| Metric | Value |
|--------|-------|
| Independent discoveries | 195 |
| Core field support | 100% |
| Material field support | 56% |
| Legacy IHG MX reference | 213 |
| Matches | 151 |
| Probable | 15 |
| Independent-only | 29 |
| Legacy-only | 63 |
| Source states | {"Available":195} |
| Data-eligible (gate) | 191 |
| Firewall blocked pre-freeze | true |
| Runtime | 119068 ms |
| Cost | $0 |

## Answers

1. **Scale beyond Indigo/Kimpton?** Yes — 195 IHG MX properties discovered directory-first.
2. **Scalable adapters today?** IHG strong; Choice CALA extract ready; Hilton GraphQL status-ready; Marriott soft partial; others planned.
3. **% natively reconstructable?** Major-group branded share high where directories exist; overall census lower due to blank-parent/long-tail (~order 40–60% branded CALA rough).
4. **Hardest fields?** Management Company, Owner, rooms (when not on page), coords, amenities depth, corridor Submarket, Chain Scale.
5. **Proprietary fields?** STR Market/Submarket/Number/performance; STR-era Chain Scale/Location — see audit.
6. **Dealality replacements?** Market/corridor Submarket; segment from Brand Setup; avoid cloning STR.
7. **Legacy-only?** Strict rediscovery (strongest) + Targeted verification (steward efficiency) — both post-freeze; no legacy field copy.
8. **FP validation?** Separate First-Party Validated provenance; steward queue; not legacy authorization.
9. **Source-rights?** Registry v1 with Allowed / Constraints / Reference Only / Unknown / Do Not Use.
10. **Production migration qualify?** Data eligibility gates + image rights separate + governance handoff — no write yet.
11. **Waves?** 1 Mexico majors → 2 CALA → 3 Americas → 4 soft → 5 long-tail → 6 legacy-only → 7 FP validation.
12. **Effort?** Multi-wave program; Wave 1 IHG MX runnable in minutes native; full census months of waves + steward.
13. **Webhound %?** ~10–25% long-tail/opaque/strict challenges — not spent now.
14. **BE completion start?** After Materially Complete census cohorts for priority brands (post Wave 1–2).
15. **Migration architecture?** Option B — Verified Independent Hotel Census staging → governed cutover; legacy quarantined archive.

## Constraints honored

No Webhound · No credits · No Airtable writes · Firewall fail-closed · No legacy pre-freeze research · Unknown over fabrication · No auto activation/images
