# GDI operational baseline — A–E answers

**As of:** 2026-10-14  
**Committed build:** `f911ff9ae4b5d7d0c83c0e94e0400f1a74271bfd` on `gdi/parallel-provider-san-diego`  
**Deployed build:** none this pass (Railway unauthorized — see FINAL-REPORT.md)

Full narrative: `FINAL-REPORT.md`

---

## A. What is deployed and visible now?

| Surface | Status |
|---------|--------|
| Production Railway deploy of consolidation | **blocked** — `railway login` required; gates PASS; upload failed |
| Local committed code | **executed** — `f911ff9a` |
| Airtable Beth/May content (titles + prior drawer repair) | **executed** — title cleanup apply + read-back |
| Customer production GDI page assets | **stale UI** until Railway deploy; **Airtable payload titles** already updated |

---

## B. What is saved in Airtable?

| Pass | Beth updates | May updates | Creates | Quarantined |
|------|-------------:|------------:|--------:|------------:|
| Drawer repair (prior) | 14 | 15 | 0 | 0 |
| Title cleanup | 14 | 15 | 0 | 0 |

Stable IDs, feedback, actions, outcomes, unmatched rows preserved. San Diego **not** published.

---

## C. What real research ran and what did it cost?

| Run ID | Role | Est. USD |
|--------|------|---------:|
| `trun_…61f` | Contaminated SD→Bethesda (preserved) | 1.00 |
| `trun_…2b38` | Connectivity probe (reconciled; not free) | 0.50 |
| `trun_…68b3` | Corrected identity-lock live (8 SD candidates) | 0.50 |
| **Total vs $2 cap** | | **2.00 / 2.00** (remaining **$0**) |

PE: Airtable read of 11 shared venues — all Bethesda; quarantined for SD. No fixture fallback. Hotel-specific PE = research queue only.

---

## D. Cards complete / pending / unresolved

| Set | Status |
|-----|--------|
| Beth / May facing | Title + drawer content applied; sales validation still operator-owned |
| SD Parallel identity-lock (8) | Internal complete draft cards; **not published** |
| SD program drafts (12) | 10 queue / 2 thematic quarantine; not published |
| SD PE partnerships | 0 confirmed; shared corpus wrong-market |

---

## E. Repeatable for another hotel?

Research→draft path: largely yes with hotel config + allowlist.  
Production publish polish: no — needs Railway deploy, human review, and hotel-specific PE evidence (not cross-market fixture reuse).

**Do not call E2E prospect delivery repeatable until correctly targeted live runs produce published hotel-specific cards through the shared workflow.** Modest internal yield this pass is acceptable; wrong-market + unfinished publish are not successful delivery.
