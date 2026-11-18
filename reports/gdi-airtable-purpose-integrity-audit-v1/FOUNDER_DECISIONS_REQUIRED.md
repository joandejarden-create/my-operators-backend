# Founder decisions required

**Gate:** Approve (or amend) this matrix before any production Airtable persistence activation.  
**This audit PR does not implement write routing changes beyond documentation.**

## D1 — Full E2E write destinations (approve / amend)

Approve proposed destinations:

1. Demand Generators (credible orgs / retainGlobally)  
2. Group Demand Opportunities (hotel-qualified / watchlist only)  
3. Private Event Venues (+ Fit/Signals when qualified)  
4. Hotel Demand Generator Fit **only when hotel-qualified with non-seed evidence**  
5. Optional GDI Research Runs **summary** linked to FS ledger `runId`

Confirm **blocked** for Full E2E discovery:

- GDI Hotel Commercial Profiles  
- GDI Hotel Need Periods (+ change history / campaigns / matches)  
- Decisions / Decision Events  
- Facility HI tables + Hotel Seasonality  
- ADP measurement tables  
- Raw unsupported Exa hits  
- Customer publication packages  

**Decision needed:** Approve as proposed / amend list.

## D2 — Bethesda pilot write set

Revalidated 7 ops (DG×3 + GDO×3 + PE×1) against table purposes: **still structurally correct**, but may shrink after idempotent collision with existing 86 GDOs / 95 DGs.

**Decision needed:**  
A) Authorize dry-run re-plan then selective apply of ≤7  
B) Require explicit opportunityId/Demand Generator ID list first  
C) Defer Bethesda apply until YOTEL/Radisson drift policy decided

## D3 — Research Targets policy

Live Targets are heavily `generic_seed` monitoring subjects.

**Decision needed:**  
A) Full E2E may create Targets only when explicitly “commit to monitor”  
B) Full E2E never writes Targets (coverage stays onboard/monitoring tools)  
C) Other

## D4 — Synthetic / provisional isolation

Waterstone synthetic commercial profile + test need periods; Sheraton provisional need periods (Status Test Scenario).

**Decision needed:**  
A) Leave as-is (rely on Status/Source + presentation guards)  
B) Require operator-visible quarantine flag / view  
C) Separate sandbox base (larger change — not recommended now)

## D5 — YOTEL inventory authority

Airtable GDO 108 vs FS 42 with CURRENT/STAGED packages present.

**Decision needed:** Which store is research SoR for YOTEL before further Airtable writes?  
A) FS opportunities.json  
B) Airtable GDO  
C) Publication package for customer; FS for research; Airtable must be reconciled first (**recommended default**)

## D6 — Radisson / Guadalajara sync expectations

- Radisson: FS 66 > Airtable 36  
- Guadalajara: FS 43 / Airtable GDO 0 (provisional need periods only)

**Decision needed:** Is FS-ahead acceptable until selective sync, or must Airtable catch up before next hotel pilot?

## D7 — Match Key contract correction

Approve documenting Match Key as upsert identity (no schema delete).

**Decision needed:** Yes / No

## D8 — Legacy Hotel Seasonality table

Mark as deprecation candidate (no deletion).

**Decision needed:** Acknowledge / defer

---

## Uncertainties (explicit)

1. Whether some YOTEL Airtable-only GDOs are intentional historical research retained only in Airtable.  
2. Whether Demand Programs near 1:1 with Generators are meaningful series or onboard mirrors.  
3. Whether Research Target Runs should ever receive Full E2E rows given FS ledger richness.

These do **not** block approving D1 blocked-tables list.
