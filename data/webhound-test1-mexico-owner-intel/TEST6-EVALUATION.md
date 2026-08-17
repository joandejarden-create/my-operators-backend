# Webhound Test 6 — Independent Brand Explorer Validation + Cursor Reconciliation

**Session:** `5922518e-c870-432f-ac29-700c99ff8dd4`  
**URL:** https://webhound.ai/session/5922518e-c870-432f-ac29-700c99ff8dd4  
**Status:** completed (`natural_complete`)  
**Spend:** **$6.11** of $10.00 authorized  
**Rows:** 5 · 48 fields · fill rate 100%  
**Governance:** Candidate intelligence + **proposed corrections only**. **No Airtable / BE / Census / OE SoT writes.** **No Test 7.**

## Census-priority confirmation (pre-launch edit)

Incorporated before launch:

1. Mexico → CALA → Americas → Global-only-if-budget  
2. Brand Explorer field quality over exhaustive global enumeration  
3. Mexico/CALA depth (resort, small-key, conversion/new-build, mixed-use, residences, owner/operator, pipeline, appetite) — **facts only, not Diego fit**  
4. Never imply global-complete unless true  

---

## Independent Webhound summary (pre-reconciliation)

| Brand | Parent | WH census | Scope | Confidence |
|-------|--------|-----------|-------|------------|
| Hotel Indigo | IHG | 8 hotels (5 MX + 3 CALA) | MX+CALA partial; not global | High / census Medium |
| Kimpton | IHG | 5 (4 MX + 1 HN) | MX strong; CALA incomplete | High / census Medium |
| Tribute Portfolio | Marriott | 7 CALA ops + Casa Nizuc pipeline | MX thin (Holbox only ops) | Medium |
| Avani | Minor Hotels | 2 (MX+CO) | Mexico+CALA **complete** for Americas debut | Medium / census High |
| Radisson Individuals Americas | Choice (Americas) / RHG elsewhere | 16 CALA; **0 Mexico** | Choice relationship strong | High |

**Useful Mexico/CALA facts for later Diego work (not a fit analysis):** small-key Kimpton Polanco (34–48 keys); Tribute soft-brand conversion model; Indigo conversion-heavy; Avani NH conversion path; Individuals = Choice soft brand with no Mexico supply yet.

---

## Success criteria (independent research quality)

| Criterion | Assessment |
|-----------|------------|
| Completeness (identity/growth/parent) | **Strong** for all five |
| Evidence quality | **Strong** (official directories + PR); owner fields weak |
| Hotel coverage (Mexico/CALA priority) | **Good** Indigo/Kimpton/Avani/Individuals; Tribute MX thin; Kimpton CALA under-covered |
| Current accuracy | **Promising** — several fresher status signals than local SoT |
| Ownership quality | **Weak** (mostly unknown) |
| Operator quality | **Partial** (Highgate, Blue Diamond/Royalton, Driftwood, Faranda, Vista Capital wins) |
| Parent-company quality | **Strong** (esp. Choice dual-region Individuals) |
| Regional structure | **Strong** (IHG MLAC; Choice vs RHG split) |
| Recent-change quality | **Strong** |
| Cost / brand | **$1.22** ($6.11 / 5) |

---

## Cursor reconciliation (proposed corrections only)

Full discrepancy tables: `TEST6-RECONCILIATION.md`  
Baselines: `test6-dealality-baselines.md`

### Material proposed corrections (do not apply yet)

| # | Brand | Issue | Classification | Proposed correction |
|---|-------|-------|----------------|---------------------|
| 1 | Indigo | Playa del Carmen + Tijuana still **Pipeline** in Dealality; WH + IHG directory/press = **Operating** | Webhound appears correct | Verify IHG.com bookability → update census Status Open if confirmed |
| 2 | Indigo | Lima Miraflores, Bridgetown Barbados Pipeline → WH Operating | Webhound appears correct | Same status hygiene |
| 3 | Kimpton | **Aluna Tulum** Open in Dealality but no safe directory match; WH lists **Tres Ríos** (pcmht) not Aluna | Evidence conflicting | Directory verify both codes; do not merge blindly |
| 4 | Tribute | Crystal Cove + Turtle Beach Barbados labeled Autograph/Elegant in Dealality; WH+PR = Tribute 2026 | Webhound appears correct | Re-affiliate if Marriott directory confirms Tribute |
| 5 | Tribute | Casa Nizuc Cancún pipeline (Sep 2026) present in WH; missing from Dealality extract | Webhound appears correct | Add pipeline census candidate if Marriott confirms |
| 6 | Tribute | “Tulum, a Tribute Portfolio Hotel” vs Design Hotels page (TQOXT) | Evidence conflicting | Fix wrong brand assignment if Design Hotels |
| 7 | Avani | **Absent from Active/Live BE**; census has Cancún Airport | Dealality product gap | Tab Factory candidate — not a WH error |
| 8 | Individuals | WH found +2 Faranda members vs Dealality 14 | Webhound appears correct | Add if Choice directory still lists them |
| 9 | Individuals | Choice dual-ownership (Americas vs RHG) | Both partially correct; WH richer | Strengthen BE regional structure copy |

### Metrics (approximate; local SoT vs WH rows)

| Metric | Result |
|--------|--------|
| BE field accuracy (parent/positioning vs WH) | **High** for Indigo/Kimpton/Tribute/Individuals; Avani N/A (no BE) |
| BE completeness | **High** for 4/5; **Avani gap** |
| Census accuracy (tested MX/CALA) | **Mixed** — WH fresher on several open/pipeline; Dealality richer on some CALA Kimpton |
| Census completeness Mexico | Indigo: both strong with status drift; Kimpton: conflict Aluna/Tres Ríos; Tribute: both incomplete; Avani: aligned; Individuals: aligned empty |
| Cross-table consistency | Issues: Elegant→Tribute Barbados; possible Tribute/Design Hotels Tulum; Avani census without BE |
| Discrepancies / brand | ~6–12 notable each |
| Material discrepancies / brand | ~1–3 each |
| Corrections WH found that local validation likely missed | Indigo open-vs-pipeline; Barbados Tribute reflags; Tres Ríos; Casa Nizuc; +2 Individuals; Avani BE gap confirmation |
| Cost / brand | **$1.22** |
| Cost / material correction | **~$0.70–$1.00** (≈6–9 material items / $6.11) |

---

## Decisive answers

### Can Webhound materially improve and maintain Dealality Brand Explorer + Hotel Census?

**Yes — as a continuous validation / freshness layer, not as SoT.**

- **Highest ROI:** open-vs-pipeline hygiene, reflags/conversions, parent/regional structure (Choice), Mexico/CALA openings, operator crumbs when public.  
- **Weak:** exhaustive global census, UBO/owners, replacing Brand Explorer narrative quality.  
- **Must keep:** Cursor challenge + human apply; never auto-write Airtable from Webhound.

### What would a production Webhound validation workflow look like?

1. **Trigger:** monthly (or pre-release) for Active/Live brands + priority Mexico/CALA affiliations.  
2. **Webhound job:** $5–$15/brand-cohort; Mexico→CALA census priority; schema = identity + growth + parent + recent changes + hotel entries.  
3. **Cursor reconcile:** WH ↔ BE ↔ Census ↔ OE; classify each discrepancy; produce **proposed correction pack** (dry-run).  
4. **Human approve:** steward applies via existing `--dry-run` → apply scripts.  
5. **Gates:** re-run PVQL / affiliation audits / protected baselines after apply.  
6. **Do not:** use Dealality as WH research input; auto-merge; burn budget on global full enumeration.

---

## Ledger

| Item | Amount |
|------|--------|
| Authorized | $10.00 |
| Actual | **$6.11** |
| Cumulative funded (T2–T6) | **~$25.48** |
| Remaining credits | **~$29.52** |
| Test 7 | **Not authorized** |

## Artifacts

- `test6-rows-compact.json`, `test6-export-raw.json`  
- `TEST6-RECONCILIATION.md`  
- `test6-dealality-baselines.md`  
- Session URL above  
