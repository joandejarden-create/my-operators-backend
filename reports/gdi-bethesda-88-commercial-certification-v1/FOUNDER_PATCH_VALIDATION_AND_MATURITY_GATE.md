# Bethesda 88 — Canonical Contact Patch Validation + Founder Gate

**Authorized store write:** `false`  
**Store SHA (unchanged):** `1279f40c2d1cdd2cb6efdc55bfa8397b44f65c58da3ca4365f2814c69e71c9c3`  
**Git baseline:** `47b44182de6714cc87b52cf4089da159e4b68845`

## Step 1 — 15 proposed contacts validated

| Verdict | Count |
|---|---:|
| PASS (approved candidates) | **12** |
| FAIL (excluded) | **3** |
| UNCERTAIN | **0** |

### PASS — ready for exact founder authorization (not yet written)

| ID | Named person | Route class | Source |
|---|---|---|---|
| `gdi_opp_amwa_2028_annual` | Kelly McMahon | ORGANIZATION_ROUTE (`associatedirector@amwa-doc.org`) | amwa-doc.org/about-amwa/our-people |
| `gdi_opp_amwa_2027_annual` | Kelly McMahon | ORGANIZATION_ROUTE | same (per-event relevance checked) |
| `gdi_opp_amwa_2027_interim` | Kelly McMahon | ORGANIZATION_ROUTE | same |
| `gdi_opp_arlington_spring_tournament_2027` | Lizzy Stell | DIRECT (`lizzy@arlingtonsoccer.com`) | arlingtonsoccer.com/staff |
| `gdi_opp_georgetown_homecoming_2027` | Vanessa Otarola Kehoe | ORGANIZATION_ROUTE (`advancementevents@georgetown.edu`) | Georgetown directory + Homecoming |
| `gdi_opp_series_new_in_the_library_august_2026` | Quandra Blackeney | DIRECT (`scudderq@nida.nih.gov`) | NIDA CCTN staff |
| `gdi_opp_series_ctn_annual_conference_2027` | Quandra Blackeney | DIRECT | same CTN family (Bethesda Natcher) |
| `gdi_opp_series_march_15_17_bethesda_md_ctn_dissemination_2027` | Quandra Blackeney | DIRECT | same |
| `gdi_opp_nih_sbpo_vos_pattern` | Annette Owens-Scarboro | DIRECT (`scarbora@od.nih.gov`) | NIH SBPO staff directory |
| `gdi_opp_umd_alumni_weekend_watch` | Natalie Ross | DIRECT (`nross1@umd.edu`) | alumni.umd.edu/about/team |
| `gdi_opp_accp_annual_meeting_20261001` | Lisa C. Miller | DIRECT (`lmiller@accp.com`) | ACCP AM26 meeting-space form — **hotel-fit still research-gated (2026 Salt Lake)** |
| `gdi_opp_ecs_251_2027_overflow` | John Lewis | ORGANIZATION_ROUTE (`meetings@electrochem.org`) | electrochem.org/contact-ecs — **Overflow Watch only** |

Exact field-level before/after: `07-founder-approved-candidate-manifest.json`.

### FAIL — excluded from write candidates

| ID | Proposed | Why rejected |
|---|---|---|
| `gdi_opp_series_request_a_conference_registration_website_2026` | Carrie Wagerman | Official 2027 RNA symposium pages list **CCR Conferences only**; Carrie not confirmed as lodging coordinator for this demand |
| `gdi_opp_2027_nci_rna_biology_symposium_7` | Carrie Wagerman | Same family rejection |
| `gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027` | Carrie Wagerman | Same family rejection |

## Step 2 — Expected coverage if PASS patches applied

| Metric | Now (persisted) | After 12 PASS only |
|---|---:|---:|
| Named relevant primary | **46** | **58** |
| Δ named | — | **+12** |
| Approx still missing named | 42 of 88 | **~30** |

**No `opportunities.json` write until you explicitly approve the 12 PASS IDs (or a subset).**

## Step 3 — Certification vs commercial maturity

### Prior weaker audit label (fixed in code)

Previously, `COMMERCIALLY_CERTIFIED` could fire on named+route+thesis without passing Sales Ready or Account Development engines.

### Corrected report behavior

`COMMERCIALLY_CERTIFIED` now requires **Sales Ready OR Account Development engine pass**. Product maturity fields are **not** mutated.

| Label / field | Count |
|---|---:|
| COMMERCIALLY_CERTIFIED (engine-aligned report) | **30** |
| CONDITIONALLY_READY | **14** |
| RESEARCH_INCOMPLETE | **33** |
| DISQUALIFIED_WITH_EVIDENCE | **11** |
| Product Sales Ready | **0** |
| Product Account Development | **0** |

### Why product Sales Ready = 0 and Account Development = 0

1. Certification is a **report label**, not a store write.
2. Sales Ready engine still fails most rows on `SPECULATIVE_DEMAND` / `MISSING_HOTEL_DEMAND_THESIS` / buyer-responsibility gaps — even when AD passes.
3. Account Development engine can pass (~30 on current projection) while **product** `commercialMaturity` remains `RESEARCH_CONTINUATION` until a separate governed promotion applies.
4. Policy preserved: no auto-promotion from contact enrichment or certification alone.

### Twelve originally highlighted “commercially certified” rows (pre-tightening)

Inspected individually in `08-certification-vs-maturity-analysis.json`:

- **5 Scaling Companies** — AD-eligible; SR blocked by speculative thesis / buyer-responsibility unknown.
- **MCSHRM / PMI / PDA** — AD-eligible after contact recovery; SR blocked similarly.
- **Greater Bethesda Chamber ×2, City of Rockville, Nature Forward** — previously over-certified; engines reject or soft-fail (role/sources). Under corrected rules they are not SR/AD-certified unless engines pass.

### Genuine defect found + correction

- **Defect:** certification label weaker than SR/AD engines.
- **Correction applied in source:** engine gate for `COMMERCIALLY_CERTIFIED`.
- **Not applied:** product maturity promotion (requires separate founder authorization).

## Integrity

- 59 / 29 / 88 preserved
- 5 Scaling Companies intact
- Woman’s Club Diane preserved
- Skip-worktree store SHA unchanged
- No paid spend
- No customer publication
- AUTHENTICATED BROWSER NOT VERIFIED for Memberstack UI

## Awaiting founder decision

Reply with exact authorization, for example:

> APPROVE canonical contact write for the 12 PASS IDs in `07-founder-approved-candidate-manifest.json`

or list a subset. Until then, canonical store remains unchanged.
