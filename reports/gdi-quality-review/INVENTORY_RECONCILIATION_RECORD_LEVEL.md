# GDI inventory reconciliation — record level

Generated: 2026-10-15 (UTC)  
Hotels: Mayflower (`rec0eTEnCqlsGJJt9`), San Diego / Wyndham Bayside (`recnPB3Q02NO8Ma1o`)

This report explains **35 → 30** (Mayflower) and **46 → 38** (San Diego) without changing eligibility rules, forcing totals, or restoring unsupported records. Machine-readable scratch output: [`_inventory-recon-scratch.json`](./_inventory-recon-scratch.json).

---

## 1. Snapshot sources and counts

| Label | Definition | Source path | Mayflower | San Diego |
|-------|------------|-------------|----------:|----------:|
| **Earlier “broad facing” (35 / 46)** | `prospectHotelFacing === true` **OR** `customerVisible === true` on canonical store | Parity backfill manifests `facingBroadCount`; live integrity probe uses the same rule | **35** | **46** |
| **Reviewed-path prospect visible (backfill)** | `isReviewedProspectCustomerVisible(opp, { hotelId })` at backfill time | `backfill-manifest-*.json` → `facingReviewedCount` | **30** | **37** |
| **Current customer list / freeze (30 / 38)** | `isCustomerFacingOpportunity(opp, { hotelId })` — list API convergence gate | `reports/gdi-research-history-opportunity-type/facing-inventory-freeze.json` (`frozenAt` 2026-10-15T12:27:57Z) | **30** | **38** |
| **Fresh local canonical (2026-10-15)** | Same filters re-run on local `opportunities.json` via deploy-repo `customer-visibility.js` | `gdi-release-main/data/group-demand-intelligence/hotels/{hotelId}/opportunities.json` | broad **35**, list **30**, reviewed prospect **30**, total **57** (`updatedAt` 2026-10-15T12:01:25Z) | broad **46**, list **38**, reviewed prospect **38**, total **77** (`updatedAt` 2026-10-15T12:01:58Z) |
| **Domain integrity live (Airtable read)** | Broad stamp only (not list gate) | `gdi-release-main/reports/gdi-quality-review/domain-integrity-results.json` (`generatedAt` 2026-10-15T11:50:43Z) | **35** | **46** |
| **Production list (post-deploy note)** | Auth’d list API; not re-fetched in this pass | Prior reconciliation note after deploy `fed35cc7` (2026-10-15) | **30** (matches freeze) | **45** (between freeze **38** and broad **46**) |

**Canonical data paths (read-only):**

- Mayflower store: `C:\Dev\dealality\gdi-release-main\data\group-demand-intelligence\hotels\rec0eTEnCqlsGJJt9\opportunities.json`
- San Diego store: `C:\Dev\dealality\gdi-release-main\data\group-demand-intelligence\hotels\recnPB3Q02NO8Ma1o\opportunities.json`
- Freeze IDs: `C:\Dev\dealality\gdi-release-main\reports\gdi-research-history-opportunity-type\facing-inventory-freeze.json`
- Parity manifests: `C:\Dev\dealality\gdi-release-main\reports\gdi-hotel-fit-assessment-parity\backfill-manifest-rec0eTEnCqlsGJJt9.json`, `backfill-manifest-recnPB3Q02NO8Ma1o.json`

---

## 2. How to read disposition columns

| Column | Meaning |
|--------|---------|
| **Earlier disposition** | Included in **broad facing** (35 / 46) — matches domain-integrity live probe and parity `facingBroadCount`. |
| **Current disposition** | Included in **customer list / freeze** (30 / 38) — `isCustomerFacingOpportunity` / `facing-inventory-freeze.json`. |
| **Change category** | Primary reconciliation bucket (see §4). |
| **Supported reason** | Evidence-backed explanation; no restore recommended unless product explicitly widens the list gate. |

**Set arithmetic (verified locally):**

- Mayflower: broad **35** − list **30** = **5** IDs (below). All **5** remain in canonical store.
- San Diego: broad **46** − freeze **38** = **8** IDs (below). All **8** remain in canonical store.
- San Diego reviewed-path count moved **37 → 38** because `gdi_sc_2340eb7f036aecac` (MVE + Partners) later satisfied the reviewed list gate and appears **inside** the freeze; it is **not** one of the eight broad−freeze IDs.

---

## 3. Record-level deltas

### Mayflower — five opportunities in earlier 35, not in current 30

| Hotel | Stable ID | Account / occasion | Earlier disposition | Current disposition | Change timestamp (canonical `updatedAt`) | Change category | Supported reason |
|-------|-----------|-------------------|---------------------|---------------------|------------------------------------------|-----------------|------------------|
| Mayflower | `gdi_opp_prospect_mv8fnmk1_c41c0a4c` | U.S. Travel Association — Destination Capitol Hill | Broad facing | Not on customer list / freeze | 2026-10-15T06:30:22.739Z | **Eligibility / visibility** (reviewed-path) | `customerVisible` true, `prospectHotelFacing` false — counted in broad stamp, excluded from reviewed prospect list (`customer_visible_but_not_prospect_hotel_facing`). |
| Mayflower | `gdi_opp_prospect_mv8fnmk1_e4668916` | U.S. Conference of Mayors — next DC cycle | Broad facing | Not on customer list / freeze | 2026-10-15T06:30:22.739Z | **Eligibility / visibility** | Same OVERFLOW / prospect-facing pattern as above. |
| Mayflower | `gdi_opp_prospect_mv8fnmk2_a845a23a` | American Academy of Dermatology — AAD Annual Meeting 2028 | Broad facing | Not on customer list / freeze | 2026-10-15T06:30:22.740Z | **Eligibility / visibility** | Same. |
| Mayflower | `gdi_opp_prospect_mv8fnmk1_ad300879` | Americans for the Arts — Arts Advocacy Summit | Broad facing | Not on customer list / freeze | 2026-10-15T06:30:22.740Z | **Eligibility / visibility** | Same. |
| Mayflower | `gdi_sc_0fe98055e8df9f75` | Wisedocs — Scaling Corporate Account | Broad facing | Not on customer list / freeze | 2026-10-15T07:33:03.166Z | **Eligibility / visibility** | `prospectHotelFacing` true but **`prospectPublicationBasis` null** — broad stamp includes row; list gate excludes (`missing_publication_basis`). |

### San Diego — eight opportunities in earlier 46, not in current freeze 38

| Hotel | Stable ID | Account / occasion | Earlier disposition | Current disposition | Change timestamp | Change category | Supported reason |
|-------|-----------|-------------------|---------------------|---------------------|------------------|-----------------|------------------|
| San Diego | `gdi_pe_295cfef5822e63c3` | Sparks Gallery — Preferred Lodging Partnership | Broad facing | Not on customer list / freeze | 2026-10-15T08:33:23.263Z | **Eligibility / visibility** | Reviewed package stamp present; **`reviewed_visibility_gate_excluded`** at backfill — broad counts PE row, list gate does not. |
| San Diego | `gdi_pe_879047c3211d2e82` | City Cruises San Diego — Preferred Lodging Partnership | Broad facing | Not on customer list / freeze | 2026-10-15T08:33:21.392Z | **Eligibility / visibility** | Same. |
| San Diego | `gdi_pe_5ce20a54b903e243` | University Club San Diego — Preferred Lodging Partnership | Broad facing | Not on customer list / freeze | 2026-10-15T08:33:39.741Z | **Eligibility / visibility** | Same. |
| San Diego | `gdi_pe_ca47fef6aa622566` | deck655 — Preferred Lodging Partnership | Broad facing | Not on customer list / freeze | 2026-10-15T08:33:17.491Z | **Eligibility / visibility** | Same. |
| San Diego | `gdi_pe_5f72b9d7665314b5` | Flagship Cruises & Events — Preferred Lodging Partnership | Broad facing | Not on customer list / freeze | 2026-10-15T08:33:19.519Z | **Eligibility / visibility** | Same. |
| San Diego | `gdi_pe_4b9b49bae4136428` | Nolita House — Preferred Lodging Partnership | Broad facing | Not on customer list / freeze | 2026-10-15T08:33:27.021Z | **Eligibility / visibility** | Same. |
| San Diego | `gdi_pe_a7a2fda2a474aa0b` | The House Next Door — Preferred Lodging Partnership | Broad facing | Not on customer list / freeze | 2026-10-15T08:35:41.610Z | **Eligibility / visibility** | Same. |
| San Diego | `gdi_sc_188e25b6b3b56637` | The Shipyard — Scaling Corporate Account | Broad facing | Not on customer list / freeze | 2026-10-15T07:33:06.722Z | **Eligibility / visibility** | **`prospectPublicationBasis` null** — broad includes; list excludes (`missing_publication_basis`). |

### San Diego — disposition change within the 46 broad set (not a 46→38 drop)

| Stable ID | Account | Backfill manifest | Current freeze / local list | Notes |
|-----------|---------|-------------------|----------------------------|-------|
| `gdi_sc_2340eb7f036aecac` | MVE + Partners — Scaling Corporate Account | Listed under `excludedFromReviewedPath` (`reviewed_visibility_gate_excluded`) when `facingReviewedCount` was **37** | **Included** in freeze **38** and local list **38** | Record gained reviewed/list eligibility (`prospectPublicationBasis`: `REVIEWED_PACKAGE_V1`, `updatedAt` 2026-10-15T09:08:58.592Z). Explains **37 → 38** reviewed-path count while broad stays **46**. |

---

## 4. Reconciliation categories (what did *not* happen)

| Category | Mayflower | San Diego |
|----------|-----------|-----------|
| **Different snapshot definitions** | **Primary driver.** Headline **35** used broad stamp; headline **30** uses list gate + freeze. | **Primary driver** for **46 → 38**. Integrity **46** = broad; freeze **38** = list gate. |
| **UI filters** | No evidence that client-only filters removed rows; ID sets match server-side gates. | Same. |
| **Duplicate merges** | `duplicateIds: 0` in domain-integrity live report. | Same. |
| **Eligibility / visibility** | All **5** deltas are reviewed-path / publication-basis rules, not deletions. | **8** deltas are reviewed PE gate or missing publication basis; MVE moved **into** list. |
| **Missing / deleted** | All **5** IDs present in canonical store (57 total). | All **8** IDs present (77 total). |
| **Environment mismatch** | Live list **30** matches freeze (post-deploy note). | Live list **45** (post-deploy note): **38** list + **7** PE preferred-lodging rows above — aligns with **7** of the **8** broad−freeze PE IDs visible on production while **Shipyard** (`gdi_sc_188e25b6b3b56637`) likely remains excluded (missing basis). Not re-verified without auth tokens in this pass. |

---

## 5. Fresh local vs freeze vs live notes

| Hotel | Canonical total | Broad | List (`isCustomerFacingOpportunity`) | Freeze | Local ≡ freeze? |
|-------|----------------:|------:|-------------------------------------:|-------:|:----------------|
| Mayflower | 57 | 35 | 30 | 30 | **Yes** — identical ID sets for list and freeze |
| San Diego | 77 | 46 | 38 | 38 | **Yes** |

**Live production (derived, no new authenticated fetch):**

- Mayflower: **30** — consistent with freeze and local list gate.
- San Diego: **45** documented after deploy `fed35cc7` — **+7** vs freeze, consistent with production re-exposing the seven gated PE partnerships while list gate on canonical JSON remains **38**.

---

## 6. Evidence gaps

| Gap | Impact |
|-----|--------|
| No time-stamped **production list API JSON** archived for every user-visible count (only notes: **30** / **45** post-deploy). | Cannot replay exact live ID set for San Diego **45** in this pass. |
| Domain integrity **live** read at 11:50Z uses **broad stamp**, not list gate — explains why QA saw **35** / **46** while freeze/list use **30** / **38**. | Historical “46” headlines may cite integrity or broad manifest, not freeze. |
| `facing-inventory-freeze.json` lives under **gdi-release-main** in this workspace; deploy repo may not carry a copy. | Freeze path cited explicitly above; re-freeze requires running the history tooling in a repo that holds the file. |
| Per-field Airtable change ledger not consulted for every row. | Timestamps use canonical `updatedAt` only; finer-grained publish events may exist in weekly refresh runs under `data/group-demand-intelligence/hotels/{hotelId}/weekly-refresh/`. |

---

## 7. Actions explicitly out of scope

- Do **not** widen the customer list gate to force **35** / **46** totals.
- Do **not** restore the eight San Diego PE rows or Shipyard into the **38** freeze set without product approval and gate evidence.
- Do **not** treat broad stamp as the salesperson-facing inventory definition — **freeze / list gate** is the consistency anchor for type counts and exports (unchanged by this documentation pass).
