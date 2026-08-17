# Census Amenities Overnight Run — Morning Audit Report

**Generated:** 2026-07-05 (UTC)  
**Scope:** Tier 1 (Marriott + Hilton), Tier 2 (Choice), Tier 3 (IHG, Wyndham, Hyatt, Accor)  
**Policy:** Plan → validate → apply; fill-blank only; no invented amenities or field names.

---

## Executive summary

| Tier | Parent(s) | Amenities written overnight | Website / ID backfill | Status |
|------|-----------|----------------------------|------------------------|--------|
| **1** | Marriott International | **~634** (prior session + straggler re-run) | Already strong pre-run | **89% filled** (550/616) |
| **1** | Hilton Worldwide | **~63** (17 enrichment + 37 sync + 9 blank patches, prior session) | Partial via enrichment | **74% filled** (263/355); refresh-index **still running** |
| **2** | Choice Hotels | **0** (fetch blocked) | **17** Website + Property ID applied | **0% amenities**; websites 2→19 |
| **3** | IHG, Wyndham, Hyatt, Accor | **0** | **0** | Blocked — no census Website/Property ID to fetch from |

**All-census amenities fill:** 817 → **818** (+1 Marriott straggler) = **5.2%** of 15,644 rows.

No amenity text was written without a verified brand-site HTML parse. Choice amenity fetch and Tier 3 amenity pipelines were **not applied** due to blockers documented below.

---

## Before / after — amenities by parent (open properties)

| Parent | Total | Filled (before) | Filled (after) | Δ | Open blank |
|--------|-------|-----------------|----------------|---|------------|
| Marriott International | 616 | 549 | **550** | +1 | 17 |
| Hilton Worldwide | 355 | 263 | **263** | 0* | 1 |
| Choice Hotels International | 191 | 0 | **0** | 0 | 177 |
| IHG Hotels & Resorts | 335 | 0 | 0 | 0 | 248 |
| Wyndham Hotels & Resorts | 332 | 0 | 0 | 0 | 239 |
| Hyatt Hotels Corporation | 184 | 0 | 0 | 0 | 132 |
| AccorHotels | 567 | 0 | 0 | 0 | 468 |

\*Hilton `--refresh-index --apply` job was **in progress** (10/26 brands crawled at report time). Expect additional amenity updates when it completes.

**Audit artifacts:**
- `reports/census-amenities-by-parent-audit.json`
- `reports/census-amenities-blank-rows.csv`

---

## Tier 1 — Marriott & Hilton

### Marriott (completed)

**Primary run:** `node scripts/run-marriott-census-scale-backfill.mjs --apply --skip-harvest`

| Phase | Updates |
|-------|---------|
| Bazaarvoice descriptions | 266 |
| Subpage amenities | 367 |
| Overview HTML import | 1 |
| **Total writes** | **634** |

**Straggler re-run:** `--apply --skip-harvest --skip-sitemap` → 0 new subpage rows (91 skipped — already filled or no fetch). One overview HTML update on final pass.

**Remaining gaps (Marriott CALA census, ~621 rows with Website):**
- **66** rows still blank Amenities (17 open)
- **46** blank Website / Property ID (legacy census rows without marriott.com URL)
- Summary: `reports/marriott-census-scale-backfill-summary.json`

### Hilton (partial + in-flight)

**Completed earlier in session:**
- `apply-hilton-census-enrichment.mjs` — 17 rows (website + Y/N flags)
- `apply-hilton-enrichment-amenities-blank.mjs` — 9 amenity patches
- `sync-hilton-census-amenities.mjs --apply` — 37 amenity text updates (index size ~1,849)

**In flight at report time:**
```text
node scripts/sync-hilton-census-amenities.mjs --apply --refresh-index
```
Progress: **10/26** brand directory crawls. Allow ~20–40 min to finish.

**Steward gap list:** `reports/hilton-census-field-gaps-steward.csv` — **92 rows** missing `ctyhocn` (mostly Pipeline status, no Hilton directory match). These cannot receive amenities until Property ID / website backfill.

---

## Tier 2 — Choice Hotels

### What ran

1. Fixed matcher: `lib/hotel-census/plan-choice-census-sitemap-match.js` now loads JSON extract with city/country/brand metadata + `deriveInferredHotelName()`.
2. Dry-run → **17** census rows matched to verified `choicehotels.com` sitemap URLs (96 CALA-included URLs in directory).
3. **Applied 17 rows** — fill-blank `Website` + `Property ID` only (no Amenities):
   - `node scripts/run-choice-census-amenities-overnight.mjs --apply`
   - Log: `reports/choice-census-overnight-apply-log.json`
   - Plan: `reports/choice-census-sitemap-match-plan.json`

### Amenity fetch — BLOCKED

| Probe | Result |
|-------|--------|
| Server fetch (`fetchChoiceHotelAmenities`) | HTTP **403** / empty body |
| Puppeteer headless | ~344 bytes (Akamai block) |
| Sample probe (`probe-choice-amenities-sample.mjs`) | **0 amenities** from Cancun + Luquillo URLs |

**Conclusion:** Do not write Choice Amenities from this environment until fetch works (residential browser, approved API, or manual steward pass).

### Match quality — steward review required

Of **17 applied** Website matches:

| Confidence | Count | Action |
|------------|-------|--------|
| medium | 2 | Comfort Inn & Suites Levittown (PR); Park Inn by Radisson Puerto Varas (CL) |
| low | 15 | Score 51–64; name/geo heuristic only |

URLs are **official sitemap URLs** with matching country/city slug + Choice property ID — not hallucinated. However **15/17 are low-confidence** name matches. Recommend spot-check in Airtable before using for product-facing links.

**174 Choice census rows** still unmatched (below score threshold or no CALA sitemap URL). Many are Brazil-heavy; sitemap CALA filter only includes **96** properties.

---

## Tier 3 — IHG, Wyndham, Hyatt, Accor

### Census readiness (live probe)

| Parent | Census rows | With Website | With Amenities |
|--------|-------------|--------------|----------------|
| IHG Hotels & Resorts | 336 | **0** | 0 |
| Wyndham Hotels & Resorts | 332 | **1** (wrong domain — hotelequities.com) | 0 |
| Hyatt Hotels Corporation | 184 | **0** | 0 |
| AccorHotels | 567 | **0** | 0 |

**Marriott comparison:** 575/621 had Website + Property ID — why Marriott pipeline worked.

### Brand-site fetch probes (`probe-tier3-brand-pages.mjs`)

| Chain | Sample URL | HTTP | Notes |
|-------|------------|------|-------|
| Wyndham | wyndhamhotels.com/.../overview | 404 | URL pattern may have changed; sitemap.xml **is** reachable |
| IHG | ihg.com/.../hoteldetail | 200 | HTML large; `amenity-list__item` present when not blocked; GraphQL endpoint in page source |
| Hyatt | hyatt.com/... | **429** | Rate limited from server IP |
| Accor | (not probed live) | — | Needs locator/sitemap research |

### Recommended Phase 0 (not executed overnight)

For each Tier 3 parent, replicate Marriott **Phase 0**:

1. Harvest property sitemap / locator index (Wyndham sitemap accessible).
2. Match to census by name + Dealality country/city + geo (not STR Market).
3. Apply fill-blank **Website** + **Property ID** only.
4. Only then build amenity fetchers from verified URLs.

**No Tier 3 amenity writes were attempted** — would violate no-hallucination policy without step 1–3.

---

## Quality audit checklist

| Check | Result |
|-------|--------|
| All writes fill-blank only | ✅ Verified in apply scripts |
| No invented Airtable field names | ✅ `Website`, `Property ID`, `Amenities` only |
| `Brand Property Code` column | ❌ Does not exist in live base (422 if used) |
| Choice amenities from HTML | ❌ Not written (fetch blocked) |
| Tier 3 amenities | ❌ Not attempted (no websites) |
| Marriott/Hilton amenities source | ✅ Official marriott.com / hilton.com parsers only |
| Spot-check sample | ⚠️ Choice 15/17 low-confidence — manual review advised |

---

## Questions / decisions for morning review

1. **Choice low-confidence matches (15 rows):** Accept sitemap URL + property ID as ground truth, or revert and require medium+ confidence only?
2. **Choice Akamai bypass:** Run amenity fetch from your local machine / approved browser session, or wait for Choice source-policy sign-off (JSON extract notes say `review_required`)?
3. **Choice CALA scope:** Expand matching to **265 uncertain** + non-CALA sitemap URLs for Brazil-heavy census, or keep CALA-only?
4. **Hilton Pipeline rows (92 without ctyhocn):** Prioritize website backfill from Hilton directory, or exclude Pipeline from amenity SLA?
5. **Tier 3 priority order:** Wyndham first (sitemap accessible) vs IHG (largest CALA count)?
6. **Wyndham bad website:** Row with `hotelequities.com` — clear as steward error?
7. **Independents (11,520 blank parent):** Confirm OSM dry-run-only policy; no OTA scraping.

---

## Challenges encountered

| Issue | Impact | Mitigation |
|-------|--------|------------|
| Choice Akamai 403 | No amenity fetch | Applied Website/ID only; documented blocker |
| Choice matcher used brand-only name initially | 0 matches | Fixed to JSON metadata + inferred hotel name → 17 matches |
| Tier 3 zero Website in census | Cannot fetch amenities | Phase 0 sitemap match needed per chain |
| Hyatt 429 rate limit | Probe inconclusive | Retry with delay / residential IP |
| Hilton refresh-index runtime | Report incomplete for Hilton Δ | Re-run audit after job completes |
| 11,520 blank Parent Company | Out of scope for brand pipelines | Documented; needs separate strategy |

---

## Commands to re-run / continue

```bash
# Fresh fill-rate audit
node scripts/audit-census-amenities-by-parent.mjs --csv

# Hilton sync (if not finished)
node scripts/sync-hilton-census-amenities.mjs --apply --refresh-index

# Marriott stragglers
node scripts/run-marriott-census-scale-backfill.mjs --apply --skip-harvest --skip-sitemap

# Choice — dry-run match only
node scripts/run-choice-census-amenities-overnight.mjs

# Choice — apply websites only (no amenities)
node scripts/run-choice-census-amenities-overnight.mjs --apply

# Tier 3 census website coverage
node scripts/probe-parent-census-websites.mjs "IHG"
```

---

## Files created / updated this session

| File | Purpose |
|------|---------|
| `lib/hotel-census/plan-choice-census-sitemap-match.js` | Choice sitemap → census match (fixed) |
| `lib/choice-hotel-content-fetch.js` | Choice HTML amenity parser (blocked at fetch) |
| `scripts/run-choice-census-amenities-overnight.mjs` | Choice orchestrator |
| `scripts/audit-census-amenities-by-parent.mjs` | Fill-rate audit |
| `scripts/probe-tier3-brand-pages.mjs` | Tier 3 fetch probe |
| `scripts/probe-parent-census-websites.mjs` | Census website coverage |
| `reports/census-amenities-overnight-run-2026-07-05.md` | This report |

---

## Regression risks

- **Choice Website URLs:** 15 low-confidence matches — wrong property link if name collision in same city.
- **Marriott/Hilton:** Fill-blank only; low risk of overwrite.
- **Hilton refresh-index:** May update amenity text on open properties — verify against hilton.com if disputes arise.

**Retest:** Hotel Census Airtable views filtered by Parent Company; spot-check 5 Marriott, 5 Hilton, 5 Choice Website URLs in browser.
