# AVA V3 — Customer PDF Quality & Evidence-Link Certification

**Contract:** `AVA_V3_CUSTOMER_PDF_QUALITY_AND_LINK_CERTIFICATION_V1`  
**Generated:** 2026-11-05  
**Cohort:** Waterstone Resort & Marina · AC Hotel A Coruña · Castillo Hotel Son Vida  
**Paid research:** $0 · **Publication:** not authorized · **Deployment:** not authorized · **ADP measurements:** unchanged  

**Success verdict:** PASS for the three-hotel quality pilot (draft channel only). Every significant customer-facing recommendation is evidence-backed where a reachable source exists; Correction Map and grouped platform assessment are present; CQA + evidence gates PASS; published PDFs untouched.

---

## 1. Page-by-page defect register (pre-fix V3 drafts)

Inspected PDF bytes (not HTML alone). Pre-fix defects observed on the prior certified V3 drafts / first quality pass:

| Area | Defect | Hotels |
|------|--------|--------|
| Evidence links | Sparse clickable annotations on findings / early action pages; many claims without human-labeled destinations | All 3 |
| Platforms | Dense rows of `IDENTIFIED_NOT_ASSESSED` without listing URLs or research notes | All 3 |
| Correction Map | Missing or weak central correction table | All 3 |
| Labels | Generic “Evidence” / raw URLs hard to scan | All 3 |
| Maps / GBP | Maps search URLs treated as verified listings | Waterstone, AC, Castillo |
| Cross-hotel risk | First discovery pass briefly cross-assigned OTA/brand URLs across hotels (fixed before final staging) | AC / Waterstone |
| Repetition | Findings restated at full length across findings + platforms + actions | All 3 |
| Visual | Oversized unassessed platform tables; weak hierarchy between corrections vs inventory | All 3 |
| Cover / Exec | Cover + Executive Audit Summary structure OK — **preserved** | All 3 |

Machine page inspection artifact: `AVA_V3_PDF_PAGE_INSPECTION_SNAP.json`.

### Final staged draft structure (Waterstone example)

| Page | Content (observed) | Links |
|------|--------------------|------:|
| 1 | Cover | 0 |
| 2 | Executive Audit Summary (preserved) | 0 |
| 3 | Action Overview | 0 |
| 4–6 | Key Digital Presence Findings (findings-first) | 1–3 |
| 7 | Digital Authority metrics + source links | 3 |
| 8–10 | Platform readiness + Correction Map + grouped rows | 9–17 |
| 11–20 | Actions / implementation / remaining evidence | remainder |

AC and Castillo follow the same section order at 20 pages.

---

## 2. Missing and repaired evidence links

| Change | Detail |
|--------|--------|
| Human evidence labels | `humanEvidenceLabel()` — e.g. View Hilton property profile, View Tripadvisor listing, View Booking.com listing, View hotel website, View destination listing |
| Platform listing URLs | Hilton / Marriott brand pages, OTAs, Tripadvisor, destination CVB pages promoted into customer tables when hotel-scoped |
| Correction Map | Platform · affected page · verified finding · recommended change · priority · owner · evidence link |
| Action evidence gate | CQA flags HIGH/CRITICAL actions lacking evidence URL unless labeled “further investigation” |
| Cross-hotel rejection | Pilot seeds scoped per `propertyId`; opaque Google Travel entity IDs and generic tokens (`hotel`, `resort`) no longer cross-assign |

**Not invented:** Apple Maps / Bing Places / Cvent remain `LISTING_NOT_FOUND_AFTER_RESEARCH` or `NOT_APPLICABLE` with research notes — not presented as verified defects.

---

## 3. Content improvements

- Findings lead with commercial conclusion; strengths separated from corrections.
- Platform section grouped: (1) verified corrections / investigations, (2) actionable inclusion/outreach, (3) additional platforms identified but not fully assessed.
- Management vs implementation separation retained via V3 implementation-by-owner blocks.
- Digital Authority distinguishes verified backlinks, mentions without links, and opportunities.
- Unassessed platforms no longer dominate the customer-facing table focus.

---

## 4. Visual refinements

- Correction Map elevated as the primary action table.
- Reduced emphasis on empty/unassessed inventory rows.
- Concise evidence link labels instead of raw URL walls where labels render.
- Executive Audit Summary unchanged in structure and KPI presentation.

---

## 5. Before / after comparison

| Hotel | Before pages | Before links | After pages | After links | Δ links |
|-------|-------------:|-------------:|------------:|------------:|--------:|
| Waterstone | 20 | 57 | 20 | 88 | +31 |
| AC A Coruña | 19 | 49 | 20 | 92 | +43 |
| Castillo Son Vida | 20 | 79 | 20 | 103 | +24 |

Prior versions preserved under each hotel’s `pdf-staged/pre-v3-quality-link/`.

| Gate | Waterstone | AC | Castillo |
|------|:----------:|:--:|:--------:|
| Evidence validation | PASS | PASS | PASS |
| PDF link annotations present | PASS (88/92/103) | PASS | PASS |
| Correction Map in PDF | PASS | PASS | PASS |
| Grouped platform sections | PASS | PASS | PASS |
| CQA overall | PASS | PASS | PASS |
| Preview/Download draft SHA parity | PASS (draft.pdf = staged SHA) | PASS | PASS |
| Published PDF untouched | PASS (`8173ca19…`) | PASS (`7ed9c2e0…`) | PASS (`76944045…`) |

---

## 6. Updated PDF SHAs (draft channel)

| Hotel | Draft SHA-256 | Filename |
|-------|---------------|----------|
| Waterstone Resort & Marina | `a56bc52bd854f30e11734a750981f889815c9393df59c706e6970865db52e340` | Dealality - AI Visibility Audit & Action Plan - Waterstone Resort & Marina - NOVEMBER2026.pdf |
| AC Hotel A Coruña | `6042d239f1673017d41e8b21b1205fbfcac08fee8092664b2935955dc309050a` | Dealality - AI Visibility Audit & Action Plan - AC Hotel A Coruña - NOVEMBER2026.pdf |
| Castillo Hotel Son Vida | `09a577336dc3e82b0d89aa50b98a03d67f7a5cbade1c96f95b930a809789ff87` | Dealality - AI Visibility Audit & Action Plan - Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca - NOVEMBER2026.pdf |

**Admin:** use **New Draft** Preview / Download only. Do not publish.

---

## 7. Certification results

| Check | Result |
|-------|--------|
| Technical PDF certification (evidence + CQA) | PASS × 3 |
| Customer usefulness (Correction Map + listing URLs + action evidence) | PASS × 3 |
| Research-coverage certification | Separate — see `AVA_PLATFORM_RESEARCH_COMPLETENESS_CERTIFICATION.md` |
| Portfolio rollout (38 hotels) | **NOT AUTHORIZED** — founder review required |
| Customer publication | **NOT AUTHORIZED** |

---

## 8. Remaining limitations

1. Exact Google Business Profile place IDs are not resolved at $0 — Maps search URLs are labeled research aids (`RESEARCH_INCOMPLETE`), not verified listings.
2. Brand / Tripadvisor / Expedia often `LISTING_FOUND_ACCESS_RESTRICTED` under automated fetch; URLs are still customer-clickable.
3. Field-level fact accuracy vs Hotel Facts Register still needs hotel confirmation.
4. Some early pages (cover, exec, action overview) intentionally have few/no annotations; evidence concentrates on findings, platforms, authority, and actions.
5. Apple Maps / Bing Places discovery remains weak without paid geo APIs.

---

## 9. Recommendation for portfolio rollout

**Hold.** Approve these three draft PDFs in Admin New Draft first. After founder sign-off on quality and platform-research completeness, authorize a staged expansion (next 3–6 hotels) before any 38-hotel regen. Do not publish or change ADP.
