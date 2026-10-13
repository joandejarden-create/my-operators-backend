# PDF Content QA — Bethesda GDI (Oct 1 snapshot semantics)

**PDF:** `reports/bethesda-pilot/day2/2026-10-02/gdi-client-pdf-p0-fix/Bethesda-Marriott-GDI-corrected.pdf`  
**Pages:** 16  
**Scan:** `PDF_CONTENT_SCAN.json`

---

## 1. Duplicate Future Watch — NCI RNA Biology Symposium

**Finding:** “2027 NCI RNA Biology Symposium — Hotel & Travel open” appeared twice on Future Watch.

**Cause:** Duplicate **display** of same series / near-identical FUTURE_WATCH cards (not two intentional cycles with distinct customer titles).

**Fix:** `dedupeFutureWatchCards` in `gdi-pdf-report-data-v1.js` collapses by normalized title key. Store opportunities untouched.

**Scan:** `rnaWatchCount: 1`

**Verdict:** FIXED (duplicate rendering)

---

## 2. Malformed NCI title

**Finding:** Scraped chrome in title — “NCI RNA Biology Symposium | NCIF Conferences Skip to main content …”

**Fix:** `customerDisplayTitle()` strips NCIF / Skip-to-main / NCI-at-Frederick nav debris; prefers clean `eventName` when title is dirty. Evidence store unchanged.

**Scan:** `rnaDirtyTitle: false`

**Verdict:** FIXED

---

## 3. ACTS contact consistency

**Finding:** Named person Adam Joyce with email `kstelmaszak@actscience.org`.

**Verification:** Local part does not match Adam Joyce — treated as **organization contact path**, not personal email.

**Customer display:** provenance note — “Email is an organization contact path; it is not confirmed as Adam Joyce's personal address.”

**Scan:** `actsProvenance: true`

**Verdict:** VERIFIED (no fabrication; provenance disclosed) → PASS for client delivery with note

**ACTS CONTACT VERIFIED:** YES

---

## 4. ACC Legislative Conference segment

**Finding:** Appeared as “University” in Top Immediate Pursuits.

**Cause:** Mapping bug — “American College of Cardiology” / “college” regex ordered before Association.

**Fix:** Association / legislative classifiers run **before** University in `segmentOf()`.

**Scan:** `accUniversity: false`, `accAssociation: true`

**Verdict:** FIXED

**ACC SEGMENT VERIFIED:** YES (Association)

---

## Cover / body visual gates

| Gate | Result |
|------|--------|
| Cover matches canonical family | YES |
| Title rendering | PASS |
| Date format (human) | PASS |
| Bare “DMV” | NO |
| Duplicate watch | RESOLVED |
| Malformed NCI title | RESOLVED |
| Blank pages | NO (16 content pages) |
| Body redesign | NOT DONE (content-only QA fixes) |
