# Active Brand CALA — Wave 9 (recommended next move)

**Generated:** 2026-07-24  
**Prior:** Waves 1–8  
**Scope:** Choice next-20 steward sprint pack · Chrome puppeteer pilot (BWH + Autograph)

## A — Choice next-20 steward sprint (primary)

Scored the **55** Choice browser-save rows still missing usable HTML; exported the **top 20**.

| File | Purpose |
|------|---------|
| `reports/choice-next20-steward-opener.html` | Click-to-open + exact save paths |
| `reports/choice-next20-steward-sprint.md` | Checklist + apply commands |
| `reports/choice-next20-steward-sprint.csv` | Ranked list |

**Top scores:** Ascend El Cid / Amberes (70), then Ascend TT/PR (65), then Comfort Brazil…

**Your steps (~30–60 min):**
1. Open `reports/choice-next20-steward-opener.html`
2. Save each as Webpage Complete to the listed path
3. `node scripts/backfill-choice-wave4-from-html.mjs` → then `--apply`

## B — Puppeteer pilot (5 properties)

Chrome channel + isolated profile `data/wave9-chrome-profile/` (gitignored).

| ID | Result |
|----|--------|
| BWH Premier Monterrey Aeropuerto (70262) | **PASS** — meta description + 10 amenities |
| BWH Aruba Signature (71032) | FAIL — blocked |
| BWH Libre Signature (76413) | FAIL — blocked |
| Marriott Autograph SJUAO | FAIL — blocked |
| Marriott Autograph CURAK | FAIL — blocked |

**Pass: 1/5 · Verdict: mixed — steward remains primary**

Report: `reports/wave9-puppeteer-pilot-report.md`

### Applied from the one pass
- **Best Western Premier Monterrey Aeropuerto** (`rec5kNnEy5ixge3bG`): fill-blank **Hotel Description** + **Amenities**
- Scripts: `scripts/run-wave9-puppeteer-pilot.mjs`, `scripts/apply-wave9-bwh-pilot-pass.mjs`

## Implication

- **Do the Choice next-20 sprint** — highest volume, proven apply path.
- **Do not scale Marriott/BWH puppeteer** after 1/5 — intermittent BWH HTML may work case-by-case; Autograph still Akamai-blocked.
- Optional: re-run BWH puppeteer only for remaining Premier/Signature rows that use `bestwestern.com` property URLs (skip captcha-heavy ones when first fetch blocks).

## Change impact

**Medium** — 1 census write (BWH Monterrey). Steward tooling only otherwise.

**Rollback:** clear Amenities + Hotel Description on `rec5kNnEy5ixge3bG` if needed.
