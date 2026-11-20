# Brand Explorer Factory Image Integrity V1 — Founder Deliverable

**Date:** 2026-11-20  
**Branch:** `feat/be-factory-image-integrity-v1`  
**STOP:** No merge / deploy / paid research / customer publication until founder review.

## Acceptance criterion

> The Brand Explorer Factory must never report an image as complete merely because an image URL exists.

**Implemented.** Only status `CERTIFIED` raises Certified Image Completion. Factory approval is blocked when certified coverage &lt; 100% of the 9 required slots.

## Deliverables checklist

| # | Deliverable | Location |
|---|-------------|----------|
| 1 | Complete image-integrity audit | `reports/.../00-audit-summary.json`, `01-brand-matrix.json`, `brands/*.json` |
| 2 | Root-cause findings | Per-brand `rootCauses` (expired Airtable CDN 410, fetch failures, visual review) |
| 3 | Updated Image Integrity Contract V1 | `docs/data-intelligence/brand-explorer-factory-image-integrity-contract-v1.md` |
| 4 | Factory pipeline integration | Orchestrator + batch approval + staging dashboard |
| 5 | Automated validation + recovery | `be-factory-image-integrity-v1.js` + `…-recovery-v1.js` + audit script |
| 6 | Updated certification + scoring | `certifiedImageCompletion` vs disclosed `legacyUrlPopulationCompletion` |
| 7 | Portfolio-level results | Dashboard `imageIntegrity` section + audit summary |
| 8 | Before/after browser screenshots | `browser/ac-hotels-by-marriott/01-proposed-materials.png` |
| 9 | Permanent regression tests | `npm run test:be-factory-image-integrity-v1` |
| 10 | PR and CI status | Open PR — **awaiting founder review before merge** |
| 11 | Recommended follow-up repairs | See below |

## Headline audit finding (correction disclosed)

| Metric | Observed (sample) |
|--------|-------------------|
| Previously claimed (URL population) | **~100%** on audited brands |
| Fully CERTIFIED brands (9/9) | **0** without browser+repair completion |
| Broken / expired URLs | Material — e.g. Airtable CDN **HTTP 410** on AC Hotels scenario/gallery slots |
| AC Hotels browser cert | **2 / 9** CERTIFIED on authenticated `:8080` |

**Root causes (ranked):**

1. **Expired Airtable attachment URLs (410)** — populated references that no longer fetch.
2. **Fetch failures / hotlink / CDN blocks** — URL present, bytes unavailable.
3. **Missing browser render evidence** — technical RENDER_VALID ≠ CERTIFIED until `:8080` proof.
4. **Visual / provenance review** — weak hotel cues or affinity (now softened for official Marriott/Hilton DAM hosts).

## Factory integration map

| Stage | Integration |
|-------|-------------|
| Discover / Select | Existing hotel image discovery + rejection ledger |
| Fetch / Decode | `fetchAndDecodeImage` + magic-byte dimension decode |
| Browser Render | `be-factory-image-integrity-browser-cert-v1.mjs` on `:8080` |
| Visual Validate | Property integrity + same-brand + hotel cues |
| Certify / Score | `scoreBrandImageIntegrity` on every factory `processBrand` |
| Approval | `recordBatchApprovals` refuses brands below certified threshold |

## Recommended follow-up repairs (no paid research yet)

1. Replace expired Airtable CDN URLs from durable `data/brand-explorer/factory-2-0/image-pools/*` and official brand DAM links.
2. Re-run `--apply-recovery --allow-discovery` for brands with broken&gt;0, then browser-cert on `:8080`.
3. Recertify Phase 3 writeback brands (AC Hotels first) under the new gate before further Presentation writes.
4. Expand audit to full frozen inventory (`--dry-run` without `--limit`).
5. After founder merge: wire CI `test:be-factory-image-integrity-v1` into BE PR checks.

## Safety

- `$0` paid research in this work  
- Customer publication **blocked**  
- No destructive Airtable deactivation  
- Dirty peer worktrees preserved  
- **STOP for founder review before merge**
