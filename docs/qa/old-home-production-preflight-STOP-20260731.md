# Old Home Production Preflight — STOP (no publish)

**Verdict: Do not publish. Gates 1 and 2 failed.**

Production publish was **not** executed.

## Timeline anchors

| Event | Timestamp (UTC) |
| --- | --- |
| Last production publish (www + apex) | `2026-07-31T15:18:27.790Z` |
| Staging-only publish (validated ecosystem package) | `2026-07-31T17:29:13.149Z` |
| Site `lastUpdated` at preflight | `2026-07-31T17:47:56.902Z` |
| Old Home `lastUpdated` at preflight | `2026-07-31T17:43:53.253Z` |

## PRE-PUBLISH GATE 1 — Unpublished-change audit

### Pages (133 scanned)

Only **Old Home** (`68108c2a063eeb5d1bd7ae90`, `/old-home`) updated after last production publish.

**Blocking:** Old Home and site Designer state continued updating **after** the staging-validated publish (`17:29:13Z`). Production would include unpublished Designer deltas that were **not** hosted-validated on staging.

### Site scripts (applied)

- `lastUpdated`: `2026-07-31T15:18:06.916Z` (at/before last prod publish)
- No applied-script delta since production publish

### Custom code

| Location | Status |
| --- | --- |
| Old Home page head | Testimonials CSS + `dealality-old-home-ecosystem.v20260731i.css` |
| Old Home page footer | Deal Desk Phase B2 script `old-home-problem-deal-desk.v1b.js` |
| Site head/footer freeform | Unchanged relative to expected site chrome (GTM, FOUC, BUYERS redirect, Memberstack, Railway auth, Phase1 storyboard **commented out**) |
| Page applied scripts API | 404 / none |

### CMS

| Collection | lastUpdated |
| --- | --- |
| Users | `2025-09-21T20:56:54.887Z` |
| Insights Posts | `2026-05-28T21:33:11.565Z` |

No CMS items newer than last production publish.

### Forms / webhooks / redirects / domains

- Webhooks: none
- Forms: many form records; staging-domain copies bumped at staging publish (`17:29:12Z`) — expected on site publish; production-domain copies remain at `15:18:2x`
- Redirects API: not available (Enterprise)
- Domain configuration: not modified

### Gate 1 result

**FAIL / STOP** — unpublished Old Home Designer changes after staging validation would ship with any production publish.

## PRE-PUBLISH GATE 2 — Staging / production parity

Runtime check: `https://mvp-deal-capture.webflow.io/old-home` (cache disabled, US geo redirect hosts aborted).

| Check | Required | Staging runtime | Production hosted |
| --- | --- | --- | --- |
| Deal Desk `data-visual="cinematic-v1"` | Yes | **0** | Present (`data-oh-problem="deal-desk"`) |
| `#about` marker | deal-desk cinematic | `data-oh-problem="manual-process"` | `data-oh-problem="deal-desk"` |
| Phase B cinematic CSS | Yes | **Absent** (manual-process CSS instead) | `oh-deal-desk-cinematic-v1-phaseB2.css` |
| Deal Desk JS v1b | Yes | Loaded | Loaded |
| Replay control | Yes | **No Replay button found** | Expected on cinematic host |
| Reduced-motion → outcome | Present | No cinematic host | N/A this gate |
| Init guard / no Phase1 storyboard live | Yes | Storyboard script not live | Storyboard commented in site footer |
| `#oh-pvl` count | 1 | **1** | (injected) |
| PVL hides during `#about` | Yes | **FAIL** — remained `is-visible` while `#about` centered | Not re-tested (stopped) |
| Ecosystem `owner-advisor-led` | Yes | Pass | Fail (still old `#eco-grid`) |
| Ecosystem CSS `v20260731i` | Yes | Pass (200) | Absent |
| No `#eco-grid` | Yes | Pass | Fail (old) |
| Probes / debug / QA query | None | Pass | — |

**Critical:** Publishing current Designer/staging package to production would **replace** production cinematic Deal Desk (`deal-desk` + `cinematic-v1` + Phase B CSS) with staging **manual-process** Problem section. That is not the Gate 2 intended package.

### Gate 2 result

**FAIL / STOP**

## PRE-PUBLISH GATE 3 — Root safety baseline (recorded; publish not run)

| Item | Value |
| --- | --- |
| Root Home page ID | `6a234ea31be55631d54044d8` |
| Root Home `lastUpdated` | `2026-07-30T13:46:47.675Z` |
| Homepage assignment | Home publishedPath `/` (unchanged); Old Home remains `/old-home` |
| Production `/` | Railway/root experience; title `Dealality \| Hotel Owner, Operator & Brand Deal Platform`; sha256 `89bc2628329413ef84e5…`; not Old Home |
| Apex → www | `dealality.com` → `www.dealality.com` |
| Redirects | API unavailable; no domain/config changes made |

## PRE-PUBLISH GATE 4 — Intended publish payload (**NOT EXECUTED**)

Intended domains only:

- `www.dealality.com` (`69c5620ab5bfde0ecff71575`)
- `dealality.com` (`69c56209b5bfde0ecff71549`)

Webflow staging subdomain: **would also be selected** via `publishToWebflowSubdomain: true` so staging stays aligned with the production compile (same as prior controlled publishes). Domain settings would not be changed.

```json
{
  "site_id": "68108c29063eeb5d1bd7ae4a",
  "customDomains": [
    "www.dealality.com",
    "dealality.com"
  ],
  "publishToWebflowSubdomain": true
}
```

Equivalent domain ID form (also valid for this API):

```json
{
  "site_id": "68108c29063eeb5d1bd7ae4a",
  "customDomains": [
    "69c5620ab5bfde0ecff71575",
    "69c56209b5bfde0ecff71549"
  ],
  "publishToWebflowSubdomain": true
}
```

**Not executed.**

## Production publish

**Skipped.**

| Field | Value |
| --- | --- |
| Publish timestamp | n/a — not published |
| Publish payload | n/a — not published |
| Production `/old-home` | still `https://www.dealality.com/old-home` at production compile `15:18:27.790Z` |

## Post-publish QA

**Not run** (no production publish).

## Final recommendation

**C. Rollback required** is not applicable (nothing published).

Operational recommendation: **Stop — do not publish.** Closest report bucket for “not production-verified / correction required before any publish”:

**B. Small production correction required** — more precisely: **pre-publish correction required**:

1. Freeze Designer edits on Old Home.
2. Restore/confirm staging hosted package matches the approved cinematic Deal Desk Phase B + PVL about-hide + ecosystem `v20260731i` package.
3. Staging-only republish and re-QA.
4. Re-run Gates 1–4; only then execute the payload above once.

Artifacts: `/opt/cursor/artifacts/prod-preflight-20260731/`
