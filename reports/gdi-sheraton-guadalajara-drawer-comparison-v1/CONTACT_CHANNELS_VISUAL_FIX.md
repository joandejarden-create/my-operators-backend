# BLOCKER FIX — Recovered contacts now appear in actual drawer

**Status:** READY FOR FOUNDER VISUAL REVIEW (local only)  
**PR:** #462 still OPEN — Integration Controller governance; no competing PR  
**Persistence:** dry-run upserts prepared — **not executed**

## 1. Root cause

The live `:8095` process was serving a **stale Node module graph** from before public-channel recovery was wired into `projectDrawerComparisonPair`.

| Stage | What founder saw | Truth |
| --- | --- | --- |
| PUBLIC SOURCE | euroguss-mexico.com/contacts has email/phone | Correct |
| Audit / MATRIX.json | Recovery reported as success | Correct — but audit ≠ drawer |
| Live API `scalingTwoContactPresentation.primary` | `email/phone/org` empty, `surfe=both` | Stale server had no live recovery |
| Contact card renderer | Only name + Find Email & Phone | Correctly rendered empty DTO |

Secondary hardening (also fixed):

1. Always rebuild two-contact presentation after recovery (avoid stale slot object).
2. Surfe CTA derives from **actual** email/phone presence (never keep a stale `both` flag).
3. Attach `officialContactUrl` / source from the verified public page.

Not caused by: hardcoded HTML, wrong opportunity ID, or renderer ignoring field names once the DTO is populated.

## 2. Files corrected

- `lib/group-demand-intelligence/presentation/gdi-sheraton-drawer-comparison-v1.js` — always re-apply who-to-contact after recovery; LEFT unchanged
- `lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js` — person-scoped mailto/phone; source URL on contact
- `lib/group-demand-intelligence/scaling-two-contact-standard-v1.js` — Direct Business / Organizational labels; generic inbox label
- `public/js/group-demand-intelligence/dealality-gdi-ui.js` — Surfe channel from live gaps only
- `public/group-demand-intelligence-drawer-comparison.html` — cache `v9-contact-channels-live`

## 3. Contact DTO — EUROGUSS `paid3_565ce4588e6d`

**LEFT / BEFORE**

```json
{ "email": null, "phone": null, "organizationEmail": null, "surfeEnrichmentChannel": "both" }
```

**RIGHT / AFTER**

```json
{
  "email": "gissella.palma@nmna.us",
  "emailLabel": "Direct Business Email",
  "phone": "+1 (770) 618-5833",
  "phoneLabel": "Direct Business Phone",
  "organizationEmail": "info@euroguss-mexico.com",
  "organizationEmailLabel": "Organizational Contact Route",
  "officialContactUrl": "https://www.euroguss-mexico.com/contacts",
  "surfeEnrichmentChannel": null
}
```

Classification remains relevant influencer (hotel-sourcing authority not invented).

## 4. Browser acceptance

- URL: http://localhost:8095/group-demand-intelligence-drawer-comparison.html?hotelId=recCAGqkERSXgiku4&opportunityId=paid3_565ce4588e6d&v=cmp9channels
- Server: ws-gdi on **8095** (`DEALALITY_ALLOW_WORKTREE_SERVER=1`)
- Screenshot: `screenshots/04-euroguss-contact-channels-left-right.png`
- DOM verified: RIGHT has email + phone + org route; **no** Find Email & Phone; LEFT retains Find Email & Phone

## 5. Sheraton inventory (live projection)

From `CONTACT_CHANNELS_LIVE_VALIDATION.json`:

| Metric | Count |
| --- | ---: |
| Opportunities | 79 |
| AFTER direct email | 11 |
| AFTER direct phone | 6 |
| AFTER org route | 12 |
| Recovery changed this pass | 19 |
| Upserts proposed (dry-run) | 20 |
| EUROGUSS regression | **PASS** |

## 6. Persistence

- Public-source upserts prepared only (`surfeDerived=false`)
- **Execute blocked** until founder-authorized persistence gate
- Surfe remains session-only / on-demand

## 7. PR state

- #462 OPEN — do not merge from GDI chat
- Local dirty work on `feat/gdi-commercial-fact-selection-v1` — await IC

## STOP

Founder visual review required before declaring complete.
