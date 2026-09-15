# Bethesda GDI Pilot — Deepen + Local Review + Share (continuation)

**Date:** 2026-09-12  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Verdict:** **PARTIAL GO**

---

## 1. Updated research results

| Metric | Before ($5) | After ($15) | Delta |
|--------|-------------|-------------|-------|
| High Priority | 3 | 5 | +2 |
| Medium | 6 | 8 | +2 |
| Watchlist | 4 | 5 | +1 |
| Disqualified | 9 | 9 | 0 |
| Qualified | 13 | 18 | +5 |

### Medium → High
- **NICE 2027** — venue still TBA; FIU/New America ops; Marriott-family host pattern; ~400–450 attendance; `nice@nist.gov` / 301-975-4470

### New High (discovery)
- **ACTS Translational Science 2027 (TS27)** — DC confirmed, hotel TBA, NIH-adjacent, est. 150–250 peak rooms

### Deepened Medium (kept Medium — honest ceilings)
- **SHOW 2026** — Jessica.Mitchell@nih.gov; reg through Sep 21; Natcher lodging precedent lists Bethesda Marriott; free/hybrid → no contracted-block promote
- **AFCEA 2027** — host closed at Bethesda North; overflow + relationship path; named board contacts
- **AHIMA / NDSS / ACC / CMSS** — new association discoveries (Cap-Hill geo discounted where needed)

### Downgraded
- **MSYSA State Cup** Medium → Watchlist (weak geography vs SoccerPlex; no stay-to-play)

### Corporate / gov contractor
- Public Webhound pass: **cannot reliably surface named group leads** without CRM/Delphi. Documented on category watch; **no fabricated corporate opportunities**.

---

## 2. Webhound economics

| Item | Amount |
|------|--------|
| Previous spend | $5.00 (wave 1) |
| Additional spend | $10.00 |
| Total | **$15.00 / $15 hard cap** |

### Calls
| Session | Focus | Cost | Useful? |
|---------|-------|------|---------|
| `4f99b00b-…` | Wave 1 seed enrichment | $5 | Yes (baseline High set) |
| `1716a70c-…` | Deepen NICE/SHOW/AFCEA/MSYSA | $5 | **Yes** — NICE promote + contacts + honest downgrades |
| `ab352b86-…` | Association discovery + corporate test | $5 | **Mixed** — ACTS + 4 associations useful; corporate = valuable negative |

### Incremental commercial result
- New actionable High: **NICE promotion + ACTS**
- New CONTACT_NOW Mediums: SHOW deepened, AHIMA, NDSS
- Cost per net new High-ish actionable lead: about **$10 / 2–3 ≈ $3–5**
- Did the second $10 materially improve sales intelligence? **Yes for associations/medical; No for corporate public prospecting.**

### Recommendation
**Keep at $15** for this workflow pattern (deepen Mediums + targeted association discovery).  
Do **not** increase further for corporate/gov-contractor public scraping.  
Stop Webhound for corporate until Delphi/CRM is in the loop.

---

## 3. Local review

### Environment flags
```
GROUP_DEMAND_INTELLIGENCE_V1=1
GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1
PORT=8080
GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1   # local share mint only
```

### Command
```bash
npm start
# or: node server.js
# note: npm run dev kills port 3000 but server still listens on PORT (default 8080)
```

### URL
**http://localhost:8080/group-demand-intelligence**

Verified: Weekly Brief first, KPIs (5/8/5, $15, 40 sources), opportunity detail drawer, Research Audit tab internal-only on admin page.

Pilot read APIs work without Memberstack when flags are on (`middleware/gdiPilotReadAuth.js`).

---

## 4. External share URL

### Local (verified)
```
http://localhost:8080/group-demand-intelligence-share.html?share=gdishare.v1.…
```
Token id: `gdisht_ba78a21714ea542573cdaafc`  
Expires: **2026-12-31**  
Mode: **read_only**  
Surfaces: brief, opportunities, opportunity_detail, summary  
**No** Research Audit, **no** Run Research

### Auth
Share token only (no Memberstack). Hotel-scoped.

### Revoke
```bash
npm run gdi:revoke-share -- --token-id=gdisht_ba78a21714ea542573cdaafc
```

### Production
Same path on the live Dealality host once deployed with `GDI_SHARE_CAPABILITY_SECRET` set (do not use dev secret in production). Example shape: `https://dealality.com/group-demand-intelligence-share.html?share=…`

---

## 5. Build changes (this continuation)

### Added
- `lib/group-demand-intelligence/discovery-opportunities-v2.js`
- `middleware/gdiPilotReadAuth.js`
- `lib/group-demand-intelligence/share/*` (prior + this wave)
- `public/group-demand-intelligence-share.html` + `share-app.js`
- `scripts/gdi-issue-share.mjs`, `scripts/gdi-revoke-share.mjs`
- Research session meta under `data/group-demand-intelligence/research-artifacts/`

### Modified
- Webhound hard cap **$15** (`claim-types`, `cost-ledger`, scoring config)
- `deepen-pass.js` — L5 deepen + discovery merge + ROI log
- `research-orchestrator.js` — multi-session Webhound accounting
- `scripts/gdi-bethesda-pilot-run.mjs` — `--full-pilot-spend`
- `scripts/test-gdi-foundation.mjs` — $15 + multi-session + share gates
- `public/js/group-demand-intelligence/app.js` — pilot unauth fetch
- `server.js` — GDI read auth + share routes
- UI Weekly Brief / KPI polish

### Migrations
None (file-backed GDI namespace only; ADP untouched).

### Tests
`npm run test:gdi-foundation` — all PASS including `$15` cap and share isolation.

---

## 6. Product verdict

**Overall: PARTIAL GO**

| Question | Answer |
|----------|--------|
| Research quality — would a DOS use this? | **Yes for the top 5 High + deepened SHOW/AFCEA.** Cap-Hill advocacy Mediums are secondary. |
| UI — comfortable showing Rad? | **Yes locally** (Weekly Brief-first, Pilot label, detail drawer). |
| External sharing — safe enough? | **Yes for tokenized read-only** (no audit/run). Deploy + real secret before sending outside localhost. |
| Economics — did $5→$15 help? | **Yes for association depth/discovery; No for corporate public leads.** Keep $15. |

**Still not ADP-integrated** (by design).
