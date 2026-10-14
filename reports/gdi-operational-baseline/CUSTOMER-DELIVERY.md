# GDI customer-facing delivery — San Diego + Mayflower cleanup

**As of:** 2026-10-14 / 2026-10-15  
**Branch:** `gdi/parallel-provider-san-diego`  
**Airtable publication:** executed for Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)  
**Production UI deploy:** blocked on `railway login` (see §6)

---

## 1. What is published and visible (Airtable + local allowlist)

### San Diego — Wyndham San Diego Bayside

| Metric | Count |
|--------|------:|
| Customer-facing (reviewed package) | **21** |
| Priority | **19** |
| Exploratory (venue referrals) | **2** |
| Drawer contract pass (post URL-route align) | **21 / 21** |
| Prior production unmatched rows preserved | **25** |

**Contact-route types (facing):** email / phone / intermediary / functional_contact_url (housing & plan-an-event pages count). Named person optional; functional or intermediary routes accepted.

**Allowlist:** `GDI_REVIEWED_PROSPECT_HOTEL_IDS` must include `recnPB3Q02NO8Ma1o` (local `.env` updated). **Railway production env must set the same** or SD facing stays 0 after deploy.

Import artifact: `san-diego-publication-apply/import-result-apply-2026-10-14T22-44-09-976Z.json`  
Package: `san-diego-publication-package/`

### Bethesda / Mayflower

Title cleanup already applied via canonical importer. Mayflower Airtable → DTO display titles are clean occasion titles (e.g. `AHIMA Advocacy Summit 2027`) — no “Verified via…”, no org double under title when title already carries the account token.

---

## 2. Mayflower title screenshot root cause (fixed)

| Layer | Finding |
|-------|---------|
| Airtable payload | Titles were already cleaned by `title-cleanup-apply` |
| Title-field precedence | UI preferred raw `title` and scrubbed occasion years; DTO `displayTitle` was ignored or re-inflated |
| API mapping | List DTO `accountLevelDisplayTitle` now returns concise titles without org prefix |
| Running build | Production still on older JS until Railway deploy |
| Browser cache | Secondary; deploy + hard refresh required for production proof |

**Code fix:** `opportunity-list-dto.js` + `dealality-gdi-ui.js` (`preserveOccasionYear`, classification meta not org repeat, prefer `displayTitle`).

---

## 3. Private Events research (hotel-specific)

Completed without Bethesda fixtures / without DMV corpus as evidence.

| Prospect | Type | Section | Contact route |
|----------|------|---------|---------------|
| USS Midway evening private events | Venue lodging referral | Exploratory | plan-an-event URL |
| Port EMPS (Embarcadero Marina Park South) | Venue referral account | Exploratory | parkpermits@portofsandiego.org |

Gaps (optional paid): named Midway events-desk phone/email; additional dated yacht / Rady Shell / Pier PE occasions.  
Detail: `san-diego-pe-research/pe-hotel-specific-results.md`

---

## 4. What remains internal (and why)

| Item | Reason |
|------|--------|
| AEFP Annual Conference 2027 | No usable contact route at publish time (`ENRICHMENT_QUEUE`) |
| Program-derived drafts without contact/evidence | Editorial incomplete or geo unresolved (e.g. SITE Global city) |
| Wrong-market Parallel (pre-identity-lock) | Quarantined; preserved as evidence, not published |
| Shared DMV PE venues | Wrong-market quarantine for SD |
| Unconfirmed room blocks / booking authority claims | Not invented; cards stay referral / overflow framing |

---

## 5. Budget

| Item | Status |
|------|--------|
| Prior **$2.00** Parallel cap | **Exhausted** — ledger preserved, not reset |
| Additional paid research | **Not started** — see `COST-PROPOSAL-REMAINING-PAID-RESEARCH.md` (**$3.50** proposed) |

Non-paid work (qualification, PE open-web, publication, UI cleanup, deploy prep) continued without spend increase.

---

## 6. Deploy — user action required

```
railway whoami  → Unauthorized. Please login with `railway login`
```

### Exact terminal steps (interactive — you must run)

```bash
cd c:\Dev\dealality\my-operators-backend
railway login
railway whoami
railway status
```

Then set production env (if not already) so SD is allowlisted:

```bash
# Ensure these are present on the Railway service (values without secrets):
# GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1
# GDI_REVIEWED_PROSPECT_HOTEL_IDS=recG66DQJKP2c0UNh,recLuxvwwxID7U2B8,rec0eTEnCqlsGJJt9,recnPB3Q02NO8Ma1o
# GDI_OPPORTUNITIES_PERSISTENCE=airtable
```

Then deploy:

```bash
npm run deploy:production
```

### Production browser verification (after deploy)

Open production `/group-demand-intelligence.html` and confirm for **Bethesda**, **Mayflower**, and **San Diego**:

- Clean occasion titles; no Verified/source commentary in titles  
- Classification meta (not full legal org repeat under title when title already names the account)  
- Priority / Exploratory distinct from Action WATCH  
- Drawer: thesis, fit, sources, contacts, uncertainty, hotel feedback controls  
- Hotel switch + hard reload  
- Spot-check an unaffected hotel for presentation regressions  

Do **not** treat Airtable read-back or local screenshots as production delivery proof.

---

## 7. Repeatable coordinator path (next hotel)

1. Hotel profile / config (`config/group-demand-intelligence/hotels/<id>.json`)  
2. Reuse prior intelligence (unset `GDI_DATA_ROOT` pollution)  
3. Local discovery (Parallel only with explicit budget + identity lock)  
4. Independent hotel fit + **geo quarantine before fit**  
5. Contact resolution (functional / intermediary OK)  
6. Evidence-backed card + drawer (shared contract)  
7. Human review = quality decision on completed drafts (not a substitute for missing research)  
8. Canonical Airtable import (`gdi-import-reviewed-prospects` dry-run → apply)  
9. Allowlist hotel id in `GDI_REVIEWED_PROSPECT_HOTEL_IDS` (code default + env)  
10. Deploy + **production** browser verification  
11. Hotel sales feedback on published cards (do not invent)

**Still bespoke per hotel:** local PE venue discovery, market-specific geo vocabulary, contact packaging, sales-review overlay copy, publication allowlist + Railway env.

---

## 8. Single actions required from you

1. **`railway login`** then `npm run deploy:production`, and confirm Railway env includes San Diego in `GDI_REVIEWED_PROSPECT_HOTEL_IDS`.  
2. Optional: approve **$3.50** paid Parallel tranche in `COST-PROPOSAL-REMAINING-PAID-RESEARCH.md` (PE contact enrichment + AEFP route) — otherwise leave internal.
