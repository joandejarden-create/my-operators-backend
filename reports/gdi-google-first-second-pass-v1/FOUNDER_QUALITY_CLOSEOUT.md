# GDI — Google-first contact quality closeout (founder approval)

**Date:** 2026-11-20  
**Branch:** `feat/gdi-google-style-contact-search-before-surfe-v1`  
**PR:** #484 (Integration Controller owns merge)  
**No full-cohort re-research · No Surfe · No STAGED→CURRENT promotion**

## 1. Quality audit (SECOND_PASS_RESULTS.json)

Audited 90 second-pass rows against named-person association rules.

| Finding | Count / action |
|---------|----------------|
| Quarantined rows | **9** |
| Suspicious phones | Abramo `93201830234`; Brittany year-string; Colonial Dames ZIP-glued |
| Generic mailboxes demoted | Laura `hall@pda.org`; Colonial `comcoor@…`; Diego `pcchair@…`; Embarcadero `parkpermits@…`; IAFL `secretary@…` |
| Suspicious extensions stripped | Carmen area-code tokens → keep `70407`; Christi `213/211/210` dropped |

Evidence: `QUARANTINED_CHANNELS.json`, `QUALITY_CLOSEOUT.json`.

## 2. Airtable persistence

| Case | Resolution | Read-back |
|------|------------|-----------|
| **Maciel** `extensionsMatch=false` | Root cause: `slimContact` omitted `phoneExtensions`. Fixed allowlist + re-write | `extensionsMatch=true` · `["35222"]` on `recFsrSCFCNkKvKJE` |
| **Abramo** `EVIDENCE_LOST` | `QUARANTINED_NO_WRITE` (bad phone) | No write |
| **Laura** `CONTACTS_LOST` | `QUARANTINED_NO_WRITE` (generic mailbox) | No write |
| **IAFL** skipped | `SKIPPED_NO_DUPLICATE_CREATE` | No new row |
| Quarantine strip on prior OK writes | Colonial / Embarcadero / Brittany phone / Diego email / Carmen exts | Verified read-back in QUALITY_CLOSEOUT |
| Diego ext `37760` | Re-persisted after slim fix (phone kept, generic email cleared) | phone + `37760` |

Slim durability also keeps overflow evidence URLs + `salesReadyContactNames` so future contact-only updates do not fail closed incorrectly.

## 3. Exclusive 100-contact totals (clean, after quarantine)

Every original cohort contact counted **exactly once**:

| Bucket | Before (pass 1) | After (pass 2 cleaned) |
|--------|-----------------|------------------------|
| Both named email + usable phone | 11 | **17** |
| Email only | 18 | **15** |
| Phone only | 8 | **15** |
| Neither | 63 | **53** |
| **Sum** | **100** | **100** |

**Separate (not exclusive buckets):**
- Organizational routes after: **31**
- Verified LinkedIn profiles after: **4**

**Incremental named recoveries (no double-count vs before):**
- Named emails: **+3** (Google 2 · Exa 0 · Google→Exa 1)
- Named phones: **+13** (Google 6 · Exa 0 · Google→Exa 7)

## 4. Google discovery parity

- Islas lookalike lock retained (`emailAllowed` blocks non-hint lookalikes).
- Candidate-on-source verification retained.
- **New permanent offline regression:** lookalike page with `francisco.islas@jalisco.gob.mx` + `fislas@hcg.gob.mx` must recover only Google’s candidate.
- Gate: `npm run test:gdi-google-discovery-parity-v1` · `npm run test:gdi-google-first-contact-discovery-v1`

## 5. Costs (second pass — already spent; no new full cohort)

| Provider | Amount |
|----------|--------|
| Billed toward $10 cap | **$4.189** |
| Google (estimated; COST_UNKNOWN) | ~$2.67 (89 calls) |
| Exa (provider-reported) | **$1.519** (217 queries) |
| Surfe | **$0** (not used) |

## 6. Integration / deploy / drawer cert

- GDI chat does **not** merge. Hand to Integration Controller after this push.
- No automatic STAGED→CURRENT.
- **Airtable inventory (Maciel `recFsrSCFCNkKvKJE`):** email + phone + ext `35222` verified by read-back.
- **localhost:8080 STAGED drawer** (`hist_9d12871f512d` / Plan Anual de Obra Pública 2026): contact **name** renders; channels still show **Find Email & Phone** CTA because `source CURRENT_STAGED_PACKAGE` has not been refreshed from inventory. Screenshots: `screenshots/gdi-maciel-local-drawer-closeout.png`, `screenshots/gdi-maciel-local-contact-section.png`.
- **Railway external:** comparison page returned auth failure (`Send Authorization: Bearer …`) — screenshot `screenshots/gdi-maciel-railway-drawer-blocked.png`. Pending IC merge + deploy + authenticated STAGED refresh (not CURRENT promotion).
- Customer-facing channel cert = **BLOCKED until IC STAGED refresh**; research/Airtable quality closeout = done.

## 7. Remaining unresolved

- **53** contacts still Neither (named email + usable phone).
- Abramo / Laura / IAFL remain without durable named-person channels (quarantine or no inventory row).
- LinkedIn coverage still thin (4 verified in cleaned after-state).

## Status for IC

`AWAITING_INTEGRATION_CONTROLLER` — quality closeout + slim/parity fixes ready on PR #484.
