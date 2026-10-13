# Bethesda Rad Validation Share

**Verdict:** READY FOR RAD VALIDATION (code + local projection ready; deploy required for live URL refresh)

**Date:** 2026-09-15  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Surfe:** globally OFF (`CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0`)  
**AUTO_ACCEPT_SAFE:** disabled  
**New research:** none  

---

## Share changes

1. **Canonical contact overlay at share read time**
   - Store: `data/group-demand-intelligence/hotels/<hotelId>/canonical-contacts.json`
   - Materialize: `npm run gdi:materialize-canonical-contacts-for-share`
   - Does **not** mutate `opportunities.json` research packages
   - Share APIs overlay current canonical fields before sanitize

2. **External projection module**
   - `lib/group-demand-intelligence/canonical-contact-external-projection.js`
   - Client-safe fields only: name, title/role, org, email, phone, phone type, why this contact
   - Strips provider names, credits, proposal history

3. **Hotel validation on share**
   - POST `/api/group-demand-intelligence/share/hotels/:hotelId/opportunities/:opportunityId/validation`
   - GET `/api/group-demand-intelligence/share/hotels/:hotelId/validation`
   - UI: §15 Hotel Validation in share detail drawer (Save / Save + Next)
   - Store: `share-validation.json` (separate from research + canonical)

4. **Summary KPIs**
   - Opportunities / High / Medium / Watchlist
   - Validated / Pending / Confirmed new / Already known / Worth now / Not relevant
   - Hotel-confirmed metrics only (never infer “new” from CI absence)

---

## Canonical contact projection rules

| Rule | Behavior |
|------|----------|
| Current canonical only | `canonicalStatus === CANONICAL` |
| Rollback | Rolled-back fields excluded (Ben Hawkins email absent) |
| Ambiguous / rejected identity | No overlay |
| Rejected / held merge | Not displayed |
| Phone type | Mobile / Direct / Office / Main·Shared — never imply main is direct |
| Provider internals | Never in share payload |
| Generic pack contact | May move to backup when named canonical person overlays |

### Bethesda current canonical (materialized)

| Person | Email | Phone | Notes |
|--------|-------|-------|-------|
| Amy Drow | adrow@ndss.org | +14074962293 (Mobile) | APPLIED |
| Ben Hawkins | — | — | Email rolled back — **not displayed** |
| Jamie McCormick | jmccormick@nado.org | +15189447999 (Mobile) | Official email + phone pilot |
| Kelly Frere | kfrere@asaecenter.org | +13015095526 (Mobile) | Official email + phone pilot |

---

## Validation schema

`share-validation.json` item:

- hotelId, opportunityId, validator, validatedAt
- familiarityStatus (Never seen / Already known / Actively pursuing / Previously pursued·lost / Booked·won / Not relevant / Unsure)
- commercialValue (Worth pursuing now / Worth watching / Not worth pursuing)
- contactPersonAssessment (Right / Relevant not DM / Wrong / Unsure)
- emailAssessment (Useful / Wrong / Generic / Not tested)
- phoneAssessment (Direct·usable / Main·shared / Wrong / Not tested)
- note (optional)

Guards: `doesNotOverwriteCanonicalFacts`, `doesNotMutateCanonicalContacts`

Mapped feedback events (for later learning, no silent mutation): RIGHT_PERSON, WRONG_PERSON, EMAIL_USEFUL/WRONG, PHONE_USEFUL/WRONG/MAIN_SHARED, etc.

---

## Phone pilot metrics

Reusable (`hotel-validation-metrics.js`):

- PHONE_TESTED_COUNT / USEFUL / WRONG / MAIN_SHARED / NOT_TESTED
- PHONE_USEFULNESS_RATE = useful / tested
- PHONE_WRONG_RATE = wrong / tested

Core hotel metrics: Validation Completion, Discovery, Actionability, High Priority Precision, Incremental Intelligence, Contact Person Accuracy, Email/Phone Usefulness.

---

## Deployment checks

### Pre-deploy artifact tree

| Surface | Required |
|---------|----------|
| GDI share HTML + JS + CSS | ✓ |
| GDI share API + validation POST | ✓ |
| Token registry | ✓ |
| Canonical contacts JSON | ✓ (Bethesda) |
| ADP share | ✓ (`owner-ai-demand-share.html`) |
| Hotel Explorer share | ✓ |
| Brand Library | ✓ (`api/brand-library.js`) |

GDI added to `config/production-required-assets.json` surface `GDI`.

### Post-deploy live checklist

- [ ] Bethesda GDI share = 200
- [ ] Share token resolves
- [ ] 29 opportunities load
- [ ] Amy / Jamie / Kelly canonical contacts render
- [ ] Ben rolled-back email **absent**
- [ ] Validation Save persists; reload retains
- [ ] ADP share = 200
- [ ] Hotel Explorer share = 200
- [ ] Brand Library healthy

### Live URL

Reuse existing ACTIVE Rad share token — **no new URL required**.

Path shape:
`https://dealality.com/group-demand-intelligence-share.html?share=gdishare.v1.…`  
(or Railway production host)

Registry ACTIVE tokenIds (token secret not stored in registry):
- `gdisht_ba78a21714ea542573cdaafc` — Bethesda Marriott GDI Pilot — Rad review
- `gdisht_79f4c3e9d1a7061afeb8a59a` — Bethesda GDI native UI review

After deploy, open the existing Rad link. If an older URL was revoked in prior deploy churn, re-issue with `npm run gdi:issue-share` and send Rad the same path with the new signed query.

---

## Known limitations

1. Overlay is filesystem canonical store — not Airtable CRM.
2. Validation does **not** yet auto-revoke canonical reuse (events mapped for later processing).
3. Jamie/Kelly phones are LIMITED-evidence phone-pilot fields — displayed for measurement; hotel can mark Wrong / Main.
4. Live deploy verification still required before Rad session.
5. No new Surfe / Webhound / qualification changes in this pass.

---

## Regressions

```bash
npm run test:gdi-share-canonical-external-safety
npm run gdi:materialize-canonical-contacts-for-share
npm run test:canonical-merge-review-apply
```

12 external-safety tests passed (applied email/mobile display, rollback absent, ambiguous/rejected blocked, no provider/credits leak, validation separate, no silent canonical mutation, multi-hotel fixture).

---

## Operating law audit

| Issue | Classification | Implementation | Regression |
|-------|----------------|----------------|------------|
| External contact projection | REUSABLE_PRODUCT_LOGIC | `canonical-contact-external-projection.js` | test:gdi-share-canonical-external-safety |
| Canonical contacts store + overlay | REUSABLE_PRODUCT_LOGIC | `canonical-contacts-store.js` | same |
| Share validation model + events | REUSABLE_PRODUCT_LOGIC | `share-validation.js` | same |
| Hotel / phone metrics | REUSABLE_PRODUCT_LOGIC | `hotel-validation-metrics.js` | same |
| Bethesda materialize inputs | HOTEL_SPECIFIC_DATA | evals + `canonical-contacts.json` | materialize script |
