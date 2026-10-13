# GDI Post-Onboarding Quality Audit

**Date:** 2026-10-03  
**Mode:** APPLY  
**Thresholds changed:** NO  
**ADP changed:** NO  
**Share tokens changed:** NO

## A. Executive Summary

All three hotels completed HI + ADP READY + a GDI terminal cycle, but **Future Watch quality was not commercially mature**. Spice/AC watch counts (78/33) were **inflated aggregates**, not Valid Future Watch. YOTEL's "11 insufficient evidence" headline **double-counted overlapping hygiene labels** on 12 candidates. Targeted research on AidEx / Changemakers / Mer Summit corrected three YOTEL rows without lowering gates. **Customer-ready remains 0** for all three — correctly.

## B. YOTEL

### Why 0 ready
Surface eligibility + WHO research not attempted + thin lodging proof on nearly all rows. No candidate cleared `isGdiCustomerOpportunityReady`.

### True primary blockers (reconcile = 12)
- **ENTITY VALIDITY GAP:** 5
- **GEOGRAPHY WEAK:** 2
- **TIMING UNCONFIRMED:** 3
- **PLACED / NO OVERFLOW:** 1
- **CONTACT / WHO GAP:** 1

Double-counted rejection issue fixed: **YES**

### Targeted research yield
- AidEx: dates/venue/housing platform **confirmed** → valid watch candidate; still CONTACT/WHO gap for ready
- Changemakers: **PLACED** at Caux Palace (package lodging) → reject
- Summit 2026: **Bordeaux** → out of market reject

### Final status
- Customer ready: **0**
- Future watch (valid): **1**
- Rejected: **9**
- Public data ceiling verified: **NO**

## C. Spice Island

- Watch items audited: **77**
- Valid Future Watch: **4**
- Invalid/stale/dup/etc reclassified: **73**
- Duplicates: **12** · Stale: **18**
- Final customer ready: **0**
- Final future watch: **4**
- Public data ceiling verified: **NO**

Spice watchlist was largely a **dumping ground** for generic incentive/wedding/retreat templates and unconfirmed cycles without triggers.

## D. AC A Coruña

- Watch items audited: **32**
- Valid Future Watch: **10**
- Invalid reclassified: **22**
- Duplicates: **0** · Stale: **11**
- Final customer ready: **0**
- Final future watch: **10**
- Public data ceiling verified: **NO**

## E. Public Data Ceiling

| Hotel | Verified | Blocking gap |
|-------|----------|--------------|
| YOTEL | NO | who_contact_not_attempted |
| SPICE | NO | who_contact_not_attempted |
| AC | NO | who_contact_not_attempted |

## F. Research Efficiency

- YOTEL: discovered 12, qualified 12, ready 0 (0%), valid watch 1 (8.3%)
- SPICE: discovered 17, qualified 17, ready 0 (0%), valid watch 4 (23.5%)
- AC: discovered 11, qualified 11, ready 0 (0%), valid watch 10 (90.9%)

## G. Final GDI Quality Status

Commercially **not mature** for customer-ready opportunities. Watch quality gate `isValidFutureWatch` now installed with regressions. Re-run with `--apply` to persist DISQUALIFIED / stripped-watch stamps to Airtable if this was a dry run.

**Watch validation helper added:** YES  
**GDI thresholds changed:** NO  
**ADP changed:** NO  
**Share tokens changed:** NO
