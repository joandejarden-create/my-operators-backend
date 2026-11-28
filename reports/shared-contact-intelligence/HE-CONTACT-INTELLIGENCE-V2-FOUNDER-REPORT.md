# Hotel Explorer Contact Intelligence V2 — Founder Report

**Branch:** `feat/he-contact-intelligence-v2`  
**Worktree:** `C:\Dev\dealality\wt-he-contact-intelligence-v2`  
**Base:** shared-canonical-contact-intelligence-v1 (`1c8f55bcc`) atop `origin/main`  
**Paid enrichment executed in this PR:** **No** (UI + hybrid path ready; click-gated)

## Acceptance checklist

| Criterion | Status |
|---|---|
| Five columns preserved | **PASS** |
| Channels left / Find Email & Phone right inside Contact Details | **PASS** |
| Button always visible; dimmed when email+phone present | **PASS** |
| Details column unchanged | **PASS** |
| Airtable SoT auto-hydration (Bethesda 3 emails) | **PASS** (live) |
| Surfe explicit-click / session-only | **PASS** |
| No column overlap CSS (relationship wrap + fixed table) | **PASS** |
| Shared GDI/HE resolver parity | **PASS** |
| No duplicate paid on complete channels | **PASS** (idempotent short-circuit) |

## HE50 spend reconciliation (before any phone spend)

| Field | Value |
|---|---|
| Cap | **$50.00** |
| Confirmed actual | **$0.15** (3 Findymail email credits) |
| Remaining | **$49.85** |
| Findymail phone cost | **10 credits / success · $0 miss** |
| Cash / success (HE50 $0.05/credit) | **$0.50** |
| Cash / success (Starter list) | **~$0.20** |
| Worst-case 5 phones | **$2.50** HE50 est. — within remaining |

Authorization token for hybrid phone: `FOUNDER_GO_HE50_CONTACT_PHONE_HYBRID_20261128`

## Bethesda live Airtable (no click)

| Person | Email | Phone | Button |
|---|---|---|---|
| Jeff Mullen | jeff.mullen@mullencap.com | Not Available | Active |
| Katherine Button | kbutton@mullencap.com | Not Available | Active |
| Patrick Reece | preece@mullencap.com | Not Available | Active |
| Meinrad Lang III | Not Available | Not Available | Active |
| Jenny Pyne | Not Available | Not Available | Active |

## Architecture

1. Load → shared canonical resolver → HI Contact Channels  
2. Click Find Email & Phone → `POST …/enrich-hybrid` → Airtable recheck → Findymail email/phone for gaps → persist + read-back  
3. If still missing → offer **Try Surfe (session only)** — never auto, never persist  

## Tests

- `npm run test:he-contact-intelligence-v2` — all permanent gates PASS / measured  
- Playwright fixture layout — run `npm run he:contact-intelligence-v2-playwright` when Playwright is installed  

## Integration

- Dirty GDI / HE / HE50 / IC worktrees preserved  
- Depends on Shared Canonical Contact Intelligence V1 (PR #694)  
- No production deploy  

**AWAITING_INTEGRATION_CONTROLLER** after PR open + CI.
