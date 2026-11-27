# Bethesda Contact Intelligence & Guest Sentiment Restore

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**FULL_HE_E2E_VALIDATED:** NO

## 1. Contact Intelligence root cause

Live fell back to HPC phone/website because `hotel-contact-intelligence.js` on the serving build stubbed API packages to `people: []`, then mounted the HPC fallback.

Canonical HI Org Contacts already held Jeff Mullen, Katherine Button, and Patrick Reece.

**Fix:** ship the package→UI mapper + commercial recommendation ordering; Live now renders full contact cards with LinkedIn + Find Email & Phone.

| Gate | Status |
|---|---|
| CONTACTS_CANONICAL_PERSISTENCE | YES (pre-existing; read back) |
| CONTACTS_LIVE_HYDRATION | YES |

Preferred Airtable IDs: `recCmiDOrJoI6kYrM`, `recNzfommzJEvCUEP`, `rechprAGnkCt7VbmP` (Validated). Duplicate Evidence Collected rows remain.

## 2. Guest Sentiment

| Gate | Status |
|---|---|
| GUEST_SENTIMENT_UI_PARITY | YES — Live + Proposed both use seven-platform shell |
| GUEST_SENTIMENT_LIVE_DATA_COVERAGE | 0 Live-eligible scores; 3 Proposed-only (TA/Booking/Expedia); 4 Not connected |

Live does **not** copy unlicensed Proposed ratings. Restricted cards show “Rights review required — Live scores withheld.”

**Persistence:** not applied — reputation tables not provisioned / not on standing allowlist; rights still `RIGHTS_REVIEW_REQUIRED`.

## 3. Eight-hotel status

Only Bethesda has recovered contacts + sentiment packet. Other seven: no fabrication; empty contact path / restricted empty sentiment shell. MREC org contacts are reusable without implying property decision-maker assignment.

## 4. Founder decisions remaining

1. Authorize `PUBLIC_METADATA_ONLY` Live scores vs keep Proposed-only until licensed feed  
2. Authorize HI Hotel Reputation Observations schema + standing-auth allowlist  
3. Deduplicate HI Org Contacts duplicate person rows  
4. Optional Bethesda GM/DOS property-leadership research
