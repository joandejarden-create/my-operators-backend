# Sheraton Final Full E2E Certification & Aimbridge Handoff

**Reference date:** 2026-11-14
**Hotel:** Sheraton Guadalajara Expo (`recCAGqkERSXgiku4`)
**Staged:** `gdi_pub_recCAGqkERSXgiku4_20261114_final_certification_v1`
**Fingerprint:** `d4b2bfc7eb0419b863a561750a748227244bf759edf1307926f89749706e2a33`
**Inventory:** All **38** · Hotel **34** · Competitive **4**
**Paid spend this run:** $0
**CUSTOMER_PUBLISHED:** No

## Certification verdict

**Status: `FOUNDER_REVIEW_READY_AIRTABLE_PERSISTENCE_PENDING`**

Sheraton is **founder-review ready** on the certified actionability v2 package. Canonical Airtable opportunity persistence remains **PENDING founder authorization** — therefore this run does **not** falsely declare `FULL_E2E_COMPLETE`.

| Gate | Result |
|---|---|
| Commercial actionability | PASS |
| CMI (4 rows + stage 4a tests) | PASS |
| Need dates provisional | PASS |
| SVG / classification | PASS |
| Top-15 reconciliation | PASS |
| Browser founder preview | PASS |
| Full E2E template + cross-hotel | PASS |
| Bethesda / V3 immutable | PASS |
| Airtable reconstruction | **BLOCKED — founder auth required** |

## Top-15 paid discovery

All **15** net-new paid opportunities remain in the customer-visible 38. **13** Hotel Market · **2** Competitive (CIICS → Barceló; Semana IMIN → Hard Rock). Prior $15.20 spend preserved; no additional paid calls.

## Next Aimbridge hotel (corrected)

**Next: Wyndham Alltra Punta Cana** (`recXh4NgF8sDbi9cQ`) — Dominican Republic · 620-room all-inclusive · Aimbridge three-hotel cohort.

**Following: voco Cancún Zona Hotelera** (`recTk5I8VOzsPhxDz`) — queued only; do not start full run yet.

**Do not use:** JW Marriott Monterrey Valle (`recsn3BUKJ9PNfeZW`) — not this cohort.  
**Do not use:** duplicate HPC `reccax9nT5J90Sc0Y`.

Recovered research (verified): Wyndham **227** ORS / **553** candidates / **14** segments · voco **381** / **646** / **14**.

Preflight pack: `reports/gdi-wyndham-alltra-punta-cana-full-e2e-preflight-v1/`

```bash
npm run gdi:full-e2e -- --hotelId recXh4NgF8sDbi9cQ --mode preflight
```

## GitHub

- Branch: `cert/gdi-sheraton-final-e2e-and-aimbridge-handoff-v1`
- PR: https://github.com/joandejarden-create/my-operators-backend/pull/285
- Related predecessor: PR #284

## Protections

- Bethesda `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66` unchanged
- Sheraton V3 `2c9bb143887af8cae49050ff67fbb4f414e05f4eea924b3326489522b09065ef` unchanged
- No CUSTOMER_PUBLISHED
- No live Airtable opportunity upserts this run

