# AVA Wave 2 Queue — Prepared (Not Executed)

**Prepared after:** Wave 1 Final Commercial Quality Certification (`PASS_WITH_LIMITATIONS`)  
**Status:** QUEUED ONLY — do not run Full E2E / paid research / publication from this file alone.

## Wave 2 hotels

### Ambridge (canonical IDs — locked from ADP Ambridge integrity re-audit)

| # | Hotel | Property ID | Payload |
|---|---|---|---|
| 1 | Sheraton Guadalajara Expo | `adp_sheraton_guadalajara_expo` | `payloads/sheraton-guadalajara-expo-v1.json` |
| 2 | voco Cancún Zona Hotelera | `adp_voco_cancun_zona_hotelera` | `payloads/voco-cancun-zona-hotelera-v1.json` |
| 3 | Wyndham Alltra Punta Cana | `adp_wyndham_alltra_punta_cana` | `payloads/wyndham-alltra-punta-cana-v1.json` |

### Bethesda enhancement

| # | Hotel | Property ID | Notes |
|---|---|---|---|
| 4 | Bethesda Marriott | `adp_bethesda_marriott` | Enhance existing validated audit; preserve ORS evidence remediation |

## Entry command (when founder authorizes Wave 2)

```bash
# Preflight / enhance only — no paid flags unless separately approved
npm run dealality:adp-full-e2e -- --hotel "Full ADP E2E — <Hotel Name>" --mode preflight
```

Then run AVA Full E2E evidence stages 6–11 + Digital Authority public enrichment per hotel.

## Hard bans (unchanged)

- No production deploy / Railway changes
- No paid research without founder approval
- No customer publication / emails
- No founder-approval flag flips
- No certified ADP measurement changes

## Wave 3

Remaining hotels in the 38-property AVA inventory after Wave 2 closes.
