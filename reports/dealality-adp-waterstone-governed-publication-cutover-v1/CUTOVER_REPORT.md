# Waterstone Governed Publication Cutover V1

## Cutover status

**PUBLISHED_AWAITING_EXTERNAL_CERT**



## 1. Publication authorization

| Field | Value |
|-------|-------|
| Type | `FOUNDER_APPROVED_FOR_PUBLICATION` |
| Active | **true** |
| Staged version | `staged_mw14o9lq_bb8bd9570c` |
| Record | `reports/dealality-adp-waterstone-governed-publication-cutover-v1/FOUNDER_APPROVED_FOR_PUBLICATION.json` |

Authorization is ACTIVE.

### Exact phrase required to activate

```
FOUNDER_APPROVED_FOR_PUBLICATION staged_mw14o9lq_bb8bd9570c
```

Set in `reports/dealality-adp-waterstone-governed-publication-cutover-v1/FOUNDER_APPROVED_FOR_PUBLICATION.json`:
- `status`: `ACTIVE`
- `active`: `true`
- `founderSignature`: your name
- `founderSignedAt`: ISO timestamp
- `explicitPhrase`: exactly the phrase above
- `stagedFileSha256` must still match `9fe5fb1929ce3f745ed1c50f74285fba0d1015efa061eb6caa855ae512ca33c6`

Then run:

```bash
node scripts/dealality-adp-waterstone-governed-publication-cutover-v1.mjs --authorize-publish
```

## 2. Pre-publication checks

**Pass: true**

- Staged identity: `staged_mw14o9lq_bb8bd9570c` OK
- File SHA-256: `9fe5fb1929ce3f745ed1c50f74285fba0d1015efa061eb6caa855ae512ca33c6`
- Methodology: `ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1` OK
- KPIs: Consideration 53.5 / Presence 76.6 / Coverage 24.1 / Gap 69 OK
- Providers all /64: OK
- Competitive rank #2: OK
- PDF present: OK
- Browser HTML/PDF parity: FAIL
- Prior published preserved: OK

## 3. Version IDs

| Role | ID |
|------|----|
| Prior published period | `adp_period_adp_waterstone_boca_raton_20260902231159_77334c` |
| Prior published at | `2026-09-10T11:04:32.682Z` |
| New published | staged_mw14o9lq_bb8bd9570c |
| Approved staged candidate | `staged_mw14o9lq_bb8bd9570c` |

## 4–7. Production / external

| Item | Status |
|------|--------|
| Production readback | N/A — not published |
| External URL | preserved existing share (no rotation) |
| External-access verification | N/A — not published |
| Safe to share corrected methodology | **NO** |
| CUSTOMER_READY | **NO** |
| Distribution hold | HOLD |

## 8. Rollback readiness

**READY** — prior Live backup at `reports/dealality-adp-waterstone-governed-publication-cutover-v1/PRIOR_PUBLISHED_BACKUP/report-adp_period_adp_waterstone_boca_raton_20260902231159_77334c.json`

No paid monitoring. No unrelated hotel changes. No publish executed unless authorization was active and `--authorize-publish` was passed.
