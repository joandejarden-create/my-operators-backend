# Validated-68 Persistence Write Scope — Bethesda

**Verdict: READY_FOR_VALIDATED_CANARY**

PR **#599** scope correction. **STOP — no live Airtable mutations executed.**

---

## Permanent founder policy

> Discover broadly. Research thoroughly. Qualify rigorously. **Persist validated opportunities.** Preserve rejected research as supporting intelligence, not as active hotel opportunities.

Enforced in bridge via `writeSetPolicy=VALIDATED_FQO_ONLY` (default for production unlock).

---

## 1. Revised 68-opportunity exact write manifest

Source: certified STAGED  
`gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1` (**68**)

**Not** the 88-record historical research universe.  
Prior 86-write set: **NOT APPROVED**.  
Prior canary `gdi_e2d_770e3b44ad97c1ae`: **NOT APPROVED** (research continuation; outside certified 68).

Full row plan: `03-VALIDATED_68_EXACT_WRITE_MANIFEST.json`

## 2. Create / update / unchanged counts

| Action | Count |
|---|---:|
| FQO write-eligible | **68** |
| WOULD_CREATE | **0** |
| WOULD_UPDATE | **68** |
| UNCHANGED | **0** |
| BLOCKED | **0** |
| Suggested production budget | **68** |
| Nonqualified mutations proposed | **0** |

All 68 certified opportunities already exist in live Airtable; proposed ops are lossless FQO/payload merges, not creates.

## 3. Record-level FQO gate verification

Evidence: `01-FQO_GATE_VERIFICATION_68.json`

| Check | Count |
|---|---:|
| Package members | 68 |
| Write-eligible (`finalizedQualified` with certification schema-compat) | 68 |
| Strict pass without historical schema-compat | 38 |
| Historical schema-compat assists evidence/pathway only | 30 |

Historical compatibility does **not** invent contact/outreach passes. Contact failures still fail. The 30 assisted rows are certified STAGED members that use the same schema-compat evidence path used when the quality package was accepted — they remain FQO `finalizedQualified === true`. Package membership alone is insufficient without that FQO pass.

`commercialEligibilityStatus` remains write-routing (`GLOBAL_ORG_ONLY`); hotel FQO stays in `researchPayloadJson.hotelSpecificQualification`.

## 4. Data-loss prevention results

- Lossless live merge retained (contacts / evidence / org / titles).
- Nonqualified / outside-68 live rows: **preserve in place; 0 mutations proposed** (`02-NONQUALIFIED_PRESERVATION.json`).
- 88-universe incomplete / expired / duplicate / disqualified identities: supporting intelligence only.
- Rejected canary deferred by FQO + allowed-id filter.

## 5. Proposed certified-opportunity canary

| Field | Value |
|---|---|
| Opportunity | `gdi_s4912_512deebd0963` |
| Title | 2027 U.S. IFE Conference |
| Airtable record | `recxVyeNwB23XsC5G` |
| Named contact | Liz Conrow |
| Outreach | econ@lle.rochester.edu |
| FQO | finalizedQualified |
| Action | **UPDATE** (existing validated row) |
| Material fields | `researchPayloadJson` only (FQO / slim overlay) |
| Max writes | **1** |

Evidence: `04-CERTIFIED_OPPORTUNITY_CANARY.json`  
**Not executed.**

## 6. Exact canary rollback steps

1. Confirm `airtableRecordId` `recxVyeNwB23XsC5G`.
2. Restore typed fields + `researchPayloadJson` from `preWriteSnapshot` in `04-CERTIFIED_OPPORTUNITY_CANARY.json` (use full live backup if preview truncated).
3. Re-read record; confirm match to pre-write snapshot.
4. Confirm STAGED pointer still `…_quality_v1` and CURRENT still gold `…_local_master_v2`.
5. Confirm no GDO create/update and no customer publication.

## 7. Idempotency test results

Mock store (`05-IDEMPOTENCY_AND_SAFETY.json`):

| Run | Result |
|---|---|
| 1 | CREATE × 68 |
| 2 | one-time merge normalization UPDATE × 68, CREATE × 0 |
| 3 | **UNCHANGED × 68** (idempotent) |
| Pollution | `gdi_e2d_770e3b44ad97c1ae` deferred — not persisted |

## 8. PR #599 integration readiness

- Branch: `feat/gdi-full-e2e-airtable-persistence-cert-v1`
- Permanent policy: `VALIDATED_FQO_ONLY` on production unlock
- Tests: `test:gdi-validated-fqo-write-set-policy-v1`, lossless merge, stage-status
- STAGED 68 / CURRENT 67 protected
- Integration Controller owns merge
- **No parallel branch**

## 9. Production write approval request

**Requesting founder approval for:**

1. **Canary only (first):** single UPDATE of `gdi_s4912_512deebd0963` / `recxVyeNwB23XsC5G` with exact auth token `FOUNDER_CANARY_gdi_s4912_512deebd0963`, writeBudget=1, VALIDATED_FQO_ONLY.
2. **After canary PASS:** authorize the remaining justified validated-68 budget (currently 67 further UPDATEs; 0 CREATEs), exact write-set token, no GDO, no STAGED/CURRENT, no nonqualified mutations.

**Not requested:** bulk 86-universe write; CREATE of `gdi_e2d_770e3b44ad97c1ae`; schema changes; paid research; other hotels.

---

## Safety checklist

| Gate | Status |
|---|---|
| Only eligible records in write set | PASS (68 FQO) |
| No duplicate creates on rerun | PASS |
| Weaker evidence cannot wipe live contacts | PASS (merge) |
| Nonqualified available for continuation | PASS (preserved) |
| GDO independently authorized | PASS (none here) |
| STAGED 68 unchanged | PASS |
| CURRENT 67 unchanged | PASS |
| No customer publication | PASS |
| Live Airtable mutations this run | **NONE** |
