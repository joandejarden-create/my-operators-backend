# Canonical Contact Reviewed Apply

**Module:** `canonical-merge-review-apply.js`
**Default:** `REVIEW_REQUIRED` for email · Phone `AUTO_APPLY_PHONE_PILOT` when gates pass
**AUTO_ACCEPT_SAFE:** disabled globally

## Flow

1. Generate field-level proposals from validated reachability/dry-run
2. **Email:** `approveCanonicalContactMergeProposal` → `applyApprovedCanonicalContactMerge`
3. **Phone pilot:** auto-approve when `evaluatePhoneAutoApplyEligibility` passes
4. Optimistic concurrency via `personVersion` + `baselineFieldHash`
5. Immutable audit on approve/apply/rollback
6. Rollback restores prior field; audit never deleted

## Phone pilot gates

- Identity ACCEPTED (LIMITED allowed when `PHONE_PILOT_ALLOW_LIMITED_EVIDENCE_MOBILE=1`)
- Field ownership PASS
- No collision / main-line-only / official conflict
- TIER_1_SAFE (or LIMITED+TIER_2 with pilot flag for mobile measurement)

## Not in scope

- Share page writes
- Global Surfe enablement
- CRM UI

## Regression

```bash
npm run test:canonical-merge-review-apply
npm run gdi:bethesda-tier1-reviewed-apply
```
