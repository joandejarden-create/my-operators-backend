# Legacy Period Policy QA

- Historical periods remain immutable observation corpora.
- Inventory classification uses LEGACY_UNCERTIFIED / LEGACY_ONLY unless period carries `globalCertificationEngineVersion` or matched-control CERTIFIED stamp.
- Live scenario builder drift vs historical period count is a **warning** (LEGACY_METADATA_MISSING), not a certification hard fail.
- Formal comparison still requires `evaluateAdpComparability` (exact scenario IDs).
- Customer read: explicit QA_FAILED / QA_REVIEW_REQUIRED blocked; LEGACY grandfathered.
- Next official publish per active hotel must pass `certifyAdpPeriod` with `forceOfficialCertification`.
