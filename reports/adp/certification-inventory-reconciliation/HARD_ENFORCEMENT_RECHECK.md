# Hard Enforcement Recheck

- **CERTIFIED_ALLOWED**: PASS — CERTIFIED → publish allowed under hard enforcement
- **QA_REVIEW_BLOCKED**: PASS — ADP_CERTIFICATION_REQUIRED: cannot publish with certificationStatus=QA_REVIEW_REQUIRED
- **QA_FAILED_BLOCKED**: PASS — ADP_CERTIFICATION_REQUIRED: cannot publish with certificationStatus=QA_FAILED
- **LEGACY_NEW_PUBLISH_BLOCKED**: PASS — ADP_CERTIFICATION_REQUIRED: cannot publish with certificationStatus=LEGACY_UNCERTIFIED
- **LEGACY_CANNOT_MASQUERADE**: PASS — ADP_CERTIFICATION_REQUIRED: cannot publish with certificationStatus=LEGACY_UNCERTIFIED
- **ENV_FLAG_ON**: PASS — ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=1
- **LEGACY_GRANDFATHER_READ**: PASS — Bethesda Live period=adp_period_adp_bethesda_marriott_20261002142154_62428a; stampedCert=null; inventory class=LEGACY_OFFICIAL
