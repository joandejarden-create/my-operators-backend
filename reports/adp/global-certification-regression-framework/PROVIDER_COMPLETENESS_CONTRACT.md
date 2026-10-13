# Provider Completeness Contract

Every period computes expected / attempted / successful / failed / timed out / parsed / rankEligible / citationEligible.

Silent denominator reduction → QA_FAILED.
Material provider failure → QA_REVIEW_REQUIRED (or QA_FAILED when unexplained with identity failure).

Zero-presence providers trigger forensic checks (execution, nonempty responses, parser, alias, rate-limit/refusal).
