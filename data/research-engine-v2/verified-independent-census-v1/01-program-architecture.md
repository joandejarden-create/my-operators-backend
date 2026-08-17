# Verified Independent Census — Program Architecture

```
INDEPENDENT DISCOVERY
→ FULL-RECORD RESEARCH
→ FIELD-LEVEL PROVENANCE
→ FREEZE (+ hash)
→ LEGACY COMPARISON (quarantined)
→ LEGACY-ONLY CHALLENGE (Strict | Targeted)
→ FIRST-PARTY VALIDATION PACKS
→ STEWARD REVIEW
→ EXISTING GOVERNANCE GATES
→ VERIFIED INDEPENDENT CENSUS (staging → future production)
```

## Firewall

Fail closed before freeze. Logged via `createResearchFirewall().getAudit()`.

## Staging model

Local verified records (`createVerifiedIndependentRecord`) — **not** Airtable writes in VIC v1.

## Maintenance handoff

After steward approval: Reconstruction Mode → Maintenance Mode (shadow freshness + steward queue).
