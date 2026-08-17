# Research Firewall Design

## Rule

**LEGACY CENSUS VALUES MUST NOT BE AVAILABLE TO THE INDEPENDENT RESEARCH PHASE.**

## Enforcement

`createResearchFirewall()` in `lib/research-engine-v2/clean-census/research-firewall.js`:

1. Phase `independent_research` — `requestLegacyCensus` throws `ResearchFirewallError`
2. Context keys `legacyHotels`, `censusHotels`, `seedHotelNames`, etc. rejected
3. `freezeIndependentUniverse` → phase `frozen`
4. Only then `beginLegacyReconciliation` / `legacy_only_challenge`

## Pilot proof

```json
{
  "blocked_as_expected": true,
  "message": "FIREWALL: legacy census access blocked during phase=independent_research. Freeze independent output before reconciliation."
}
```

Firewall audit after run:

```json
{
  "phase": "legacy_only_challenge",
  "legacyAccessAttempts": 1,
  "blockedAttempts": [
    {
      "at": "2026-08-04T22:12:13.169Z",
      "action": "read_legacy",
      "phase": "independent_research"
    }
  ],
  "freezeTimestamp": "2026-08-04T22:12:18.611Z",
  "legacyLoadedAt": "2026-08-04T22:12:18.752Z",
  "frozenRecordCount": 9
}
```
