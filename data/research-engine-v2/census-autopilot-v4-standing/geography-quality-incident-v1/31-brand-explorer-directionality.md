# Brand Explorer Directionality

**Required direction**

```
Verified Property Census (property-level Current Brand / affiliation)
        ↓
Brand Explorer census / affiliation validation
```

**Forbidden direction**

```
Brand Explorer assumption
        ↓
Census Current Brand factual value
```

## Rules

1. Current Brand is researched **property-first** from official property/directory evidence.
2. Brand Explorer may **consume** Census affiliation data.
3. An existing Brand Explorer record must **not** force a Census property into a brand affiliation when current property evidence disagrees.
4. Soft collections (Ascend, Radisson Individuals, etc.) still require property-level confirmation — collection membership is not inferred from parent alone.

## Incident note

Choice-family V3/V3.1 production damage was **not** caused by Brand Explorer override; it was caused by **source-family → Current Brand** contamination in Autopilot discovery/write. Directionality rule is locked to prevent a second failure mode.
