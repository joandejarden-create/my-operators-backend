# Bethesda Marriott — Market Alerts discovery gap

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Date:** 2026-11-19  
**Status:** Gap recorded — **do not manufacture articles**

## Finding

Live Market Alerts search for property-specific Bethesda Marriott coverage returned **0** rows (Title / Hotel / Project / Summary).

Hotel Explorer Brand / Operator → Hotel News & Articles correctly **hides** the subsection.

## Implications for future Market Alerts runs

When ingesting or enriching alerts, prefer property-specific signals over brand/sponsor-only coverage:

1. Verified aliases: `Bethesda Marriott`, `Bethesda Marriott Hotel`, `Marriott Bethesda`
2. Renovation / CapEx announcements naming the property
3. Ownership / PropCo / sponsor transactions naming the hotel
4. Management / operator changes naming the hotel
5. After founder authorization: persist `hpc:recLuxvwwxID7U2B8` Tags (and optional `HPC Hotel Record IDs`) on matched MarketAlerts rows — never duplicate the article

## Related

- Closeout: `HE-MARKET-ALERTS-INTEGRATION-CLOSEOUT-V1.md`
- Live probe: `HE-MARKET-ALERTS-CLOSEOUT-LIVE-PROBE-V1.json`
