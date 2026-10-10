# Owned Domain Audit — Hilton Times Square

## Definition (unchanged methodology)
OWNED_SOURCE means a source directly controlled by (A) the subject hotel/property OR (B) the hotel's official parent brand/operator when the page directly represents the specific property. Brand corporate pages that do not represent the subject property are not owned. UNKNOWN is retained separately and must not silently become OWNED or EXTERNAL.

## Configuration
- officialBrandDomain: `hilton.com`
- canonicalPropertyDomain: `null`
- ownedDomains: []
- brandPropertyPathHints: ["nyctshh","hilton-times-square","hilton-new-york-times-square"]
- ownedDomainSet size: 0

## Why Owned Sources = 0.0% while hilton.com appears in landscape
1. Source Landscape counts **all** cited domains across monitored responses (subject OR competitors).
2. Owned Mix requires a citation classified OWNED for the subject — brand corporate host `hilton.com` is **EXTERNAL** unless the URL path matches property hints (`nyctshh`, `hilton-times-square`, …).
3. Sept 27 observed hilton.com citations were predominantly non-property-page (or competitor context) → Owned Share 0% is **method-correct**, not a mapping bug.
4. `marriott.com` as Top Cited Source is **competitive frequency**, not misattribution of Hilton ownership.

## Classification correctness
- **hilton.com owned classification correct?** YES (property-page path rule).
- **marriott.com misattributed as Hilton source?** NO (data); YES historically as **misleading KPI label** ("Top Source") — label fixed to "Top Cited Source Across Monitored Responses".
