# Census Confidence-Tiered Internal Completion v1

**Status:** `production_census_autopilot_policy_controller_v1_partial_source_remaining`  
**Policy:** `census-autopilot-approved-policy-v2-confidence-tiered-internal`  
**Generated:** 2026-08-08T00:25:00.000Z  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Goal shift

From official-only High completion → **confidence-tiered internal census completion**.  
Medium fields are written for internal use with provenance; they are **not** public-facing until steward review.

## Blanks before → after

| Gap | Before | After | Delta |
|---|---:|---:|---:|
| Missing address | 621 | 622 | +1 (new Census Only inserts) |
| Missing coordinates | 729 | 732 | +3 (inserts; −21 Mapbox on existing) |
| Missing phone | 872 | 841 | **−31** (55 phone writes − insert blanks) |
| Missing rooms | 1033 | 1057 | +24 (inserts) |
| Missing market | 254 | 254 | 0 |
| Missing official URL | 0 | 6 | +6 (inserts without website) |

Net existing-record fills this cycle: **55 phones** + **21 Mapbox Medium coords** + **24 Census Only inserts**.

## Fields written by confidence tier

| Tier | Count | Fields |
|---|---:|---|
| **Medium** | **76** | Phone (55) + Mapbox coords from Medium address (21) |
| **High** | 0 | Rooms / Market / Website this cycle |
| **Internal inserts** | **24** | Census Only / Hold / Human Review Required |

## Source type split

| Source | Writes | Notes |
|---|---:|---|
| DataForSEO local match_high | 55 phones | Medium; provenance in Notes for Steward |
| Mapbox Permanent | 21 coords | After Medium match_high street address |
| Secondary rooms | 0 | No High evidence matches this pass |
| DataForSEO direct coordinates | 0 | Held by policy |
| New hotel insert (Maps discovery queue) | 24 | Census Only / Public+Radar Hold / HR=true |

## Public-safe vs internal-only

| Exposure | Count |
|---|---:|
| Public-safe writes | **0** |
| Internal-only writes | **100** |

Medium phone / Medium address-geocoded coords / Census Only inserts are internal-only.

## Records held / blockers

- Mapbox rejects: **134** (centroid / low relevance / country / eligibility)
- Direct DataForSEO coordinates: held
- Secondary phone sources: held (DataForSEO local match_high only)
- Rooms: no secondary High evidence this pass
- **Schema TODO:** Phone Confidence / Phone Source URL / Phone Source Type fields do not exist — Medium phone provenance stored in `Notes for Steward`

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer: **0**
- Owner / operator / dates / Company Validated / Brand Verified / Brand Status / Recent Momentum: **0**
- Phone Confidence field: missing (documented)
- Inserts: Production Use Status = Census Only / Not Owner-Facing; Public Display Review Status = Hold; Radar Display Status = Hold; Human Review Required = true

## Commands used

```bash
ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION=1 \
ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES=1 \
ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS=1 \
ENABLE_DATAFORSEO_LOCAL_INSERTS=1 \
ENABLE_HIGH_CONFIDENCE_INSERTS=1 \
npm run census:autopilot -- ... --census-mode field-completion-only --enable-production-writes
```

## Next backlog

1. Add Phone Confidence + Phone Source URL schema fields (replace Notes provenance)
2. Continue address scale + Mapbox passes for remaining Medium eligible coords
3. Rooms secondary High evidence still thin — expand approved secondary adapters
4. Re-run growth discovery to refresh insert queue after 24 inserts
5. Steward review Census Only Hold inserts before any public exposure
