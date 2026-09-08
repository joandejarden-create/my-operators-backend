# ADP Monterrey Valle — Identity Freeze V1

**Status:** FROZEN for first-official ADP onboarding  
**Base checkpoint:** `recovery/combined-restored-2026-09-08` @ `98a4e32` (`recovered-working-state-2026-09-08`)  
**Operator:** Aimbridge LATAM (Census / Aimbridge first-party; not asserted as economic owner)  
**Market pack:** one shared `monterrey_valle` standard scenario universe for both subjects

## Properties

| Field | JW Marriott Monterrey Valle | The Westin Monterrey Valle |
|-------|-----------------------------|----------------------------|
| ADP Property ID | `adp_jw_marriott_monterrey_valle` | `adp_westin_monterrey_valle` |
| Registry entityId | `jw_marriott_monterrey_valle` | `westin_monterrey_valle` |
| Canonical name | JW Marriott Hotel Monterrey Valle | The Westin Monterrey Valle |
| Census Record ID | `recsn3BUKJ9PNfeZW` | `recD17Kxn6BcJjGFh` |
| Census identity_key | `ind_marriott_mx_mtyjw` | `ind_marriott_mx_mtywi` |
| MARSHA / Marriott slug | `mtyjw` | `mtywi` |
| Official Marriott page | https://www.marriott.com/en-us/hotels/mtyjw-jw-marriott-hotel-monterrey-valle/overview/ | https://www.marriott.com/en-us/hotels/mtywi-the-westin-monterrey-valle/overview/ |
| Aimbridge page | https://aimbridgelatam.com/en/hotel/jw-marriott-monterrey-valle/ | https://aimbridgelatam.com/en/hotel/the-westin-monterrey-valle-2/ |
| Address | Av. del Roble 670, Valle del Campestre, San Pedro Garza García, NL 66265 | Av. Manuel Gómez Morín y Río Missouri, Punto Valle, San Pedro Garza García, NL 66220 |
| City / market | San Pedro Garza García / Monterrey | San Pedro Garza García / Monterrey |
| Submarket | Monterrey Valle | Monterrey Valle |
| Rooms | 250 (Travel Weekly + Aimbridge opening materials) | 174 (Travel Weekly) |
| Room confidence | HIGH | HIGH |
| Floors | 14 (Travel Weekly) | TBD (leave null until confirmed) |
| Chain scale | Luxury | Upper Upscale |
| Brand / affiliation | JW Marriott / Marriott International | Westin / Marriott International |
| Management | Aimbridge LATAM | Aimbridge LATAM |
| Economic owner | Unresolved — do not invent | Unresolved — do not invent |
| Dropdown visible | `false` until certified | `false` until certified |

## Declared competitive set (research starting set)

Shared Valle Oriente / San Pedro set used for entity registry + CORE drafting:

- JW Marriott Hotel Monterrey Valle
- The Westin Monterrey Valle
- AC Hotel by Marriott Monterrey Valle
- Hilton Monterrey Valle
- Holiday Inn Monterrey Valle
- Wyndham Garden Monterrey Valle Real
- Novotel Monterrey Valle

Select-service / economy peers (observe-only / non-comparable for numeric CORE): Comfort Inn Monterrey Valle, ibis Monterrey Valle, Holiday Inn Express & Suites Monterrey Valle.

## Explicit non-goals for this freeze

- No paid provider measurement until preflight PASS + founder cost approval
- No expansion of `ADP_CERTIFIED_PROPERTY_IDS` until each property is certified
- No Leak Audit substitute for full ADP
- Do not assert Fibra Inn / PropCo ownership without separate diligence

## Next gates

1. Property profiles + scenarios + entity/CORE land in repo  
2. Preflight-only for each property  
3. Founder cost approval (~$8–12/hotel at Phillips sizing)  
4. Dry-run → apply → post-audit → certify → publish → share token
