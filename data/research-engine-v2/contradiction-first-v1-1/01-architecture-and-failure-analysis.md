# V1.1 Architecture & Failure Analysis

## Exact V1 false-positive causes

| Hotel | V1 proposal | Failure mechanism |
|-------|-------------|-------------------|
| Hotel Indigo Mexico City Downtown | Pipeline→Open + Indigo→InterContinental | **Parent-brand contamination** + **fuzzy name collision** + **wrong geography/brand sibling** (matched `intercontinental/.../mexha`). Match score 0.50 Medium but treated as material. |
| Hotel Indigo Tulum | Pipeline→Open + Indigo→Holiday Inn | **Same-city sibling** + **parent-brand contamination** (Holiday Inn Tulum). Match **Low** (0.43) still emitted material correction. |
| Hotel Indigo Guadalajara Providencia | Pipeline→Open | **Same-brand sibling property** (likely Expo `gdlal` vs Providencia pipeline). Match **Low** (0.43). |
| Faranda Collection Cali | Individuals→Ascend | **Same-city sibling** + **weak official-directory evidence** (HTTP 403) + Low confidence 0.25. |
| Casa Francia Autograph | → Tribute Casa Nizuc | **Fuzzy name collision** ("Casa") + **wrong property** + Match **Low** (0.35). |

## Failure classes addressed in V1.1

1. Fuzzy name collision → distinctive-token + Exact/High gate
2. Wrong geography → hard country/city reject + explicit alias map only
3. Same-brand sibling → shared distinctive place-token requirement
4. Parent-brand contamination → same property-level brand filter before IHG fetch; brand conflict ≠ identity
5. Weak official-directory evidence → 403/no-page blocks reflags; corroboration tiers
6. Insufficient corroboration → Pipeline→Open needs Exact/High match + official bookable; Medium → Review only

## Modules

- `match-confidence.js` / `geo-normalize.js` / `corroboration.js` / `directory-gaps.js`
- Adapters: IHG / Marriott soft-brand / Choice / Hilton thin / generic
- `checkHotelFreshness` V1.1 gated corrections
