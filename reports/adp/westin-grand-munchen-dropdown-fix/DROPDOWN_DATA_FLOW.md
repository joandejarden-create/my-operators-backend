# ADP Dropdown Data Flow

## Canonical path

1. **Frontend:** `public/js/ai-demand-positioning/ai-demand-positioning.js` → `loadProperties()`
2. **API:** `GET /api/ai-demand-positioning/properties` (`api/ai-demand-positioning.js`)
3. **Catalog:** `listPropertyProfiles()` in `lib/ai-demand-positioning/data-model.js`
4. **Source:** `fixtures/ai-demand-positioning/*-property-profile.json`
5. **Eligibility filter:** skip when `customerDropdownVisible === false`
6. **Sort:** `name.localeCompare`
7. **Label:** `formatPropertySelectorLabel({ name, city, state, region, country })`

No frontend hardcode of hotel options. Share tokens resolve a single property without enumerating others.

## Post-cert registration (new shared step)

`registerCertifiedAdpPropertyForCustomerDropdown` in  
`lib/ai-demand-positioning/client-readiness/register-certified-adp-dropdown-v1.js`

Invoked from `publishExistingHotelAdpSnapshot` after CERTIFIED publish so future hotels do not skip the dropdown flip.
