# Hotel Explorer Cross-Tab Consistency Report

- **Version:** he-cross-tab-consistency-report-v1
- **Generated:** 2026-11-27T17:22:07.409Z
- **Mode:** Fixture
- **Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)
- **Browser:** skipped → NOT_TESTED
- **E2E certification:** **PASS**

## Summary

| PASS | FAIL | UNVERIFIED | NOT_TESTED | Unresolved verified/proposed FAILs |
| --- | --- | --- | --- | --- |
| 13 | 0 | 2 | 0 | — |

## Gates

- `HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT` = **YES**
- `HE_CROSS_TAB_PARITY_VALIDATED` = **YES**
- `HE_ORG_PORTFOLIO_HYDRATION_VALIDATED` = **YES**
- `HE_SUBMARKET_COHORT_VALIDATED` = **YES**
- `HE_SUBMARKET_COVERAGE_DISCLOSED` = **YES**
- `HE_CROSS_TAB_CONSISTENCY_REPORT_GENERATED` = **YES**
- `HE_E2E_CROSS_TAB_CERTIFICATION` = **YES**

## Attributes

### Guestrooms (Rooms / Keys) (`rooms`) — PASS

- Canonical source: Hotel Property Census · Rooms / Keys
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `407` (rooms)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | 407 | null | PASS | — | — |
| rooms | 407 | null | PASS | — | — |
| organization | 407 | null | PASS | — | — |
| submarket | 407 | null | PASS | — | — |
| area | 407 | null | PASS | — | — |
| ownership | 407 | null | PASS | — | — |

### Current brand (`brand`) — PASS

- Canonical source: Hotel Property Census · Current Brand / Affiliation
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"Marriott Hotels"` (brand_name)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Marriott Hotels" | null | PASS | — | — |
| ownership | "Marriott Hotels" | null | PASS | — | — |
| organization | "Marriott Hotels" | null | PASS | — | — |
| submarket | "Marriott Hotels" | null | PASS | — | — |
| area | "Marriott Hotels" | null | PASS | — | — |
| sources | "Marriott Hotels" | null | PASS | — | — |

### Dealality market (`market`) — PASS

- Canonical source: Hotel Property Census · Market (Dealality)
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"Bethesda / Montgomery County"` (market_label)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Bethesda / Montgomery County" | null | PASS | — | — |
| submarket | "Bethesda / Montgomery County" | null | PASS | — | — |
| area | "Bethesda / Montgomery County" | null | PASS | — | — |
| organization | "Bethesda / Montgomery County" | null | PASS | — | — |
| ownership | "Bethesda / Montgomery County" | null | PASS | — | — |

### Dealality submarket / corridor (`submarket`) — PASS

- Canonical source: Hotel Property Census · Submarket (Dealality corridor)
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"Pooks Hill / Rockville Pike"` (submarket_label)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Pooks Hill / Rockville Pike" | null | PASS | — | — |
| submarket | "Pooks Hill / Rockville Pike" | null | PASS | — | — |
| area | "Pooks Hill / Rockville Pike" | null | PASS | — | — |

### Operator / management company (`operator`) — PASS

- Canonical source: Hotel Property Census · Operator / Management Company
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"Marriott International"` (org_name)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Marriott International" | null | PASS | — | — |
| ownership | "Marriott International" | null | PASS | — | — |
| organization | "Marriott International" | null | PASS | — | — |
| relationships | "Marriott International" | null | PASS | — | — |
| sources | "Marriott International" | null | PASS | — | — |

### Property company (legal / PropCo) (`propertyCompany`) — PASS

- Canonical source: HI Hotel–Org Relationships · Legal Owner / PropCo
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"Marbeth Acquisition"` (org_name)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Marbeth Acquisition" | null | PASS | — | — |
| ownership | "Marbeth Acquisition" | null | PASS | — | — |
| organization | "Marbeth Acquisition" | null | PASS | — | — |
| relationships | "Marbeth Acquisition" | null | PASS | — | — |
| sources | "Marbeth Acquisition" | null | PASS | — | — |

### Economic owner / sponsor (`economicOwner`) — PASS

- Canonical source: HI Hotel–Org Relationships · Economic Owner
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Proposed): `"Mullen Real Estate Capital"` (org_name)
- Notes: Proposed economic sponsorship must not silently render as Validated in any tab.

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Mullen Real Estate Capital" | null | PASS | — | — |
| ownership | "Mullen Real Estate Capital" | null | PASS | — | — |
| organization | "Mullen Real Estate Capital" | null | PASS | — | — |
| relationships | "Mullen Real Estate Capital" | null | PASS | — | — |
| contacts | "Mullen Real Estate Capital" | null | PASS | — | — |
| sources | "Mullen Real Estate Capital" | null | PASS | — | — |

### Economic ownership classification (`economicOwnerClassification`) — PASS

- Canonical source: Founder classification / HI Economic Owner
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Proposed): `"Proposed"` (classification)
- Notes: Classification drift Validated↔Proposed across tabs is FAIL.

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Proposed" | null | PASS | — | — |
| ownership | "Proposed" | null | PASS | — | — |
| organization | "Proposed" | null | PASS | — | — |
| relationships | "Proposed" | null | PASS | — | — |
| sources | "Proposed" | null | PASS | — | — |

### Chain scale (`chainScale`) — PASS

- Canonical source: Hotel Property Census · Chain Scale
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"Upper Upscale"` (chain_scale)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Upper Upscale" | null | PASS | — | — |
| submarket | "Upper Upscale" | null | PASS | — | — |
| area | "Upper Upscale" | null | PASS | — | — |

### Operating / radar status (`status`) — PASS

- Canonical source: Hotel Property Census · Radar Status
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"Open"` (status)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| hotel | "Open" | null | PASS | — | — |
| submarket | "Open" | null | PASS | — | — |
| area | "Open" | null | PASS | — | — |
| organization | "Open" | null | PASS | — | — |

### Total meeting space (`meetingSpaceTotal`) — UNVERIFIED

- Canonical source: HPC physical-product / staged meeting inventory
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Unverified): `null` (sq_ft)
- Notes: Compare sq ft only; never mix marketing private-space figures with capacity-chart totals.

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| rooms | null | null | UNVERIFIED | Canonical value not verified — do not invent to satisfy tests | — |
| hotel | null | null | UNVERIFIED | Canonical value not verified — do not invent to satisfy tests | — |
| sources | null | null | UNVERIFIED | Canonical value not verified — do not invent to satisfy tests | — |

### Meeting room count (`meetingRoomCount`) — UNVERIFIED

- Canonical source: HPC physical-product / staged meeting inventory
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Unverified): `null` (rooms)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| rooms | null | null | UNVERIFIED | Canonical value not verified — do not invent to satisfy tests | — |
| hotel | null | null | UNVERIFIED | Canonical value not verified — do not invent to satisfy tests | — |
| sources | null | null | UNVERIFIED | Canonical value not verified — do not invent to satisfy tests | — |

### Contact intelligence presence (`contactsPresence`) — PASS

- Canonical source: Contact Intelligence / HI contacts
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `true` (boolean_or_count)

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| contacts | true | null | PASS | — | — |
| organization | true | null | PASS | — | — |

### Portfolio focus-hotel rooms (`portfolioFocusRooms`) — PASS

- Canonical source: HPC Rooms / Keys via shared hotel view (Org Portfolio focus row)
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `407` (rooms)
- Notes: Verified rooms missing on Org focus row = FAIL (mapping defect).

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| organization | 407 | null | PASS | — | — |

### Submarket cohort identity (like-for-like) (`submarketCohortId`) — PASS

- Canonical source: HE HPC Submarket Snapshot · matched_hpc_inventory cohort
- Canonical record: `recLuxvwwxID7U2B8`
- Expected (Verified): `"matched_hpc_inventory"` (cohort_id)
- Notes: Totals must identify cohort; compare like-for-like only.

| Tab | API value | Rendered | Result | Root cause | Correction |
| --- | --- | --- | --- | --- | --- |
| submarket | "matched_hpc_inventory" | null | PASS | — | — |

## Rules

- Verified canonical values missing from a tab → FAIL (not missing research)
- Genuine unverified facts → UNVERIFIED (never invent)
- Proposed ownership must not silently become Validated
- Submarket totals must identify cohort; compare like-for-like only
- Consistent units/definitions across tabs
- Browser-skipped checks → NOT_TESTED, never PASS
- Any unresolved verified/proposed cross-tab FAIL → E2E certification FAIL
