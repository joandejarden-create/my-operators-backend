# CHANGELOG — Westin ADP dropdown fix

- Root cause: `customerDropdownVisible: false` after CERTIFIED publish
- Added shared `registerCertifiedAdpPropertyForCustomerDropdown`
- Wired into `publishExistingHotelAdpSnapshot` for all future CERTIFIED publishes
- Flipped Westin fixture flags (no frontend hardcode)
- ADP metrics / certification untouched
