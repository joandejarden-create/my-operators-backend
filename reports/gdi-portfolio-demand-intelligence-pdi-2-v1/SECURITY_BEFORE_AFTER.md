# PDI-2 security before/after

| # | Vulnerability (before) | Mitigation (after) | Test |
|---|---|---|---|
| 1 | Client `x-dealality-member-id` / session headers could spoof identity | Ignored; `clientSuppliedIdentityAttempt` fail-closed; only `req.dealalityUser.memberstackId` | PASS |
| 2 | Query `sessionId` accepted | Removed from auth path | PASS |
| 3 | Company-name / notes auth | Rejected | PASS |
| 4 | `customerSurface` omission exposed research-only | Customer-published is **default**; internal requires `GDI_INTERNAL_RESEARCH_READ` | PASS |
| 5 | `customerSurface=0` bypass | Privilege-gated | PASS |
| 6 | Missing marketId opportunities included | Exact marketId match required | PASS |
| 7 | Null allowlist broadened access | Missing allowlist → `MARKET_ALLOWLIST_REQUIRED` | PASS |
| 8 | Empty allowlist unclear | Empty → deny all markets | PASS |
| 9 | Market publish implied all hotels | Per-hotel `publishedHotelIds` / `hotelPublication` | PASS |
| 10 | Unpublished hotel drawer links | Withheld when not hotel-published | PASS |
| 11 | HTTP loaded Aimbridge fixtures | Live path fail-closed without `data/…` registry | PASS |
| 12 | Pilot unauth read on portfolio routes | `gdiPortfolioReadAuth` requires Bearer + Dealality user | PASS |
