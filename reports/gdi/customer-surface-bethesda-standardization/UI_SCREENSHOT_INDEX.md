# UI Screenshot Index

## Browser-verified (2026-10-04)

| View | Result |
|---|---|
| YOTEL browse `?hotelId=recrPQcZg7SFARRb2` | **13** tiles; account-first titles; buyer org path in footer; **no** Demand Campaigns section |
| Bethesda browse `?hotelId=recLuxvwwxID7U2B8` | **35** tiles; **no** Demand Campaigns section |
| DOM check | `htmlHasCampaigns=false` on both |

## Capture checklist
1. Bethesda Opportunities browse (reference)
2. YOTEL Opportunities browse — 13 cards, no campaigns
3. YOTEL Ready card View Details — buyer + contact path
4. Empty/filter state — no campaign copy
5. Mobile viewport YOTEL browse
6. Share page empty/ready copy

Routes:
- `/group-demand-intelligence.html?hotelId=...`
- `/group-demand-intelligence-share.html`
