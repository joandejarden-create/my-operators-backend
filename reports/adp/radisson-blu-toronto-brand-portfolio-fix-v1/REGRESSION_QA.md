# REGRESSION QA

Shared resolver change: hotels without measurement-available BPP still fall through to awaiting copy.

| Property | Status | Lens | Rank / peers | Notes |
|----------|--------|------|--------------|-------|
| Bethesda Marriott | READY | Marriott Bonvoy | #1 of 4 | Unchanged populated |
| Waterstone (`adp_waterstone_boca_raton`) | READY | Hilton Honors | #4 of 6 | Unchanged populated |
| Mayflower Hotel | AWAITING_FIRST_MONITORING | Marriott Bonvoy | — | Fallback retained |
| Wyndham San Diego Bayside | AWAITING_FIRST_MONITORING | Wyndham Rewards | — | Fallback retained |
| YOTEL Geneva Lake | AWAITING_FIRST_MONITORING | YOTEL | — | Fallback retained |
| Nobu London Portman | AWAITING_FIRST_MONITORING | Nobu Hotels | — | Fallback retained |
| Radisson Blu Toronto Downtown | READY | Choice Privileges | #1 of 6 | Fixed |

Toronto-specific shortcuts: NONE.
