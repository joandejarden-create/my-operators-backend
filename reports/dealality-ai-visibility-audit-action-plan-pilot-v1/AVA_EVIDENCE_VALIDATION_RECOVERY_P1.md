# AVA Evidence Validation Recovery — Priority 1 Progress

**Generated:** 2026-11-05T14:14:23.660Z
**Scope:** Waterstone · Castillo Son Vida · Sheraton Mallorca Arabella Golf
**Paid research:** $0 · No publication · No founder-approval changes

## Results

| Hotel | Before | After | Gate | Replaced | Claims corrected | Manual left | Broken |
|---|---|---|---|---|---|---|---|
| Waterstone Resort & Marina | `VALIDATION_REQUIRED` | `AUDIT_VALIDATED` | PASS | 1 | 2 | 0 | 0 |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | `AUDIT_VALIDATED` | `AUDIT_VALIDATED` | PASS | 0 | 1 | 0 | 0 |
| Sheraton Mallorca Arabella Golf Hotel | `VALIDATION_REQUIRED` | `AUDIT_VALIDATED` | PASS | 1 | 0 | 0 | 0 |

### Waterstone Resort & Marina

- **Original unresolved:** https://www.hilton.com/en/hotels/bctbuqq-waterstone-resort-and-marina-boca-raton/; https://www.waterstoneresort.com/
- **Replaced:** https://www.waterstoneresort.com/ → (removed from hyperlink register — prose only)
- **Claims corrected:** Executive theme: Hilton 7,500 sq ft figure withdrawn as live evidence; hotel-site 18,899 retained with verification; Domain-conflict finding: removed waterstoneresort.com customer-facing hyperlink (DNS NXDOMAIN); claim retained in prose
- **Remaining manual:** none
- **PDF:** `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/waterstone-boca-raton/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Waterstone Resort & Marina - NOVEMBER2026.pdf`
- **SHA-256:** `c0c9d57c870915e798f18466c7b83fba048c20fde41540d86cf50a28cec07e29`

### Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca

- **Original unresolved:** https://www.arabellagolf.com/
- **Replaced:** —
- **Claims corrected:** Arabella golf relationship retained; evidence URL swapped to verified third-party golf source
- **Remaining manual:** none
- **PDF:** `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/castillo-hotel-son-vida/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca - NOVEMBER2026.pdf`
- **SHA-256:** `97bbd498da8f9a489913644909639bd277e91ccb9f538a3a6c89b4564f9c3638`

### Sheraton Mallorca Arabella Golf Hotel

- **Original unresolved:** https://www.arabellagolf.com/
- **Replaced:** https://www.arabellagolf.com/ → https://golfnews.co.uk/golf-news/arabella-resort-mallorca/
- **Claims corrected:** —
- **Remaining manual:** none
- **PDF:** `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/sheraton-mallorca-arabella-golf/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Sheraton Mallorca Arabella Golf Hotel - NOVEMBER2026.pdf`
- **SHA-256:** `e48ee14bc99ea11253faea8f17cab1e1c1d3b13357c4452c3ad34b1dd78ab93c`

Escalation: L1 HTTP → L2 Playwright browser → L3 alternative source (applied above) → L4 manual (none required if gate PASS).

Next: Wyndham San Diego Bayside and Spice Island Beach Resort after this Priority 1 report.
