# Wave 1 Mexico Outreach Plan

Generated: 2026-07-04T11:32:42.920Z

## Strategy (no SIGER / RNT signup)

1. **Corporate web / IR / LinkedIn first** — fastest path to founder sends.
2. **Tier A advisors in parallel** — lawyers and deal carriers with live Mexico deals.
3. **SIGER + RNT optional** — only if you need V1R legal-rep proof and have CURP access.

- Mexico Tier A in queue: **12**
- Import-ready verified contacts: **2**
- Needs manual research: **10**

## Send now (import-ready)

### Fibra Inn (A, 28 CALA props)

- **Contact:** Sergio Martinez Richo — Director of Investor Relations & ESG
- **Email:** ir@fibrainn.mx
- **LinkedIn:** https://mx.linkedin.com/in/sergiomr
- **Tier:** V1R
- **Website:** https://fibrainn.mx

### Grupo Brisas (A, 11 CALA props)

- **Contact:** Antonio Cosío Pando — Director General / CEO
- **Email:** info@brisas.com.mx
- **Tier:** V1R
- **Website:** https://brisas.com.mx

## Research queue

- **Fibra Hotel Mexico** (public_reit) — https://fibrahotel.mx → Eduardo López García
- **Irawadi Corp S.A.** (foreign_hq) — https://rcdhotels.com
- **Park Royal Hotels & Resorts** (private_operator) — https://parkroyalhotels.com
- **Estancias Extendidas** (private_operator) — https://estanciashoteles.com
- **Pueblo Bonito Hotels and Resorts** (private_operator) — https://pueblobonito.com
- **Eurostars Hotel Company S.L.** (foreign_hq) — https://eurostarshotels.com
- **Grupo Diestra** (private_operator) — https://grupodiestra.com
- **Hoteles MX** (private_operator) — https://hotelesmx.com
- **Pulso Inmobiliario** (private_operator) — find website
- **Arriva Hospitality Group** (private_operator) — https://arrivahospitality.com

## Commands

```bash
# Regenerate queue + drafts
node scripts/report-gtm-owner-registry-enrichment-queue.mjs --tier-a-eligible --limit=30
node scripts/draft-gtm-mx-registry-enrichments.mjs

# Preview Wave 1 imports
node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run

# Apply to GTM Contacts
node scripts/import-gtm-wave1-mx-enrichments.mjs --apply
```