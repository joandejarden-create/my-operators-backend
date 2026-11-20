# Public Contact Discovery 2.0 — Two-case certification gate

**Status:** READY FOR FOUNDER REVIEW (STOP — no full DB backfill)

## Provider configuration

| Provider | Status |
|----------|--------|
| Exa (`EXA_API_KEY`) | **configured** · connectivity OK |
| Serper (`SERPER_API_KEY`) | **missing** |
| Chosen mode | **EXA_ONLY** (hybrid ready when Serper is added) |

No secrets printed. No Surfe spend. No Serper account created.

## Case A — Maritza Jeanette Medina García

- Email: `maritza.medina@jalisco.gob.mx`
- Phone: `+52 33 3819 2333` (office w/ ext `42333` / `47542`)
- Surfe CTA: **none**
- Authority: RELEVANT_COMMERCIAL_ENTRY_POINT (not hotel-booking buyer)

## Case B — Alejandra Vázquez (Ecomondo / IGECO)

- Email: `alejandra.vazquez@igeco.mx`
- Phone: **not invented** (unresolved)
- Surfe CTA: **Find Phone** only
- Secondary: Paulina Sánchez · `paulina.sanchez@igeco.mx`
- Authority: event-commercial Sales Manager · hotel-sourcing unverified

### Search-first proof (Exa)

Founder-style queries under Exa-only mode surfaced `ecomondomexico.com.mx/…/contact/` (see `TWO_CASE_CERT.json` searchHit).  
Note: plain `name + title + org` is sometimes insufficient on Exa neural search; the plan also runs `name + org + contact` and `site:` variants. Snippets are candidates only — page fetch + person-scoped extract certifies channels.

Cert cost (Exa search path): ~**$0.014** for this run. Surfe: **0**.

## Persistence

- Local opportunities.json updated for both cases.
- Airtable: **dry-run proposals only** (idempotent upsert shape; `surfeDerived: false`).
- Apply + read-back + gated publication refresh: **awaiting founder OK**.

## Screenshots

- `screenshots/01-maritza-public-contact-card-surfe-last.png`
- `screenshots/02-local-drawer-comparison-maritza-before-after.png`
- `screenshots/03-alejandra-public-contact-card-surfe-last.png`

## Commands

```bash
npm run test:gdi-public-contact-discovery-v1
npm run gdi:two-case-public-contact-discovery-cert -- --offline-seed --airtable-dry-run
npm run gdi:two-case-public-contact-discovery-cert -- --exa-search --airtable-dry-run
```

## Next (after founder OK)

1. Authorized Airtable apply + read-back for both contacts.
2. Gated publication refresh so default `/app` + external share clear Surfe CTAs.
3. Bounded 100-contact pilot (≤ ~$10 search ceiling) then all-hotel backfill.
4. Integration Controller merge + deploy.

**STOP — two-case certification complete. No broad backfill.**
