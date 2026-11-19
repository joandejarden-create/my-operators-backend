# Secondary contact coverage — Sheraton Guadalajara Expo

**Date:** 2026-11-19  
**Scope:** Local drawer-comparison inventory (79 deduped). No Airtable writes / no publication overwrite.

## Finding

Secondary is **optional by design** (`scaling-two-contact-standard-v1`). Empty slot copy is intentional research guidance, not a broken required field.

| Metric | Count |
| --- | --- |
| Opportunities audited | 79 |
| Named primary in package | 79 |
| Named `secondaryContact` in package | **0** |
| Latent candidates in package (`namedStakeholders` / `backupContacts` / CIB secondary) | **0** |
| Primary with About / profile / source URL | 42 |

Presentation was not dropping secondaries — **packages never carried a distinct second person**. Attach logic cannot invent people.

## What improves consistency (without inventing contacts)

1. **Research-time (durable):** runners already call `attachSecondaryNamedContact` when `namedStakeholders` exist. Require a second distinct person on team/about/directorio pages before publish when public evidence allows.
2. **Candidate harvest (done):** also read `namedContacts`, `secondaryContacts[]`, and CIB `whoToContact.secondary|additional`.
3. **Team-page fill for comparison preview (done):** when primary has a reachable About/team URL that lists another named person + role, attach that person as optional secondary (profile = same page; no invented email/phone).
4. **Copy (done):** empty secondary reads *“Alternative / referral contact not yet identified (ideal, not required).”*

## Example

Ecomondo / Alejandra Vázquez About page also lists **Paulina Sánchez — Sales Manager** → comparison drawer now surfaces her as secondary with Company / team page link.
