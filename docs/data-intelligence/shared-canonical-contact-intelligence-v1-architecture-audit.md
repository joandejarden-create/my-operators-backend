# Shared Canonical Contact Intelligence V1 — Architecture & Field-Mapping Audit

**Status:** Phase 1 complete (read-only).  
**Authority:** Founder directive — Airtable SoT across GDI and Hotel Explorer.  
**Prohibited:** Legacy Hotel Census as person/contact SoT.

## 1. Canonical Airtable schema (reuse — do not recreate)

| Table | Role | Key fields |
|---|---|---|
| **HI Org Contacts** | Canonical person × org affiliation | `Person ID`, `Display Name`, `Organization ID`, `Title`, `Function / Role`, `Hotel / Commercial Responsibility`, `Verification State`, `Person Dedupe Key`, `Affiliation Dedupe Key`, linked `HI Contact Channels` |
| **HI Contact Channels** | Canonical email / phone / LinkedIn | `Channel ID`, `Channel Kind`, `Channel Value`, `Subject Type/ID`, `Person` link, `Classification`, `Verification Status`, `Provider`, `Source Name/URL`, `Access / Publication Restriction`, `License / Retention Class`, `Last Checked At`, `Provider Request ID` |
| **HI Organizations** | Employer org | `Organization ID`, `Legal/Trading Name`, `Official Website` |
| **HI Hotel–Org Relationships** | Hotel ↔ org | `HPC Hotel ID`, `Organization ID`, `Relationship Type` |

Contract: `lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js`.

## 2. Where named professionals are stored today

| Store | Product | Notes |
|---|---|---|
| HI Org Contacts | HE / shared | **Canonical person SoT** |
| GDO `primaryContact*` + payload | GDI | Embedded; must reference HI `Person ID` going forward |
| CI local store packages | HE seed/demo | Not ownership writes; overlay from HI when available |
| GDI durable file index | GDI interim | Live Airtable person writes hard-off historically |

## 3. Where channels are stored

| Channel | Durable SoT | Transient |
|---|---|---|
| Business email | HI Contact Channels (Findymail-eligible) | Surfe session only |
| Business phone | HI Contact Channels | Surfe session only |
| LinkedIn | HI Contact Channels (preferred); HI Org Contacts Notes fallback | — |
| GDI typed email/phone | GDO columns (must hydrate from HI, not diverge) | — |

## 4. Person identifiers

| ID | Format | Use |
|---|---|---|
| **HI `Person ID`** | Canonical string on Org Contact | **Shared Person ID** |
| `personDedupeKey` | `person::{name}::{org}` | HI uniqueness |
| GDI `personIdentityKey` | sha256(name\|org).slice(16) | GDI grouping; **map → Person ID** |
| GDO `primaryContactId` | Opportunity-scoped | Not HI Person ID |

## 5. Duplicate identity findings (Bethesda Marriott `recLuxvwwxID7U2B8`)

- 8 HI Org Contact rows → 5 unique people (Validated vs Evidence Collected forks).
- Overlay prefers Validated + LinkedIn evidence by display name.
- Findymail dedupes by name+org before paid lookup.
- **Do not** merge on similar names alone; keep reversible GDI↔HI map; no destructive FK rewrite without migration plan.

## 6. Provider posture

| Provider | Persist PII? | Auto in E2E? |
|---|---|---|
| Findymail | Yes — eligible verified business email to HI Channels | Only when unresolved + authorized |
| Surfe | **Never** | User-initiated click only |

## 7. Schema gaps (no unauthorized create)

1. GDI still embeds contacts on GDO instead of always linking HI Person ID.
2. Optional typed GDO `personIdentityKey` / rights fields — founder-gated.
3. Two historical Findymail write destinations (GDO vs HI Channels) — consolidate on HI Channels + shared resolver.
4. Duplicate HI Org Contact rows undermine uniqueness (Bethesda).
5. HE hotel GET historically overlaid HI only when local package was sparse — **fixed in V1** to always prefer HI graph when present.

## 8. Architecture target

```
Findymail (authorized) ──persist──► HI Contact Channels
Surfe (click) ──session only──► UI (never Airtable PII)

HI Org Contacts + Channels ──shared resolver──► HE Contact Intelligence UI
                                             └► GDI Who to Contact / Contact Details
```

**Acceptance:** One Person ID → one eligible channel record → one shared resolver → consistent GDI + HE display.
