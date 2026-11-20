# Bethesda Marriott — Phase 3 Research Plan (16 genuine gaps)

**Pilot:** Bethesda Marriott · `recLuxvwwxID7U2B8`  
**Base:** `appCCUsuGsE1ifoLk`  
**Status:** APPROVED · W0 EXECUTED · **$0 spent** · STOP before Airtable apply — see `HE-BETHESDA-PHASE3-FOUNDER-REVIEW-V1.md`  
**Budget ceiling:** ≤ **$25** · actual **$0** (W1/W2 unused)  
**Architecture:** Free authoritative sources first · never treat brand parent/operator as economic owner without evidence

## Dependency order (commercial value)

```
Economic Owner → PropCo → UBO (if discoverable)
                 ↓
        Named ownership / AM / decision contacts
                 ↓
     Agreement profile · property history · evidence pack
                 ↓
        Remaining Overview gaps (star / phase)
```

## Gap catalog (all 16)

| # | Gap field | Tab | Priority | Commercial value | Depends on | Free-first sources | Paid only if | Canonical write destination |
|---|---|---|---|---|---|---|---|---|
| 1 | `economic_owner` | ownership | **P0** | Highest — outreach target | — | MD SDAT / county land records for 5151 Pooks Hill Rd; Maryland business entity search | Public records blocked / ambiguous | **HI Organizations** (new or matched org) + **HI Hotel–Org Relationships** (`Economic Owner`, Validated) + **HI Graph Evidence** |
| 2 | `property_company` | ownership | **P0** | High — legal title / PropCo | Economic owner research | Same deed/title package as #1 | Same | **HI Organizations** + **HI Hotel–Org Relationships** (`Legal Owner` / PropCo type) + **HI Graph Evidence** |
| 3 | `ubo_group` | ownership | **P1** | High — controlling sponsor | #1–2 | SEC / parent filings if REIT/public; MD entity ownership where public | Only if owner known but UBO opaque and commercially needed | **HI Organizations** (controlling org) + Rel edge + **HI Graph Evidence** — or leave UNRESOLVED with note |
| 4 | `decision_authority.people` | ownership | **P1** | High — who to speak with | #1 (owner identity) | Company websites, LinkedIn public, press | Surfe/enrichment ≤ remaining budget after #1–2 | **HI Org Contacts** + **HI Contact Channels** + Evidence |
| 5 | `qualified_signatories` | ownership | **P2** | Medium | #4 | Same as people paths; avoid inventing authority | Only with clear role evidence | **HI Org Contacts** (role/authority fields) + Evidence |
| 6 | `people_groups` | contacts | **P1** | High | #1, #4 | Same | Same | **HI Org Contacts** (grouped by role) |
| 7 | `emails_phones` | contacts | **P1** | High | #4 | Official directories before paid | Paid channel resolution only for verified people | **HI Contact Channels** |
| 8 | `key_people` | organization | **P1** | Medium–High | Org = owner/operator clarity | Marriott IR/leadership only if org is brand/operator context; **owner-side people** if economic owner org | Same | **HI Org Contacts** linked to correct org (not Marriott-only by default) |
| 9 | `asset_agreement_profile` | ownership | **P2** | Medium | #1–2 | Franchise/management disclosure if public; do not invent from brand name | Rarely paid | **HPC** (`Management Model` already present) + optional HI Rel notes / Evidence — **no fabricated franchise claim** |
| 10 | `property_history` | ownership | **P2** | Medium | #1 | Prior sale comps, MD filings, press | Optional | **HI Graph Evidence** + Ownership Research Run notes |
| 11 | `development_pursuit` | ownership | **P3** | Lower for pilot | — | Pipeline/news only if material | Defer | **HI Ownership Research Runs** |
| 12 | `source_coverage` | ownership | **P2** | Supporting | #1–10 | Derived from evidence set | $0 | **HI Graph Evidence** inventory + Run summary |
| 13 | `what_we_know[]` | ownership | **P2** | Supporting | #1–10 | Derived product truth set from validated edges + evidence | $0 | Derived read-model from HI Rel + Evidence (persist facts, not prose dump) |
| 14 | `sources` | ownership | **P2** | Supporting | All | Evidence URLs | $0 | **HI Graph Evidence** |
| 15 | `starRating` | hotel | **P3** | Low for ownership outreach | — | Official hotel / AAA if steward wants | Defer | **HPC** star/rating field if schema exists; else steward |
| 16 | `projectPhase` | hotel | **P3** | N/A for Open asset often | Radar Status=Open | Usually N/A | $0 | **HPC** `project_phase` only if pipeline; else leave blank |

## Spend plan (≤ $25)

| Wave | Scope | Est. cost | Stop condition |
|---|---|---:|---|
| W0 | Free SDAT / MD entity / deed for economic owner + PropCo | **$0** | Owner+PropCo identified with document URLs |
| W1 | Paid ownership packet only if W0 blocked | **≤ $15** | Validated Economic Owner + PropCo or explicit UNRESOLVED |
| W2 | Named contacts for **owner-side** org only | **≤ $10** remaining | 1–2 verified people + channels or stop |
| — | **Hard cap** | **$25** | No further paid spend without new founder approval |

Provider-level tracking: record provider, query, USD, timestamp on each **HI Ownership Research Run**.

## Hard rules

1. Marriott International / Marriott Hotels = brand parent / operator — **not** economic owner without a Validated Economic Owner relationship.
2. No KGPV fixture migration into Airtable.
3. No production Airtable writes until founder accepts this plan **and** authorizes apply (dry-run first).
4. Every new finding → Evidence URL + verification state before HE display as verified.
5. Idempotent upserts via existing HI graph upsert planner (no duplicate orgs/edges).

## Persistence readiness checklist

| Artifact | Table | Ready? |
|---|---|---|
| Owner / PropCo / UBO orgs | HI Organizations | Yes (existing schema) |
| Economic Owner / Legal Owner edges | HI Hotel–Org Relationships | Yes |
| People | HI Org Contacts | Yes |
| Emails / phones | HI Contact Channels | Yes |
| Source URLs / docs | HI Graph Evidence | Yes |
| Run cost / method | HI Ownership Research Runs | Yes |
| Property star / phase | HPC | Steward-only if needed |

## Explicit non-goals for Phase 3 pilot

- Completing KGPV depth with unverified content  
- Broad Surfe portfolio enrichment  
- Overwriting HPC Parent Company with economic owner  
- Creating a parallel ownership store  

---

**Founder gate:** Accept integration merge closeout + this plan before any paid enrichment.
