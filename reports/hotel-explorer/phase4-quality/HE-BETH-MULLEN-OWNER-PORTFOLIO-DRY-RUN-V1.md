# Bethesda / Mullen — Owner Portfolio Dry-Run (v1)

**Status:** DRY-RUN ONLY — `applyAuthorized: false`  
**Generated:** 2026-11-19  
**Staged packet:** `HE-BETH-MULLEN-OWNER-PORTFOLIO-STAGED-V1.json`  
**Source:** https://mullencap.com/portfolio/ (public, no paid research)

## Owner → Hotels → Operators model

```
Mullen Real Estate Capital (economic sponsor / proposed)
  └─ Marbeth Acquisition LLC (PropCo / legal title — Bethesda)
       └─ Bethesda Marriott (HPC recLuxvwwxID7U2B8)
            └─ Managed by Marriott International
  └─ [7 UNRESOLVED_HOTEL_CANDIDATE portfolio listings]
            └─ Operator Not Verified (pending HPC match + management evidence)
```

Legal title, economic ownership/control, and management remain separate relationship types.

## Verified vs proposed coverage

| Bucket | Count | Notes |
| --- | --- | --- |
| Verified current owned/controlled | 0 | None Airtable-confirmed yet in this staged packet |
| Proposed (HPC matched) | 1 | Bethesda Marriott — SDAT PropCo + portfolio association; still **Proposed** |
| Unresolved org→hotel match | 7 | Website listings only — **Pending HPC Match** |
| Historical / realized (excluded) | 18 | Asterisked MREC assets — not in current totals |
| Completeness | PARTIAL | Scope **Verified only** → 0 hotels; **Verified + proposed** → 8 |

A published portfolio listing is association evidence, not verified current title/control.

## HPC matching

| Hotel | Status | HPC ID |
| --- | --- | --- |
| Bethesda Marriott | MATCHED | `recLuxvwwxID7U2B8` |
| AC Hotel Newtown Square | UNRESOLVED | — |
| Element Ewing | UNRESOLVED | — |
| Element Ewing / Princeton area listings | UNRESOLVED | — |
| Remaining MREC current US listings in staged JSON | UNRESOLVED | — |

## Reconciliation (owned set)

Using `owner-portfolio-operator-view-v1` on `owned_controlled_current`:

- Owned hotels = operator-group hotels
- Known rooms = 407 (Bethesda only); unresolved-room hotels excluded from known sum
- Operator groups: Marriott International (1) + Operator Not Verified (7)

## Airtable write plan (not executed)

Destination: HI Organizations + HI Hotel–Org Relationships + HI Graph Evidence (+ Ownership Research Runs metadata).

Proposed idempotent upserts (founder approval required):

1. Upsert org: Mullen Real Estate Capital
2. Upsert org: Marbeth Acquisition LLC (PropCo)
3. Upsert OWNED_BY / CONTROLLED_BY for matched HPC hotels only
4. Stage unresolved candidates for census match — do not invent HPC duplicates
5. Do not copy portfolio assertions into HPC free-text fields

**No production Airtable apply in this cycle.**

## UI surfaces

Founder Preview Organization & Portfolio:

- **Owned / Controlled Hotels** (ada-table)
- **Who Manages These Hotels?** (expand/collapse operator groups)

Same hotel set; totals must reconcile.
