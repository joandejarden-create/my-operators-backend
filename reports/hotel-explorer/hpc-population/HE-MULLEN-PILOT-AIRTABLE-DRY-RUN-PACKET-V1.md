# Mullen Pilot — Airtable Dry-Run Packet (v1)

**Status:** DRY-RUN ONLY · `applyAuthorized: false` · **STOP before production apply**  
**Base:** `appCCUsuGsE1ifoLk`  
**Companion identity report:** `HE-MULLEN-PILOT-IDENTITY-RESOLUTION-V1.json`

## Founder authorization boundary

This packet prepares CREATE/UPDATE proposals. It does **not** execute Airtable mutations.

Required before apply:

1. Founder review of identity outcomes (especially Cap Cana near-neighbors and Papagayo census→HPC path).
2. Explicit authorization of the mutation list below.
3. Re-read + validate after apply.

## Identity summary (live read)

| Hotel | Outcome | Canonical id | Proposed Airtable action |
| --- | --- | --- | --- |
| Bethesda Marriott | MATCHED_HPC | `recLuxvwwxID7U2B8` | UPDATE (null-fill / HI edges only — no duplicate CREATE) |
| AC Hotel Newtown Square | NEW_HPC_CANDIDATE | — | CREATE HPC after min identity research packet |
| Carton House, a Fairmont Hotel | NEW_HPC_CANDIDATE | — | CREATE HPC after min identity research packet |
| Hilton Garden Inn Newtown Square Radnor | NEW_HPC_CANDIDATE | — | CREATE HPC after min identity research packet |
| JW Marriott Costa Elena Resort All-Inclusive | MATCHED_HPC | `recq9KWr8UE7z2549` | UPDATE / alias linkage (Dreams Las Mareas) + HI edges |
| Secrets Cap Cana Resort & Spa | MATCHED_HPC (exact name amid neighbors) | `recd7XAM6tLjzQhaI` | UPDATE / confirm not Dreams Cap Cana |
| Secrets Papagayo Costa Rica | MATCHED_HOTEL_CENSUS | Hotel Census `recyX1wLsHBJYRPw7` | Steward: link/promote to HPC — do not duplicate |
| Secrets St. Lucia | NEW_HPC_CANDIDATE | — | CREATE HPC after confirm operating name |

## Mutation classes (none executed)

### A. HPC property facts (`tbl9aY5ijiuIzzWam`)

- Prefer `hpc-shared-upsert-v1` null-fill for matched records.
- CREATE only via `hpc-authorized-create-reuse-v1` after collision recheck.
- Never copy ownership graphs into HPC free-text.

### B. HI graph (`HI Organizations`, `HI Hotel–Org Relationships`, `HI Graph Evidence`, `HI Ownership Research Runs`)

- Upsert Mullen Real Estate Capital (PROPOSED until verified).
- Upsert PropCo / operator orgs when verified.
- OWNED_BY / CONTROLLED_BY / MANAGED_BY only for matched HPC hotels.
- Preserve proposed ≠ verified.

### C. Explicitly out of scope this batch

- Paid provider spend
- Radar Status mutation
- Legacy Hotel Census writes
- Customer publication

## Duplicate prevention evidence

- Live HPC collision search reused (`HPC_LIVE_IDENTITY_COLLISION_V1`).
- Hotel Census searched read-only before NEW_HPC.
- Cap Cana exact-name promotion retains near-neighbor list for founder review.
- JW Costa Elena matched existing HPC; Dreams Las Mareas retained as alias evidence — not a second CREATE.

## Next steps (Phases 2–4)

1. Minimum verified research packets per NEW_HPC_CANDIDATE + Papagayo census→HPC bridge.
2. Field-level provenance dry-run JSON (before/after per field).
3. Founder approve → apply → re-read → Founder Preview deep-links.
