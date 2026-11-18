# Founder decisions required

**Gate:** Approve or amend before any new production Airtable apply beyond PR #367.

Prior gates still binding: PR #362 table-purpose matrix (D1–D8), commercial eligibility standard (PR #366 / adapter V1).

---

## RC1 — Rock Creek structured Venue ID

**Decision:** Authorize single-record update on `recf35Q9B6jpX3Ivm` to restore structured Venue ID to `pev_53b689545914754d`?

- [ ] **Approve** as proposed in `ROCK_CREEK_NON_DESTRUCTIVE_REMEDIATION_PLAN.md`
- [ ] **Defer** until after broader venue-ID policy review
- [ ] **Reject** — keep pilot structured ID (not recommended; breaks link/query consistency)

## RC2 — Bethesda GDO priority update

**Decision:** Authorize UPDATE on `gdi_opp_accp_annual_meeting_20261001` (HIGH → DISQUALIFIED)?

- [ ] Approve in next pilot allowlist
- [ ] Defer — keep Airtable as-is until hotel review

## RC3 — Org-name CONFLICT resolution policy

Records: `rx_d_f5c3bd3d073a` (Radisson), `gdi_opp_series_ctn_annual_conference_2027` (Bethesda).

- [ ] Adopt filesystem org string as authoritative on sync
- [ ] Adopt Airtable org string
- [ ] Manual merge with evidence note — no auto-sync

## RC4 — YOTEL disjoint namespaces (0 ID overlap)

108 AT-only + 42 FS-only, all DEFERRED.

- [ ] Preserve both sides indefinitely; reconcile later by evidence project
- [ ] Authorize targeted ID bridge study (read-only) before any import
- [ ] Do not import AT-only into filesystem

## RC5 — Radisson FS-only 36 rows

- [ ] Keep DEFERRED until per-row eligibility review
- [ ] Authorize future qualified subset only via named allowlist
- [ ] No bulk create

## RC6 — Guadalajara zero GDO persistence

43 FS-only, all DEFERRED.

- [ ] Confirm: no GDO create until hotel enters authorized Full E2E persistence wave
- [ ] Schedule separate Guadalajara persistence design review

## RC7 — Multi-hotel production sync scope

- [ ] **Minimal:** Rock Creek venue only
- [ ] **Minimal + Bethesda:** RC1 + optional RC2
- [ ] **Hold:** no production writes until PRIMARY hotels reconciled

## RC8 — Publication and customer surface

Confirm: no automatic promotion from Airtable sync to CURRENT packages.

- [ ] Acknowledged (required)

---

**How to proceed:** Reply with RC1–RC7 selections, or approve `PRODUCTION_SYNC_ACTIVATION_PROPOSAL.md` Bundle A/B explicitly.