# LMC / Planner contact-depth — Mayflower + San Diego

Generated: 2026-10-15T22:15:00.000Z

## Verdict

Contact-depth is now a **required stage** of the standard GDI coordinator and weekly refresh path (shared `buyer_contact_enrichment_v1` — no second pipeline). Bounded named-person research ran for all **13** Mayflower + San Diego LMC/planner accounts from the prior publication package. **WHF published** after contact-route repair. **N Street Village** historical Mayflower hosting preserved without inventing current contract/familiarity. Queued six-hotel batch **not started**. No broad discovery.

## Budget (authoritative shared $11 ledger)

| Item | Value |
|------|-------|
| Authorized | **$11** (existing — no new allowance) |
| Before this pass | $5.25 spent / **$5.75** remaining |
| This pass (Surfe justified channel enrichment) | **~$1.25** (5 × $0.25 est.; ledger remaining **$4.50**) |
| After | Spent ~$6.50 / remaining **$4.50** |

Surfe used only where a relevant named person was established and a person-specific channel was still missing. Org inboxes did not block the queue. André Wells Surfe → identity conflict (no person email accepted). Gaslamp `sin@clubvipsd.com` → **quarantined** (employer/domain conflict).

## Process change (durable)

| Path | Before | After |
|------|--------|-------|
| Coordinator `buyer_contact_enrichment` | Default **queue_only** unless flags | **Always executes** public contact-depth; Surfe still `--authorize-surfe`; debug opt-out `--skip-buyer-enrichment-execute` |
| Weekly refresh | Queue only | Executes same `runBuyerContactEnrichmentPass` (public); Surfe only if `authorizeSurfe` |
| Axes A/B/C/D | Partial | Publication route ≠ named research complete; `NOT_ATTEMPTED` ≠ `EXHAUSTED_ORG_FALLBACK`; org inbox does not suppress queue |

Applies across associations, scaling/corporate, LMC, planners, PE referrals, sports (shared contract role buckets).

## Account register (13)

| Hotel | Opportunity ID | Person / role (after) | Responsibility | Person email / phone | Org fallback | Outcome | Remaining gap | Persist | Production visibility |
|-------|----------------|----------------------|----------------|----------------------|--------------|---------|---------------|---------|----------------------|
| Mayflower | `gdi_lmc_9a30c08278a92953` | Aimee Dominick — Owner/Principal | Confirmed relevant | aimee@adominick.com | info@adominick.com + phone | Named + channel | — | OK | Facing |
| Mayflower | `gdi_lmc_ceab66971a48a9f8` | Makenzie Delmotte — Senior Events Manager | Confirmed relevant | mdelmotte@nstreetvillage.org | info@ + phone | Named + channel | — | OK | Facing; **historical Mayflower 2015** documented |
| Mayflower | `gdi_lmc_5b4d51be7726617c` | Danielle Kambic — Dir. Sales, Wash. DC | Confirmed/potential | danielle.kambic@csi-dmc.com (Surfe) | contact form | Named + channel | Not confirmed booking authority | OK | Facing |
| Mayflower | `gdi_lmc_860cde704de88d7c` | André Wells — Founder/CEO | Confirmed relevant | — | info@eventsbyandrewells.com | Named, channels pending | Surfe identity conflict | OK | Facing |
| Mayflower | `gdi_lmc_00cd663a9f282697` | Roger J. Whyte II — Founder/CEO | Confirmed relevant | rwhyte@stratusfirm.com (Surfe) | phone + form | Named + channel | — | OK | Facing |
| Mayflower | `gdi_lmc_d670fde177ad418b` | Jodi Moraru — Founder | Confirmed relevant | jodi@evokedc.com (Surfe) | INFO@EVOKEDC.COM | Named + channel | — | OK | Facing |
| Mayflower | `gdi_lmc_82918469d12cfbc1` | — | Exhausted org fallback | — | **whf@whfdc.org** + **703-683-4742** + Contact form | Org fallback after research | Named events staff TBD | OK | **Facing (WHF published)** |
| Mayflower | `gdi_lmc_8bf1156a3b6bc8dc` | Liz DeBarros — CEO | **Referral entry only** | — (CEO email not used as buyer channel) | info@dcbia.org + phone | Referral lead | Meetings/events staff | OK | Facing |
| SD Bayside | `gdi_lmc_fa0049bb82e08bde` | Sahar Pearose — GM San Diego | Confirmed relevant | sahar.pearose@meetcohera.com / 310-570-6094 | ContactSanDiego@… | Named + channel | — | OK | Facing |
| SD Bayside | `gdi_lmc_c8d0bd468ce1cc64` | Cathy Abarca — Events/sponsorship | Confirmed relevant | cathy@sdpolicefoundation.org | phone + contact page | Named + channel | — | OK | Facing |
| SD Bayside | `gdi_lmc_97fb727376abb83b` | Sin Bosier — President & CEO | Confirmed relevant | — (clubvipsd quarantined) | gaslampevent@gmail.com + phone | Named, channels pending | Domain conflict | OK | Facing |
| SD Bayside | `gdi_lmc_94d719381b8dca86` | Karina Sousa — Club Secretary | Potentially relevant | karinakiwanis@gmail.com | phone + info@ | Named + channel | Not confirmed venue buyer | OK | Facing |
| SD Bayside | `gdi_lmc_9d0050cfbdb5925c` | Thomas Bui — Owner/President | Confirmed relevant | thomas@thomasbuilifestyle.com / phone | contact form | Named + channel | — | OK | Facing |

## Totals

| Metric | Count |
|--------|-------|
| Named person + usable person business channel | **9** |
| Named person, channels still pending | **2** (André Wells; Sin Bosier after quarantine) |
| Referral-only named lead | **1** (Liz DeBarros / DCBIA) |
| Org fallback after real named research | **1** (WHF) |
| Still “not researched” | **0** |
| Relying on organization routes after research (no person channel) | **3** (WHF org-only; André Wells; Gaslamp) |

## WHF disposition

- Prior: internal, `no_usable_route` (URL `…/general?type=CONTACT` failed path regex; email/phone not stored).
- Inspected live Contact Us: **usable inquiry form** + **whf@whfdc.org** + **703-683-4742**.
- Eligibility updated to accept `?type=CONTACT` forms; org fields repaired; **published hotel-facing**.
- Named events staff not established after bounded search → outcome `organization_fallback_after_bounded_person_research` (not “not researched”).

## N Street Village

- Prior Mayflower hosting (Empowerment Luncheon, 18 Jun 2015) preserved as **documented historical hotel connection**.
- Action framing: reactivation / relationship development.
- Does **not** claim current sales familiarity, active contract, open sourcing, or a confirmed new Mayflower event.
- Named contact: Makenzie Delmotte (Senior Events Manager) with person email from official events page.

## Local / production proof

- Canonical Airtable read-back: `canonical-readback-proof.json` — all 13 `customerVisible` + `filterCustomerFacingOpportunities` pass; list DTO provider-leak check clean.
- Artifacts: `account-contact-depth-register.json`, per-id `results/*.json`, backups under `backups/`.
- Coordinator wiring verified in source: required execute + weekly `runBuyerContactEnrichmentPass`.
- Production list/detail serve Airtable — contact patches are live without a separate data deploy. UI Proper Case / contact-depth code ships via PR when merged.

## Tests

```bash
node scripts/test-gdi-buyer-contact-enrichment-v1.mjs
node scripts/test-gdi-tile-header-proper-case.mjs
```

Coverage added: org inbox does not suppress named queue; exhausted ≠ not attempted; functional form vs homepage; planner owner ≠ referral-only; publication metadata preserved; provider leak check.

## Explicit non-claims

- Not every card with an org route has a complete named buyer.
- Named role ≠ confirmed buying authority; Surfe verification ≠ deliverability.
- Provider names/costs/request IDs are backend-only.
- Six-hotel batch remains queued; no unattended paid publish activation.
