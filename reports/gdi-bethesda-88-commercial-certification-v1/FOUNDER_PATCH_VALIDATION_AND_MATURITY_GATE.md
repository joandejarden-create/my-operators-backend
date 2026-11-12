# Bethesda 88 — Corrected Contact Patches + Event-Family Gate

**Authorized store write:** `false`  
**Store SHA (unchanged):** `1279f40c2d1cdd2cb6efdc55bfa8397b44f65c58da3ca4365f2814c69e71c9c3`  
**PR:** [#187](https://github.com/joandejarden-create/my-operators-backend/pull/187)

## Contact field consistency (fixed)

Direct person emails and organization routes are now **separate fields**.

| ID | Person | Direct email | Organization route | Coverage |
|---|---|---|---|---|
| Arlington | Lizzy Stell | `lizzy@arlingtonsoccer.com` | `tournaments@arlingtonsoccer.com` | A |
| NIH SBPO | Annette Owens-Scarboro | `scarbora@od.nih.gov` | `NIHSmallBusiness@…` | A |
| CTN primary + linked | Quandra Blackeney | `scudderq@nida.nih.gov` (former Scudder) | — | A |
| AMWA ×3 | Kelly McMahon | — | `associatedirector@amwa-doc.org` | B |
| Georgetown | Vanessa Otarola Kehoe | — | `advancementevents@georgetown.edu` | B |
| UMD | Natalie Ross | `nross1@umd.edu` | `umdevents@umd.edu` | A |
| ACCP | Lisa C. Miller | `lmiller@accp.com` | — | A (hotel-fit **out of market**) |
| ECS | John Lewis | — | `meetings@electrochem.org` | B (**overflow only**) |

**FAIL (unchanged):** 3× Carrie Wagerman / NCI RNA named promotion.

Exact patches: `07-founder-approved-candidate-manifest.json`

## Duplicate commercial families (presentation only)

Raw research IDs remain **88**. Actionable list collapses source-siblings:

| Family | Primary | Linked / quarantined | Unique demand? |
|---|---|---|---|
| CTN Annual / Dissemination Mar 15–17 2027 Bethesda | `gdi_opp_series_ctn_annual_conference_2027` | 2 linked + 1 TITLE_SOURCE_INVALID quarantined | **One** campus meeting cycle |
| NCI RNA Biology Symposium 2027 | `gdi_opp_2027_nci_rna_biology_symposium_7` | 2 linked | **One** symposium |
| AMWA 2027 annual / 2028 annual / interim | each standalone | — | **Distinct** meetings |

Counts: raw **88** · unique commercial list **83** · linked hidden **4** · quarantined **1**

Crosswalk: `09-commercial-event-family-crosswalk.json`

## Hotel-fit honesty

- **CTN Bethesda campus** — lodging corridor credible; not a confirmed Marriott block
- **NCI RNA** — Bethesda Marriott on accommodations list; named lodging owner unconfirmed
- **ECS 251** — Marquis/WEWCC official housing → Overflow Watch only
- **AMWA** — destination unconfirmed → speculative fit
- **ACCP** — 2026 Salt Lake → out-of-market; contact OK, no SR/AD promotion

## Coverage if 12 PASS applied (still not written)

Named relevant primary: **46 → 58** (+12 on raw IDs).  
Do **not** count linked CTN/NCI siblings as extra qualified opportunities.

## Safety

- No canonical store write  
- No paid research  
- No customer publication  
- Product Sales Ready / Account Development remain **0** until separate promotion approval  

## Awaiting founder

1. Approve merge of PR #187 (code + presentation grouping).  
2. Separately approve exact contact write for the 12 PASS IDs (or subset).  
