# Bethesda Phase 3 — Evidence → Airtable → Sources Tab Mapping

**Hotel:** `recLuxvwwxID7U2B8` · **SoT table:** HI Graph Evidence (`tblc9JhKT1edr4grE`)  
**UI path:** Ownership API `report.evidence.sources` → `buildCitations()` → Sources & Evidence tab  
**Apply:** BLOCKED until founder approval

## Architecture (no parallel store)

```
Research source URL
  → HI Graph Evidence (1 canonical row per hotel+URL)
  → he-airtable-graph-reader listEvidenceForHotel
  → projectEvidenceToSourcesCitations
  → golden-demo-ownership report.evidence.sources
  → HI Workspace Sources tab
```

## Source documents (11 CREATE)

| # | Source | Type | Current? | Primary claims | Linked entities |
|---|---|---|---|---|---|
| 1 | MD SDAT acct 03547343 | Government Record | Yes | Legal Owner / PropCo Marbeth; 2019 & 2013 transfers | Hotel + Marbeth Legal Owner |
| 2 | mullencap.com/portfolio | Company Website | Yes | Economic Owner *sponsor association* (MREC) | Hotel + MREC Economic Owner |
| 3 | mullencap.com/contact | Company Website | Yes | Org email + HQ; mailing match to PropCo | MREC channels |
| 4 | mullencap.com/about | Company Website | Yes | Jeff Mullen; Button; Reece roles | MREC contacts |
| 5 | mullenhospitality.com/portfolio | Company Website | Yes | Ops & Investment Supervision posture | MHM org |
| 6 | HWE sale release 2018-12-03 | Transaction Announcement | **No** | Sale announced; Marriott management continuity | History / agreement |
| 7 | Hotel Online Northview 2013 | Transaction Announcement | **No** | Historical acquisition only | History |
| 8 | Ramsfield loan page | Lender Case Study | **No** | $63M financing corroboration | History |
| 9 | WBJ related headline | Trade Press | Yes* | Corroborates Mullen family sale (*body paywalled) | Economic Owner corroboration |
| 10 | SAM / OpenGovUS Marriott Hotel Services | Government Registry | Yes | Operator identity — **not owner** | Operator (anti-inference) |
| 11 | Trivago 4★ | OTA | Yes | starRating VERIFIED_DERIVED | Hotel overview (steward) |

\* Headline corroboration only; SDAT + portfolio remain primary.

## Claim distinctions

| Claim | Supporting source(s) | Not sufficient alone for |
|---|---|---|
| Legal title = Marbeth Acquisition LLC | SDAT | Economic/UBO control |
| Economic sponsor = MREC | Portfolio + Contact mailing match (+ WBJ headline) | Natural-person UBO |
| Named decision-makers | About page | Qualified signatory authority |
| Org contact channel | Contact page | Personal emails |
| Historical ownership | Hotel Online / SDAT 2013 / HWE | Current ownership |
| Operator = Marriott | HWE + SAM | Economic ownership |

**UBO:** explicitly UNRESOLVED — no evidence row asserts a natural-person ultimate owner.

## Completeness gate

1. All Phase 3 research sources in proposed evidence write-back — **PASS (11)**  
2. Every new verified ownership/org/contact claim has supporting evidence — **PASS**  
3. Sources tab retrieves via established ownership API citation shape — **PASS (preview)**  
4. Evidence not only in local JSON after apply — **GATED (await apply)**  
5. Relationships survive refresh/dedupe — **PASS (evidenceDedupeKey + UPDATE replay)**  
6. Reuse not duplicate — **PASS (1 row per URL; 2nd pass 0 CREATE)**  
7. Idempotent apply plan — **PASS**

## Dry-run totals

22 CREATE · 0 UPDATE · 0 held · **11 evidence CREATE** (prior dry-run had 0 — corrected)

**STOP before production Airtable apply.**
