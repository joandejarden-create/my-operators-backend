# Webhound Test 3 Evaluation — Los Cabos / East Cape Government-Record Discovery

**Session:** `fd9955e1-eb1e-426a-bfbd-fa0c60fa2986`  
**URL:** https://webhound.ai/session/fd9955e1-eb1e-426a-bfbd-fa0c60fa2986  
**Status:** completed (`budget_complete`)  
**Spend:** **$4.86** of $5.00 (funded credits)  
**Rows:** 4 · Expected fields: 70 · Fill rate: ~64%  
**Alerts:** none  
**Geography compliance:** All four rows in Los Cabos / East Cape BCS (no QROO / Test 1–2 targets)  
**Governance:** Candidate intelligence only. Not ingested into Airtable / SoT. **No Test 4 launched.**

## Decisive question

> Was the government-record early-discovery success from Test 1 genuinely repeatable in a different Mexican hospitality market?

**Answer: Partially yes — SEMARNAT Gaceta discovery transfers; ownership/contact still fail; municipal primary sources remain hard.**

Test 1’s core pattern (SEMARNAT MIA / Gaceta → hotel-component project → no public brand) **did repeat in BCS**. Three of four rows rest on directly accessed SEMARNAT Gaceta PDFs. The same failure modes recur: opaque trusts/SPVs, unresolved UBOs, empty contact fields, and over-strong Class 1 labels.

## Executive verdict

**Promising geography transfer; not yet a complete Owner Intelligence engine**

| Metric | Result |
|--------|--------|
| Total records | **4** (below 5–8 aim; within “prefer 3–5 excellent”) |
| Webhound Class 1 | **4** |
| Independent credible Class 1 (after challenge) | **2–3** |
| Class 2 | **0** |
| Downgraded after too-late / primary-source challenge | **1–2** (La Capilla fragile; Tamarindos hotel-component caution) |
| Primary government filing directly accessed | **3 / 4** |
| Discovery from secondary press about filings only | **1 / 4** (La Capilla) |
| Applicants identified | **4 / 4** |
| Parent / UBO resolved | **0 / 4** |
| Decision-makers / usable contact routes in schema | **0 / 4** |
| Verified email / phone | **0 / 4** |

## Independent Class 1 challenge

| ID | Project | Webhound | Independent | Disposition |
|----|---------|----------|-------------|-------------|
| BCS-CSL-2026-001 | Punta Nayu (~2,540 keys, Mifel F/7952/2024) | Class 1 / Strong | Class 1 **Conditional** | Research Further — primary MIA yes; UBO opaque; mega-scale may hide private brand talks |
| BCS-LRB-2026-002 | Punta Colorada (Artibus Opus, 60-key boutique, La Ribera) | Class 1 / Strong | Class 1 **Conditional→near Strong** | Best of set — residential builder + boutique hotel + primary MIA |
| BCS-BNV-2026-003 | DTI La Capilla (Buena Vista / Santiago) | Class 1 / Strong | **Fragile Class 1 / Track** | Downgrade — **primary municipal Cabildo minutes not accessed**; Plan Maestro via press only; no MIA |
| BCS-SJD-2026-004 | Tamarindos Tierra Viva (SJD) | Class 1 Strong (hotel) | Class 1 **Conditional** | Primary MIA yes; villa sales active on project site; hotel brand still undisclosed; LinkedIn signal for Petraglia **not filled into contact fields** |

## Primary vs secondary origin

| Origin | Count | Projects |
|--------|-------|----------|
| Direct primary government/public record accessed | **3** | Punta Nayu (Gaceta 0017-26); Punta Colorada (Gaceta 0036-25); Tamarindos (Gacetas 0039-24 / 0001-26) |
| Secondary reporting about filings (primary not accessed) | **1** | La Capilla (OEM / Hoy BCS on Cabildo approval) |

Even primary-led rows used secondary press for keys, investment, and consultation dates — acceptable enrichment, not silent substitution, when `primary_source_accessed=Yes`.

## BCS public sources — worked / failed / not accessed

### Worked
- **SEMARNAT SINAT Gaceta Ecológica PDFs** — primary discovery engine (same as Test 1 QROO)
- Secondary BCS/national press (OEM El Sudcaliforniano / El Sol de México, Hoy BCS, Diario Humano, Ni Perra Idea, etc.)
- Project marketing site (Tamarindos) for too-late / product-mix check
- EMIS company snippet (Artibus Opus)
- LinkedIn URL cited for Raul Petraglia (not written into contact schema)

### Failed or weak
- **Los Cabos Cabildo / municipal transparency** — minutes/agendas not reachable via direct URL (explicitly noted on La Capilla)
- **Fideicomiso UBO resolution** — Mifel / GFMTAMARINDOS trusts hide beneficial owners
- **Property registry / commerce registry depth** — no land-title or controlling-owner resolution
- **Contact enrichment** — 0 verified emails/phones; contact fields empty despite one LinkedIn source

### Not accessed (called out)
- H. Ayuntamiento de Los Cabos Cabildo session minutes (La Capilla)
- Full property/commerce registry for Artibus Opus principals, Consorcio de Ingeniería Integral, and trust beneficiaries

## Ownership / decision-maker / contact / too-late

| Dimension | Assessment |
|-----------|------------|
| Ownership | Applicants/SPVs/trusts named; **no ultimate controlling owner** resolved; confidence Low/null |
| Decision-makers | Schema contacts empty; only latent LinkedIn for Petraglia on Tamarindos |
| Contact routes | **0 usable** in delivered fields |
| Too-late | No public brand/HMA found on any row; Tamarindos villa marketing is a **partial** late signal for the residential slice, not proof hotel flag is set |

## Cost / value (actual $4.86)

| Metric | Value |
|--------|--------|
| Cost / record | **$1.22** |
| Cost / credible pre-decision (independent, n≈3) | **~$1.62** |
| Cost / ownership-resolved (parent/UBO) | **N/A (0)** |
| Cost / applicant-identified | **$1.22** (4/4) |
| Cost / usable contact route (schema) | **N/A (0)** |
| Cost / primary-filing-backed opportunity | **$1.62** (3) |

## Key learnings

1. **SEMARNAT Gaceta early-discovery is geographically transferable** (QROO → BCS) at $5.
2. **Municipal entitlement signals** are discoverable via press but **primary Cabildo access failed** — do not treat as Strong Class 1.
3. **Fideicomiso-heavy Cabo market** makes UBO resolution harder than QROO SPV-named promoventes in Test 1’s best hits.
4. **Contact remains the budget sink / failure mode** — discovery wins; outreach packages still need a separate enrichment run (Test 2 style).
5. Row count (4) under 5–8 target is acceptable under quality-over-quantity rule; stretching would have burned remaining ~$0.14.

## Is another test justified?

**Only with explicit approval.** Optional threads (not launched):

- Enrichment package for Punta Colorada + Punta Nayu (ownership/contacts)
- Municipal primary-source access experiment (Los Cabos Cabildo)
- Too-late deep dive on Tamarindos hotel vs villa product

**Do not auto-start Test 4.** Remaining funded balance after this spend: **$45.30**.

## Artifacts

- `test3-session-meta.json`
- `test3-rows.jsonl`
- `test3-rows-compact.json`
- `test3-prompt.txt` / `test3-schema.json`
- `TESTING-LEDGER.md`
