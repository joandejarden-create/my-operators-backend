# Track B Chat B — Critical-Mass V3 Evidence Gaps

**As of:** 2026-11-23  
**Publication:** CUSTOMER_BLOCKED · **Research cost:** $0  
**PR #568:** OPEN — **not modified** (parallel Track B pack)  
**Permanent-rule Qualified:** baseline ~5 retained; **Chat B net new Qualified = 0**

## Verdict matrix

| Candidate | Class | Contract trigger | Unresolved | Competing pathway | Deadline | Decision entity |
|---|---|---|---|---|---|---|
| Hotel Oromana | QUALIFIED → **QUALIFIED_RETAINED_STRENGTHENED** | YES | YES | YES | 2026-11-27T23:59:00+01:00 | Junta de Gobierno Local — Ayuntamiento de Alcalá |
| Águeda Passivhaus eco-hotel (Pateira de Fe | QUALIFIED → **QUALIFIED_RETAINED_STRENGTHENED** | YES | YES | YES | 2027-11-06 | Portuguese sole-proprietorship rural-tourism dev |
| Antiga Colónia Balnear da Areia Branca (RE | QUALIFIED → **QUALIFIED_RETAINED_STRENGTHENED** | YES | YES | YES | 2027-03-17T23:59:00+00:00 | Turismo de Portugal, I.P. (tramitação); property |
| Cap Cana condohotel projects — Spain Inmob | EMERGING_PROSPECTIVE → **EMERGING_RETAINED** | NO | YES | NO | n/a | Spain Inmobiliaria / Mercedes Solís Díez (broker |
| Fibra Inn — residual Aimbridge HMAs (Westi | EMERGING / UNDER_RESEARCH → **UNDER_RESEARCH** | NO | YES | NO | n/a | Fibra Inn Technical Committee + co-owners |
| Punta Bergantín — residual / fourth hotel  | EMERGING_PROSPECTIVE → **EMERGING_RETAINED** | NO | YES | NO | n/a | Banreservas / Punta Bergantín project SPE (state |
| Hotel Torre de Uriz | AMBIGUOUS_REVIEW → **AMBIGUOUS_REVIEW** | YES | YES | NO | 2026-11-19T13:15:00+01:00 | Dirección General de Turismo — Gobierno de Navar |

## Upgraded to Qualified
**None.** Chat B did not invent new Qualified. The three Track A V3 Qualified (Oromana / Águeda / Areia Branca) are **retained and strengthened** with primary-source corroboration.

## Retained Qualified (strengthened)
1. **Hotel Oromana** — Restricted concession C-2026/032 open to **27 Nov 2026 23:59**; PCAP/PPT + Anexo III staff subrogation; TARCJA 610/2026 re-tender after partial annulment; 40-year MCT; no mandated franchise brand (4★ category target).
2. **Águeda Passivhaus** — EEN BOPT20261105031 hybrid lease/management open to **6 Nov 2027**; sole proprietorship Jul 2026 Águeda; **legal name/NIF still unpublished**; partner not selected.
3. **Areia Branca REVIVE** — DR **27619/2026** + VORTAL; proposals to **17 Mar 2027**; 50-year hotelaria concession; CIM Beira Baixa owner / Turismo de Portugal tramitación; brand not locked in notice.

## Remain Emerging
4. **Cap Cana (Spain Inmobiliaria)** — gated email teaser only; no public RFP / parcels. Do **not** conflate with committed AC Residences by Marriott (Gesproin) or Lamborghini/Huxley Cap Cana.
5. **Punta Bergantín residual / 4th hotel** — Meliá/Westin/Dreams named; fourth still undefined; Hilton remains conversations; no open competitive pathway.

## Ambiguous / Under research (not Qualified)
6. **Torre de Uriz** — bid window **closed 19 Nov 2026**; award pending as of 20–23 Nov. New competitors cannot enter → **AMBIGUOUS_REVIEW** (monitor award/desertion). Not Historical until award.
7. **FINN residual Aimbridge HMAs** — still active Westin Valle + JW Arboleda/Valle; **no hotel-level HMA expiry dates**; internalization-at-expiry constrains competing MCT → **UNDER_RESEARCH** (reuse prior CALA rows; no duplicate write).

## Committed / Historical from this gap set
**None newly classified as committed/historical.** (Separate Cap Cana brand-committed projects noted only as disambiguation.)

## Evidence still required
| Gap | Next evidence |
|---|---|
| Oromana | Full PPT brand-affiliation clause text; post-deadline award |
| Águeda | Legal entity / NIF; draft hybrid HMA term sheet |
| Areia Branca | Full VORTAL peças (CapEx floor / brand constraints) |
| Cap Cana | Named parcel teaser + selection rules |
| FINN | HMA exhibits with expiry / notice |
| Bergantín | Named 4th-hotel SPE + open RFP |
| Torre de Uriz | Award or desertion notice |

## Persistence (applied 2026-11-23)
- Persistable dossiers: **6** (FINN excluded — reuse prior CALA/fwd-qualify rows)
- Contract-eligible: **4** (Oromana / Águeda / Areia / Uriz)
- Cap Cana + Bergantín: findings (+ inventory where new) only — no contract rows
- **APPLY ledger:** creates **18** · noops **2** · staged **2** · failures **0**
  - inventory 4 · findings 6 · contracts 4 · milestones 4
  - Oromana inventory linked by existing name (`recDt9UuO1yVK4Ceq`) — noop, no duplicate hotel
- All writes **CUSTOMER_BLOCKED**. Dedup by discoveryPropertyId / findingId / contractId / milestoneId.
- Ledgers: `PERSISTENCE_LEDGER_DRY_RUN.json`, `PERSISTENCE_LEDGER_APPLY.json` (read-back included)
- Do **not** re-apply CALA / coverage-audit 54 / V4 DRH-SVC ledgers.

## IC coordination
- Open a **new Track B PR** on `feat/hif-v6-track-b-chat-b-cm-v3-gaps` — do not push commits onto #568.
- Track A: retain three V3 Qualified; keep Cap Cana/Bergantín Emerging; Uriz Ambiguous; FINN under research.
- Toward ~20 Qualified: still need additional Europe/CALA BCT/MCT with unresolved + pathway — this pack protects quality, does not inflate count.
