# GDI — Google-first second pass (original 100 cohort)

**Date:** 2026-11-20  
**PR sequence:** extends [#484](https://github.com/joandejarden-create/my-operators-backend/pull/484) (OPEN)  
**Workspace:** `C:\Dev\dealality\ws-gdi` · HEAD `6c4ebdd59`+ (second-pass artifacts)  
**Budget authorized:** $10 combined Google+Exa · **Surfe: 0**

## 1. Original vs second-pass

| Metric | Pass 1 (EXA_ONLY) | Pass 2 (Google→Exa) |
|--------|-------------------|---------------------|
| Cohort | 100 | **same 100** (not expanded) |
| Researched this pass | 100 | 89 (+1 reuse) · **10 complete skipped** |
| Both channels | 11 | researched both≈10 · **10 prior completes untouched** |
| One channel | 32 | still one **28** |
| None | 57 | still none **52** |
| None→one | — | **11** |
| One→both | — | **9** |
| New named emails (this pass only) | 14 total pass1 | **+4** |
| New named phones (this pass only) | 14 total pass1 | **+16** |
| New office extensions | — | **5** |
| Verified LinkedIn | — | **4** |
| Org routes touched | 31 | **28** (not all new) |
| Better commercial entry points | 2 (CCI pilot) | **1** |
| New verified direct hotel buyers | 0 | **0** |
| Incorrect identity rejects | 0 | **1** |
| Surfe | 0 | **0** (38 Surfe CTAs avoided on public hits) |

## 2. Google vs Exa recovery (incremental this pass)

| Channel | Google-only | Exa-only | Google then Exa |
|---------|-------------|----------|-----------------|
| Named emails | **2** | 0 | **2** |
| Named phones | **6** | 0 | **10** |

Google was **configured and used** (`GOOGLE_GEMINI_THEN_EXA`, 89 grounded calls).  
Exa ran only when Google verify left gaps (`GOOGLE_VERIFY_INCOMPLETE`).

## 3. Notable verified recoveries (examples)

- **Abraham Yasir Maciel Montoya** — `abraham.maciel@jalisco.gob.mx` · +52 33 3030 5000 · Ext. 35222 (Google-attributed email/phone path)
- **Elena Hurtado** — `ehurtado@expoguadalajara.mx` · +52 33 1865 6427
- **Bacilio Sánchez** — `bsanchez@spaconsultores.com.do` · 809-224-7903
- **Christi R. Nam** — `christi@esa.org` · (202) 833-8773 · extensions preserved when extracted
- LinkedIn verified examples include Abbas Zavar (+ others in `SECOND_PASS_RESULTS.json`)

Full row list: `SECOND_PASS_RESULTS.json` / progress checkpoints in `PROGRESS.json`.

## 4. Airtable write / read-back

| Outcome | Count |
|---------|------:|
| OK (read-back match, not Surfe-derived) | **37** |
| Fail | 2 |
| Skipped (no inventory row) | 1 |

Idempotent merge onto existing Hotel Research Opportunities; stronger channels not blanked.  
**Customer publication / STAGED→CURRENT:** not changed.

## 5. Actual spend

| Component | USD |
|-----------|-----|
| Billed toward $10 cap | **$4.189** |
| Google actual (API amount) | $0.00 (COST_UNKNOWN) |
| Google estimated toward cap ($0.03/call × 89) | **$2.67** |
| Exa actual (provider) | **$1.519** |
| Surfe | **$0** |
| Budget stop | false |

## 6. Contact cards / deployment

| Surface | Status |
|---------|--------|
| Airtable research payloads | Updated for OK writes |
| Local opportunities JSON | Used as input; publication packages **not** auto-promoted |
| localhost:8080 drawer | **NOT re-verified this pass** (requires IC merge/deploy of #484 + STAGED refresh) |
| Railway external GDI | **Still prior STAGED** until IC deploy |
| Shared contact component | Uses existing email/phone/ext/LinkedIn fields from #484 |

## 7. Remaining unresolved

- **52** still with no named-person channel after Google+Exa  
- **28** still one channel  
- Quality note: a minority of recovered phones/emails look weak (switchboard-ish, odd digit strings, org mailboxes like `hall@pda.org`) — treat as leads until human review; extractor did not invent formats but snippet association can still be noisy  
- Commercial relevance: **0** new direct hotel buyers; **1** better entry point; originals retained in 88/89 cases  

## 8. Certifications

```
COHORT_REUSED = YES (original 100)
GOOGLE_FIRST = YES (configured + executed)
EXA_SECONDARY = YES
SURFE = 0
SPEND_UNDER_10 = YES ($4.189)
AIRTABLE_READBACK = PARTIAL (37 OK / 2 fail / 1 no-row)
CUSTOMER_VISIBLE_DRAWER = NOT_CLAIMED
STAGED_TO_CURRENT = NOT_REQUESTED
COHORT_EXPANSION = STOPPED
```

Evidence: `reports/gdi-google-first-second-pass-v1/`
