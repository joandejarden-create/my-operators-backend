# Bethesda GDI PDF — Visual QA

**Property:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Sample PDF:** `samples/Dealality_GDI_Bethesda_Marriott_2026-09-30.pdf`  
**Generated:** 2026-09-30  
**Byte length:** 216,524  
**PDF page count (page objects):** 16  

## Automated gate

| Check | Result |
|-------|--------|
| PDF magic `%PDF-` | PASS |
| Cover section | PASS |
| Executive section | PASS |
| Markdown leak | NONE |
| Internal ID leak | NONE |
| Revenue disclaimer | PRESENT |
| Hotel name | Bethesda Marriott |
| Report type | Group & Demand Intelligence |

## Visual inspection (HTML A4 scroll screenshots + PDF)

Screenshots: `samples/bethesda-visual-qa/page-01.png` … `page-12.png` (+ `full-scroll.png`)

| Criterion | Result |
|-----------|--------|
| PAGE COUNT | 16 |
| CLIPPING | NO |
| OVERFLOW | NO |
| BLANK PAGES | NO |
| BROKEN CARDS | NO |
| TYPOGRAPHY | PASS |
| BRANDING | PASS |
| CONTENT | PASS |

### Page notes

- **Cover / snapshot:** Dark Dealality cover chrome; Commercial Demand Snapshot KPIs (37 / 16 / 6 / 10 / 12 / 81%).  
- **Revenue + Top 5:** LOW/BASE/HIGH scenarios with required disclaimer; Top 5 = Potomac Memorial, ACTS TS27, NICE, AHIMA, ACC Legislative.  
- **Opportunities / action plan / pipeline / HI / notes:** Readable cards; segment mix; supporting inventory (407 keys / meeting space); methodology without internal jargon.

### Live vs prior GM package

- Future Watch **12** vs package **4** — live FUTURE_WATCH corpus; not forced to old count.  
- Named contact **81%** vs package **75%** — live action-set coverage.

## Verdict

**PASS** — client-ready Bethesda golden-reference PDF.
