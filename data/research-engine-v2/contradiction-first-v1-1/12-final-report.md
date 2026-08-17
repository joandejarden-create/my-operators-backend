# Contradiction-First V1.1 — Final Report

## Did V1.1 preserve Webhound-like freshness while becoming safe for shadow monitoring?

**YES.** Targeted V1 material false positives dropped from **6 → 0**, while retaining **4/4** Indigo Pipeline→Open true positives and **100%** high-confidence rediscovery proxy (includes Casa Nizuc, Crystal Cove Tribute gap, Tres Ríos). Unseen cohort produced **0** material proposals and **0** false positives under the same frozen gates.

Production readiness: **Ready for Shadow Monitoring**

No automated writes. Next step = shadow monitoring only.

## 1. Exact V1 FP causes

| Case | Mechanism |
|------|-----------|
| Indigo CDMX Downtown → InterContinental | parent-brand contamination + fuzzy sibling |
| Indigo Tulum → Holiday Inn | same-city sibling + Low match treated as material |
| Indigo GDL Providencia → Expo | same-brand sibling + Low match |
| Faranda Cali → Ascend | same-city sibling + HTTP 403 weak evidence |
| Casa Francia → Casa Nizuc | fuzzy "Casa" collision + Low match |

## 2. Modules changed

`match-confidence.js`, `geo-normalize.js`, `corroboration.js`, `directory-gaps.js`, adapters ihg/marriott/choice/hilton, `check-hotel-freshness.js` V1.1 gates, `scripts/research-engine-v2-contradiction-first-v1-1.mjs`

## 3–4. Match + corroboration

Exact/High required for material; Medium→Review; Low/Reject blocked. Geography hard gate + explicit aliases. Pipeline→Open needs official bookable + Exact/High; New Hotel banner dual-signal → High band.

## 5–6. Marriott / Choice

Soft-brand cross only at Exact/High; Tribute catalog gaps include Casa Nizuc + Crystal Cove Barbados. Choice Individuals gap engine present (403/weak pages no longer emit reflags).

## 7. Known V1 vs V1.1

- Material: 14 → 5
- Targeted FPs: 6 → **0**
- Indigo TP: **4/4**
- Rediscovery proxy: **100%**

## 8–9. Unseen

36 hotels; 0 material; TP/FP 0/0; 8 no-change controls sampled.

## 10. Runtime / cost

Known 9175 ms; Unseen 257 ms; **$0**.

## 11. Readiness

**Ready for Shadow Monitoring**

## 12. Shadow mode

See `11-shadow-mode-design.md` (design only).

## 13. Boundary

Native: routine status / affiliation / directory gaps / light cross-table.  
Webhound: blind audits, gov/project discovery, opaque ownership, long-tail.

## 14. Top 3 next actions

1. Shadow digest (read-only) for Indigo+Kimpton Mexico daily  
2. Backfill census city + property IDs to raise Exact rate  
3. Opening-announcement secondary fetcher for Medium single-primary Pipeline→Open cases  
