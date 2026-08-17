# Approved Intelligence Field Suggestions v1

**Date:** 2026-07-06  
**Status:** Report-only — Mode B review queue  
**Command:** `npm run approved-intelligence-field-suggestions`

> **Authority:** [approved-intelligence-platform-field-publishing-v1.md](./approved-intelligence-platform-field-publishing-v1.md)

---

## 1. Purpose

Field Suggestions v1 is the **human review layer** between approved Partner Intelligence facts and any future platform field writes.

The publishing **audit** classifies mappings; the **suggestions report** turns eligible classifications into a steward-facing queue with risk, live vs proposed diffs, and explicit recommendations — **without writing** Brand Setup, Operator Setup, or governance fields.

---

## 2. Audit vs suggestion vs publish

| Stage | Tool | Output |
|-------|------|--------|
| **Audit** | `approved-intelligence-field-publishing-audit` | All approved-fact mappings by mode (Evidence / Suggested / Controlled / Blocked) |
| **Suggestion** | `approved-intelligence-field-suggestions` | Review queue for Suggested + Controlled only |
| **Publish** | *Not in v1* | Future: write only **Approved For Publish** suggestions |

---

## 3. Suggestion statuses

| Status | Meaning |
|--------|---------|
| **Proposed** | New suggestion from audit; default for low-risk controlled candidates |
| **Needs Review** | Requires steward attention (populated destination, select validation, identity) |
| **Approved For Publish** | Steward approved — eligible for future controlled publish script |
| **Rejected** | Steward declined — never auto-publish |
| **Superseded** | Replaced by newer fact or manual edit |

**v1:** All rows start as **Proposed** or **Needs Review** in reports only (no Airtable persistence).

---

## 4. Required suggestion fields

Each suggestion includes:

- entity type, record ID, name  
- source fact ID, source ID  
- fact key, approved value  
- destination table + field  
- current live value, proposed value  
- classification (audit publish mode)  
- risk level (Low / Medium / High)  
- recommendation  
- evidence + source summary  

---

## 5. Safety rules

- No writes to platform Setup fields  
- No writes to governance fields  
- No Company Validated / Company Validation Date changes  
- No overwrites by default — populated destinations → **Needs Review**  
- Blank safe destinations → **Controlled publish candidate** (Low risk when governance allows)  
- Pending / rejected / evidence-only / blocked mappings excluded  

---

## 6. Review workflow

1. Run audit: `npm run approved-intelligence-field-publishing-audit -- --entity-type … --target-rec-id rec…`  
2. Run suggestions: `npm run approved-intelligence-field-suggestions -- --entity-type … --target-rec-id rec…`  
3. Steward reviews per-entity report (`reports/approved-intelligence-field-suggestions-<recId>.md`)  
4. Approve selected rows (manual / future Airtable table)  
5. Future controlled publish writes **only Approved For Publish** suggestions  

---

## 7. GHL example

**Operator:** `reciI2tYQBfMoMK9G`

| Fact key | Classification | Risk | Notes |
|----------|----------------|------|-------|
| `op.markets.regionsSupported` | Suggested only (post-publish) | **Medium** | `specificMarkets` populated after controlled publish v2 — corrections require steward correction path |
| `op.snapshot.companyDescription` | Suggested only | Medium | Live description populated |
| `op.snapshot.companyName` | Suggested only | **High** | Identity — no overwrite |
| `op.brand.familiesOperated` | Suggested only | Medium | multipleSelects validation |
| `op.platform.offeredServices` | Suggested only | Medium | Events/MICE — select validation |

**Post-publish note:** Controlled publish v2 blocks overwrites once `specificMarkets` is populated. Value corrections (e.g. removing regional label "Latin America" in favor of country names from the destinations page) go through **correction mode** — see [controlled-platform-field-publishing-v2.md §9](./controlled-platform-field-publishing-v2.md#9-steward-correction-path-populated-destinations).

`specificMarkets` should store country/market names; regional framing such as "Latin America" belongs in PI evidence unless the field is explicitly regional.

**Fact alignment:** After product field correction, update the approved PI fact via [approved-fact-correction-v1.md](./approved-fact-correction-v1.md) so suggestions and audit proposed values match the corrected platform value.

---

## 8. Hotel Equities example

**Operator:** `recWPKu5laVZxsvpn`

All 5 approved facts → **Suggested only** (destinations populated). No controlled publish candidates until steward clears or approves overwrites. PDF Pending facts remain excluded.

---

## 9. Future Airtable table option

**v1:** Report-only JSON + markdown (per-entity + latest alias).

**v2 option:** `Partner Intelligence - Field Suggestions` table with status, steward, approved-at — not created in v1.

**v2 controlled publish:** [controlled-platform-field-publishing-v2.md](./controlled-platform-field-publishing-v2.md) — `npm run controlled-platform-field-publishing` (allowlisted single-field writes; default dry-run).

**v2.1 batch queue:** [controlled-publish-queue-v2-1.md](./controlled-publish-queue-v2-1.md) — `npm run controlled-publish-queue -- --plan` (preferred batch view for field publishing opportunities).

---

## Reports

```bash
reports/approved-intelligence-field-suggestions-<recId>.{md,json}
reports/approved-intelligence-field-suggestions.{md,json}   # latest alias
```

## Change Impact

| Tier | **Low** — read-only reports |
| Rollback | Remove package script + lib module |
