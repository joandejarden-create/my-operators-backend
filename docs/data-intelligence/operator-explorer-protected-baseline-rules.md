# Operator Explorer — Protected Baseline Usage Rules

> **Status:** Binding for agents and PRs  
> **Current protected quality baselines:** **Arbor Lodging (CALA)** + **Hotel Equities (CALA)**  
> **Baseline type:** Golden quality freeze — tab-by-tab / field-by-field product pattern  
> **Freeze decision (current):** `frozen_2_operator_quality_baseline`  
> **No Airtable / explorer-materials / image writes in this rules doc.**

Future Operator Explorer work that builds or remediates operator profiles **must compare against** Arbor Lodging and Hotel Equities. Do not invent a weaker “good enough” bar, and do not treat Partner Intelligence governance publish alone as Explorer quality readiness.

---

## 1. Quality source of truth

| Rule | Detail |
|------|--------|
| **Golden benchmarks** | Arbor Lodging (CALA) · Hotel Equities (CALA) |
| **Record IDs** | Arbor `recF5Z87OAqFgndoq` · Hotel Equities `recWPKu5laVZxsvpn` |
| **Explorer URLs** | `/operator-explorer-gold-mock.html?id=recF5Z87OAqFgndoq` · `…?id=recWPKu5laVZxsvpn` |
| **Loader / registry** | `lib/partner-intelligence/operator-explorer-quality-baseline.js` |
| **Fixture packs** | `fixtures/operator-*-arbor-cala.json` · `fixtures/operator-*-he-cala.json` |

**Not a substitute for quality:**

- Partner Intelligence source/fact count alone
- Profile governance trust chip alone
- “Tab rendered” / non-empty string checks without pattern parity
- Antillano Norte or other sample/demo operators as the quality bar

---

## 2. Two-operator quality freeze (not a large Active universe yet)

Unlike Brand Explorer’s 27 Active/Live public-full universe, Operator Explorer currently freezes **two golden quality profiles**.

- Both profiles define the **minimum bar** for every publishable Explorer tab.
- Future operators must meet **Arbor/HE parity** tab-by-tab and field-by-field before `founder_review_ready` / `active_profile_ready`.
- If either golden profile is intentionally rewritten, stop and run an explicit baseline revision task — do not silently absorb quality regression.

When an Active/Live operator universe is large enough to freeze like Brand Explorer, revise this doc and add count/PVQL-style gates. Until then, **quality baseline = Arbor + Hotel Equities**.

---

## 3. Required regression gates for baseline / factory changes

Any change that can affect Operator Explorer content, fixtures, tab contracts, or golden profiles must run **before merge / after apply** (as gates are implemented):

```bash
npm run test:operator-explorer-quality-baseline
npm run test:operator-explorer-mandatory-release-gates
npm run test:operator-explorer-tab-factory-audit
npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=fixtures --dry-run
```

Also suggested when quality risk is high:

```bash
npm run operator-explorer-tab-factory-audit -- --source=merged --dry-run
npm run operator-explorer-section-pattern-parity-audit -- --dry-run
npm run test:operator-explorer-mandatory-release-gates
```

PR automation: `npm run dealality:pr-check-suggest` should suggest these when matching Operator Explorer paths change (wire as commands land).

---

## 4. Broad remediation is forbidden unless explicitly approved

- Default posture after freeze: **targeted** cleanup only (exact findings, named operators/tabs/fields).
- **Forbidden without explicit founder/task approval:** full-profile rebuilds, wholesale fixture replacement across both goldens, multi-operator “fix everything” writers.
- Prefer: audit → minor cleanup → re-audit → baseline regression.
- **Never modify golden baseline operators** from tab-factory remediation unless the task explicitly authorizes a baseline revision.

---

## 5. Protected fields (content / profile cleanup must not write)

Content, fixture, materials, and cleanup tasks **must not** change:

| Field / area | Notes |
|--------------|-------|
| Company Validated / Company Validation Date | Direct company confirmation only |
| Source Library approval / status | Stewardship scripts only |
| Partner Intelligence Registry approval | Registry workflows only |
| Operator Status / External Display Status | Explicit release tasks only |
| Schema / select options | `ensure-*` scripts + schema docs only |

---

## 6. Factory Preview Mode (not Active / not baseline)

Internal visual review of Draft / Under Review factory candidates **must not** use golden-baseline write paths.

| Rule | Detail |
|------|--------|
| Display state | `factory_preview_internal` (never `active_profile_ready`) |
| Banner | `Factory Preview — Not Public / Not Quality Baseline` |
| Writes | Must not touch Arbor/HE golden records or Company Validated |

---

## 7. Manual QA checklist (every operator quality task)

- [ ] Compared tab-by-tab against Arbor **and** Hotel Equities pattern
- [ ] No visible empty fields/cards/sections on publishable tabs
- [ ] Operator-specific copy (not name-swappable generic)
- [ ] Source-supported or intentionally suppressed / clean unavailable
- [ ] Golden baselines unchanged unless task is an explicit baseline revision
- [ ] Loading / empty / error / success states still work in Explorer UI

---

## Related docs

| Doc | Path |
|-----|------|
| Quality baseline freeze | `docs/data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md` |
| Tab Factory | `docs/data-intelligence/operator-explorer-tab-factory-build-operation.md` |
| Mandatory gates | `docs/data-intelligence/operator-explorer-mandatory-release-gates.md` |
| DNA field audit | `docs/operator-explorer-dna-tab-field-audit.md` |
| Brand parallel | `docs/data-intelligence/brand-explorer-protected-baseline-rules.md` |

## Change impact

**High** — defines quality SoT for all future Operator Explorer builds.

Rollback: revert this rules doc + baseline registry module; do not silently lower the Arbor/HE bar.
