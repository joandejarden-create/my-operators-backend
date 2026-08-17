# Operator Explorer — Trust Footnote (always-on)

**Problem:** Only 3/36 Production OEs showed the hero trust chip (Arbor, HE, GHL). Most were missing because Master governance fields were blank, so `normalizeProfileGovernance` returned no `displayLabel`.

**Fix (2026-08-11):**

1. Module: `lib/partner-intelligence/operator-explorer-trust-footnote.js` — always-on enricher + per-operator posture registry
2. API: `api/third-party-operator-detail.js` applies enricher on every new-base detail read
3. Client fallback: `public/js/operator-explorer-new-base-profile.js` completes incomplete chips
4. Master backfill: Validation Status, External Display Status = Show Trust Label, Usage Permission, Source Region, Last Reviewed Date, Source Type

**Live verify:** 36/36 gate PASS

| Label | Typical use |
| ----- | ----------- |
| Source-Informed Profile | Third-party / regional research packages · Source Basis: Reviewed Sources |
| AI-Assisted Profile | Company Published brand-managed / corporate materials · Source Basis: Company Materials |

Arbor kept **Jun 10, 2026 · CALA-specific**. HE/GHL kept Jul 6 dates; HE Region filled to **CALA-specific**.

Report: `reports/operator-explorer-trust-footnote-audit.md`  
Backup: `backups/operator-setup/oe-trust-footnote/2026-08-11T14-19-40/`

```bash
node scripts/operator-explorer-trust-footnote-backfill.mjs --dry-run
node scripts/operator-explorer-trust-footnote-backfill.mjs --apply --approve-operator-explorer-trust-footnote
```
