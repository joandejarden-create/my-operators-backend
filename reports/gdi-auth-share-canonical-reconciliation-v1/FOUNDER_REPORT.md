# GDI Auth↔Share Canonical Reconciliation V1

**Verdict: `B_C_PUBLICATION_PARITY_PASS`**  
**Not declared:** universal authenticated LIVE ≡ external share.  
**Certified:** Founder Publication Preview (Mode B) ≡ External Customer Share (Mode C).

Fingerprint unchanged: `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

---

## 1. Why the previous five certifications failed

They repeatedly compared **share ↔ share**, or treated **authenticated LIVE RESEARCH** as if it were the approved customer publication.

Founder visual inspection compared:

**Local authenticated LIVE store (Mode A)** vs **External share PACKAGE (Mode C)**

That comparison will always show commercial-action drift (Build Account / Monitor vs Qualify Opportunity) because Mode A recomputes from the live research store.

Share↔share PASS does **not** prove authenticated≡share. Those certificates are retained but **INVALIDATED** for authenticated-vs-share claims (`00-PRIOR-CERTIFICATE-INVALIDATION.json` and prior-report invalidation overlays).

## 2. Was local authenticated GDI using live research?

**Yes — when the founder compared screenshots that showed Build Account / Monitor.**

- Mode A URL: `/opportunities?live=1` → `listScope: BETHESDA_SALES_TEAM_INVENTORY_67`
- Mode B URL: `/opportunities?publicationPackage=1&surface=customer` → `PUBLICATION_PACKAGE`
- Mode C share → `PUBLICATION_PACKAGE` (same fingerprint)

Default authenticated Opportunities (without `?live=1`) are Mode B package-first. Authenticated UI now shows an explicit **Mode A / Mode B banner** so LIVE cannot be mistaken for publication preview.

## 3. Screenshot examples — root cause

| Opportunity | Mode A LIVE | Mode B PREVIEW | Mode C SHARE | Classification |
|-------------|-------------|----------------|--------------|----------------|
| Potomac Memorial | Build Account | Qualify Opportunity | Qualify Opportunity | INTENTIONAL_LIVE_VS_PACKAGE |
| Bethesda Premier Cup | Build Account | Qualify Opportunity | Qualify Opportunity | INTENTIONAL_LIVE_VS_PACKAGE |
| RIC | Monitor | Qualify Opportunity | Qualify Opportunity | INTENTIONAL_LIVE_VS_PACKAGE |
| Lorna Kipfer | displayRole: RIC Conference Contact; `opportunityRole` may still be Group Travel Coordinator on LIVE | RIC Conference Contact | RIC Conference Contact | B≡C; LIVE may expose activity-role field separately |
| Woman's Club summary | Same repetitive association line currently in package | Same | Same | B≡C; cleaner copy needs **new fingerprint** if founder wants content change |
| SVG gold vs muted | Live class/thesis may light gold | Package attach + demotion | Same as B | A vs B/C intentional when class/thesis differ |

**Approved customer content remains the package values (B/C).** Publishing live Build Account / Monitor into the customer report requires a new package + founder approval.

## 4. Duplicated renderers

| Shared | Thin shells |
|--------|-------------|
| `dealality-gdi-ui.js` (cards, icons, contacts, pills, drawers, filters) | `app.js` (auth) · `share-app.js` (share) |
| `gdi-package-customer-presentation-attach-v1.js` | permissions / share token / feedback |
| `gdi-canonical-customer-presentation-contract-v1.js` | Mode A live recompute path |

Client now **prefers** `canonicalCustomerPresentation` for icons, contacts, and commercial-action pills.

## 5. Intentional vs publication-change

**Intentional (do not “fix” by copying live into share):**

- Mode A commercial actions / theses vs Mode B/C package (65 opportunities documented)
- Auth-only admin / feedback / Surfe on-click

**Requires new fingerprint (founder approval):**

- Any desire to publish live Build Account / Monitor instead of package Qualify Opportunity
- Cleaner Woman's Club partnership copy (B and C currently match the package wording)

## 6. B and C now use the same customer components?

**Yes** — same `dealality-gdi-ui.js`, same package attach, same canonical contract. Browser B↔C card actions/icons/contacts: **PASS** (67/67).

## 7. All 67 opportunities — browser parity B↔C?

**PASS** under Mode B vs Mode C local share. Contract diffs: 0. UI card diffs: 0. Open MATERIAL B↔C: 0.

Mode A↔B: 65 INTENTIONAL live diffs documented (not defects).

## 8. Merge / deploy status

This reconciliation pack + mode banner + `test:gdi-auth-share-canonical-parity` ship on the open architecture PR. Production marker may still lag until deploy of this tip; **do not** treat share↔share or A↔C as authenticated parity after deploy either.

## Modes (permanent)

| Mode | Meaning | Customer parity target |
|------|---------|------------------------|
| **A LIVE RESEARCH** | `?live=1` evolving store | No |
| **B FOUNDER PUBLICATION PREVIEW** | auth package | **Yes — must ≡ C** |
| **C EXTERNAL CUSTOMER SHARE** | share package | **Yes — must ≡ B** |

## Gate

```bash
npm run test:gdi-auth-share-canonical-parity
```

Fails on B≠C commercial actions, contacts, SVG states, card UI, fingerprint, or incomplete browser runs. Documents A≠B as INTENTIONAL live research — never as CLIENT READY for authenticated≡share.
