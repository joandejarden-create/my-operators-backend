# Brand Explorer — Phase 2H-A Gate 1 Founder Review

**GATE 1: RENDERER PARITY ONLY**  
**GATE 2: CONTENT PARITY — not started**

**FOUNDER_APPROVED = false** (automated evidence ≠ approval)

---

## Working URLs (port 8082)

| Env | URL |
|---|---|
| **B — Comparison Studio (Gate 1)** | http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1 |
| **A — Product mount** | http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json |
| Equal-width geometry | http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&equalWidth=1 |
| Customer-facing reference | http://127.0.0.1:8082/brand-explorer-gold-mock.html?id=receQkxgjlezsc1xg&beInternalPreview=1 |

---

## What was tested

Identical **saved Curio Presentation payload** (no regeneration / rewrite / summarize):

- Payload: `public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json`
- Blocks: **225**
- SHA-256: `3bdbd3927e8bf7ccb19cbcd8f60dfcec3dd87ac5ccf278653be9987ecd7ef37f`
- Source: live Brand Library Presentation (read-only freeze)

**Canonical renderer (shared):**

- JS: `public/js/brand-explorer-atelier-from-api.js`
- CSS: `brand-explorer-atelier-oe-panels.css` (+ gold / combined detail parity)
- A uses `mountIntoRoot`
- B uses `renderTabInnerHtml` → paired `.oe-section` mounts (same HTML both columns)

**No simplified comparison-specific subsection renderer** for Gate 1 content.

---

## Initial test sections (Curio)

Covered via Overview / Value / Loyalty / Economics / Footprint / Materials tabs in Gate 1 mode:

1. Hero and Brand Snapshot  
2. Where This Brand Creates the Most Value  
3. What This Brand Is Best At  
4. Owner Value Snapshot  
5. Loyalty Architecture  
6. Economics & Obligations  
7. Geographic Footprint  
8. Brand Materials / Property Examples  

---

## Evidence pack

`reports/brand-explorer/phase2h-a-renderer-parity/`

- `01-payload-and-renderer-evidence.*` — payload hash + URLs  
- `02-browser-capture.json` — DOM length, computed styles, geometry, visual-diff notes  
- `screenshots/A-product-curio-brand-snapshot.png`  
- `screenshots/B-compare-curio-brand-snapshot-identical-columns.png`

### Key measurements (Brand Snapshot)

| Check | A Product | B Compare | Match |
|---|---|---|---|
| Payload hash | `3bdb…ef37f` | `3bdb…ef37f` | Yes |
| Snapshot `outerHTML` length | 2834 | 2834 | Yes |
| Overview `.oe-section` count | 8 | 8 | Yes |
| Title font | Inter 17px / 700 | Inter 17px / 700 | Yes |
| Title color | `rgb(255,255,255)` | `rgb(255,255,255)` | Yes |
| Left/right compare HTML identity | — | true at render | Yes |

### Documented acceptable differences

1. **Compare chrome** (topbar / sidebar) outside OE markup.  
2. **Default column width** ~half viewport vs product `max-width: 1120px` — use **Equal-width geometry check** for matched containers.

### Fix applied during Gate 1

Removed a compare-only light theme that forced dark title text (`rgb(26,26,26)`). Gate 1 columns now use the product dark atelier surface so OE CSS applies unchanged.

---

## Governance

- Worktree: `C:\Dev\dealality\ws-brand-explorer` · branch `feat/be-phase2h-component-parity-v1`
- PR #457 / Phase 2F history preserved (not merged/closed by this work)
- No paid research · no mass population · no Airtable writes · no publication
- **Do not begin Gate 2 content reconstruction until Gate 1 is founder-approved**

---

## STOP

Await founder Gate 1 review on the Comparison Studio URL above with screenshot evidence.
