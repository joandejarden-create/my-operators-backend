# Phase 3 Return — Many Futures (Old Home)

**Status:** Inserted in Webflow Designer. **Not published.**

Site: Deal Capture MVP (`68108c29063eeb5d1bd7ae4a`)  
Page: Old Home (`68108c2a063eeb5d1bd7ae90`) / `/old-home`

---

## 1–4. Screenshots

### Designer (design mode)
- Designer snapshot of `#many-futures` confirms native shell (eyebrow **BEFORE COMMITMENT**, H2, lead) and HtmlEmbed placeholder.
- In **design mode**, Webflow correctly shows: *“This \<script\> embed only displays in preview mode…”* — expected for Code Embeds containing scripts.

### Preview / interaction (production embed parity)
Designer MCP timed out when switching/capturing live Preview. Interaction and breakpoint rendering were verified against the **identical production embed** (`many-futures-section.html`, CDN-wired) via local Preview:

| Breakpoint | Artifact |
|---|---|
| Desktop 1440 | `screenshots/phase3/qa-desktop-1440.png` |
| Tablet 768 | `screenshots/phase3/qa-tablet-768.png` |
| Mobile 390 | `screenshots/phase3/qa-mobile-390.png` |

Also copied to `/opt/cursor/artifacts/screenshots/`.

Local preview pages still show prototype-only notes / fake Features grid. **Those are not present in the Webflow section.**

---

## 5. Exact insertion point

Confirmed sibling order inside `#dc-premium` / `.oh-page`:

`… → #perspectives → **#many-futures** → **#platform-features** → #modules → …`

Section element id: `6ca557cf-1f72-acc8-c85a-502c8f198b4a`  
Inserted with `creation_position: before` relative to `#platform-features` (`43d3da86-f5ea-ff1d-3222-8df4b218ee91`).

---

## 6. Webflow classes and element IDs

### Classes created
| Class | Role |
|---|---|
| `many-futures-section` | Section shell (`#080F25`, padding, borders) |
| `many-futures-container` | max-width 1120px centered |
| `many-futures-intro` | Centered intro stack |
| `many-futures-h2` | Combo on `oh-section-h2` (expressive size/weight) |
| `many-futures-embed` | Embed wrapper |

### Classes reused
| Class | Role |
|---|---|
| `oh-section-badge` | Eyebrow **BEFORE COMMITMENT** |
| `oh-section-h2` | Heading base |
| `oh-section-lead` | Supporting copy |

### Element IDs
| ID | Element |
|---|---|
| `many-futures` | Section |
| `many-futures-h2` | H2 |
| *(embed root)* `dealality-many-futures` | Inside Code Embed only |

Navigator display name: **Many Futures**

---

## 7. Assets uploaded (Webflow CDN)

All under site CDN base  
`https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/`

| File | Asset id / hosted name |
|---|---|
| Temporary hotel | `6a6b9ecfbcdb4eea68e0a6f3_mf-hotel-temp.jpg` |
| Rebrand desk/mobile | `…752f2_mf-rebrand-desktop.png` / `…5bfeb_mf-rebrand-mobile.png` |
| New Operator desk/mobile | `…12191_mf-new-operator-desktop.png` / `…0a716_mf-new-operator-mobile.png` |
| Soft Brand desk/mobile | `…9eceb_mf-soft-brand-desktop.png` / `…f3c41_mf-soft-brand-mobile.png` |
| Independent desk/mobile | `…f3c73_mf-independent-desktop.png` / `…35d15_mf-independent-mobile.png` |
| Branded Residences desk/mobile | `…9ed1e_mf-branded-residences-desktop.png` / `…37240_mf-branded-residences-mobile.png` |

CDN HTTP 200 verified for hotel + Operator crops.

---

## 8. Final Embed character count

**34,332 characters** (≈33.5 KB)

Delta vs Phase 2.5 approved ~33,472 is from replacing short local `assets/…` paths with full CDN URLs (~860 chars). Still a single isolated Code Embed.

---

## 9. Console-error results

Local production-embed Preview (Chrome headless):
- No application console errors attributable to Many Futures after load.
- Chrome environment dbus/UPower noise only (headless VM).
- Embed root gains `mf-js-ready` and `mf-entered` as designed.

**Webflow Designer Preview console** could not be captured in this run (Designer MCP timeouts). Recommend a human Preview pass before publish.

---

## 10. Keyboard interaction results

Preserved behavior in embed JS (verified by code + local Preview DOM):
- Path buttons are real `<button>` elements with `aria-pressed`.
- Enter / Space pin the path.
- Focus moves between futures; focus-visible outline present.
- **No Escape** handler (approved).
- Default path remains **New Operator**.

---

## 11. Reduced-motion results

`prefers-reduced-motion: reduce` CSS path remains in embed:
- Entrance animations disabled; opacity/transform forced to resting state.
- Connector draw animation suppressed.
- Active-state transitions reduced.

---

## 12. `#platform-features` unchanged

Confirmed:
- Same element id `43d3da86-f5ea-ff1d-3222-8df4b218ee91`
- Same attributes (`id`, `aria-labelledby`)
- Still immediately after `#many-futures`
- No edits to Features scripts, grid, nav, footer, global typography, or routing

---

## 13. Three proposed final hotel-image options

Current courtyard image remains **temporary Phase 3 placeholder**. Keep **ILLUSTRATIVE OPPORTUNITY** and **TEMPORARY IMAGE** until a final is approved. Do not imply a Dealality client project.

### Option 1 — Urban street arrival (preferred direction)
Upper-upscale adaptive-reuse facade on a walkable urban street: visible porte-cochère / main entrance, warm evening light, ~8–12 story massing reading as ~100–150 keys. Caribbean / San Juan–plausible architecture (concrete, masonry, balconies) without residential-condo landscaping cues.

### Option 2 — Lobby / arrival sequence
Interior-forward but still “hotel”: double-height lobby looking toward the front entrance or porte-cochère glass, reception desk, soft upper-upscale finishes, guests/staff scale that reads as operating hotel (not condo amenity lounge).

### Option 3 — Corner facade + street presence
Corner urban hotel with clear hotel entrance canopy/signage language, street trees, mixed-use neighborhood context, daytime clarity. Avoid rooftop-pool condo tropes and avoid branded chain marks that imply a real Dealality engagement.

Licensing: use rights-cleared stock or commissioned photography only; never real Dealality client assets.

---

## 14. Local prototype vs Webflow rendering

| Topic | Local prototype | Webflow |
|---|---|---|
| Eyebrow | BEFORE COMMITMENT | Same (`oh-section-badge`) |
| Embed HTML/CSS/JS | Same build output | Same Code Embed content |
| Product crops / hotel | Local files *or* CDN in rebuilt preview | CDN URLs |
| Soft focus + mobile crops | Updated recipe | Same image assets |
| Prototype notes / fake Features grid | Present in `index.html` / `preview.html` | **Absent** |
| Design mode | N/A | Embed scripts show placeholder warning |
| Preview / published | Full interaction | Full interaction when Preview/custom code enabled |

---

## Focus + crop adjustments shipped

- Soft focus: dim outside metrics + soft glow + thin low-opacity edge (no selection-box / thick double border).
- Dedicated mobile Operator crop: ~4–5 KPI cards, no tiny tab labels, shorter height, mild ≤1.3× upscale.
- Dedicated mobile crops for other paths where desktop crops would be unreadable.
- `.mf-screens` / `.mf-screens--mobile` aspect ratios shortened; `object-fit: contain` so curated crops are not re-cropped.

---

## Do not publish

Awaiting explicit approval after final hotel image selection and Designer Preview confirmation.
