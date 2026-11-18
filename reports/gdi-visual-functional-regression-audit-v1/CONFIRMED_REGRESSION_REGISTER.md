# Confirmed regression register

| ID | Component | Previous behavior | Current behavior (before fix) | Root cause | Introducing change | Impact | Severity | Action | Regression test |
|---|---|---|---|---|---|---|---|---|---|
| REG-SVG-001 | Opportunity card revenue-stream SVGs (Research Preview) | Class+sales-key rows light guestroom / catering gold | All icons muted gray despite classified streams | Live resolve demoted icons after thesis scrub; PACKAGE restore not shared | Gap vs package SVG restore (`gdi-package-customer-presentation-attach-v1`); visible after Research Preview inventory restore (#357) | Sales teams lose quick revenue-type scan | Critical | Restore shared class+sales-key icon path | `npm run test:gdi-visual-svg-icon-color-regression-v1` |
| REG-SVG-002 | UI resolver demotion | Explicit API icon states trusted | Client thesis demotion could mute COMBINED after API restore | `resolveRevenueStreamIconState` demoted active+active when thesis empty | Client consistency guard overreach | Even corrected API payloads could render one/both gray | High | Trust API/package icons; class+sales-key restore fallback | Same test + UI cache-bust `gdi-svg-icon-color-restore-v1` |

## Investigated — not confirmed as unintended regressions

| Topic | Finding | Action |
|---|---|---|
| Need-date calendar often gray | Legitimate `not-assessed` / no-match states; CSS color tokens intact | Preserve |
| COMBINED→GUESTROOMS demotion without event thesis | Intentional anti-invention (package + sanitize) | Preserve |
| San Diego rows without class/keys | Icons derive from evidence only; many remain muted honestly | Preserve |
| Filters / sidebar inventory toggle | PR #358 deliberate enhancement | Preserve |
| Research Preview count collapse | Fixed in PR #357; not reintroduced | Preserve |
| Drawer section removal | No evidenced wholesale removal in this audit | No change |
| Commercial field invention | Forbidden — no fabricated guestroom/catering values | Preserve |

Only evidence-backed unintended changes are listed as confirmed regressions.
