# ADI dropdown title — UI QA

## Control
`#adpProperty.filter-select` on `/owner-ai-demand.html`.

## Desktop
| Check | Result |
|---|---|
| Selected title not clipped top/bottom | PASS (browser verify — W Rome + AC Coruña) |
| Full title via tooltip when needed | PASS (`select.title` / `option.title`) |
| Chevron aligned | PASS |
| Short title (W Rome) | PASS |
| Long title (AC Hotel A Coruña — A Coruña, Galicia) | PASS — full glyphs readable |
| Focus styles | PASS (unchanged) |

## Mobile (~390px)
| Check | Result |
|---|---|
| No vertical glyph clip | PASS |
| Horizontal ellipsis OK when narrow | PASS — full string on `title` |
| Layout / Load Report still usable | PASS |

## Shared dropdown regression
| Control | Result |
|---|---|
| Other `.filter-select` (GDI property, filters) | PASS — shared shell CSS; GDI W Rome selector readable |
| `.filter-input` | PASS — inherits normal line-height / min-height only |
| Action Plan feedback `<select>`s on ADI | PASS — not forced to 3rem unless `.filter-select` |

## Business logic
ADI business logic changed? **NO**
