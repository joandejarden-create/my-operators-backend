# Contact Intelligence wave loader parity — browser evidence

**Reference component:** `DealalityWaveLoader.toastHtml` → `HeLoadingUx.sectionHtml("Loading Contacts…")`  
**Reference CSS:** `public/css/dealality-wave-loader.css` (`.dealality-wave-loader--toast`)

## Computed styles (:8080 Bethesda Marriott)

| | REFERENCE toast | LIVE Contact Intelligence |
|---|---|---|
| background | `rgba(0, 0, 0, 0.9)` | `rgba(0, 0, 0, 0.9)` |
| height | 50px | 50px |
| border-radius | 12px | 12px |
| text | Loading Contacts… | Loading Contacts… |
| wave animation | `bl-wave-1` running | `bl-wave-1` running |

`bgMatch=true` · `animRunning=true` · not 200px map `.loading` height.

## Root cause

`.hex-root .dealality-wave-loader--toast` in `hotel-explorer.css` remapped toast chrome to elevated navy `#212c4d`. Contact Intelligence now restores the canonical black toast under `.hci-root` only.
