# Runtime file safety pass — 2026-10-01

**Branch:** `cursor/local-system-startup-recovery`  
**Purpose:** Ensure no Dealality runtime/render file remains untracked.

## Classification of untracked files (at pass start)

### A. RUNTIME REQUIRED → must track
| Path | Status |
|------|--------|
| `public/js/hotel-contact-intelligence.js` | Already tracked (`47006b6`) |
| `public/css/hotel-contact-intelligence.css` | Already tracked |
| `public/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json` | Already tracked (referenced by HI JS) |

**Newly tracked this pass:** none required (A set already empty under protected roots).

### B. PRODUCT SOURCE / FIXTURE REQUIRED
| Path | Status |
|------|--------|
| `fixtures/hotel-intelligence/contact-intelligence/*` | Already tracked |
| `fixtures/hotel-intelligence/owner-portfolio/*` | Already tracked |

### C. GENERATED / RECREATABLE — intentionally untracked
| Path | Reason | Nightly FS backup? | `.gitignore` |
|------|--------|--------------------|--------------|
| `data/census-map/**` | Tier-C snapshot cache; rebuilt by `census-map-snapshot` boot | Yes (project tree) | Added `data/census-map/` |
| `reports/startup-recovery-test-*.txt` | Ephemeral recovery test dumps | Yes while present | Added pattern |
| `reports/ma-contact-intel-v1-*.txt` | Ephemeral test dumps | Yes while present | Added pattern |
| `reports/untracked-runtime-files-audit.json` | Audit output (regenerated) | Optional | leave untracked / may regenerate |

### D. TEMPORARY RECOVERY TOOL — intentionally untracked
| Path | Reason | Nightly FS backup? | `.gitignore` |
|------|--------|--------------------|--------------|
| `scripts/_*.mjs` (recovery helpers) | One-off restore/scan tools from crash recovery | Yes while present | Added `scripts/_*.mjs` |

### E. LARGE LOCAL DATA — intentionally untracked
| Path | Reason | Nightly FS backup? | `.gitignore` |
|------|--------|--------------------|--------------|
| `data/hotel-intelligence/research/**` | Local research run artifacts | Yes | Added `data/hotel-intelligence/research/` |

## Runtime files remaining untracked under protected roots

**ZERO** (`npm run dealality:audit-untracked-runtime` → `criticalUntrackedCount: 0`)

## `.gitignore` findings

1. **No broad hide** of `api/**`, `lib/**`, `public/js/**`, `public/css/**` as whole trees.
2. **Intentional WIP ignore (clean-checkout risk):** Capital Provider Explorer files (lines 59–67) — local-only “do not deploy”. Audit reports these as `WARNING` / `KNOWN_IGNORED_WIP` (8 files present on disk).
3. **Added this pass:** census-map cache, HI research data, recovery `_*.mjs`, ephemeral recovery report dumps.

## Permanent guard

- Script: `scripts/audit-untracked-runtime-files.mjs`
- npm: `dealality:audit-untracked-runtime`
- Nightly: `C:\Dev\Backup-Scripts\Backup-Dealality.ps1` runs audit and prints SAFETY AUDITS in the backup report

## Unrelated WIP

Left untouched (modified tokens, MA/GDI dirty files, etc.).
