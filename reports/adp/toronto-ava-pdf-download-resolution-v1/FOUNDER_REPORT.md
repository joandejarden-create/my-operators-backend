# FOUNDER REPORT — Toronto AVA PDF Download Resolution Fix V1

## Verdict

Generic **Download PDF** on AI Visibility Audit now uses lifecycle **`current`**.  
For published Toronto that resolves to the **published AVA** November PDF — not a missing draft.

Explicit **Download Draft** still refuses published fallback.

## Before → After

| | Before | After |
|--|--------|-------|
| Download PDF | `channel=draft` → `DRAFT_PDF_FILE_MISSING` | `channel=current` → published AVA PDF |
| HTTP | 404 | **200** |
| Artifact | — | AI Visibility Audit November 2026 |

## Governance

- GENERIC DOWNLOAD PDF USES CURRENT LIFECYCLE ARTIFACT: **YES**
- EXPLICIT DRAFT DOWNLOAD STILL REFUSES PUBLISHED FALLBACK: **YES**
- DRAFT SAFETY WEAKENED: **NO**
