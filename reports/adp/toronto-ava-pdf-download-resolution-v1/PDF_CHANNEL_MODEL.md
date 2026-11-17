# PDF CHANNEL MODEL

| Channel | Meaning | Silent fallback |
|---------|---------|-----------------|
| `current` | Lifecycle-aware Admin default for **Download PDF** | None |
| `published` | Frozen customer-visible AVA PDF | None → draft |
| `draft` | New staged draft only | None → published |
| `history` | Exact `versionId` | None → other versions |

## Lifecycle → `current` target

| Lifecycle / flags | `current` resolves as |
|-------------------|------------------------|
| `PUBLISHED` / `CUSTOMER_READY` / `FOUNDER_APPROVED_FOR_CUSTOMER` / customerPublished | `published` |
| Staged / founder-reviewed / validated | `draft` |

Generic **Download PDF** → `current`.  
Explicit **Download Draft** → `draft` (still refuses published fallback).
