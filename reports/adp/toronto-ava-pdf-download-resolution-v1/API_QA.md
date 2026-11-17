# API QA

Toronto `channel=current&download=1`:

| Check | Result |
|-------|--------|
| HTTP | **200** |
| Content-Type | `application/pdf` |
| X-AVA-PDF-Channel | `current` |
| X-AVA-PDF-Resolved-As | `published` |
| Bytes | 279308 |
| SHA | `a75de648…050bca1e` |
| DRAFT_PDF_FILE_MISSING | **No** |

Controls (Bethesda / Waterstone / Spice Island): `current`/`draft`/`published` all HTTP 200 with correct channel headers.
