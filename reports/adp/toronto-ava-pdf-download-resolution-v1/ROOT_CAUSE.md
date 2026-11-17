# ROOT CAUSE

## WHY DID “DOWNLOAD PDF” REQUIRE A NEW DRAFT PDF FOR AN ALREADY PUBLISHED TORONTO REPORT?

The AI Visibility Audit kebab/primary **Download PDF** action was wired to the **draft** channel.

Exact chain:

1. `public/js/admin-ai-visibility-audit.js` → `buildActions` / `handleAct`
2. Generic acts `download-pdf` / `view-pdf` called `deliverAvaPdf(..., "draft")`
3. `fetchAvaPdf` → `GET /api/admin/ai-visibility-audit/:id/pdf?channel=draft`
4. `api/admin-ai-visibility-audit.js` → `getAdminAvaPdf` → `resolveAvaPdfByChannel({ channel: "draft" })`
5. `lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js` → draft resolver

When a published property (Toronto) has no distinct New Draft file (or draft pointers only alias `current.pdf` / `published.pdf`, which the draft resolver correctly rejects), the API returns:

`DRAFT_PDF_FILE_MISSING`

and the UI alerts:

`NEW DRAFT PDF NOT AVAILABLE — refusing to fall back to published.`

That safety rule is correct **for explicit draft downloads**. It was incorrectly applied to the generic **Download PDF** label, which users reasonably expect to mean the **current lifecycle artifact** (for Toronto: the published AVA PDF).

Toronto metadata was already `PUBLISHED` / founder approved / customer ready / customer published with a valid published AVA PDF — the row should never have entered a draft-only branch for generic Download PDF.
