#!/usr/bin/env node
/**
 * Render read-only founder review HTML+JSON for Wyndham locked proposed 26.
 * Does not activate STAGED or publish CURRENT.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertWyndhamLocked26Shape,
  buildWyndhamProposedWriteSetFounderReview,
} from "../../lib/group-demand-intelligence/publication-package/gdi-proposed-write-set-founder-review-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-executive-summary-v1",
);
const OUT_JSON = path.join(OUT_DIR, "WYNDHAM_PROPOSED_26_FOUNDER_REVIEW.json");
const OUT_HTML = path.join(OUT_DIR, "WYNDHAM_PROPOSED_26_FOUNDER_REVIEW.html");

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function main() {
  const review = buildWyndhamProposedWriteSetFounderReview({});
  if (!review.ok) {
    console.error(review);
    process.exit(1);
  }
  const shape = assertWyndhamLocked26Shape(review);
  if (!shape.ok) {
    console.error("LOCKED_26_SHAPE_FAILED", shape);
    process.exit(1);
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(review, null, 2));

  const rows = [...review.proposedQualified, ...review.hold]
    .map(
      (r) =>
        `<tr><td>${esc(r.opportunityId)}</td><td>${esc(r.org || "—")}</td><td>${esc(
          r.bucket,
        )}</td><td>${esc(r.visibilityState)}</td><td>${esc(
          r.publicationStatus,
        )}</td><td>${esc(r.contact || "—")}</td></tr>`,
    )
    .join("\n");

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Wyndham Proposed 26 — Founder Review (read-only)</title>
  <style>
    body { font-family: Georgia, serif; margin: 2rem; color: #102a43; background: #f5f7fa; }
    h1 { margin-bottom: 0.25rem; }
    .banner { background: #fff3cd; border: 1px solid #e0a800; padding: 0.75rem 1rem; margin: 1rem 0; }
    .meta { color: #486581; margin-bottom: 1rem; }
    table { border-collapse: collapse; width: 100%; background: #fff; }
    th, td { border: 1px solid #d9e2ec; padding: 0.45rem 0.6rem; text-align: left; font-size: 0.92rem; }
    th { background: #243b53; color: #fff; }
    code { background: #e4e7eb; padding: 0.1rem 0.3rem; }
  </style>
</head>
<body>
  <h1>Wyndham San Diego Bayside — Locked Proposed 26</h1>
  <p class="meta">Founder review only · No STAGED activate · No CURRENT publish · No Airtable writes</p>
  <div class="banner">
    <strong>Root cause of UI &quot;11 of 11&quot;:</strong> ${esc(review.rootCause)}<br/>
    STAGED package: <code>${esc(review.staged.publicationVersion || "none")}</code>
    (${review.staged.count}) · CURRENT: ${review.current.available ? review.current.count : "none"}
  </div>
  <p>Locked proposed qualified: <strong>${review.counts.lockedProposedQualified}</strong>
    · Present in STAGED: <strong>${review.counts.proposedPresentInStaged}</strong>
    · Missing from STAGED: <strong>${review.counts.proposedMissingFromStaged}</strong>
    · HOLD (not qualified): <strong>${review.counts.hold}</strong></p>
  <p>Admin API (auth required):
    <code>/api/admin/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/proposed-write-set-review</code></p>
  <table>
    <thead>
      <tr>
        <th>Opportunity ID</th><th>Org</th><th>Bucket</th>
        <th>Visibility state</th><th>Publication</th><th>Contact</th>
      </tr>
    </thead>
    <tbody>
      ${rows}
    </tbody>
  </table>
</body>
</html>`;

  fs.writeFileSync(OUT_HTML, html);
  console.log(
    JSON.stringify(
      {
        ok: true,
        shape,
        rootCause: review.rootCause,
        counts: review.counts,
        outJson: OUT_JSON,
        outHtml: OUT_HTML,
      },
      null,
      2,
    ),
  );
}

main();
