#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const code = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8"
);
const sandbox = { window: {}, globalThis: {} };
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
// eslint-disable-next-line no-new-func
const UI = new Function(
  "window",
  "globalThis",
  `${code}\n;return window.DealalityGdiUi;`
)(sandbox, sandbox);

const html = UI.customerFeedbackLifecycleHtml(
  {
    id: "demo",
    shareValidation: {
      familiarityStatus: "ALREADY_KNOWN",
      commercialValue: "WORTH_PURSUING_NOW",
      validatedAt: "2026-11-12T14:18:00.000Z",
      hotelValidationUpdatedAt: "2026-11-12T14:18:00.000Z",
      note: "Demo",
    },
    founderReviewDecision: "ASSIGN_FOR_DISCOVERY",
  },
  UI.CUSTOMER_VALIDATION_ENUMS,
  {
    canValidate: true,
    canRecordAction: true,
    canRecordOutcome: true,
    canFounderReview: true,
  },
  {
    latestActionType: "CONTACTED",
    salesActionUpdatedAt: "2026-11-12T13:42:00.000Z",
    latestActionLabel: "Contacted",
    latestOutcomeType: null,
  }
);

const outDir = path.join(
  ROOT,
  "reports/gdi-hotel-feedback-airtable-persistence-v1"
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(path.join(outDir, "ui-fragment.html"), html);
fs.writeFileSync(
  path.join(outDir, "UI-VERIFICATION.html"),
  `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>GDI Hotel Feedback UI verify</title>
  <link rel="stylesheet" href="../../public/css/group-demand-intelligence.css" />
  <style>
    body {
      background: #0b1220;
      color: #e8eefc;
      font-family: "Segoe UI", sans-serif;
      padding: 24px;
      max-width: 720px;
      margin: 0 auto;
    }
  </style>
</head>
<body>
  <h1>Hotel Validation / Action / Outcome</h1>
  ${html}
</body>
</html>
`
);
console.log("wrote", outDir);
