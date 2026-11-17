import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadExecutionRun,
  buildMonitorDashboardPayload,
} from "../lib/group-demand-intelligence/observability/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(
  ROOT,
  "reports/gdi-full-e2e-all-provider-execution-observability-v1",
);
const run = loadExecutionRun(
  "recrPQcZg7SFARRb2",
  "hist_yotel_geneva_exa_30usd_scale_v1",
);
const dash = buildMonitorDashboardPayload(run);
fs.writeFileSync(
  path.join(outDir, "yotel-historical-dashboard.json"),
  `${JSON.stringify(dash, null, 2)}\n`,
);

const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>GDI Execution Monitor Preview</title>
  <link rel="stylesheet" href="/css/admin-gdi-execution-monitor.css" />
  <style>
    body { font-family: Segoe UI, sans-serif; background: #0b1220; color: #e8eef7; margin: 24px; }
    h1 { font-size: 20px; }
  </style>
</head>
<body>
  <h1>GDI Full E2E Execution Monitor — YOTEL historical preview</h1>
  <p id="meta"></p>
  <h2 class="gem-section-title">14-segment matrix</h2>
  <div class="gem-table-wrap" id="gemSegmentMatrix"></div>
  <h2 class="gem-section-title">Providers</h2>
  <div class="gem-table-wrap" id="gemProviderMatrix"></div>
  <h2 class="gem-section-title">Apify lifecycle</h2>
  <div class="gem-table-wrap" id="gemApifyLifecycle"></div>
  <h2 class="gem-section-title">Stages</h2>
  <div class="gem-table-wrap" id="gemStageTimeline"></div>
  <pre class="gem-certificate" id="gemCertificate"></pre>
  <script>
    const dash = ${JSON.stringify(dash)};
    document.getElementById("meta").textContent =
      dash.run.hotelId + " / " + dash.run.runId + " / " + (dash.certificate || {}).verdict;
    document.getElementById("gemCertificate").textContent = JSON.stringify(
      dash.certificate,
      null,
      2,
    );
    function esc(v) {
      return String(v ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;");
    }
    function table(headers, rows) {
      return (
        '<table class="gem-table"><thead><tr>' +
        headers.map((h) => "<th>" + h + "</th>").join("") +
        "</tr></thead><tbody>" +
        rows
          .map(
            (r) =>
              "<tr>" + r.map((c) => "<td>" + c + "</td>").join("") + "</tr>",
          )
          .join("") +
        "</tbody></table>"
      );
    }
    document.getElementById("gemSegmentMatrix").innerHTML = table(
      ["Segment", "Discovery", "Deep", "Providers", "Requests", "Cost", "Reason"],
      dash.segmentMatrix.map((r) => [
        esc(r.displayTitle),
        esc(r.discoveryStatus),
        esc(r.deepResearchStatus),
        esc((r.providersUsed || []).join(", ") || "—"),
        esc(r.requestCount),
        "$" + Number(r.costUsd || 0).toFixed(2),
        esc(r.errorOrSkipReason || "—"),
      ]),
    );
    document.getElementById("gemProviderMatrix").innerHTML = table(
      ["Provider", "Config", "Calls", "Spend", "Reason"],
      dash.providerMatrix.map((r) => [
        esc(r.name),
        esc(r.configurationState),
        esc(r.callsSubmitted),
        "$" + Number(r.actualSpendUsd || 0).toFixed(2),
        esc(r.failureOrSkipReason || "—"),
      ]),
    );
    document.getElementById("gemApifyLifecycle").innerHTML = table(
      ["State", "Reason"],
      (dash.apifyLifecycle || []).map((r) => [esc(r.state), esc(r.reason || "—")]),
    );
    document.getElementById("gemStageTimeline").innerHTML = table(
      ["Stage", "Status"],
      dash.stageTimeline.map((r) => [
        esc((r.letter ? r.letter + ". " : "") + (r.title || r.stageId)),
        esc(r.status),
      ]),
    );
  </script>
</body>
</html>
`;

fs.writeFileSync(path.join(outDir, "preview-yotel-monitor.html"), html);
fs.writeFileSync(
  path.join(ROOT, "public/gdi-execution-monitor-preview.html"),
  html,
);
console.log("preview written");
