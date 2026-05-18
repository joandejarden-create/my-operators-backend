import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const jsPath = path.join(root, "public", "js", "deal-readiness-snapshot.js");
let js = fs.readFileSync(jsPath, "utf8");

const i = js.indexOf("  function renderPage1Narrative(data, options, ctx) {");
const j = js.indexOf("  function renderPage2Technical(data, options, ctx) {");
const k = js.indexOf("  function wrapBookPage(index, innerHtml, active) {");
if (i < 0 || j < 0) throw new Error("render functions not found");

const narrative = `  function renderPage1Narrative(data, options, ctx) {
    var meta = ctx.meta;
    var fields = ctx.fields;
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var reviewStatus = mapReviewStatusLabel(stage, score);
    var areaRows = buildReviewAreaRows(data);
    var clarifications = buildClarificationAreas(data);
    var strengths = buildNarrativeStrengths(data, meta, fields);
    var workflowRows = buildWorkflowRows(data, stage, score);

    var html = '<motionless></motionless>'.slice(0,0);
`;

// Build without the bad line - use array join
const L = (s) => s;
const narrativeLines = [
  "  function renderPage1Narrative(data, options, ctx) {",
  "    var meta = ctx.meta;",
  "    var fields = ctx.fields;",
  "    var score = data.dealReadinessScore;",
  '    var stage = data.readinessStage || "—";',
  "    var reviewStatus = mapReviewStatusLabel(stage, score);",
  "    var areaRows = buildReviewAreaRows(data);",
  "    var clarifications = buildClarificationAreas(data);",
  "    var strengths = buildNarrativeStrengths(data, meta, fields);",
  "    var workflowRows = buildWorkflowRows(data, stage, score);",
  "",
  '    var html = \'<div class="drs-book-page-inner drs-content-page drs-page-narrative">\';',
  "",
  '    html += \'<div class="drs-brief-highlights">\';',
  '    html += \'<p class="drs-brief-kicker">Readiness Narrative</p>\';',
  '    html += \'<div class="drs-brief-score-cards">\';',
  '    html += \'<div class="drs-brief-card"><div class="drs-brief-card-title">Readiness Score</div>\';',
  "    html += '<div class=\"drs-brief-card-body drs-brief-card-body--score\"><span class=\"drs-score-num\">' + esc(score != null && score !== \"\" ? score : \"—\") + '</span><span class=\"drs-score-of\"> / 100</span></div></div>';",
  '    html += \'<motionless></motionless>\';',
];
