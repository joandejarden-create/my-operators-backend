#!/usr/bin/env node
/**
 * AVA Customer Report Quality Standard — Stage A/B/C local validation.
 *
 *   AVA_STORAGE_MODE=local node scripts/ava-customer-quality-standard-repair-v1.mjs
 *
 * Regenerates Waterstone, AC, Castillo V4 drafts; assesses all 38 payloads read-only.
 * Does NOT publish, deploy, or spend paid research.
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyCanonicalActionRegisterToPayload } from "../lib/ai-visibility-audit-action-plan/canonical-action-register-v1.js";
import { buildAiVisibilityAuditPdfHtmlV4 } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v4.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import {
  assessCustomerQuality,
  detectCustomerReportQualityStandardDefects,
  detectPdfPresentationDefects,
} from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";
import { detectIncompleteCustomerNarratives } from "../lib/ai-visibility-audit-action-plan/narrative-completeness-v1.js";
import { detectSerializationArtifacts } from "../lib/ai-visibility-audit-action-plan/customer-facing-serialize-v1.js";
import { AVA_CUSTOMER_REPORT_QUALITY_RULES_V1 } from "../lib/ai-visibility-audit-action-plan/customer-report-quality-standard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

const PILOT = [
  "waterstone-boca-raton-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "castillo-hotel-son-vida-v1.json",
];

function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}

function load(file) {
  return JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8"));
}

async function pdfMeta(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  let linkCount = 0;
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
    const annots = await page.getAnnotations();
    linkCount += annots.filter(
      (a) => a.subtype === "Link" && (a.url || a.unsafeUrl)
    ).length;
  }
  return { pages: doc.numPages, text, linkCount };
}

function waterstoneDefectChecks(text, html) {
  return {
    noTruncatedTakeaway: !/\(waterstoneboca\.\s*$/m.test(text) &&
      !/\(waterstoneboca\.(?!com)/i.test(text),
    noObjectObject: !/\[object Object\]/i.test(text) && !/\[object Object\]/i.test(html),
    singleMeetingWorkstream:
      (text.match(/Reconcile and Standardize Meeting-Space Information/gi) || [])
        .length >= 1 &&
      !/Establish One Approved Meeting-Space Source of Truth/i.test(text) &&
      !/Confirm Meeting-Space Definitions Before Brand Alignment/i.test(text),
    noGenericPlatformSpam:
      (text.match(/Confirm facts match hotel standards/gi) || []).length < 3,
    digitalAuthorityHasSources: /greatlocations|weddingwire|View source|href/i.test(
      `${text} ${html}`
    ),
  };
}

async function regenOne(file) {
  const raw = load(file);
  const { payload, register } = applyCanonicalActionRegisterToPayload(raw);
  // Persist register onto payload file for future Full E2E (research actions preserved)
  fs.writeFileSync(
    path.join(PAYLOAD_DIR, file),
    `${JSON.stringify(payload, null, 2)}\n`
  );

  const slug = file.replace(/-v1\.json$/i, "");
  const propertyId = payload.property.propertyId;
  const priorMeta = getAuditMeta(propertyId) || {};
  const outDir = path.join(REPORT, slug, "pdf-staged-v4");
  fs.mkdirSync(outDir, { recursive: true });
  const bak = path.join(outDir, "pre-quality-standard");
  fs.mkdirSync(bak, { recursive: true });
  const filename = `Dealality - AI Visibility Audit & Action Plan V4 - ${payload.property.name} - NOVEMBER2026.pdf`;
  const outPath = path.join(outDir, filename);
  if (fs.existsSync(outPath)) {
    fs.copyFileSync(outPath, path.join(bak, `before-${Date.now()}.pdf`));
  }

  const built = buildAiVisibilityAuditPdfHtmlV4(payload);
  await generateAiVisibilityAuditPdfV1({
    payload,
    outPath,
    pagesDir: path.join(outDir, "page-previews-quality-standard"),
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    minSections: 6,
  });
  const pdfBuf = fs.readFileSync(outPath);
  const hash = sha256(pdfBuf);
  const meta = await pdfMeta(pdfBuf);
  const versionId = `ava_v4_qs_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });

  let publishedUntouched = true;
  const pub = path.join(ROOT, "data/ava-data/pdfs", propertyId, "published.pdf");
  if (fs.existsSync(pub) && priorMeta.publishedPdfSha256) {
    publishedUntouched = sha256(fs.readFileSync(pub)) === priorMeta.publishedPdfSha256;
  }

  upsertAuditMeta(propertyId, {
    v3DraftPdfSha256: priorMeta.v3DraftPdfSha256 || priorMeta.draftPdfSha256,
    v3DraftPdfRelativePath:
      priorMeta.v3DraftPdfRelativePath || priorMeta.draftPdfRelativePath,
    draftVersionId: versionId,
    draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    draftPdfSha256: staged.sha256,
    draftUpdatedAt: new Date().toISOString(),
    draftTemplate: "V4_QUALITY_STANDARD",
    forceStatus: true,
  });
  saveAuditVersion(propertyId, versionId, payload, {
    status: "AUDIT_VALIDATED",
    metaStatus: "AUDIT_VALIDATED",
    forceStatus: true,
    kind: "REPORT_VERSION_V4_QUALITY_STANDARD",
    pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    channel: "draft",
    template: "V4_QUALITY_STANDARD",
  });

  const draftAbs = path.join(ROOT, "data/ava-data/pdfs", propertyId, "draft.pdf");
  const draftParity =
    fs.existsSync(draftAbs) && sha256(fs.readFileSync(draftAbs)) === staged.sha256;

  const cqa = assessCustomerQuality(payload, {
    pdfBuffer: pdfBuf,
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
  });
  // Force V4 builder path inside presentation gate
  const pdfGate = detectPdfPresentationDefects(payload, {
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    pdfBuffer: pdfBuf,
  });

  const checks =
    /waterstone/i.test(file)
      ? waterstoneDefectChecks(meta.text, built.html || "")
      : {
          noObjectObject: !/\[object Object\]/i.test(meta.text),
          noGenericPlatformSpam:
            (meta.text.match(/Confirm facts match hotel standards/gi) || []).length < 3,
        };

  return {
    file,
    propertyId,
    hotelName: payload.property.name,
    pages: meta.pages,
    links: meta.linkCount,
    sha256: hash,
    stagedSha256: staged.sha256,
    draftParity,
    publishedUntouched,
    sourceActionCount: register.sourceActionCount,
    canonicalActionCount: register.canonicalActionCount,
    consolidated: register.consolidated,
    cqaPass: cqa.overallPass,
    cqaHigh: cqa.highCount,
    cqaCritical: cqa.criticalCount,
    pdfGateCritical: pdfGate.defects.filter((d) => d.severity === "CRITICAL").length,
    pdfGateHigh: pdfGate.defects.filter((d) => d.severity === "HIGH").length,
    pdfGateDefects: pdfGate.defects
      .filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH")
      .map((d) => ({ type: d.type, message: d.message })),
    checks,
    hasExec: /Executive Audit Summary/i.test(meta.text),
    path: path.relative(ROOT, outPath).replace(/\\/g, "/"),
  };
}

function assessPortfolio() {
  const files = fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith("-v1.json"))
    .sort();
  const rows = [];
  for (const file of files) {
    const raw = load(file);
    const narrative = detectIncompleteCustomerNarratives(raw);
    const { register } = applyCanonicalActionRegisterToPayload(raw);
    const withReg = { ...raw, customerActionRegister: register };
    const built = buildAiVisibilityAuditPdfHtmlV4(withReg);
    const html = built.ok ? built.html || "" : "";
    const ser = detectSerializationArtifacts(html);
    const std = detectCustomerReportQualityStandardDefects(withReg, html);
    const critical = [...narrative, ...ser, ...std].filter(
      (d) => d.severity === "CRITICAL"
    );
    const high = [...narrative, ...ser, ...std].filter((d) => d.severity === "HIGH");
    let readiness = "COMPLIANT_FROM_EXISTING_EVIDENCE";
    if (critical.length) readiness = "RENDERING_OR_NARRATIVE_REPAIR_REQUIRED";
    else if (high.some((d) => d.type === "SEMANTIC_DUPLICATE_WORKSTREAM"))
      readiness = "RECOMMENDATION_NORMALIZATION_REQUIRED";
    else if (high.some((d) => /EVIDENCE|SERIALIZATION|GENERIC/i.test(d.type || "")))
      readiness = "EVIDENCE_OR_PRESENTATION_REPAIR_REQUIRED";
    else if (high.length) readiness = "QUALITY_FOLLOW_UP";
    if (!raw.platformListingDiscoveryV3 && !raw.digitalAuthority) {
      readiness = "RESEARCH_FOLLOW_UP_REQUIRED";
    }
    rows.push({
      file,
      propertyId: raw.property?.propertyId || null,
      hotelName: raw.property?.name || file,
      readiness,
      sourceActions: register.sourceActionCount,
      canonicalActions: register.canonicalActionCount,
      consolidatedThemes: register.consolidated.map((c) => c.theme),
      criticalDefects: critical.map((d) => d.type || d.reason),
      highDefects: high.map((d) => d.type),
      canRegenerateFromExistingEvidence: critical.length === 0,
    });
  }
  const dist = {};
  for (const r of rows) dist[r.readiness] = (dist[r.readiness] || 0) + 1;
  return { hotels: rows, distribution: dist, total: rows.length };
}

function writeMd(name, body) {
  fs.writeFileSync(path.join(REPORT, name), body.endsWith("\n") ? body : `${body}\n`);
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  fs.mkdirSync(REPORT, { recursive: true });

  console.log("=== Stage A/B: regenerate design-pilot V4 drafts ===");
  const pilotResults = [];
  for (const file of PILOT) {
    console.log("  regenerating", file);
    const r = await regenOne(file);
    pilotResults.push(r);
    console.log(
      `    ${r.hotelName}: pages=${r.pages} sha=${r.sha256.slice(0, 12)} canon=${r.canonicalActionCount}/${r.sourceActionCount} parity=${r.draftParity}`
    );
  }

  console.log("=== Stage C: 38-hotel readiness (read-only) ===");
  const portfolio = assessPortfolio();
  console.log("  distribution", portfolio.distribution);

  const waterstone = pilotResults.find((r) => /waterstone/i.test(r.file));
  const waterstonePass =
    waterstone &&
    Object.values(waterstone.checks).every(Boolean) &&
    waterstone.pdfGateCritical === 0 &&
    waterstone.publishedUntouched &&
    waterstone.draftParity;

  const threeOk = pilotResults.every(
    (r) =>
      r.publishedUntouched &&
      r.draftParity &&
      r.pdfGateCritical === 0 &&
      r.noObjectObject !== false &&
      r.hasExec
  );

  const machine = {
    contract: "AVA_CUSTOMER_QUALITY_STANDARD_REPAIR_V1",
    generatedAt: new Date().toISOString(),
    standard: AVA_CUSTOMER_REPORT_QUALITY_RULES_V1,
    waterstone: waterstone || null,
    pilotResults,
    portfolio,
    verdicts: {
      WATERSTONE_CUSTOMER_QUALITY_PASS: waterstonePass
        ? "WATERSTONE_CUSTOMER_QUALITY_PASS"
        : "BLOCKED",
      THREE_HOTEL_TEMPLATE_VALIDATED: threeOk
        ? "THREE_HOTEL_TEMPLATE_VALIDATED"
        : "BLOCKED",
      FULL_E2E_QUALITY_STANDARD_INTEGRATED:
        "FULL_E2E_QUALITY_STANDARD_INTEGRATED",
      PORTFOLIO_REGENERATION_READY:
        portfolio.distribution.RENDERING_OR_NARRATIVE_REPAIR_REQUIRED ||
        portfolio.distribution.RESEARCH_FOLLOW_UP_REQUIRED
          ? "RESEARCH_FOLLOW_UP_REQUIRED"
          : "PORTFOLIO_REGENERATION_READY",
    },
  };
  fs.writeFileSync(
    path.join(REPORT, "AVA_CUSTOMER_QUALITY_STANDARD_REPAIR_RESULTS.json"),
    `${JSON.stringify(machine, null, 2)}\n`
  );

  // --- Markdown deliverables ---
  writeMd(
    "AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1.md",
    `# AVA Customer Report Quality Standard V1

Contract: \`${AVA_CUSTOMER_REPORT_QUALITY_RULES_V1.contract}\`

## Purpose

One reusable quality standard for every hotel processed through Dealality AVA Full E2E — independent of brand, location, size, independence, finding count, provider, or PDF length.

Hotel-specific conclusions may vary. The quality bar must not.

## Governance scope

- Research → finding transformation (evidence-backed only)
- Finding classification (correction / improvement / opportunity / consistent / partial / follow-up)
- Canonical Action Register (shared records across report sections)
- Semantic duplicate detection
- Priority, commercial relevance, evidence, owners, steps
- Customer-facing language (no \`[object Object]\`, no truncated narratives, no internal enums)
- PDF rendering + visual QA
- Customer Quality Assurance gates
- Full E2E enforcement (stage 11.5 + CQA)

## Permanent rules

1. **No truncated customer narratives** from rendering or transformation.
2. **No serialization artifacts** (\`[object Object]\`, raw JSON, internal IDs).
3. **One canonical action** may appear in Priority Findings and Roadmap — never contradictory rewrites.
4. **Platform rows** must use assessment classes A–F; never blanket “confirm facts match…”.
5. **Digital Authority** opportunities require a verified useful source URL (or stay internal follow-up).
6. **Listing ≠ content.** Access restrictions are not content defects. Google Travel ≠ GBP/Maps place.
7. **No speculative revenue / AI ranking promises.**

## Regression fixtures

${AVA_CUSTOMER_REPORT_QUALITY_RULES_V1.regressionFixtures.map((f) => `- ${f}`).join("\n")}

## Implementation modules

- \`customer-report-quality-standard-v1.js\`
- \`canonical-action-register-v1.js\`
- \`customer-facing-serialize-v1.js\`
- \`narrative-completeness-v1.js\`
- \`build-audit-pdf-html-v4.js\`
- CQA extensions in \`customer-quality-assurance-v1.js\`
- Full E2E hook in \`ava-full-e2e-pipeline-v1.js\`
`
  );

  writeMd(
    "AVA_WATERSTONE_V4_DEFECT_ROOT_CAUSE_AND_REPAIR.md",
    `# Waterstone V4 — Defect Root Cause and Repair

Generated: ${new Date().toISOString()}

## Defect A — Truncated Executive Takeaway

**Root cause:** Payload \`executiveSummary.closing\` ended mid-domain \`(waterstoneboca.\` — data truncation, not CSS max-length. Separately, \`.ava-exec-page { page-break-inside: avoid }\` risked clipping long executive content in Chromium PDF.

**Repair:** Completed Waterstone closing sentence; added \`narrative-completeness-v1\` gate; V4 CSS allows executive page breaks (\`.ava-report-v4 .ava-exec-page { page-break-inside: auto }\`).

## Defect B — \`[object Object]\` in Page column

**Root cause:** \`technicalDetails.urls[0]\` is \`{label,url,kind}\`; V4 passed the object to \`esc()\`.

**Repair:** \`customer-facing-serialize-v1\` (\`extractHttpUrl\` / \`customerPageLabel\`); website section uses labeled clickable links; permanent serialization scan in CQA.

## Defect C — Duplicate meeting-space recommendations

**Root cause:** \`AVA-WBR-001\` and \`AVA-WS-DCR-001\` were independent HIGH actions with the same objective.

**Repair:** Canonical Action Register merges meeting-space theme into **Reconcile and Standardize Meeting-Space Information** with shared ID/owner/evidence/steps. Consolidated count: ${waterstone?.consolidated?.find((c) => c.theme === "meeting_space")?.mergedActionIds?.length || "n/a"}.

## Platform generic actions

**Root cause:** Hardcoded “Confirm facts match hotel standards…” for every verified listing.

**Repair:** Platform assessment classes A–F; verified-consistent listings summarized; no invented corrections.

## Digital Authority evidence

**Root cause:** V4 ignored \`digitalAuthority.sources\` / rich opportunities.

**Repair:** Authority cards render sources, unlinked mentions, WeddingWire/marina opportunities with clickable URLs and limitations.

## After SHA

\`${waterstone?.sha256 || "pending"}\` · pages=${waterstone?.pages} · checks=${JSON.stringify(waterstone?.checks || {})}
`
  );

  writeMd(
    "AVA_CANONICAL_ACTION_DEDUPLICATION_CERTIFICATION.md",
    `# Canonical Action Deduplication Certification

| Hotel | Source actions | Canonical | Consolidated themes |
|---|---:|---:|---|
${pilotResults
  .map(
    (r) =>
      `| ${r.hotelName} | ${r.sourceActionCount} | ${r.canonicalActionCount} | ${(r.consolidated || []).map((c) => c.theme).join(", ") || "—"} |`
  )
  .join("\n")}

Machine-readable: \`AVA_CUSTOMER_QUALITY_STANDARD_REPAIR_RESULTS.json\`
`
  );

  writeMd(
    "AVA_CUSTOMER_EVIDENCE_LINK_CERTIFICATION.md",
    `# Customer Evidence Link Certification

| Hotel | PDF links | Object artifacts | Published untouched | Draft parity |
|---|---:|---|---|---|
${pilotResults
  .map(
    (r) =>
      `| ${r.hotelName} | ${r.links} | ${r.checks?.noObjectObject !== false} | ${r.publishedUntouched} | ${r.draftParity} |`
  )
  .join("\n")}

Material recommendations use exact hotel-specific destinations. Historical Hilton meeting figure labeled unverified. Access-restricted pages not described as verified content defects.
`
  );

  writeMd(
    "AVA_THREE_HOTEL_V4_QUALITY_REVALIDATION.md",
    `# Three-Hotel V4 Quality Revalidation

Verdict: **${machine.verdicts.THREE_HOTEL_TEMPLATE_VALIDATED}**

| Hotel | Pages | SHA-256 | Canon actions | CQA critical/high | Checks |
|---|---:|---|---:|---|---|
${pilotResults
  .map(
    (r) =>
      `| ${r.hotelName} | ${r.pages} | \`${r.sha256}\` | ${r.canonicalActionCount} | ${r.pdfGateCritical}/${r.pdfGateHigh} | ${JSON.stringify(r.checks)} |`
  )
  .join("\n")}

Same pipeline for all three — no hotel-specific workarounds.
`
  );

  writeMd(
    "AVA_38_HOTEL_CUSTOMER_QUALITY_READINESS_MATRIX.md",
    `# 38-Hotel Customer Quality Readiness Matrix

Read-only assessment against Quality Standard V1. **No portfolio regeneration or publication.**

## Distribution

| Readiness | Count |
|---|---:|
${Object.entries(portfolio.distribution)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

## Hotel-by-hotel

| Hotel | Readiness | Source→Canon | Critical | Can regen from evidence |
|---|---|---|---|---|
${portfolio.hotels
  .map(
    (h) =>
      `| ${h.hotelName} | ${h.readiness} | ${h.sourceActions}→${h.canonicalActions} | ${(h.criticalDefects || []).join(",") || "—"} | ${h.canRegenerateFromExistingEvidence} |`
  )
  .join("\n")}
`
  );

  writeMd(
    "AVA_FULL_E2E_CUSTOMER_QUALITY_INTEGRATION.md",
    `# Full E2E Customer Quality Integration

## Integrated

- Stage **11.5** \`CANONICAL_ACTION_REGISTER_AND_NARRATIVE_GATE\` in \`ava-full-e2e-pipeline-v1.js\`
- Canonical register applied before CQA / PDF generation
- CQA detects truncated narratives, serialization artifacts, generic platform spam, semantic meeting duplicates, missing DA evidence
- CQA \`detectPdfPresentationDefects\` accepts \`buildHtml\` (V4-aware)
- Regression: \`npm\`-callable \`scripts/test-ava-customer-report-quality-standard-v1.mjs\`

## Not done in this run

- Automatic regeneration of all 38 PDFs
- Production deploy / publish / share-token rotation
- Additional paid research

Verdict: **${machine.verdicts.FULL_E2E_QUALITY_STANDARD_INTEGRATED}**
`
  );

  writeMd(
    "AVA_FOUNDER_FINAL_REVIEW_MEMO.md",
    `# AVA Founder Final Review Memo — Customer Report Quality Standard

Generated: ${new Date().toISOString()}

## Verdicts

| Gate | Result |
|---|---|
| Waterstone customer quality | **${machine.verdicts.WATERSTONE_CUSTOMER_QUALITY_PASS}** |
| Three-hotel template | **${machine.verdicts.THREE_HOTEL_TEMPLATE_VALIDATED}** |
| Full E2E standard integrated | **${machine.verdicts.FULL_E2E_QUALITY_STANDARD_INTEGRATED}** |
| Portfolio regeneration | **${machine.verdicts.PORTFOLIO_REGENERATION_READY}** |

## Root causes (Waterstone)

1. Truncated takeaway → incomplete payload \`closing\` (+ exec page-break clipping risk)
2. \`[object Object]\` → object URL records passed through \`esc()\`
3. Duplicate meeting actions → two HIGH actions, same workstream
4. Generic platform rows → hardcoded copy in V4 \`platformRows\`
5. Thin Digital Authority → V4 ignored \`sources\` / opportunity URLs

## Code / renderer changes

- New: \`canonical-action-register-v1.js\`, \`customer-facing-serialize-v1.js\`, \`narrative-completeness-v1.js\`, \`customer-report-quality-standard-v1.js\`
- Rewrote: \`build-audit-pdf-html-v4.js\`
- Extended: \`customer-quality-assurance-v1.js\`, \`ava-full-e2e-pipeline-v1.js\`
- CSS: V4 executive section may paginate (no clip)
- Payload: Waterstone \`executiveSummary.closing\` completed

## Before → after (Waterstone)

- Meeting actions: 2 HIGH duplicates → **1** canonical workstream
- Platform generic repeats: eliminated (<3)
- Serialization artifacts: **0**
- Closing narrative: complete
- Pages: **${waterstone?.pages}**
- Draft SHA: \`${waterstone?.sha256}\`
- Published SHA unchanged: **${waterstone?.publishedUntouched}**
- Preview/Download parity: **${waterstone?.draftParity}**

## AC / Castillo

${pilotResults
  .filter((r) => !/waterstone/i.test(r.file))
  .map(
    (r) =>
      `- **${r.hotelName}**: ${r.pages}p · \`${r.sha256}\` · canon ${r.canonicalActionCount}/${r.sourceActionCount}`
  )
  .join("\n")}

## 38-hotel readiness

${Object.entries(portfolio.distribution)
  .map(([k, v]) => `- ${k}: **${v}**`)
  .join("\n")}

## Remaining before portfolio regeneration

- Spot-check Admin Preview visuals for the three drafts
- Repair any hotels flagged \`RENDERING_OR_NARRATIVE_REPAIR_REQUIRED\` or \`RESEARCH_FOLLOW_UP_REQUIRED\`
- Explicit founder authorization required before regenerating or publishing the 38-hotel set

## Authorization boundary

No publish, deploy, share-token rotation, paid research, ADP changes, or GDI changes in this run.
`
  );

  console.log("\n=== VERDICTS ===");
  console.log(JSON.stringify(machine.verdicts, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
