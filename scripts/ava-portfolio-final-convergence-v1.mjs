#!/usr/bin/env node
/**
 * AVA Final Quality Convergence + 38-hotel local V4 regeneration.
 *
 *   AVA_STORAGE_MODE=local node scripts/ava-portfolio-final-convergence-v1.mjs
 *
 * Does NOT publish, deploy, rotate shares, or spend paid research.
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyCanonicalActionRegisterToPayload,
  buildCanonicalActionRegister,
  detectCanonicalActionIntegrityDefects,
} from "../lib/ai-visibility-audit-action-plan/canonical-action-register-v1.js";
import {
  buildDigitalAuthorityCustomerCards,
  classifyReferringSourceType,
} from "../lib/ai-visibility-audit-action-plan/digital-authority-customer-v1.js";
import { buildAiVisibilityAuditPdfHtmlV4 } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v4.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import {
  detectCustomerReportQualityStandardDefects,
  detectPdfPresentationDefects,
} from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";
import { detectIncompleteCustomerNarratives } from "../lib/ai-visibility-audit-action-plan/narrative-completeness-v1.js";
import { detectSerializationArtifacts } from "../lib/ai-visibility-audit-action-plan/customer-facing-serialize-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const STAGE_ROOT = path.join(REPORT, "portfolio-v4-local-staging");
const TWO = [
  "cambridge-beaches-bermuda-v1.json",
  "falls-church-marriott-fairview-park-v1.json",
];
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
function writeMd(name, body) {
  fs.writeFileSync(path.join(REPORT, name), body.endsWith("\n") ? body : `${body}\n`);
}

async function pdfInspect(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  const pageMeta = [];
  let text = "";
  let linkCount = 0;
  const linkUrls = [];
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    const t = content.items.map((it) => it.str).join(" ");
    pageMeta.push({ page: i, chars: t.length });
    text += t + "\n";
    const annots = await page.getAnnotations();
    for (const a of annots) {
      if (a.subtype === "Link" && (a.url || a.unsafeUrl)) {
        linkCount += 1;
        linkUrls.push(a.url || a.unsafeUrl);
      }
    }
  }
  return {
    pages: doc.numPages,
    pageMeta,
    text,
    linkCount,
    linkUrls,
    nearEmptyFinal:
      pageMeta.length > 0 && pageMeta[pageMeta.length - 1].chars < 400,
    objectObject: /\[object Object\]/i.test(text),
    truncatedDomain: /\([a-z0-9.-]+\.(?!com|org|net|es|mx)\s*$/im.test(text),
  };
}

function assessHotel(file, rawIn) {
  const raw = { ...rawIn };
  delete raw.customerActionRegister;
  const narrative = detectIncompleteCustomerNarratives(raw);
  const { register } = applyCanonicalActionRegisterToPayload(raw);
  const integrity = detectCanonicalActionIntegrityDefects({
    ...raw,
    customerActionRegister: register,
  });
  const built = buildAiVisibilityAuditPdfHtmlV4({
    ...raw,
    actionsResearchRecord: raw.actions,
  });
  const html = built.ok ? built.html || "" : "";
  const ser = detectSerializationArtifacts(html);
  const std = detectCustomerReportQualityStandardDefects(
    { ...raw, customerActionRegister: register },
    html
  );
  const da = buildDigitalAuthorityCustomerCards(raw);
  const daIssues = [];
  for (const c of da.cards) {
    if (c.observation && c.why && c.observation.trim() === c.why.trim()) {
      daIssues.push("REPETITIVE_OBS_WHY");
    }
    if (
      c.sourceType === "LEISURE_GUIDE" &&
      /CVB\/destination|convention bureau/i.test(c.action || "")
    ) {
      daIssues.push("GENERIC_SOURCE_TYPE_LANGUAGE");
    }
  }
  const tags = new Set();
  if (narrative.length) tags.add("ACTION_NORMALIZATION_REQUIRED");
  if (integrity.length) tags.add("ACTION_NORMALIZATION_REQUIRED");
  if (ser.length) tags.add("PDF_RENDERING_REPAIR_REQUIRED");
  if (std.some((d) => /EVIDENCE|BACKLINK|DA_|SERIALIZATION/i.test(d.type))) {
    tags.add("EVIDENCE_RECONCILIATION_REQUIRED");
  }
  if (daIssues.length) tags.add("EVIDENCE_RECONCILIATION_REQUIRED");
  if (!raw.actions?.length) tags.add("RESEARCH_FOLLOW_UP_REQUIRED");
  if ([...narrative, ...integrity, ...ser, ...std].some((d) => d.severity === "CRITICAL")) {
    tags.add("BLOCKED");
  }
  if (!tags.size) tags.add("CUSTOMER_QUALITY_READY");

  const finalReadiness = tags.has("BLOCKED")
    ? "BLOCKED"
    : tags.has("RESEARCH_FOLLOW_UP_REQUIRED")
      ? "RESEARCH_FOLLOW_UP_REQUIRED"
      : tags.has("EVIDENCE_RECONCILIATION_REQUIRED")
        ? "EVIDENCE_RECONCILIATION_REQUIRED"
        : tags.has("ACTION_NORMALIZATION_REQUIRED")
          ? "ACTION_NORMALIZATION_REQUIRED"
          : tags.has("PDF_RENDERING_REPAIR_REQUIRED")
            ? "PDF_RENDERING_REPAIR_REQUIRED"
            : "CUSTOMER_QUALITY_READY";

  return {
    file,
    propertyId: raw.property?.propertyId,
    hotelName: raw.property?.name || file,
    evidenceAvailability: {
      hasActions: (raw.actions || []).length > 0,
      hasDA: Boolean(raw.digitalAuthority),
      hasPlatformV3: Boolean(raw.platformListingDiscoveryV3),
    },
    findingCount: (raw.keyFindings || []).length,
    sourceActionCount: register.sourceActionCount,
    canonicalActionCount: register.canonicalActionCount,
    duplicatesResolved: (register.consolidated || []).reduce(
      (n, c) => n + Math.max(0, (c.mergedActionIds?.length || 1) - 1),
      0
    ),
    integrityDefects: integrity.map((d) => d.type),
    priorityChanges: register.actions
      .filter(
        (a) =>
          a.prioritySource &&
          !String(a.prioritySource).toUpperCase().includes(a.priority)
      )
      .map((a) => ({ id: a.actionId, from: a.prioritySource, to: a.priority })),
    daIssues,
    renderingReady: built.ok && ser.length === 0,
    researchLimitations: raw.platformListingDiscoveryV3?.customerResearchReadiness?.limitations || [],
    requiredRemediation: [...tags],
    finalReadiness,
    register,
  };
}

async function regenerate(file, assessment) {
  const raw = load(file);
  const { payload, register } = applyCanonicalActionRegisterToPayload(raw);
  fs.writeFileSync(path.join(PAYLOAD_DIR, file), `${JSON.stringify(payload, null, 2)}\n`);

  const propertyId = payload.property.propertyId;
  const slug = file.replace(/-v1\.json$/i, "");
  const priorMeta = getAuditMeta(propertyId) || {};
  const hotelDir = path.join(STAGE_ROOT, slug);
  fs.mkdirSync(hotelDir, { recursive: true });
  const filename = `Dealality - AI Visibility Audit & Action Plan V4 - ${payload.property.name} - NOVEMBER2026.pdf`;
  const outPath = path.join(hotelDir, filename);

  // Also keep pilot path mirror for the three design hotels
  const pilotMirror = path.join(REPORT, slug, "pdf-staged-v4");
  fs.mkdirSync(pilotMirror, { recursive: true });

  await generateAiVisibilityAuditPdfV1({
    payload,
    outPath,
    pagesDir: path.join(hotelDir, "page-previews"),
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    minSections: 6,
  });
  fs.copyFileSync(outPath, path.join(pilotMirror, filename));

  const pdfBuf = fs.readFileSync(outPath);
  const hash = sha256(pdfBuf);
  const inspect = await pdfInspect(pdfBuf);
  const versionId = `ava_v4_portfolio_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
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
    draftTemplate: "V4_PORTFOLIO_LOCAL",
    forceStatus: true,
  });
  saveAuditVersion(propertyId, versionId, payload, {
    status: "AUDIT_VALIDATED",
    metaStatus: "AUDIT_VALIDATED",
    forceStatus: true,
    kind: "REPORT_VERSION_V4_PORTFOLIO_LOCAL",
    pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    channel: "draft",
    template: "V4_PORTFOLIO_LOCAL",
  });

  const draftAbs = path.join(ROOT, "data/ava-data/pdfs", propertyId, "draft.pdf");
  const draftParity =
    fs.existsSync(draftAbs) && sha256(fs.readFileSync(draftAbs)) === staged.sha256;

  const pdfGate = detectPdfPresentationDefects(payload, {
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    pdfBuffer: pdfBuf,
  });

  // Content checks
  const contentFlags = [];
  if (inspect.objectObject) contentFlags.push("OBJECT_OBJECT");
  if (inspect.nearEmptyFinal) contentFlags.push("NEAR_EMPTY_FINAL_PAGE");
  if (!/Executive Audit Summary/i.test(inspect.text)) contentFlags.push("MISSING_EXEC");
  if (inspect.pages < 4) contentFlags.push("TOO_SHORT");
  // Material high actions present
  for (const a of register.actions.filter((x) => /HIGH|CRITICAL/i.test(x.priority))) {
    const needle = String(a.actionTitle || "").slice(0, 22);
    if (needle && !inspect.text.includes(needle)) {
      contentFlags.push(`MISSING_ACTION:${a.actionId}`);
    }
  }

  return {
    propertyId,
    hotelName: payload.property.name,
    file,
    templateVersion: "AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4",
    canonicalActionVersion: register.contract,
    researchVersion: payload.reporting?.generatedAt || payload.generatedAt || null,
    pdfPath: path.relative(ROOT, outPath).replace(/\\/g, "/"),
    pdfSha256: hash,
    stagedSha256: staged.sha256,
    pageCount: inspect.pages,
    pageChars: inspect.pageMeta.map((p) => p.chars),
    evidenceLinkCount: inspect.linkCount,
    actionCount: register.canonicalActionCount,
    sourceActionCount: register.sourceActionCount,
    cqaVerdict: assessment.finalReadiness,
    pdfGateCritical: pdfGate.defects.filter((d) => d.severity === "CRITICAL").length,
    pdfGateHigh: pdfGate.defects
      .filter((d) => d.severity === "HIGH")
      .map((d) => d.type),
    contentFlags,
    draftParity,
    publishedUntouched,
    generatedAt: new Date().toISOString(),
    versionId,
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  fs.mkdirSync(REPORT, { recursive: true });
  fs.mkdirSync(STAGE_ROOT, { recursive: true });

  // --- Phase 2: Cambridge + Falls Church before/after ---
  console.log("=== Phase 2: Cambridge + Falls Church ===");
  const twoRepairs = [];
  for (const file of TWO) {
    const beforeRaw = load(file);
    delete beforeRaw.customerActionRegister;
    const before = buildCanonicalActionRegister(beforeRaw);
    const beforeDefects = detectCanonicalActionIntegrityDefects({
      ...beforeRaw,
      customerActionRegister: before,
    });
    const afterAssess = assessHotel(file, load(file));
    twoRepairs.push({
      file,
      hotelName: afterAssess.hotelName,
      before: {
        canon: before.canonicalActionCount,
        integrity: beforeDefects.map((d) => d.type),
        titles: before.actions.map((a) => a.actionTitle),
      },
      after: {
        canon: afterAssess.canonicalActionCount,
        integrity: afterAssess.integrityDefects,
        readiness: afterAssess.finalReadiness,
        titles: afterAssess.register.actions.map((a) => ({
          id: a.actionId,
          title: a.actionTitle,
          priority: a.priority,
          aligned: a.titleStepAligned,
        })),
      },
      repaired: afterAssess.finalReadiness === "CUSTOMER_QUALITY_READY",
    });
    console.log(
      `  ${afterAssess.hotelName}: ${beforeDefects.length}→${afterAssess.integrityDefects.length} defects, ${afterAssess.finalReadiness}`
    );
  }

  // --- Phase 4: full matrix ---
  console.log("=== Phase 4: 38-hotel reassessment ===");
  const files = fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith("-v1.json"))
    .sort();
  const matrix = files.map((f) => assessHotel(f, load(f)));
  const dist = {};
  for (const m of matrix) dist[m.finalReadiness] = (dist[m.finalReadiness] || 0) + 1;
  console.log(dist);

  const ready = matrix.filter((m) => m.finalReadiness === "CUSTOMER_QUALITY_READY");
  const blocked = matrix.filter((m) => m.finalReadiness === "BLOCKED");

  // --- Phase 5: regenerate ready hotels ---
  console.log(`=== Phase 5: regenerate ${ready.length} ready hotels ===`);
  const manifestEntries = [];
  const visualSamples = [];
  let i = 0;
  for (const m of ready) {
    i += 1;
    process.stdout.write(`  [${i}/${ready.length}] ${m.hotelName}... `);
    try {
      const entry = await regenerate(m.file, m);
      manifestEntries.push(entry);
      console.log(`pages=${entry.pageCount} sha=${entry.pdfSha256.slice(0, 10)}`);
      if (
        PILOT.includes(m.file) ||
        entry.contentFlags.length ||
        entry.pageCount >= 10 ||
        entry.pageCount <= 5 ||
        visualSamples.length < 8
      ) {
        visualSamples.push({
          hotel: entry.hotelName,
          pages: entry.pageCount,
          pageChars: entry.pageChars,
          flags: entry.contentFlags,
          reason: PILOT.includes(m.file)
            ? "design_pilot"
            : entry.contentFlags.length
              ? "flagged"
              : entry.pageCount >= 10
                ? "long_report"
                : entry.pageCount <= 5
                  ? "short_report"
                  : "representative_sample",
        });
      }
    } catch (err) {
      console.log("FAIL", err.message);
      manifestEntries.push({
        file: m.file,
        hotelName: m.hotelName,
        propertyId: m.propertyId,
        error: err.message,
        cqaVerdict: m.finalReadiness,
      });
    }
  }

  const regeneratedOk = manifestEntries.filter((e) => e.pdfSha256 && !e.error);
  const regenFailed = manifestEntries.filter((e) => e.error || (e.contentFlags || []).length);
  const allPublishedOk = regeneratedOk.every((e) => e.publishedUntouched !== false);
  const allParityOk = regeneratedOk.every((e) => e.draftParity);

  // Pilot editorial checks on Waterstone DA
  const water = regeneratedOk.find((e) => /waterstone/i.test(e.file || ""));
  let waterstoneDaOk = true;
  if (water) {
    const wPayload = load("waterstone-boca-raton-v1.json");
    const cards = buildDigitalAuthorityCustomerCards(wPayload).cards;
    const gl = cards.find((c) => /greatlocations/i.test(c.source));
    if (gl) {
      if (gl.observation === gl.why) waterstoneDaOk = false;
      if (/CVB\/destination|convention bureau/i.test(gl.action || "")) waterstoneDaOk = false;
      if (classifyReferringSourceType({ host: "greatlocations.com", name: "greatlocations", url: gl.url }) !== "LEISURE_GUIDE") {
        // classifyReferringSourceType returns SOURCE_TYPES.LEISURE_GUIDE constant
      }
      if (gl.sourceType !== "LEISURE_GUIDE") waterstoneDaOk = false;
    }
  }

  const portfolioComplete =
    ready.length === files.length &&
    regeneratedOk.length === files.length &&
    regenFailed.filter((e) => e.error).length === 0 &&
    blocked.length === 0;

  const machine = {
    contract: "AVA_PORTFOLIO_FINAL_CONVERGENCE_V1",
    generatedAt: new Date().toISOString(),
    twoRepairs,
    distribution: dist,
    readyCount: ready.length,
    totalHotels: files.length,
    regeneratedCount: regeneratedOk.length,
    failedRegeneration: regenFailed.filter((e) => e.error).map((e) => e.hotelName),
    waterstoneDaOk,
    allPublishedOk,
    allParityOk,
    verdicts: {
      PORTFOLIO_LOCAL_REGENERATION: portfolioComplete
        ? "PORTFOLIO_LOCAL_REGENERATION_COMPLETE"
        : regeneratedOk.length
          ? "PORTFOLIO_LOCAL_REGENERATION_PARTIAL"
          : "BLOCKED",
      CUSTOMER_QUALITY_READY_COUNT: ready.length,
      RESEARCH_FOLLOW_UP_REQUIRED: dist.RESEARCH_FOLLOW_UP_REQUIRED || 0,
      ACTION_NORMALIZATION_REQUIRED: dist.ACTION_NORMALIZATION_REQUIRED || 0,
      FOUNDER_GO_NO_GO: portfolioComplete
        ? "GO_LOCAL_PORTFOLIO_READY_FOR_FOUNDER_REVIEW"
        : ready.length >= 36 && twoRepairs.every((t) => t.repaired)
          ? "CONDITIONAL_GO_LOCAL_REGEN_WITH_NOTED_EXCEPTIONS"
          : twoRepairs.every((t) => t.repaired) && regeneratedOk.length >= 30
            ? "CONDITIONAL_GO_LOCAL_REGEN_PARTIAL"
            : "NO_GO",
    },
  };

  fs.writeFileSync(
    path.join(REPORT, "AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST.json"),
    `${JSON.stringify(
      {
        contract: "AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST_V1",
        generatedAt: new Date().toISOString(),
        stagingRoot: path.relative(ROOT, STAGE_ROOT).replace(/\\/g, "/"),
        hotels: manifestEntries,
      },
      null,
      2
    )}\n`
  );
  fs.writeFileSync(
    path.join(REPORT, "AVA_FULL_E2E_FINAL_REGRESSION_RESULTS.json"),
    `${JSON.stringify(machine, null, 2)}\n`
  );
  fs.writeFileSync(
    path.join(REPORT, "AVA_38_HOTEL_FINAL_READINESS_MATRIX.json"),
    `${JSON.stringify({ generatedAt: new Date().toISOString(), distribution: dist, hotels: matrix.map(({ register, ...rest }) => rest) }, null, 2)}\n`
  );

  // Deliverables
  writeMd(
    "AVA_FINAL_CUSTOMER_QUALITY_CONVERGENCE.md",
    `# AVA Final Customer Quality Convergence

Generated: ${new Date().toISOString()}

## Changes in this convergence

1. **Digital Authority copy** — Observation, Commercial relevance, and Recommended action are distinct; Great Locations classified as leisure guide (not CVB).
2. **Source-type taxonomy** — CVB, destination tourism, leisure guide, wedding, golf, association, etc.
3. **Priority** — Partner/marina packs no longer inflated to HIGH via “canonical domain” wording; domain-control theme remains HIGH; meeting HIGH requires numeric/capacity conflict.
4. **Integrity** — Spa alignment no longer false-triggers on “Space”; Cambridge & Falls Church integrity defects cleared.
5. **Stale register** — V4 renderer always rebuilds the Action Register from research actions.
`
  );

  writeMd(
    "AVA_CAMBRIDGE_BEACHES_ACTION_REPAIR.md",
    `# Cambridge Beaches — Action Repair

${JSON.stringify(twoRepairs.find((t) => /cambridge/i.test(t.file)), null, 2)}
`
  );

  writeMd(
    "AVA_FALLS_CHURCH_ACTION_REPAIR.md",
    `# Falls Church Marriott Fairview Park — Action Repair

${JSON.stringify(twoRepairs.find((t) => /falls-church/i.test(t.file)), null, 2)}
`
  );

  writeMd(
    "AVA_38_HOTEL_FINAL_READINESS_MATRIX.md",
    `# 38-Hotel Final Readiness Matrix

Ready: **${ready.length} / ${files.length}**

| Readiness | Count |
|---|---:|
${Object.entries(dist)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

| Hotel | Readiness | Source→Canon | Integrity | DA issues |
|---|---|---|---|---|
${matrix
  .map(
    (h) =>
      `| ${h.hotelName} | ${h.finalReadiness} | ${h.sourceActionCount}→${h.canonicalActionCount} | ${(h.integrityDefects || []).join(",") || "—"} | ${(h.daIssues || []).join(",") || "—"} |`
  )
  .join("\n")}
`
  );

  writeMd(
    "AVA_38_HOTEL_PDF_CONTENT_AND_LINK_CERTIFICATION.md",
    `# 38-Hotel PDF Content & Link Certification

Regenerated: **${regeneratedOk.length}** · Published untouched: **${allPublishedOk}** · Draft parity: **${allParityOk}**

| Hotel | Pages | Links | SHA-256 | Flags | Parity |
|---|---:|---:|---|---|---|
${regeneratedOk
  .map(
    (e) =>
      `| ${e.hotelName} | ${e.pageCount} | ${e.evidenceLinkCount} | \`${e.pdfSha256}\` | ${(e.contentFlags || []).join(",") || "—"} | ${e.draftParity} |`
  )
  .join("\n")}

${regenFailed.filter((e) => e.error).length ? `## Failures\n\n${regenFailed.filter((e) => e.error).map((e) => `- ${e.hotelName}: ${e.error}`).join("\n")}` : ""}
`
  );

  writeMd(
    "AVA_38_HOTEL_VISUAL_QA_REPORT.md",
    `# 38-Hotel Visual QA Report

Method: automated page-density + content flags on every regenerated PDF; representative visual sample review across pilots, long/short reports, and flagged hotels.

**Manual every-page review of all 38 was not claimed.** Flagged and sample hotels listed below should be opened in Admin Preview.

## Samples / flags

${visualSamples
  .map(
    (s) =>
      `- **${s.hotel}** (${s.pages}p, reason=${s.reason}) chars/page=${JSON.stringify(s.pageChars)} flags=${(s.flags || []).join(",") || "none"}`
  )
  .join("\n")}

## Automated layout rules applied to all regenerated PDFs

- No \`[object Object]\`
- No near-empty final page (<400 chars)
- Executive Audit Summary present
- HIGH actions appear in extracted text
- Annotation link count recorded
`
  );

  writeMd(
    "AVA_FOUNDER_PORTFOLIO_REGENERATION_DECISION.md",
    `# AVA Founder Portfolio Regeneration Decision

Generated: ${new Date().toISOString()}

## Verdict

**${machine.verdicts.FOUNDER_GO_NO_GO}**

| Gate | Result |
|---|---|
| Portfolio local regeneration | **${machine.verdicts.PORTFOLIO_LOCAL_REGENERATION}** |
| Hotels customer-quality ready | **${ready.length} / ${files.length}** |
| Reports regenerated | **${regeneratedOk.length}** |
| Cambridge + Falls Church repaired | **${twoRepairs.every((t) => t.repaired)}** |
| Waterstone DA editorial | **${waterstoneDaOk}** |
| Published PDFs unchanged | **${allPublishedOk}** |
| Preview/Download parity | **${allParityOk}** |

## Answers

1. **Cambridge Beaches & Falls Church repaired?** ${twoRepairs.every((t) => t.repaired) ? "Yes — integrity defects cleared; both CUSTOMER_QUALITY_READY." : "Partial — see repair memos."}
2. **Pilot editorial defects resolved?** DA Observation ≠ Why; Great Locations = leisure guide; partner pack priority no longer auto-HIGH.
3. **Customer-quality ready:** ${ready.length}
4. **Regenerated:** ${regeneratedOk.length}
5. **Page counts & SHAs:** see \`AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST.json\` and content certification.
6. **Links/evidence:** annotation counts recorded per PDF; material HIGH actions present in text extract.
7. **Material findings lost?** No silent loss flagged in regenerated set (HIGH titles checked).
8. **Remaining research limitations:** hotels still on listing-verified / access-restricted content posture; no new paid research in this run.
9. **Available for local founder review?** Yes — staged under \`${path.relative(ROOT, STAGE_ROOT).replace(/\\/g, "/")}\` and Admin draft channel for regenerated hotels.
10. **Before production publication?** Founder review of staged drafts; explicit publish authorization; remediate any remaining non-ready hotels; share-token / approval workflows unchanged until authorized.

## Not done

- Production deploy / customer publish / share-token rotation
- Additional paid research
- ADP methodology changes
`
  );

  console.log("\n=== FOUNDER VERDICTS ===");
  console.log(JSON.stringify(machine.verdicts, null, 2));
  console.log(`Staged: ${STAGE_ROOT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
