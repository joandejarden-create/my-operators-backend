/**
 * Brand Explorer Visual Copy Cleanup Writer v24B.
 *
 * Gated writer for v24A founder-reviewed safe-copy slots only.
 * Updates existing Brand Setup - Brand Explorer Presentation rows — never images,
 * Sort Order, Brand Basics, Company Validated, or non-target slot families.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const WRITER_VERSION = "24B";
export const REPORT_JSON_NAME = "brand-explorer-visual-copy-cleanup-writer.json";
export const REPORT_MD_NAME = "brand-explorer-visual-copy-cleanup-writer.md";
export const DOC_MD_NAME = "brand-explorer-visual-copy-cleanup-writer-v24B.md";
export const V24A_REPORT_PATH = "reports/brand-explorer-screenshot-seeded-remediation-review-package.json";
export const REQUIRED_APPLY_FLAG = "--approve-brand-explorer-v24B-copy-cleanup";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_BRAND_NAME = "Tribute Portfolio";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const COPY_LABEL =
  "AI-drafted / pending founder review · Not company-validated · Not Marriott-validated";

/** Exactly 10 existing-row v24B slots (featured_application handled separately). */
export const TARGET_SLOT_KEYS = [
  "overview.why_value",
  "valueOwners.watchouts",
  "overview.differentiators.identity",
  "overview.differentiators.commercial",
  "overview.bestAt.1",
  "overview.bestAt.2",
  "overview.bestAt.3",
  "valueOwners.lifecycle.1",
  "valueOwners.lifecycle.2",
  "valueOwners.lifecycle.6",
];

/** Founder-reviewed copy — authoritative v24B source when v24A report drifts. */
export const V24B_FOUNDER_COPY_BY_SLOT = {
  "overview.why_value":
    "Operator fit matters: strongest with teams that can deliver design-forward full-service or resort operations, Marriott systems cutover, and ongoing collection QA.",
  "valueOwners.watchouts":
    "Collection affiliation is not a one-time reflag; owners should plan for ongoing QA, systems participation, and brand-standard upkeep through the hold period.",
  "overview.differentiators.identity":
    "Soft-brand structure: independent hotel character with Marriott affiliation, systems, and quality expectations.",
  "overview.differentiators.commercial":
    "Conversion and repositioning path: confirm development milestones, PIP expectations, approval steps, and commercial terms directly with Marriott for the specific asset.",
  "overview.bestAt.1":
    "Strongest for independent or boutique full-service assets that already have local identity, design character, or a clear story worth preserving.",
  "overview.bestAt.2":
    "A fit for resort and leisure-led destinations where experience, design, F&B, and sense of place can support a higher-touch operating model.",
  "overview.bestAt.3":
    "A fit for urban character hotels where neighborhood story, design point of view, and independent programming can help the asset stand apart in a competitive comp set.",
  "valueOwners.lifecycle.1":
    "Evaluate collection fit, market tier, conversion scope, and whether the asset's design, F&B, and service model can support Tribute Portfolio positioning.",
  "valueOwners.lifecycle.2":
    "Align the design narrative, PIP scope, and identity-preservation strategy before committing capital or affiliation timing.",
  "valueOwners.lifecycle.6":
    "Plan hold-period QA, brand-standard upkeep, re-licensing considerations, and change-of-control assumptions with Marriott development contacts.",
};

const BULLET_LIST_SLOTS = new Set([
  "overview.why_value",
  "valueOwners.watchouts",
  "overview.differentiators.identity",
  "overview.differentiators.commercial",
]);

const CARD_BODY_SLOTS = new Set([
  "overview.bestAt.1",
  "overview.bestAt.2",
  "overview.bestAt.3",
  "valueOwners.lifecycle.1",
  "valueOwners.lifecycle.2",
  "valueOwners.lifecycle.6",
]);

const APPLY_BLOCKLIST_PATTERNS = [
  /^loyalty\./i,
  /^footprint\./i,
  /^standards\./i,
  /^overview\.scenario\./i,
  /^materials\.gallery\./i,
  /^overview\.portfolio_context$/i,
  /^valueOwners\.scenario\./i,
  /^economics\./i,
];

const FORBIDDEN_COPY_PATTERNS = [
  /company-validated/i,
  /marriott-validated/i,
  /marriott validated/i,
  /validated by marriott/i,
  /item\s*19/i,
  /profile caveats/i,
  /(Curio Collection by Hilton|Kimpton Hotels|Radisson Blu by Choice|Ascend Hotel Collection):/i,
];

const UNSUPPORTED_CLAIM_PATTERNS = [
  /\d+%/,
  /\$\d/,
  /\d+\+?\s*(hotels|properties|openings|members)/i,
];

const PRESENTATION_WRITE_FIELDS = {
  body: "Body",
  title: "Title",
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeBodyText(v) {
  return nz(v)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function short(v, max = 200) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  return nz(raw) || DEFAULT_BRAND_ID;
}

function readJsonFromRepo(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function splitBullets(val) {
  if (!nz(val)) return [];
  return normalizeBodyText(val)
    .split(/\n+/)
    .map((s) => s.replace(/^\s*[•*\-]\s*/, "").trim())
    .filter(Boolean);
}

function joinBullets(bullets) {
  return bullets.map((b) => nz(b)).filter(Boolean).join("\n");
}

function normalizeBulletKey(text) {
  return normalizeBodyText(nz(text))
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function bulletAlreadyPresent(bullets, newBullet) {
  const key = normalizeBulletKey(newBullet);
  return bullets.some((b) => normalizeBulletKey(b) === key);
}

function hadEmptyBulletLines(val) {
  return String(val)
    .split(/\n/)
    .some((line) => !nz(line));
}

/**
 * Append founder bullet or skip if duplicate. Removes trailing empties via split/join.
 */
export function patchBulletListBody(currentBody, founderBullet) {
  const normalizedCurrent = normalizeBodyText(currentBody);
  const preserved = splitBullets(normalizedCurrent);
  const emptyBulletsRemoved = hadEmptyBulletLines(currentBody) && preserved.length > 0;

  if (bulletAlreadyPresent(preserved, founderBullet)) {
    return {
      bodyAfter: joinBullets(preserved),
      changed: normalizeBodyText(joinBullets(preserved)) !== normalizedCurrent,
      action: "duplicate_skipped",
      preservedBulletCount: preserved.length,
      emptyBulletsRemoved,
      nonTargetBulletsPreserved: true,
    };
  }

  const bodyAfter = joinBullets([...preserved, founderBullet]);
  return {
    bodyAfter,
    changed: normalizeBodyText(bodyAfter) !== normalizedCurrent,
    action: preserved.length === 0 ? "replaced_empty_body" : "appended_bullet",
    preservedBulletCount: preserved.length,
    emptyBulletsRemoved: true,
    nonTargetBulletsPreserved: true,
  };
}

function isBlockedNonTargetSlot(slotKey) {
  return APPLY_BLOCKLIST_PATTERNS.some((rx) => rx.test(nz(slotKey)));
}

function detectWordingRisks(slotKey, title, body) {
  const combined = `${title}\n${body}`;
  const risks = [];
  for (const rx of FORBIDDEN_COPY_PATTERNS) {
    if (rx.test(combined)) risks.push(`forbidden: ${rx}`);
  }
  for (const rx of UNSUPPORTED_CLAIM_PATTERNS) {
    if (rx.test(combined)) risks.push(`unsupported claim: ${rx}`);
  }
  if (/marriott-approved|marriott approved/i.test(combined)) {
    risks.push("marriott validation language");
  }
  return risks;
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"] || f.slot_key),
        title: nz(f.Title),
        body: nz(f.Body),
        sortOrder: f["Sort Order"],
        imageAttachmentCount: Array.isArray(f.Image) ? f.Image.length : 0,
        companyValidated: f["Company Validated"],
        companyValidationDate: f["Company Validation Date"],
      };
    })
    .filter((r) => r.slotKey);
}

function groupRowsBySlot(rows) {
  const grouped = new Map();
  for (const row of rows) {
    if (!grouped.has(row.slotKey)) grouped.set(row.slotKey, []);
    grouped.get(row.slotKey).push(row);
  }
  return grouped;
}

function buildV24ADriftAdvisory(report) {
  const safeSlots = report.copyCleanupSafe || [];
  const safeSet = new Set(safeSlots);
  const targetSet = new Set(TARGET_SLOT_KEYS);
  const v24aCopyBySlot = new Map();
  for (const row of report.exactProposedCopyForSafeFixes || []) {
    if (row.slotKey && row.proposedBody) v24aCopyBySlot.set(row.slotKey, row);
  }

  const copyCleanupSafeMissingFromV24A = TARGET_SLOT_KEYS.filter((k) => !safeSet.has(k));
  const copyCleanupSafeExtraInV24A = safeSlots.filter((k) => !targetSet.has(k));
  const exactProposedCopyMissing = TARGET_SLOT_KEYS.filter((k) => !v24aCopyBySlot.has(k));
  const usingEmbeddedCopyFallback = exactProposedCopyMissing.slice();

  return {
    copyCleanupSafeMissingFromV24A,
    copyCleanupSafeExtraInV24A,
    exactProposedCopyMissing,
    usingEmbeddedCopyFallback,
    featuredApplicationExcludedFromV24B: safeSlots.includes("overview.featured_application"),
    blocking: false,
    advisoryOnly: true,
    detected:
      copyCleanupSafeMissingFromV24A.length > 0 ||
      copyCleanupSafeExtraInV24A.length > 0 ||
      exactProposedCopyMissing.length > 0,
  };
}

function loadV24ACopyMap() {
  const report = readJsonFromRepo(V24A_REPORT_PATH);
  if (!report) {
    throw new Error(`Missing v24A package: ${V24A_REPORT_PATH}. Run screenshot-seeded remediation package first.`);
  }

  const v24aDriftAdvisory = buildV24ADriftAdvisory(report);
  const v24aCopyBySlot = new Map();
  for (const row of report.exactProposedCopyForSafeFixes || []) {
    if (row.slotKey && row.proposedBody) v24aCopyBySlot.set(row.slotKey, row);
  }

  const copyBySlot = new Map();
  for (const key of TARGET_SLOT_KEYS) {
    if (v24aCopyBySlot.has(key)) {
      copyBySlot.set(key, v24aCopyBySlot.get(key));
      continue;
    }
    const embeddedBody = V24B_FOUNDER_COPY_BY_SLOT[key];
    if (!embeddedBody) {
      throw new Error(`No v24B copy source for target slot: ${key}`);
    }
    copyBySlot.set(key, {
      slotKey: key,
      proposedTitle: "",
      proposedBody: embeddedBody,
      copyLabel: COPY_LABEL,
      sourceBasis: "v24B embedded founder copy (v24A drift fallback)",
    });
  }

  return { report, copyBySlot, v24aDriftAdvisory };
}

function buildProposedPatch(slotKey, liveRow, v24aCopy) {
  const founderBody = normalizeBodyText(v24aCopy.proposedBody);
  const founderTitle = nz(v24aCopy.proposedTitle);
  const currentBody = normalizeBodyText(liveRow?.body || "");
  const currentTitle = nz(liveRow?.title || "");

  let bodyAfter = currentBody;
  let titleAfter = currentTitle;
  let patchAction = "noop";
  let emptyBulletsRemoved = false;
  let nonTargetBulletsPreserved = true;
  let preservedBulletCount = null;

  if (BULLET_LIST_SLOTS.has(slotKey)) {
    const patched = patchBulletListBody(currentBody, founderBody);
    bodyAfter = patched.bodyAfter;
    patchAction = patched.action;
    emptyBulletsRemoved = patched.emptyBulletsRemoved;
    nonTargetBulletsPreserved = patched.nonTargetBulletsPreserved;
    preservedBulletCount = patched.preservedBulletCount;
  } else if (CARD_BODY_SLOTS.has(slotKey)) {
    bodyAfter = founderBody;
    patchAction = currentBody === founderBody ? "noop" : "body_replaced";
    if (!currentTitle && founderTitle) {
      titleAfter = founderTitle;
    }
  } else {
    throw new Error(`Unknown slot patch mode: ${slotKey}`);
  }

  const bodyChanged = normalizeBodyText(bodyAfter) !== currentBody;
  const titleChanged = titleAfter !== currentTitle && !currentTitle && founderTitle;

  return {
    slotKey,
    recordId: liveRow?.recordId || null,
    patchMode: BULLET_LIST_SLOTS.has(slotKey) ? "bullet_list" : "card_body",
    currentTitle,
    currentBody,
    proposedTitle: titleAfter,
    proposedBody: bodyAfter,
    founderReviewedBody: founderBody,
    copyLabel: v24aCopy.copyLabel || COPY_LABEL,
    sourceBasis: v24aCopy.sourceBasis || "v24A screenshot-seeded remediation package",
    patchAction,
    bodyChanged,
    titleChanged,
    wouldChange: bodyChanged || titleChanged,
    emptyBulletsRemoved,
    nonTargetBulletsPreserved,
    preservedBulletCount,
    writableFields: [
      ...(bodyChanged ? [PRESENTATION_WRITE_FIELDS.body] : []),
      ...(titleChanged ? [PRESENTATION_WRITE_FIELDS.title] : []),
    ],
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched: true,
  };
}

export async function buildBrandExplorerVisualCopyCleanupWriterReport(options = {}) {
  const brandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const apply = Boolean(options.apply);
  const applyApproved = Boolean(options.applyApproved);
  const applyMode = apply && applyApproved;

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const { report: v24aReport, copyBySlot, v24aDriftAdvisory } = loadV24ACopyMap();
  const brandRecordId = v24aReport.brand?.recordId || brandIdOrName || DEFAULT_BRAND_ID;
  const brandName = v24aReport.brand?.name || DEFAULT_BRAND_NAME;

  const applyBlockers = [];
  if (apply && !applyApproved) {
    applyBlockers.push(`--apply requires ${REQUIRED_APPLY_FLAG}`);
  }

  const leakedNonTarget = TARGET_SLOT_KEYS.filter((k) => isBlockedNonTargetSlot(k));
  if (leakedNonTarget.length) {
    applyBlockers.push(`Target list contains blocked patterns: ${leakedNonTarget.join(", ")}`);
  }

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);
  const rowsBySlot = groupRowsBySlot(presentationRows);

  const missingTargetRows = [];
  const preflightRows = [];
  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const rowsMatched = [];
  const beforeAfterBySlot = [];
  const wordingRisksBySlot = [];

  for (const slotKey of TARGET_SLOT_KEYS) {
    const liveRows = rowsBySlot.get(slotKey) || [];
    if (liveRows.length > 1) {
      applyBlockers.push(`${slotKey} has ${liveRows.length} rows — v24B updates first row only; review manually`);
    }
    const liveRow = liveRows[0] || null;
    if (!liveRow?.recordId) {
      missingTargetRows.push(slotKey);
      applyBlockers.push(`Missing presentation row for ${slotKey} — v24B does not create rows`);
      preflightRows.push({
        slotKey,
        recordId: null,
        action: "blocked_missing_row",
        wouldChange: false,
      });
      continue;
    }

    const v24aCopy = copyBySlot.get(slotKey);
    const patch = buildProposedPatch(slotKey, liveRow, v24aCopy);
    const risks = detectWordingRisks(slotKey, patch.proposedTitle, patch.proposedBody);
    if (risks.length) {
      wordingRisksBySlot.push({ slotKey, risks });
      applyBlockers.push(`Wording risk on ${slotKey}: ${risks.join("; ")}`);
    }

    const action = patch.wouldChange ? "would_update" : "matched";
    preflightRows.push({
      ...patch,
      action,
      imageAttachmentsOnRow: liveRow.imageAttachmentCount,
    });

    beforeAfterBySlot.push({
      slotKey,
      recordId: liveRow.recordId,
      before: { title: patch.currentTitle, body: patch.currentBody },
      after: { title: patch.proposedTitle, body: patch.proposedBody },
      patchAction: patch.patchAction,
      emptyBulletsRemoved: patch.emptyBulletsRemoved,
      nonTargetBulletsPreserved: patch.nonTargetBulletsPreserved,
      preservedBulletCount: patch.preservedBulletCount,
    });

    if (patch.wouldChange) {
      rowsWouldUpdate.push({
        slotKey,
        recordId: liveRow.recordId,
        fields: {
          ...(patch.bodyChanged ? { [PRESENTATION_WRITE_FIELDS.body]: patch.proposedBody } : {}),
          ...(patch.titleChanged ? { [PRESENTATION_WRITE_FIELDS.title]: patch.proposedTitle } : {}),
        },
        writableFields: patch.writableFields,
      });
    } else {
      rowsMatched.push({ slotKey, recordId: liveRow.recordId });
    }
  }

  const nonTargetSlotsInPlan = preflightRows
    .map((r) => r.slotKey)
    .filter((k) => k && !TARGET_SLOT_KEYS.includes(k));
  if (nonTargetSlotsInPlan.length) {
    applyBlockers.push(`Non-target slots leaked into plan: ${nonTargetSlotsInPlan.join(", ")}`);
  }

  const sourceEvidenceSlots = [
    "valueOwners.scenario.1",
    "valueOwners.scenario.2",
    "valueOwners.scenario.3",
    "valueOwners.scenario.4",
    "loyalty.*",
    "footprint.region.*",
    "standards.requirement",
  ];
  const mediaSlots = ["overview.scenario.3", "materials.gallery.3"];
  const touchedNonTarget = presentationRows
    .filter((r) => !TARGET_SLOT_KEYS.includes(r.slotKey))
    .filter((r) => rowsWouldUpdate.some((u) => u.recordId === r.recordId));
  if (touchedNonTarget.length) {
    applyBlockers.push(`Non-target rows would be touched: ${touchedNonTarget.map((r) => r.slotKey).join(", ")}`);
  }

  let applyResult = { updated: [], errors: [], skipped: true };
  let airtableModified = false;

  if (applyMode) {
    if (missingTargetRows.length) {
      throw new Error(`Apply blocked: missing target rows: ${missingTargetRows.join(", ")}`);
    }
    if (applyBlockers.length) {
      throw new Error(`Apply blocked: ${[...new Set(applyBlockers)].join("; ")}`);
    }
    applyResult = { updated: [], errors: [], skipped: false };
    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.recordId
      );
      if (!res.ok) {
        applyResult.errors.push({
          slotKey: row.slotKey,
          recordId: row.recordId,
          message: json.error?.message || res.statusText,
        });
      } else {
        applyResult.updated.push({ slotKey: row.slotKey, recordId: row.recordId });
        airtableModified = true;
      }
    }
  }

  const preflightPassed = missingTargetRows.length === 0 && wordingRisksBySlot.length === 0;

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: applyMode ? "apply" : "dry-run",
    airtableModified,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: { recordId: brandRecordId, name: brandName },
    v24AReportPath: V24A_REPORT_PATH,
    v24BWriterExists: true,
    targetSlotsInspected: TARGET_SLOT_KEYS.length,
    targetSlotKeys: TARGET_SLOT_KEYS.slice(),
    copyCleanupSafeSlots: v24aReport.copyCleanupSafe || [],
    onlyV24BCopyCandidates: (v24aReport.copyCleanupSafe || []).every((k) => TARGET_SLOT_KEYS.includes(k)),
    v24aDriftAdvisory,
    v24aDriftDetected: v24aDriftAdvisory.detected,
    v24BIdempotent: rowsWouldUpdate.length === 0 && missingTargetRows.length === 0,
    preflightPassed,
    missingTargetRows,
    rowsWouldCreate,
    rowsWouldUpdate,
    rowsMatched,
    wouldUpdateCount: rowsWouldUpdate.length,
    wouldCreateCount: 0,
    matchedCount: rowsMatched.length,
    beforeAfterBySlot,
    preflightRows,
    sourceEvidenceSlotsUntouched: sourceEvidenceSlots,
    mediaSlotsUntouched: mediaSlots,
    sortOrderSlotsUntouched: ["(multi-row slots)"],
    nonTargetSlotsLeakedIntoPlan: nonTargetSlotsInPlan,
    wordingRisksBySlot,
    wordingRisksRemain: wordingRisksBySlot.length > 0,
    applyBlockers: [...new Set(applyBlockers)],
    applyGatesRequired: ["--apply", REQUIRED_APPLY_FLAG],
    applyResult,
    v24BSafeToBuild: true,
    v24BSafeToApplyAfterReview:
      preflightPassed &&
      rowsWouldUpdate.length > 0 &&
      applyBlockers.length === 0 &&
      !v24aDriftAdvisory.blocking,
    exactApplyCommand:
      "npm run brand-explorer-visual-copy-cleanup-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v24B-copy-cleanup",
    exactDryRunCommand:
      "npm run brand-explorer-visual-copy-cleanup-writer -- --brand tribute-portfolio --dry-run",
    filesRead: [
      "AGENTS.md",
      V24A_REPORT_PATH,
      "reports/brand-explorer-visual-display-defect-audit.md",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "live Tribute presentation rows",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-visual-copy-cleanup-writer.js",
      "scripts/brand-explorer-visual-copy-cleanup-writer.mjs",
      "docs/data-intelligence/brand-explorer-visual-copy-cleanup-writer-v24B.md",
      "reports/brand-explorer-visual-copy-cleanup-writer.md",
      "reports/brand-explorer-visual-copy-cleanup-writer.json",
      "package.json",
    ],
  };
}

export function buildBrandExplorerVisualCopyCleanupWriterMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Visual Copy Cleanup Writer v24B");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Scope");
  lines.push(`- Target slots: **${report.targetSlotsInspected}**`);
  lines.push(`- Would update: **${report.wouldUpdateCount}**`);
  lines.push(`- Would create: **${report.wouldCreateCount}**`);
  lines.push(`- Matched (no-op): **${report.matchedCount}**`);
  lines.push(`- Missing rows: **${report.missingTargetRows.length}**`);
  lines.push(`- Only v24B copy candidates: **${report.onlyV24BCopyCandidates ? "yes" : "no"}**`);
  lines.push(`- v24A drift detected: **${report.v24aDriftDetected ? "yes (advisory)" : "no"}**`);
  lines.push(`- Idempotent (0 pending updates): **${report.v24BIdempotent ? "yes" : "no"}**`);
  lines.push(`- Preflight passed: **${report.preflightPassed ? "yes" : "no"}**`);
  if (report.v24aDriftDetected && report.v24aDriftAdvisory) {
    lines.push("");
    lines.push("## v24A drift advisory (non-blocking)");
    const drift = report.v24aDriftAdvisory;
    if (drift.copyCleanupSafeMissingFromV24A?.length) {
      lines.push(`- copyCleanupSafe missing v24B targets: ${drift.copyCleanupSafeMissingFromV24A.join(", ")}`);
    }
    if (drift.copyCleanupSafeExtraInV24A?.length) {
      lines.push(`- copyCleanupSafe extra vs v24B: ${drift.copyCleanupSafeExtraInV24A.join(", ")}`);
    }
    if (drift.exactProposedCopyMissing?.length) {
      lines.push(`- exactProposedCopy missing (embedded fallback): ${drift.exactProposedCopyMissing.join(", ")}`);
    }
    if (drift.featuredApplicationExcludedFromV24B) {
      lines.push("- overview.featured_application remains excluded from v24B apply batch");
    }
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Sort Order untouched: **${report.sortOrderUntouched ? "yes" : "no"}**`);
  lines.push(`- Brand Basics untouched: **${report.brandBasicsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Before / after by slot");
  for (const row of report.beforeAfterBySlot) {
    lines.push(`### \`${row.slotKey}\``);
    lines.push(`- Record: \`${row.recordId}\``);
    lines.push(`- Patch: ${row.patchAction}`);
    if (row.preservedBulletCount != null) {
      lines.push(`- Preserved bullets: ${row.preservedBulletCount}`);
    }
    lines.push("- **Before**");
    lines.push(`  - Title: ${row.before.title || "—"}`);
    lines.push(`  - Body: ${row.before.body || "—"}`);
    lines.push("- **After**");
    lines.push(`  - Title: ${row.after.title || "—"}`);
    lines.push(`  - Body: ${row.after.body || "—"}`);
    lines.push("");
  }
  if (report.applyBlockers.length) {
    lines.push("## Apply blockers");
    report.applyBlockers.forEach((b) => lines.push(`- ${b}`));
    lines.push("");
  }
  lines.push("## Apply command (gated — do not run without founder approval)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  return lines.join("\n");
}
