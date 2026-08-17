/**
 * Brand Explorer Remaining Editorial Slot Completion Writer v21B.
 *
 * Gated writer for v21A-approved editorial presentation slots only.
 * Creates/updates Brand Setup - Brand Explorer Presentation rows — never images,
 * Brand Basics, sourceLinks, Company Validated, or excluded slot families.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "21B";
export const REPORT_JSON_NAME = "brand-explorer-remaining-editorial-slot-completion-writer.json";
export const REPORT_MD_NAME = "brand-explorer-remaining-editorial-slot-completion-writer.md";
export const DOC_MD_NAME = "brand-explorer-remaining-editorial-slot-completion-writer-v21B.md";
export const V21A_REPORT_PATH = "reports/brand-explorer-remaining-editorial-slot-review-package.json";
export const REQUIRED_APPLY_FLAG = "--approve-brand-explorer-editorial-slot-completion-v21B";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_BRAND_NAME = "Tribute Portfolio";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PRESENTATION_WRITE_FIELDS = {
  slotKey: "Slot Key",
  title: "Title",
  body: "Body",
  brand: "Brand",
  brandName: "Brand Name",
  active: "Active",
  sortOrder: "Sort Order",
};

const MULTI_ROW_SLOTS = new Set(["insight.similar"]);

const APPLY_BLOCKLIST_KEYS = new Set([
  "footprint.momentum",
  "footprint.openings",
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
  "materials.caseStudy",
  "overview.proof_operator",
  "standards.last_reviewed",
  "standards.requirement",
]);

const APPLY_BLOCKLIST_PATTERNS = [
  /^economics\./i,
  /^loyalty\./i,
  /^overview\.proof\./i,
  /^materials\.gallery\./i,
  /openings/i,
];

const FORBIDDEN_COPY_PATTERNS = [
  /profile caveats/i,
  /company-validated/i,
  /marriott-validated/i,
  /Illustrative mechanics only/i,
  /(Radisson Blu by Choice|Kimpton Hotels|Curio Collection by Hilton|Ascend Hotel Collection):/i,
];

const UNSUPPORTED_CLAIM_PATTERNS = [
  /\d+%/,
  /\$\d/,
  /\d+\+?\s*(hotels|properties|openings|members)/i,
  /recent(ly)?\s+(opened|opening|growth|momentum|pipeline)/i,
];

const TAB_ORDER = ["Overview", "Owner Considerations", "Dealality Insight"];

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

function normalizeTitle(v) {
  return nz(v);
}

function normalizeActive(v) {
  if (v === true || v === "true" || v === 1 || v === "1" || v === "Yes" || v === "yes") return true;
  if (v === false || v === "false" || v === 0 || v === "0" || v === "No" || v === "no") return false;
  return Boolean(v);
}

function normalizeSortOrder(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeBrandIds(brandField) {
  if (!brandField) return [];
  const arr = Array.isArray(brandField) ? brandField : [brandField];
  return arr.map((x) => String(x).trim()).filter(Boolean).sort();
}

function arraysEqual(a, b) {
  if (a.length !== b.length) return false;
  return a.every((val, idx) => val === b[idx]);
}

function short(v, max = 280) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
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

function isApplyBlockedSlot(slotKey) {
  const key = nz(slotKey);
  if (APPLY_BLOCKLIST_KEYS.has(key)) return true;
  return APPLY_BLOCKLIST_PATTERNS.some((rx) => rx.test(key));
}

function isReferenceBrandPaste(text) {
  return /(Radisson Blu by Choice|Kimpton Hotels|Curio Collection by Hilton|Ascend Hotel Collection|by Choice):/i.test(
    nz(text)
  );
}

function detectCriticalWordingRisk(slotKey, title, body) {
  const combined = `${title}\n${body}`;
  const risks = [];
  for (const rx of FORBIDDEN_COPY_PATTERNS) {
    if (rx.test(combined)) risks.push(`forbidden pattern: ${rx}`);
  }
  for (const rx of UNSUPPORTED_CLAIM_PATTERNS) {
    if (rx.test(combined)) risks.push(`unsupported claim pattern: ${rx}`);
  }
  if (/equivalent to|same performance|matches performance/i.test(combined)) {
    risks.push("equivalency claim");
  }
  if (/\bguarantee\b/i.test(combined) && !/not a guarantee/i.test(combined)) {
    risks.push("guarantee language");
  }
  return risks;
}

function scoreFromPresentKeys(presentCount, totalRequiredKeys = 110) {
  return Math.max(0, Math.round((presentCount / totalRequiredKeys) * 80));
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
      const brandField = f[PRESENTATION_WRITE_FIELDS.brand] || f.Brand || [];
      return {
        recordId: rec.id,
        slotKey: nz(f[PRESENTATION_WRITE_FIELDS.slotKey] || f.slot_key),
        title: nz(f[PRESENTATION_WRITE_FIELDS.title]),
        body: nz(f[PRESENTATION_WRITE_FIELDS.body]),
        brandIds: normalizeBrandIds(brandField),
        brandName: nz(f[PRESENTATION_WRITE_FIELDS.brandName]),
        active: f[PRESENTATION_WRITE_FIELDS.active],
        sortOrder: f[PRESENTATION_WRITE_FIELDS.sortOrder],
        imageAttachmentCount: Array.isArray(f.Image) ? f.Image.length : 0,
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
  for (const [key, list] of grouped) {
    list.sort((a, b) => (normalizeSortOrder(a.sortOrder) ?? 0) - (normalizeSortOrder(b.sortOrder) ?? 0));
    grouped.set(key, list);
  }
  return grouped;
}

function buildProposedRowState(slotKey, proposedTitle, proposedBody, brandRecordId, brandName, sortOrder) {
  return {
    slotKey,
    title: proposedTitle,
    body: proposedBody,
    brandIds: [brandRecordId],
    brandName,
    active: true,
    sortOrder,
    titleOwnedByWriter: Boolean(proposedTitle),
  };
}

function compareLiveToProposed(live, proposed) {
  const differingFields = [];
  const integrityIssues = [];

  if (normalizeTitle(live.slotKey) !== normalizeTitle(proposed.slotKey)) {
    integrityIssues.push("Slot Key");
  }

  const liveBrandIds = live.brandIds || [];
  if (!arraysEqual(liveBrandIds, proposed.brandIds)) {
    differingFields.push("Brand");
    if (!liveBrandIds.includes(proposed.brandIds[0])) {
      integrityIssues.push("Brand");
    }
  }

  if (normalizeTitle(live.brandName) !== normalizeTitle(proposed.brandName)) {
    differingFields.push("Brand Name");
  }

  if (normalizeBodyText(live.body) !== normalizeBodyText(proposed.body)) {
    differingFields.push("Body");
  }

  if (normalizeActive(live.active) !== normalizeActive(proposed.active)) {
    differingFields.push("Active");
  }

  if (normalizeSortOrder(live.sortOrder) !== normalizeSortOrder(proposed.sortOrder)) {
    differingFields.push("Sort Order");
  }

  if (proposed.titleOwnedByWriter) {
    if (normalizeTitle(live.title) !== normalizeTitle(proposed.title)) {
      differingFields.push("Title");
    }
  } else if (normalizeTitle(live.title)) {
    integrityIssues.push("Title (live-only, not writer-owned)");
  }

  let status;
  if (integrityIssues.length > 0) {
    status = "unexpected_difference";
  } else if (differingFields.length === 0) {
    status = "matched";
  } else {
    status = "would_update";
  }

  return { status, differingFields, integrityIssues };
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function loadV21AReport() {
  const report = readJsonFromRepo(V21A_REPORT_PATH);
  if (!report) {
    throw new Error(
      `Missing v21A review package: ${V21A_REPORT_PATH}. Run brand-explorer-remaining-editorial-slot-review-package first.`
    );
  }
  return report;
}

function tabFromSlot(slotKey) {
  if (slotKey.startsWith("overview.") || slotKey.startsWith("hero.")) return "Overview";
  if (slotKey.startsWith("standards.")) return "Owner Considerations";
  if (slotKey.startsWith("insight.")) return "Dealality Insight";
  return "Unknown";
}

function expandWriteTargets(reviewRowsBySlot, targetSlotKeys, brandRecordId, brandName) {
  const targets = [];
  targetSlotKeys.forEach((slotKey, slotIndex) => {
    const review = reviewRowsBySlot.get(slotKey);
    const baseSort = slotIndex * 10;

    if (MULTI_ROW_SLOTS.has(slotKey) && Array.isArray(review?.proposedRows) && review.proposedRows.length) {
      review.proposedRows.forEach((row, rowIndex) => {
        targets.push({
          slotKey,
          rowIndex,
          multiRow: true,
          tab: review.tab || tabFromSlot(slotKey),
          proposedTitle: nz(row.title),
          proposedBody: nz(row.body),
          sortOrder: baseSort + rowIndex,
          review,
        });
      });
      return;
    }

    targets.push({
      slotKey,
      rowIndex: 0,
      multiRow: false,
      tab: review?.tab || tabFromSlot(slotKey),
      proposedTitle: nz(review?.proposedTitle),
      proposedBody: nz(review?.proposedBody),
      sortOrder: baseSort,
      review,
    });
  });
  return targets;
}

export async function buildBrandExplorerRemainingEditorialSlotCompletionWriterReport(options = {}) {
  const brandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const apply = Boolean(options.apply);
  const applyApproved = Boolean(options.applyApproved);
  const applyMode = apply && applyApproved;

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const v21a = loadV21AReport();
  if (!v21a.v21BWriterSafeToBuild) {
    throw new Error("v21A review package reports v21B writer is not safe to build — regenerate review package.");
  }
  if (v21a.wordingRisksRemain) {
    throw new Error("v21A wordingRisksRemain is true — apply blocked.");
  }
  if (v21a.proposedCopyReferenceBrandLanguagePresent) {
    throw new Error("v21A proposed copy still contains reference-brand language — apply blocked.");
  }
  if (v21a.unsupportedClaimsRemain) {
    throw new Error("v21A unsupportedClaimsRemain is true — apply blocked.");
  }

  const targetSlotKeys = (v21a.v21BApplyBatchAfterReview || []).slice();
  const reviewRowsBySlot = new Map((v21a.reviewPackageRows || []).map((r) => [r.slotKey, r]));

  const brandRecordId = v21a.brand?.recordId || brandIdOrName || DEFAULT_BRAND_ID;
  const brandName = v21a.brand?.name || DEFAULT_BRAND_NAME;

  const leakedExcludedSlots = targetSlotKeys.filter((slotKey) => isApplyBlockedSlot(slotKey));

  const applyBlockers = [];
  if (apply && !applyApproved) {
    applyBlockers.push(`--apply requires ${REQUIRED_APPLY_FLAG}`);
  }
  if (leakedExcludedSlots.length) {
    applyBlockers.push(`Excluded slot(s) in target list: ${leakedExcludedSlots.join(", ")}`);
  }
  if (!targetSlotKeys.length) {
    applyBlockers.push("v21BApplyBatchAfterReview is empty");
  }

  const tributeBrand = await fetchBrandApiShape(brandRecordId);
  if (!tributeBrand) throw new Error(`Unable to read brand: ${brandRecordId}`);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);
  const rowsBySlot = groupRowsBySlot(presentationRows);

  const writeTargets = expandWriteTargets(reviewRowsBySlot, targetSlotKeys, brandRecordId, brandName);

  const preflightRows = [];
  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];
  const rowsUnexpectedDifference = [];
  const wordingRisksBySlot = [];
  let unsupportedClaimsDetected = false;
  let referenceBrandLanguageDetected = false;

  for (const target of writeTargets) {
    const { slotKey, rowIndex, review } = target;
    if (!review) {
      applyBlockers.push(`Missing v21A review row for ${slotKey}`);
      continue;
    }
    if (!review.safeForV21BWriter) {
      applyBlockers.push(`Slot not safe for v21B: ${slotKey}`);
    }

    const proposedTitle = target.proposedTitle;
    const proposedBody = target.proposedBody;
    if (!proposedBody) {
      applyBlockers.push(`Empty proposed body for ${slotKey}${target.multiRow ? `[${rowIndex}]` : ""}`);
    }

    const risks = detectCriticalWordingRisk(slotKey, proposedTitle, proposedBody);
    if (risks.length) {
      wordingRisksBySlot.push({ slotKey, rowIndex, risks });
      applyBlockers.push(`Wording risk on ${slotKey}: ${risks.join("; ")}`);
    }
    if (isReferenceBrandPaste(proposedBody) || isReferenceBrandPaste(proposedTitle)) {
      referenceBrandLanguageDetected = true;
      applyBlockers.push(`Reference-brand paste on ${slotKey}`);
    }
    if (UNSUPPORTED_CLAIM_PATTERNS.some((rx) => rx.test(`${proposedTitle}\n${proposedBody}`))) {
      unsupportedClaimsDetected = true;
    }

    const existingRows = rowsBySlot.get(slotKey) || [];
    const existing = target.multiRow ? existingRows[rowIndex] || null : existingRows[0] || null;

    const proposedState = buildProposedRowState(
      slotKey,
      proposedTitle,
      proposedBody,
      brandRecordId,
      brandName,
      target.sortOrder
    );

    let action = "create";
    let reconciliationStatus = "would_create";
    let differingFields = [];
    let integrityIssues = [];

    if (existing?.recordId) {
      const comparison = compareLiveToProposed(existing, proposedState);
      reconciliationStatus = comparison.status;
      differingFields = comparison.differingFields;
      integrityIssues = comparison.integrityIssues;
      if (comparison.status === "matched") action = "matched";
      else if (comparison.status === "unexpected_difference") action = "unexpected_difference";
      else action = "update";
    }

    const writableFields = [
      PRESENTATION_WRITE_FIELDS.slotKey,
      PRESENTATION_WRITE_FIELDS.body,
      PRESENTATION_WRITE_FIELDS.brand,
      PRESENTATION_WRITE_FIELDS.brandName,
      PRESENTATION_WRITE_FIELDS.active,
      PRESENTATION_WRITE_FIELDS.sortOrder,
    ];
    if (proposedState.titleOwnedByWriter) writableFields.push(PRESENTATION_WRITE_FIELDS.title);

    const preflight = {
      slotKey,
      rowIndex,
      multiRow: target.multiRow,
      tab: target.tab,
      rowExists: Boolean(existing?.recordId),
      action,
      reconciliationStatus,
      differingFields,
      integrityIssues,
      recordId: existing?.recordId || null,
      currentTitle: existing?.title || "",
      proposedTitle,
      currentBody: existing?.body || "",
      proposedBody,
      writableFields,
      riskStatus: risks.length ? "blocked" : "clear",
      reviewStatus: review.reviewStatus,
      sourceBasis: review.sourceBasis,
      imageAttachmentsOnRow: existing?.imageAttachmentCount || 0,
      imagesWillRemainUntouched: true,
      sortOrder: target.sortOrder,
    };
    preflightRows.push(preflight);

    const targetRow = {
      ...preflight,
      fieldsPreview: {
        [PRESENTATION_WRITE_FIELDS.slotKey]: slotKey,
        [PRESENTATION_WRITE_FIELDS.title]: proposedTitle,
        [PRESENTATION_WRITE_FIELDS.body]: proposedBody,
        [PRESENTATION_WRITE_FIELDS.brand]: [brandRecordId],
        [PRESENTATION_WRITE_FIELDS.brandName]: brandName,
        [PRESENTATION_WRITE_FIELDS.active]: true,
        [PRESENTATION_WRITE_FIELDS.sortOrder]: target.sortOrder,
      },
    };

    if (action === "create") rowsWouldCreate.push(targetRow);
    else if (action === "update") rowsWouldUpdate.push(targetRow);
    else if (action === "matched") rowsMatched.push(targetRow);
    else if (action === "unexpected_difference") rowsUnexpectedDifference.push(targetRow);
  }

  const slotsTargetedByTab = {};
  for (const tab of TAB_ORDER) slotsTargetedByTab[tab] = [];
  for (const slotKey of targetSlotKeys) {
    const tab = tabFromSlot(slotKey);
    if (!slotsTargetedByTab[tab]) slotsTargetedByTab[tab] = [];
    slotsTargetedByTab[tab].push(slotKey);
  }

  const applyResult = { created: [], updated: [], skippedMatched: [], errors: [], blocked: false };
  if (applyMode && applyBlockers.length === 0) {
    for (const target of [...rowsWouldCreate, ...rowsWouldUpdate]) {
      if (target.action === "create") {
        const fields = { ...target.fieldsPreview };
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields, typecast: true }),
        });
        if (!res.ok) {
          applyResult.errors.push({
            slotKey: target.slotKey,
            rowIndex: target.rowIndex,
            message: json.error?.message || res.status,
          });
        } else {
          applyResult.created.push({ recordId: json.id, slotKey: target.slotKey, rowIndex: target.rowIndex });
        }
      } else if (target.recordId) {
        const patchFields = {
          [PRESENTATION_WRITE_FIELDS.body]: target.proposedBody,
          [PRESENTATION_WRITE_FIELDS.active]: true,
          [PRESENTATION_WRITE_FIELDS.sortOrder]: target.sortOrder,
        };
        if (target.proposedTitle) {
          patchFields[PRESENTATION_WRITE_FIELDS.title] = target.proposedTitle;
        }
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          {
            method: "PATCH",
            body: JSON.stringify({ fields: patchFields, typecast: true }),
          },
          target.recordId
        );
        if (!res.ok) {
          applyResult.errors.push({
            slotKey: target.slotKey,
            rowIndex: target.rowIndex,
            message: json.error?.message || res.status,
          });
        } else {
          applyResult.updated.push({
            recordId: target.recordId,
            slotKey: target.slotKey,
            rowIndex: target.rowIndex,
          });
        }
      }
    }
    for (const target of rowsMatched) {
      applyResult.skippedMatched.push({
        recordId: target.recordId,
        slotKey: target.slotKey,
        rowIndex: target.rowIndex,
      });
    }
  } else if (applyMode && applyBlockers.length > 0) {
    applyResult.blocked = true;
  }

  const currentPresent = v21a.currentSlotCoverageScore
    ? Math.round((v21a.currentSlotCoverageScore / 100) * 110)
    : 88;
  const projectedPresent = currentPresent + targetSlotKeys.length;
  const projectedScore =
    v21a.projectedSlotCoverageScoreAfterV21B ?? scoreFromPresentKeys(projectedPresent, 110);

  const mode = applyMode && !applyResult.blocked && applyResult.errors.length === 0 ? "apply" : "dry-run";
  const airtableModified =
    applyMode && !applyResult.blocked && (applyResult.created.length > 0 || applyResult.updated.length > 0);

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: applyMode && applyBlockers.length === 0 && !applyResult.blocked ? mode : "dry-run",
    airtableModified,
    imagesUntouched: true,
    brandBasicsUntouched: true,
    sourceLinksUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    filesRead: [
      "AGENTS.md",
      V21A_REPORT_PATH,
      "reports/brand-explorer-remaining-editorial-slot-review-package.md",
      "lib/partner-intelligence/brand-explorer-remaining-editorial-slot-review-package.js",
      "reports/brand-explorer-slot-completion-remaining-plan.md",
      "reports/brand-explorer-slot-completion-remaining-plan.json",
      "reports/brand-explorer-slot-completion-writer.md",
      "reports/brand-explorer-slot-completion-writer.json",
      "reports/brand-explorer-slot-standard-manifest.md",
      "reports/brand-explorer-presentation-slot-coverage-audit.md",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "fixtures/brand-explorer-presentation-curio-full.json",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-remaining-editorial-slot-completion-writer.js",
      "scripts/brand-explorer-remaining-editorial-slot-completion-writer.mjs",
      "docs/data-intelligence/brand-explorer-remaining-editorial-slot-completion-writer-v21B.md",
      "reports/brand-explorer-remaining-editorial-slot-completion-writer.md",
      "reports/brand-explorer-remaining-editorial-slot-completion-writer.json",
      "package.json",
    ],
    v21BWriterExists: true,
    brand: { recordId: brandRecordId, name: brandName },
    v21ASource: {
      v21BWriterSafeToBuild: v21a.v21BWriterSafeToBuild,
      v21BApplyBatchCount: v21a.v21BApplyBatchCount,
      wordingRisksRemain: v21a.wordingRisksRemain,
    },
    slotsTargetedCount: targetSlotKeys.length,
    slotsTargeted: targetSlotKeys,
    writeTargetsCount: writeTargets.length,
    insightSimilarRowModel: {
      model: "multiple_presentation_rows",
      rowCount: 3,
      rationale:
        "Completed brands (e.g. Curio) and Brand Explorer UI (explorerCardRowsForSlot) use multiple Brand Explorer Presentation rows sharing slot key insight.similar — Title = peer brand name, Body = qualitative diligence subtitle. v21B creates/updates three rows, not one structured Body row.",
      uiReference: "public/js/brand-explorer-atelier-from-api.js — explorerCardRowsForSlot(brand, 'insight.similar')",
      fixtureReference: "fixtures/brand-explorer-presentation-curio-full.json — three insight.similar rows with sort 0/1/2",
    },
    slotsTargetedByTab,
    slotsExplicitlyExcluded: [
      "footprint.momentum",
      ...APPLY_BLOCKLIST_KEYS,
      "economics.*",
      "loyalty.*",
      "overview.proof.*",
      "materials.gallery.*",
      "*openings*",
    ],
    leakedExcludedSlotsIntoTargetList: leakedExcludedSlots,
    preflightRows,
    rowsWouldCreate,
    rowsWouldUpdate,
    rowsMatched,
    rowsUnexpectedDifference,
    matchedRowCount: rowsMatched.length,
    trueWouldUpdateCount: rowsWouldUpdate.length,
    wouldCreateCount: rowsWouldCreate.length,
    wordingRisksBySlot,
    wordingRisksRemain: wordingRisksBySlot.length > 0,
    unsupportedClaimsRemain: unsupportedClaimsDetected || Boolean(v21a.unsupportedClaimsRemain),
    proposedCopyReferenceBrandLanguagePresent: referenceBrandLanguageDetected,
    applyBlockers: [...new Set(applyBlockers)],
    applyGatesRequired: ["--apply", REQUIRED_APPLY_FLAG],
    applyResult,
    projectedScoreAfterApply: projectedScore,
    tributeCompletedBrandComparableAfterApply: projectedScore >= 85,
    exactApplyCommand:
      "npm run brand-explorer-remaining-editorial-slot-completion-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-editorial-slot-completion-v21B",
    exactDryRunCommand:
      "npm run brand-explorer-remaining-editorial-slot-completion-writer -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerRemainingEditorialSlotCompletionWriterMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Remaining Editorial Slot Completion Writer v21B");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Scope");
  lines.push(`- Slot keys targeted: **${report.slotsTargetedCount}**`);
  lines.push(`- Write targets (rows): **${report.writeTargetsCount}**`);
  lines.push(`- Would create: **${report.wouldCreateCount}**`);
  lines.push(`- Would update: **${report.trueWouldUpdateCount}**`);
  lines.push(`- Matched (no-op): **${report.matchedRowCount}**`);
  lines.push(`- Leaked excluded slots: **${report.leakedExcludedSlotsIntoTargetList.length}**`);
  lines.push(`- Wording risks remain: **${report.wordingRisksRemain ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## insight.similar row model");
  lines.push(`- Model: **${report.insightSimilarRowModel.model}** (${report.insightSimilarRowModel.rowCount} rows)`);
  lines.push(`- Rationale: ${report.insightSimilarRowModel.rationale}`);
  lines.push("");
  lines.push("## Score projection");
  lines.push(`- Projected score after apply: **${report.projectedScoreAfterApply}/100**`);
  lines.push(
    `- Completed-brand comparable after apply: **${report.tributeCompletedBrandComparableAfterApply ? "yes" : "no"}**`
  );
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Brand Basics untouched: **${report.brandBasicsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Preflight (sample)");
  report.preflightRows.slice(0, 15).forEach((row) => {
    const rowLabel = row.multiRow ? `${row.slotKey}[${row.rowIndex}]` : row.slotKey;
    lines.push(
      `- \`${rowLabel}\` · ${row.action} · risk: ${row.riskStatus} · title: ${short(row.proposedTitle, 60) || "—"} · body: ${short(row.proposedBody, 80)}`
    );
  });
  if (report.preflightRows.length > 15) {
    lines.push(`- …${report.preflightRows.length - 15} more in JSON`);
  }
  lines.push("");
  lines.push("## Apply command (gated)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
