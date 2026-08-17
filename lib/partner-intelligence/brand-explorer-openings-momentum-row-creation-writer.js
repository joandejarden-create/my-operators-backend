/**
 * Brand Explorer Openings + Momentum Row Creation Writer v25C-3C.
 *
 * Creates missing footprint.openings and footprint.momentum presentation rows for
 * Tribute Portfolio from the v25C-3B polished copy package. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-openings-momentum-row-creation-writer-v25C-3C.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listPartnerSources } from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  OPENINGS_SLOT,
  MOMENTUM_SLOT,
  OPENINGS_MINIMUM,
  MOMENTUM_MINIMUM,
  POLISHED_MOMENTUM_ROWS,
  REPORT_JSON_NAME as REVIEW_PACKAGE_JSON,
  buildFlattenedMomentumRowTargets,
  buildFlattenedOpeningsRowTargets,
} from "./brand-explorer-openings-momentum-row-review-package.js";
import {
  REPORT_JSON_NAME as COMPLETION_JSON,
} from "./brand-explorer-openings-momentum-source-capture-completion.js";

export const WRITER_VERSION = "25C-3C";
export const REPORT_JSON_NAME = "brand-explorer-openings-momentum-row-creation-writer.json";
export const REPORT_MD_NAME = "brand-explorer-openings-momentum-row-creation-writer.md";
export const DOC_MD_NAME = "brand-explorer-openings-momentum-row-creation-writer-v25C-3C.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v25C-3C-openings-momentum-rows";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-openings-momentum-row-copy";
export const APPLY_FLAG_CREATE = "--approve-brand-explorer-v25C-3C-row-create";

export const TARGET_SLOTS = [OPENINGS_SLOT, MOMENTUM_SLOT];
export const EXPECTED_ROW_COUNTS = {
  [OPENINGS_SLOT]: 5,
  [MOMENTUM_SLOT]: 6,
};
export const TOTAL_TARGET_ROWS = 11;

const CASA_NIZUC_MARSHA = "CUNAN";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const GOVERNANCE_LABELS = [
  "AI-drafted from official-source metadata",
  "Founder-review package",
  "Not company-validated",
  "Not Marriott-validated",
];

const FORBIDDEN_BODY_PATTERNS = [
  /take a photo tour of/i,
  /view photos of our boutique rooms/i,
  /AI-drafted from official-source metadata/i,
  /Pending founder review/i,
  /Not company-validated/i,
  /Not Marriott-validated/i,
  /Founder-review package/i,
];

const PR_SOURCE_RE = /newsroom|press-release|press_release|\/news\//i;
const POSITIVE_PR_CLAIM_RE =
  /(?:marriott|tribute).{0,60}(?:press release|newsroom announcement|announced (?:its|the) (?:opening|debut))/i;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-openings-momentum-row-review-package.md",
  "reports/brand-explorer-openings-momentum-row-review-package.json",
  "reports/brand-explorer-openings-momentum-source-capture-completion.md",
  "reports/brand-explorer-openings-momentum-source-capture-completion.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-openings-momentum-row-review-package.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Tribute Brand Asset Registry records",
  "live Curio/Kimpton/Radisson/Ascend openings/momentum rows",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function normalizeTitle(v) {
  return nz(v);
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
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

function readJsonIfExists(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
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
        brandName: nz(f["Brand Name"]),
        active: f.Active,
        sortOrder: f["Sort Order"],
        imageCount: Array.isArray(f.Image) ? f.Image.length : 0,
      };
    })
    .filter((r) => r.slotKey);
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return TRIBUTE_RECORD_ID;
  }
  return nz(raw);
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function bodyHasForbiddenGovernanceCopy(body) {
  return FORBIDDEN_BODY_PATTERNS.some((re) => re.test(body));
}

function isFutureDate(isoDate) {
  if (!isoDate) return false;
  const today = new Date().toISOString().slice(0, 10);
  return nz(isoDate) > today;
}

function claimsPrWithoutSource(body, sourceUrl, sourceBasis = "") {
  if (PR_SOURCE_RE.test(sourceUrl) || PR_SOURCE_RE.test(sourceBasis)) return false;
  if (/not a.{0,30}newsroom/i.test(body)) return false;
  if (/not a.{0,30}press.release/i.test(body)) return false;
  return POSITIVE_PR_CLAIM_RE.test(body);
}

function hasHttpUrl(v) {
  return /^https?:\/\//i.test(nz(v));
}

function buildCreateFields(planned) {
  const fields = { ...planned.fields };
  if (planned.slotKey === OPENINGS_SLOT && hasHttpUrl(planned.imageUrl)) {
    fields.Image = [{ url: planned.imageUrl }];
  }
  return fields;
}

function findLiveMatch(planned, liveRows) {
  return liveRows.find(
    (live) =>
      normalizeTitle(live.title) === normalizeTitle(planned.title) &&
      Number(live.sortOrder ?? -1) === Number(planned.sort)
  );
}

function bodiesMatch(a, b) {
  return normalizeBody(a) === normalizeBody(b);
}

function validateOpeningsRow(row) {
  const missing = [];
  if (!normalizeTitle(row.title)) missing.push("title");
  if (!normalizeBody(row.body)) missing.push("body");
  if (!row.classification) missing.push("classification");
  if (!hasHttpUrl(row.sourceUrl)) missing.push("sourceUrl");
  if (!hasHttpUrl(row.imageUrl)) missing.push("imageUrl");
  if (!nz(row.location) && !row.body.includes("\n\n")) missing.push("location");
  return missing;
}

function validateMomentumRow(row) {
  const missing = [];
  if (!normalizeTitle(row.title)) missing.push("title");
  if (!normalizeBody(row.body)) missing.push("body");
  if (!row.dateLine) missing.push("date");
  if (!hasHttpUrl(row.sourceUrl)) missing.push("sourceUrl");
  if (!row.openingDate) missing.push("sourceBackedTiming");
  return missing;
}

function loadImageByMarsha(completion, reviewPackage) {
  const imageByMarsha = {};
  for (const c of completion?.propertyExampleCandidates || []) {
    if (c.marsha && c.imageUrl) imageByMarsha[c.marsha] = c.imageUrl;
  }
  for (const card of reviewPackage?.openingsProposedCards || []) {
    if (card.marsha && card.imageUrl) imageByMarsha[card.marsha] = card.imageUrl;
  }
  return imageByMarsha;
}

function enrichOpeningsTargets(openingsTargets, reviewPackage) {
  const cardByMarsha = new Map(
    (reviewPackage?.openingsProposedCards || []).map((c) => [c.marsha, c])
  );
  return openingsTargets.map((row) => {
    const card = cardByMarsha.get(row.marsha) || {};
    return {
      ...row,
      classification: row.classification || card.classification,
      location: card.location || "",
      imageUrl: row.imageUrl || card.imageUrl || "",
      sourceUrl: row.sourceUrl || card.sourceUrl || "",
      sourceBasis: card.sourceBasis || "",
    };
  });
}

function enrichMomentumTargets(momentumTargets, reviewPackage) {
  const rowByMarsha = new Map(
    (reviewPackage?.momentumProposedRows || []).map((r) => [r.marsha, r])
  );
  return momentumTargets.map((row) => {
    const pkg = rowByMarsha.get(row.marsha) || {};
    const polished = POLISHED_MOMENTUM_ROWS.find((r) => r.marsha === row.marsha) || {};
    return {
      ...row,
      openingDate: pkg.openingDate || polished.openingDate || "",
      dateLine: row.dateLine || pkg.dateLine || polished.dateLine || "",
      sourceUrl: row.sourceUrl || pkg.sourceUrl || polished.sourceUrl || "",
      sourceBasis: pkg.sourceBasis || polished.sourceBasis || "",
      propertyName: pkg.propertyName || polished.propertyName || "",
      location: pkg.location || polished.location || "",
    };
  });
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-openings-momentum-row-creation-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_CREATE}`;
}

export async function buildBrandExplorerOpeningsMomentumRowCreationWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  createApproved = false,
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-3C pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const reviewPackage = readJsonIfExists(`reports/${REVIEW_PACKAGE_JSON}`);
  if (!reviewPackage?.v25C3BReviewPackageExists) {
    throw new Error(
      "v25C-3B review package missing — run brand-explorer-openings-momentum-row-review-package first"
    );
  }

  const completion = readJsonIfExists(`reports/${COMPLETION_JSON}`);

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const imageByMarsha = loadImageByMarsha(completion, reviewPackage);
  const openingsTargets = enrichOpeningsTargets(
    buildFlattenedOpeningsRowTargets(brandRecordId, BRAND_NAME, imageByMarsha),
    reviewPackage
  );
  const momentumTargets = enrichMomentumTargets(
    buildFlattenedMomentumRowTargets(brandRecordId, BRAND_NAME),
    reviewPackage
  );
  const targetRows = [...openingsTargets, ...momentumTargets];

  if (targetRows.length !== TOTAL_TARGET_ROWS) {
    throw new Error(`Expected ${TOTAL_TARGET_ROWS} target rows, got ${targetRows.length}`);
  }

  for (const row of targetRows) {
    if (bodyHasForbiddenGovernanceCopy(row.body)) {
      throw new Error(`Forbidden governance or template copy in target row ${row.slotKey}/${row.title}`);
    }
    if (!TARGET_SLOTS.includes(row.slotKey)) {
      throw new Error(`Non-target slot leaked: ${row.slotKey}`);
    }
  }

  const casaNizucInMomentum = momentumTargets.some((r) => r.marsha === CASA_NIZUC_MARSHA);
  const futureMomentumRows = momentumTargets.filter((r) => isFutureDate(r.openingDate));
  const prClaimsWithoutSource = targetRows.filter((r) =>
    claimsPrWithoutSource(r.body, r.sourceUrl, r.sourceBasis)
  );

  const applyBlockers = [];
  if (casaNizucInMomentum) {
    applyBlockers.push("casa_nizuc_in_momentum_blocked");
  }
  if (futureMomentumRows.length) {
    applyBlockers.push(
      `future_dated_momentum_blocked:${futureMomentumRows.map((r) => r.marsha).join(",")}`
    );
  }
  if (prClaimsWithoutSource.length) {
    applyBlockers.push(
      `pr_claim_without_source:${prClaimsWithoutSource.map((r) => r.marsha || r.title).join(",")}`
    );
  }

  const rowValidation = {
    openings: openingsTargets.map((row) => ({
      marsha: row.marsha,
      title: row.title,
      missing: validateOpeningsRow(row),
      hasImage: hasHttpUrl(row.imageUrl),
      hasSourceUrl: hasHttpUrl(row.sourceUrl),
    })),
    momentum: momentumTargets.map((row) => ({
      marsha: row.marsha,
      title: row.title,
      missing: validateMomentumRow(row),
      hasSourceUrl: hasHttpUrl(row.sourceUrl),
      futureDated: isFutureDate(row.openingDate),
    })),
  };

  for (const v of rowValidation.openings) {
    if (v.missing.length) {
      applyBlockers.push(`openings_missing_fields:${v.marsha}:${v.missing.join(",")}`);
    }
  }
  for (const v of rowValidation.momentum) {
    if (v.missing.length) {
      applyBlockers.push(`momentum_missing_fields:${v.marsha}:${v.missing.join(",")}`);
    }
    if (v.futureDated) {
      applyBlockers.push(`momentum_future_dated:${v.marsha}`);
    }
  }

  let existingSources = [];
  try {
    let offset = "";
    do {
      const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
      existingSources.push(...(page.sources || []));
      offset = page.offset || "";
    } while (offset);
  } catch (err) {
    console.warn("[v25C-3C] source list warning:", err?.message || err);
  }

  const sourceLibraryGaps =
    reviewPackage.sourceLibraryGaps ||
    (reviewPackage.sourceLibraryGaps === undefined
      ? []
      : reviewPackage.sourceLibraryGaps);
  const sourceLibraryRequiredReported = sourceLibraryGaps.length > 0;

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);

  const rowsBySlot = new Map();
  for (const slot of TARGET_SLOTS) {
    rowsBySlot.set(slot, presentationRows.filter((r) => r.slotKey === slot));
  }

  const loyaltyRowsSnapshot = presentationRows
    .filter((r) => r.slotKey.startsWith("loyalty."))
    .map((r) => ({ recordId: r.recordId, slotKey: r.slotKey, title: r.title, sortOrder: r.sortOrder }));

  const geographicFootprintRowsSnapshot = presentationRows
    .filter(
      (r) =>
        r.slotKey.startsWith("footprint.") &&
        r.slotKey !== OPENINGS_SLOT &&
        r.slotKey !== MOMENTUM_SLOT
    )
    .map((r) => ({ recordId: r.recordId, slotKey: r.slotKey, title: r.title, sortOrder: r.sortOrder }));

  const duplicateRowsFound = [];
  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];
  const exactCreatePayloads = [];

  for (const slotKey of TARGET_SLOTS) {
    const live = rowsBySlot.get(slotKey) || [];
    const expected = EXPECTED_ROW_COUNTS[slotKey] || 0;
    if (live.length > expected) {
      duplicateRowsFound.push({
        slotKey,
        liveCount: live.length,
        expectedCount: expected,
        recordIds: live.map((r) => r.recordId),
        reason: "duplicate_cleanup_required",
      });
      applyBlockers.push(`duplicate_cleanup_required:${slotKey}:${live.length}>${expected}`);
    }
  }

  for (const planned of targetRows) {
    const liveForSlot = rowsBySlot.get(planned.slotKey) || [];
    const match = findLiveMatch(planned, liveForSlot);

    if (!match) {
      if (liveForSlot.length >= (EXPECTED_ROW_COUNTS[planned.slotKey] || 0)) {
        continue;
      }
      const fields = buildCreateFields(planned);
      rowsWouldCreate.push({
        slotKey: planned.slotKey,
        title: planned.title,
        body: planned.body,
        sort: planned.sort,
        marsha: planned.marsha,
        classification: planned.classification,
        imageUrl: planned.imageUrl,
        sourceUrl: planned.sourceUrl,
        dateLine: planned.dateLine,
        action: "create",
        fields,
      });
      exactCreatePayloads.push({
        table: PRESENTATION_TABLE,
        fields,
      });
      continue;
    }

    if (bodiesMatch(match.body, planned.body) && normalizeTitle(match.title) === normalizeTitle(planned.title)) {
      rowsMatched.push({
        slotKey: planned.slotKey,
        recordId: match.recordId,
        title: match.title,
        marsha: planned.marsha,
        action: "matched",
      });
    } else {
      rowsWouldUpdate.push({
        slotKey: planned.slotKey,
        recordId: match.recordId,
        marsha: planned.marsha,
        action: "update_required",
        currentTitle: match.title,
        currentBody: match.body,
        proposedTitle: planned.title,
        proposedBody: planned.body,
        note: "Report-only — v25C-3C apply creates missing rows only; does not patch existing copy or images.",
      });
    }
  }

  const openingsPlanned = rowsWouldCreate.filter((r) => r.slotKey === OPENINGS_SLOT);
  const momentumPlanned = rowsWouldCreate.filter((r) => r.slotKey === MOMENTUM_SLOT);

  const allOpeningsHaveSourceUrls = rowValidation.openings.every((r) => r.hasSourceUrl);
  const allOpeningsHaveImages = rowValidation.openings.every((r) => r.hasImage);
  const allMomentumHaveSourceUrls = rowValidation.momentum.every((r) => r.hasSourceUrl);

  const applyGatesReady = apply && approveBatch && founderReviewed && createApproved;
  const canApply =
    applyGatesReady &&
    applyBlockers.length === 0 &&
    rowsWouldCreate.length > 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const created = [];
    const skipped = [];
    const errors = [];
    for (const row of rowsWouldCreate) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: row.fields, typecast: true }),
      });
      if (!res.ok) {
        errors.push({ slotKey: row.slotKey, title: row.title, message: json.error?.message || res.status });
      } else {
        created.push({
          recordId: json.id,
          slotKey: row.slotKey,
          title: row.title,
          sort: row.sort,
          marsha: row.marsha,
        });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = created.length > 0;
    applyResults = { created, skipped, errors };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  } else if (apply && applyBlockers.length > 0) {
    applyResults = { created: [], skipped: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const openingsAfterCreate =
    (rowsBySlot.get(OPENINGS_SLOT) || []).length + openingsPlanned.length;
  const momentumAfterCreate =
    (rowsBySlot.get(MOMENTUM_SLOT) || []).length + momentumPlanned.length;
  const openingsMeetsMinimumAfterCreate = openingsAfterCreate >= OPENINGS_MINIMUM;
  const momentumMeetsMinimumAfterCreate = momentumAfterCreate >= MOMENTUM_MINIMUM;

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C3CWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    sourcePackage: REVIEW_PACKAGE_JSON,
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-openings-momentum-row-creation-writer.js",
      "scripts/brand-explorer-openings-momentum-row-creation-writer.mjs",
      "docs/data-intelligence/brand-explorer-openings-momentum-row-creation-writer-v25C-3C.md",
      "reports/brand-explorer-openings-momentum-row-creation-writer.md",
      "reports/brand-explorer-openings-momentum-row-creation-writer.json",
      "package.json",
    ],
    targetRowCounts: EXPECTED_ROW_COUNTS,
    totalTargetRows: TOTAL_TARGET_ROWS,
    openingsRowsPlanned: openingsPlanned,
    momentumRowsPlanned: momentumPlanned,
    openingsProposedCards: reviewPackage.openingsProposedCards || [],
    momentumProposedRows: reviewPackage.momentumProposedRows || [],
    rowValidation,
    casaNizucExcludedFromMomentum: !casaNizucInMomentum,
    casaNizucHandledCorrectly: reviewPackage.casaNizucHandledCorrectly || {
      inOpeningsAsFutureOpeningExample: true,
      excludedFromRecentMomentum: true,
    },
    futureDatedRowsExcludedFromMomentum: futureMomentumRows.length === 0,
    futureMomentumHeldCount: (reviewPackage.futureDatedRowsExcludedFromMomentum?.excluded || []).length,
    prNewsroomClaimsAvoided: prClaimsWithoutSource.length === 0,
    allRowsHaveSourceUrls: allOpeningsHaveSourceUrls && allMomentumHaveSourceUrls,
    allOpeningsRowsHaveImages: allOpeningsHaveImages,
    sourceLibraryGapsReported: sourceLibraryRequiredReported,
    sourceLibraryGaps,
    sourceLibraryRequiredBeforeV25C3C: sourceLibraryRequiredReported,
    sourceLibraryBlocking: false,
    partnerFactsCreated: false,
    sourceLibraryRowsCreated: false,
    geographicFootprintRowsUntouched: true,
    geographicFootprintRowsSnapshot,
    loyaltyRowsUntouched: true,
    loyaltyRowsSnapshot,
    registryAssetsUntouched: true,
    existingRowImagesUntouched: true,
    sortOrderSafelyDerived: true,
    sortOrderUntouchedOnExisting: true,
    existingOpeningsRows: (rowsBySlot.get(OPENINGS_SLOT) || []).map((r) => ({
      recordId: r.recordId,
      title: r.title,
      sortOrder: r.sortOrder,
    })),
    existingMomentumRows: (rowsBySlot.get(MOMENTUM_SLOT) || []).map((r) => ({
      recordId: r.recordId,
      title: r.title,
      sortOrder: r.sortOrder,
    })),
    rowsWouldCreate,
    rowsWouldUpdate,
    rowsMatched,
    duplicateRowsFound,
    exactCreatePayloads,
    openingsMeetsMinimumAfterCreate,
    momentumMeetsMinimumAfterCreate,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      createApproved,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    exactApplyCommand: buildApplyCommand(),
    idempotentAfterApply:
      rowsWouldCreate.length === 0 && duplicateRowsFound.length === 0 && rowsWouldUpdate.length === 0,
    doesNotDo: [
      "Create Partner Facts or Source Library records",
      "Modify geographic footprint region rows or loyalty rows",
      "Update existing matched rows or their images",
      "Modify Brand Asset Registry assets",
      "Change Brand Basics, Sort Order on existing rows, or Company Validated",
      "Write governance labels into presentation Body copy",
      "Use future-dated listings in Recent Momentum",
      "Place Casa Nizuc in Recent Momentum",
      "Claim newsroom/PR support without a PR/newsroom source",
      "Imply Marriott validated anything",
    ],
  };
}

export function buildBrandExplorerOpeningsMomentumRowCreationWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Openings + Momentum Row Creation Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Writer exists: **${report.writerExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Source package: \`${report.sourcePackage}\``,
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Rows update required | ${report.rowsWouldUpdate.length} |`,
    `| Rows matched (idempotent) | ${report.rowsMatched.length} |`,
    `| Duplicate rows found | ${report.duplicateRowsFound.length} |`,
    `| Openings planned | ${report.openingsRowsPlanned.length} |`,
    `| Momentum planned | ${report.momentumRowsPlanned.length} |`,
    `| Casa Nizuc excluded from momentum | ${report.casaNizucExcludedFromMomentum ? "yes" : "no"} |`,
    `| Future-dated momentum excluded | ${report.futureDatedRowsExcludedFromMomentum ? "yes" : "no"} |`,
    `| PR/newsroom claims avoided | ${report.prNewsroomClaimsAvoided ? "yes" : "no"} |`,
    `| All rows have source URLs | ${report.allRowsHaveSourceUrls ? "yes" : "no"} |`,
    `| All openings have images | ${report.allOpeningsRowsHaveImages ? "yes" : "no"} |`,
    `| Source Library gaps reported (non-blocking) | ${report.sourceLibraryGapsReported ? "yes" : "no"} |`,
    `| Geographic footprint untouched | ${report.geographicFootprintRowsUntouched ? "yes" : "no"} |`,
    `| Loyalty rows untouched | ${report.loyaltyRowsUntouched ? "yes" : "no"} |`,
    `| Registry assets untouched | ${report.registryAssetsUntouched ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Governance labels (report metadata only)",
    "",
    ...report.governanceLabels.map((l) => `- ${l}`),
    "",
  ];

  if (report.openingsRowsPlanned.length) {
    lines.push("## Openings rows planned", "");
    for (const row of report.openingsRowsPlanned) {
      lines.push(`- **${row.title}** · ${row.classification} · sort ${row.sort}`);
    }
    lines.push("");
  }

  if (report.momentumRowsPlanned.length) {
    lines.push("## Momentum rows planned", "");
    for (const row of report.momentumRowsPlanned) {
      lines.push(`- **${row.title}** · ${row.dateLine} · sort ${row.sort}`);
    }
    lines.push("");
  }

  if (report.rowsWouldCreate.length) {
    lines.push("## Rows would create", "");
    for (const row of report.rowsWouldCreate) {
      lines.push(`### ${row.slotKey} — ${row.title}`, "", row.body, "");
    }
  }

  if (report.rowsWouldUpdate.length) {
    lines.push("## Rows update required (report only)", "");
    for (const row of report.rowsWouldUpdate) {
      lines.push(`- \`${row.slotKey}\` \`${row.recordId}\`: copy differs from v25C-3B package`);
    }
    lines.push("");
  }

  if (report.duplicateRowsFound.length) {
    lines.push("## Duplicate rows (blocks apply)", "");
    for (const dup of report.duplicateRowsFound) {
      lines.push(`- \`${dup.slotKey}\`: ${dup.liveCount} live vs ${dup.expectedCount} expected`);
    }
    lines.push("");
  }

  if (report.sourceLibraryGaps?.length) {
    lines.push("## Source Library gaps (reported, non-blocking)", "");
    for (const gap of report.sourceLibraryGaps) {
      lines.push(`- ${gap}`);
    }
    lines.push("");
  }

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) {
      lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");

  if (report.applyResults) {
    lines.push(
      "## Apply results",
      "",
      `- Created: ${report.applyResults.created?.length || 0}`,
      `- Errors: ${report.applyResults.errors?.length || 0}`,
      `- Blocked: ${report.applyResults.blocked ? "yes" : "no"}`,
      ""
    );
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) {
    lines.push(`- ${item}`);
  }

  return lines.join("\n");
}
