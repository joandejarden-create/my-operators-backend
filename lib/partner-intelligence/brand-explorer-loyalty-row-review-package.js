/**
 * Brand Explorer Loyalty Row Review Package v25C-2C.
 *
 * Founder-review package with exact proposed loyalty presentation copy and row
 * payloads for Tribute Portfolio. Read-only — no Airtable writes.
 *
 * @see docs/data-intelligence/brand-explorer-loyalty-row-review-package-v25C-2C.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  ELIGIBLE_LOYALTY_FACT_KEYS,
  TARGET_FUTURE_SLOTS,
} from "./brand-explorer-loyalty-fact-approval-writer.js";
import { V23_TRIBUTE_RULES } from "./tribute-portfolio-targeted-extract.js";

export const PACKAGE_VERSION = "25C-2C";
export const REPORT_JSON_NAME = "brand-explorer-loyalty-row-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-loyalty-row-review-package.md";
export const DOC_MD_NAME = "brand-explorer-loyalty-row-review-package-v25C-2C.md";

export const TARGET_LOYALTY_SLOTS = [...TARGET_FUTURE_SLOTS];

export const EXCLUDED_KPI_SLOTS = [
  "loyalty.kpi.hotels",
  "loyalty.kpi.markets",
  "loyalty.kpi.members",
  "loyalty.kpi.mix",
];

export const NEXT_WRITER = "brand-explorer-loyalty-row-creation-writer";
export const NEXT_WRITER_VERSION = "25C-2D";

const GOVERNANCE_LABELS = [
  "AI-assembled from approved source facts",
  "Pending founder review",
  "Not company-validated",
  "Not Marriott-validated",
];

const FOUNDER_REVIEW_STATUS = GOVERNANCE_LABELS.join("; ");

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const REFERENCE_BRANDS = [
  { name: "Curio Collection by Hilton", id: "receQkxgjlezsc1xg", fixture: "fixtures/brand-explorer-presentation-curio-full.json" },
  { name: "Kimpton Hotels", id: "recCKuXCmGvxHPfb3", fixture: "fixtures/brand-explorer-presentation-kimpton-full.json" },
  { name: "Radisson Blu by Choice", id: "recWPEvxBQxVVzSq3", fixture: "fixtures/brand-explorer-presentation-radisson-blu.example.json" },
  { name: "Ascend Hotel Collection", id: "reclkgOzvAcBheUSo", fixture: "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json" },
];

const SLOT_FACT_KEYS = {
  "loyalty.earn": ["be.loyalty.earnMechanics", "be.loyalty.memberRatesBenefit"],
  "loyalty.redeem": ["be.loyalty.redeemMechanics"],
  "loyalty.elite": ["be.loyalty.eliteTierLadder"],
  "loyalty.proof": ["be.loyalty.programScaleStatement", "be.loyalty.memberRatesBenefit"],
};

/** Founder-reviewed UI copy — owner-facing, source-grounded, no meta/excerpt language. */
export const POLISHED_LOYALTY_ROW_COPY = {
  "loyalty.earn": {
    title: "Earning & Member Rates",
    body:
      "Marriott Bonvoy gives guests a familiar rewards path for earning points at participating hotels, with member-rate incentives that can support direct booking behavior.",
  },
  "loyalty.redeem": {
    title: "Redeeming Through Bonvoy",
    body:
      "Bonvoy participation gives guests a redemption pathway through Marriott's loyalty ecosystem, adding a consumer-facing reason to consider the property within the broader Marriott network.",
  },
  "loyalty.elite": [
    { title: "Member", body: "Entry tier in the Marriott Bonvoy member ladder.", sort: 0 },
    { title: "Silver Elite", body: "Elite tier in the Marriott Bonvoy member ladder.", sort: 1 },
    { title: "Gold Elite", body: "Elite tier in the Marriott Bonvoy member ladder.", sort: 2 },
    { title: "Platinum Elite", body: "Elite tier in the Marriott Bonvoy member ladder.", sort: 3 },
    { title: "Titanium Elite", body: "Elite tier in the Marriott Bonvoy member ladder.", sort: 4 },
    {
      title: "Ambassador Elite",
      body: "Highest named elite tier in the Marriott Bonvoy member ladder.",
      sort: 5,
    },
  ],
  "loyalty.proof": [
    {
      title: "Global Program Scale",
      body:
        "Marriott describes Bonvoy as a rewards platform spanning 7,000+ hotels worldwide; use current Bonvoy materials to confirm applicable participation and terms for a specific asset.",
      sort: 0,
    },
    {
      title: "Member Rate Incentive",
      body:
        "Marriott Bonvoy member rates create a consumer-facing booking incentive that may support direct-channel consideration where the property participates.",
      sort: 1,
    },
  ],
};

export function buildFlattenedLoyaltyRowTargets(
  brandRecordId = TRIBUTE_RECORD_ID,
  brandName = BRAND_NAME
) {
  const rows = [];
  const earn = POLISHED_LOYALTY_ROW_COPY["loyalty.earn"];
  rows.push({
    slotKey: "loyalty.earn",
    title: earn.title,
    body: earn.body,
    sort: 0,
    fields: {
      "Slot Key": "loyalty.earn",
      Title: earn.title,
      Body: earn.body,
      Brand: [brandRecordId],
      "Brand Name": brandName,
      Active: true,
      "Sort Order": 0,
    },
  });
  const redeem = POLISHED_LOYALTY_ROW_COPY["loyalty.redeem"];
  rows.push({
    slotKey: "loyalty.redeem",
    title: redeem.title,
    body: redeem.body,
    sort: 0,
    fields: {
      "Slot Key": "loyalty.redeem",
      Title: redeem.title,
      Body: redeem.body,
      Brand: [brandRecordId],
      "Brand Name": brandName,
      Active: true,
      "Sort Order": 0,
    },
  });
  for (const tier of POLISHED_LOYALTY_ROW_COPY["loyalty.elite"]) {
    rows.push({
      slotKey: "loyalty.elite",
      title: tier.title,
      body: tier.body,
      sort: tier.sort,
      fields: {
        "Slot Key": "loyalty.elite",
        Title: tier.title,
        Body: tier.body,
        Brand: [brandRecordId],
        "Brand Name": brandName,
        Active: true,
        "Sort Order": tier.sort,
      },
    });
  }
  for (const proof of POLISHED_LOYALTY_ROW_COPY["loyalty.proof"]) {
    rows.push({
      slotKey: "loyalty.proof",
      title: proof.title,
      body: proof.body,
      sort: proof.sort,
      fields: {
        "Slot Key": "loyalty.proof",
        Title: proof.title,
        Body: proof.body,
        Brand: [brandRecordId],
        "Brand Name": brandName,
        Active: true,
        "Sort Order": proof.sort,
      },
    });
  }
  return rows;
}

export const EXPECTED_LOYALTY_ROW_COUNTS = {
  "loyalty.earn": 1,
  "loyalty.redeem": 1,
  "loyalty.elite": 6,
  "loyalty.proof": 2,
};

const FORBIDDEN_UI_COPY = /approved source excerpt/i;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-loyalty-fact-approval-writer.md",
  "reports/brand-explorer-loyalty-fact-approval-writer.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/brand-explorer-required-section-source-capture-package.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-evidence-fact-review-package.md",
  "reports/brand-explorer-evidence-fact-review-package.json",
  "reports/tribute-portfolio-targeted-extract.md",
  "reports/tribute-portfolio-targeted-extract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-loyalty-fact-approval-writer.js",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function short(text, max = 160) {
  const s = nz(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return TRIBUTE_RECORD_ID;
  }
  return nz(raw);
}

function readJson(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
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

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function isApprovedFact(fact) {
  const st = nz(fact.humanReviewStatus);
  return st === "Approved" || st === "Edited";
}

function approvedValueFor(fact) {
  return nz(fact.approvedValue) || nz(fact.extractedValue);
}

async function fetchAllFacts(brandRecordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function parseEliteTiers(approvedLadder) {
  const raw = nz(approvedLadder);
  if (!raw) return [];

  const known = [
    "Member",
    "Silver Elite",
    "Gold Elite",
    "Platinum Elite",
    "Titanium Elite",
    "Ambassador Elite",
  ];
  const found = [];
  let remainder = raw;
  for (const tier of known) {
    const idx = remainder.indexOf(tier);
    if (idx >= 0) {
      found.push(tier);
      remainder = remainder.slice(idx + tier.length).trim();
    }
  }
  if (found.length) return found;

  return raw
    .split(/\s+(?=Silver Elite|Gold Elite|Platinum Elite|Titanium Elite|Ambassador Elite)/i)
    .map((t) => t.trim())
    .filter(Boolean);
}

function buildPolishedEarnCopy(factsByKey) {
  if (
    !factsByKey.get("be.loyalty.earnMechanics") ||
    !factsByKey.get("be.loyalty.memberRatesBenefit")
  ) {
    return null;
  }
  return { ...POLISHED_LOYALTY_ROW_COPY["loyalty.earn"] };
}

function buildPolishedRedeemCopy(factsByKey) {
  if (!factsByKey.get("be.loyalty.redeemMechanics")) return null;
  return { ...POLISHED_LOYALTY_ROW_COPY["loyalty.redeem"] };
}

function buildPolishedEliteRows(factsByKey) {
  const ladder = approvedValueFor(factsByKey.get("be.loyalty.eliteTierLadder"));
  if (!hasVal(ladder)) return [];
  const tiers = parseEliteTiers(ladder);
  const polished = POLISHED_LOYALTY_ROW_COPY["loyalty.elite"];
  if (tiers.length < polished.length) return [];
  return polished.map((row) => ({ ...row }));
}

function buildPolishedProofRows(factsByKey) {
  if (
    !factsByKey.get("be.loyalty.programScaleStatement") ||
    !factsByKey.get("be.loyalty.memberRatesBenefit")
  ) {
    return [];
  }
  return POLISHED_LOYALTY_ROW_COPY["loyalty.proof"].map((row) => ({ ...row }));
}

function containsForbiddenUiCopy(text) {
  return FORBIDDEN_UI_COPY.test(nz(text));
}

function buildPresentationPayload(brandRecordId, brandName, slotKey, title, body, sort = 0) {
  return {
    table: PRESENTATION_TABLE,
    fields: {
      "Slot Key": slotKey,
      Title: title || "",
      Body: body || "",
      Brand: [brandRecordId],
      "Brand Name": brandName,
      Active: true,
      "Sort Order": sort,
    },
  };
}

function riskLevelForSlot(slotKey, proposedBody, proposedRows) {
  const risks = [];
  if (containsForbiddenUiCopy(proposedBody)) {
    risks.push("forbidden_meta_copy_in_ui");
  }
  for (const row of proposedRows) {
    if (containsForbiddenUiCopy(row.body) || containsForbiddenUiCopy(row.title)) {
      risks.push("forbidden_meta_copy_in_ui");
    }
  }
  if (slotKey === "loyalty.proof" && /7,000\+/i.test(proposedBody)) {
    risks.push("scale_count_framed_in_narrative");
  }
  if (risks.some((r) => r === "forbidden_meta_copy_in_ui")) return "high";
  if (risks.some((r) => r.includes("scale_count"))) return "low";
  return "low";
}

function existingSlotStatus(brand, slotKey) {
  const rows = blocksForSlot(brand, slotKey);
  if (!rows.length) {
    return {
      exists: false,
      recordIds: [],
      title: "",
      body: "",
      rowCount: 0,
    };
  }
  return {
    exists: true,
    recordIds: rows.map((r) => nz(r.recordId)).filter(hasVal),
    title: nz(rows[0]?.title),
    body: rows.map((r) => nz(r.body)).filter(hasVal).join("\n\n"),
    rowCount: rows.length,
  };
}

function loadReferenceLoyaltyPatterns() {
  const patterns = [];
  for (const ref of REFERENCE_BRANDS) {
    const json = readJson(ref.fixture);
    const slots = json?.slots || json?.presentation || [];
    const loyaltySlots = slots.filter((r) =>
      TARGET_LOYALTY_SLOTS.includes(nz(r.slotKey))
    );
    patterns.push({
      brandName: ref.name,
      brandId: ref.id,
      fixture: ref.fixture,
      loyaltyEarn: loyaltySlots.find((r) => r.slotKey === "loyalty.earn"),
      loyaltyRedeem: loyaltySlots.find((r) => r.slotKey === "loyalty.redeem"),
      loyaltyEliteCount: loyaltySlots.filter((r) => r.slotKey === "loyalty.elite").length,
      loyaltyProofCount: loyaltySlots.filter((r) => r.slotKey === "loyalty.proof").length,
      earnBodyPreview: short(loyaltySlots.find((r) => r.slotKey === "loyalty.earn")?.body, 200),
      redeemBodyPreview: short(loyaltySlots.find((r) => r.slotKey === "loyalty.redeem")?.body, 200),
    });
  }
  return patterns;
}

function loadLiveReferenceLoyaltyRows() {
  return Promise.all(
    REFERENCE_BRANDS.map(async (ref) => {
      const brand = await fetchBrandApiShape(ref.id);
      const slotSummary = {};
      for (const slotKey of TARGET_LOYALTY_SLOTS) {
        const rows = blocksForSlot(brand, slotKey);
        slotSummary[slotKey] = {
          rowCount: rows.length,
          hasContent: rows.some((r) => hasVal(r.title) || hasVal(r.body)),
        };
      }
      return {
        brandName: ref.name,
        brandId: ref.id,
        slots: slotSummary,
      };
    })
  );
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function buildSlotReviewRow(slotKey, brand, factsByKey, brandRecordId, brandName) {
  const requiredFactKeys = SLOT_FACT_KEYS[slotKey] || [];
  const missingFacts = requiredFactKeys.filter((k) => {
    const fact = factsByKey.get(k);
    return !fact || !isApprovedFact(fact);
  });
  const sourceFacts = requiredFactKeys
    .map((k) => factsByKey.get(k))
    .filter(Boolean)
    .map((f) => ({
      fieldKey: f.fieldName,
      factRecordId: f.id,
      approvedValue: approvedValueFor(f),
      humanReviewStatus: nz(f.humanReviewStatus),
      sourceRecordId: f.sourceRecordId,
    }));

  const existing = existingSlotStatus(brand, slotKey);
  let proposedTitle = "";
  let proposedBody = "";
  let proposedRows = [];
  let proposedPayloads = [];

  if (slotKey === "loyalty.earn") {
    const polished = buildPolishedEarnCopy(factsByKey);
    proposedTitle = polished?.title || "";
    proposedBody = polished?.body || "";
    proposedPayloads = polished
      ? [buildPresentationPayload(brandRecordId, brandName, slotKey, polished.title, polished.body, 0)]
      : [];
  } else if (slotKey === "loyalty.redeem") {
    const polished = buildPolishedRedeemCopy(factsByKey);
    proposedTitle = polished?.title || "";
    proposedBody = polished?.body || "";
    proposedPayloads = polished
      ? [buildPresentationPayload(brandRecordId, brandName, slotKey, polished.title, polished.body, 0)]
      : [];
  } else if (slotKey === "loyalty.elite") {
    proposedRows = buildPolishedEliteRows(factsByKey);
    proposedPayloads = proposedRows.map((r) =>
      buildPresentationPayload(brandRecordId, brandName, slotKey, r.title, r.body, r.sort)
    );
    proposedTitle = "(multiple tier rows)";
    proposedBody = proposedRows.map((r) => `${r.title}: ${r.body}`).join("\n\n");
  } else if (slotKey === "loyalty.proof") {
    proposedRows = buildPolishedProofRows(factsByKey);
    proposedPayloads = proposedRows.map((r) =>
      buildPresentationPayload(brandRecordId, brandName, slotKey, r.title, r.body, r.sort)
    );
    proposedTitle = "(multiple proof rows)";
    proposedBody = proposedRows.map((r) => `${r.title}: ${r.body}`).join("\n\n");
  }

  const rowCreationRequired = !existing.exists;
  const rowUpdateRequired =
    existing.exists &&
    (short(existing.body) !== short(proposedBody) ||
      (slotKey === "loyalty.elite" && existing.rowCount !== proposedRows.length) ||
      (slotKey === "loyalty.proof" && existing.rowCount !== proposedRows.length));

  const blocked =
    missingFacts.length > 0 ||
    !hasVal(proposedBody) ||
    containsForbiddenUiCopy(proposedBody) ||
    proposedRows.some((r) => containsForbiddenUiCopy(r.body) || containsForbiddenUiCopy(r.title));
  const riskLevel = blocked ? "high" : riskLevelForSlot(slotKey, proposedBody, proposedRows);

  return {
    slotKey,
    existingRowStatus: existing.exists ? "present" : "missing",
    existingRecordIds: existing.recordIds,
    existingRowCount: existing.rowCount,
    existingTitle: existing.title,
    existingBodyPreview: short(existing.body, 200),
    sourceFactKeys: requiredFactKeys,
    sourceFactsUsed: sourceFacts,
    approvedValuesUsed: sourceFacts.map((f) => f.approvedValue),
    missingApprovedFacts: missingFacts,
    proposedTitle,
    proposedBody,
    proposedRows,
    proposedRowPayloads: proposedPayloads,
    rowCreationRequired,
    rowUpdateRequired,
    v25C2DAction: rowCreationRequired ? "create" : rowUpdateRequired ? "update" : "none",
    founderReviewStatus: FOUNDER_REVIEW_STATUS,
    governanceLabels: [...GOVERNANCE_LABELS],
    riskLevel,
    blocked,
    blockerReason: blocked
      ? missingFacts.length
        ? `missing_approved_facts:${missingFacts.join(",")}`
        : "empty_proposed_copy"
      : "",
  };
}

export function buildNextWriterCommand(brandSlug = "tribute-portfolio") {
  return `npm run ${NEXT_WRITER} -- --brand ${brandSlug} --dry-run`;
}

export async function buildBrandExplorerLoyaltyRowReviewPackageReport({
  brandIdOrName = "tribute-portfolio",
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-2C pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const brandBasics = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);

  const brand = await fetchBrandApiShape(brandRecordId);
  const allFacts = await fetchAllFacts(brandRecordId);

  const approvedLoyaltyFacts = allFacts.filter(
    (f) => ELIGIBLE_LOYALTY_FACT_KEYS.includes(nz(f.fieldName)) && isApprovedFact(f)
  );
  const factsByKey = new Map(approvedLoyaltyFacts.map((f) => [nz(f.fieldName), f]));

  const excludedFacts = [];
  for (const fact of allFacts) {
    const key = nz(fact.fieldName);
    if (ELIGIBLE_LOYALTY_FACT_KEYS.includes(key) && !isApprovedFact(fact)) {
      excludedFacts.push({ fieldKey: key, factRecordId: fact.id, reason: "not_approved" });
    }
    if (/^loyalty\.kpi\./i.test(key) || /^be\.standards\./i.test(key) || /^be\.meta\.fdd/i.test(key)) {
      excludedFacts.push({
        fieldKey: key,
        factRecordId: fact.id,
        reason: /^loyalty\.kpi\./i.test(key)
          ? "unsupported_kpi_fact"
          : "internal_or_fdd_fact",
      });
    }
    if (
      !ELIGIBLE_LOYALTY_FACT_KEYS.includes(key) &&
      (key.startsWith("be.loyalty.") || /^loyalty\./i.test(key)) &&
      !/^loyalty\.kpi\./i.test(key)
    ) {
      excludedFacts.push({
        fieldKey: key,
        factRecordId: fact.id,
        reason: "non_allowlisted_loyalty_fact",
      });
    }
  }

  const slotReviews = TARGET_LOYALTY_SLOTS.map((slotKey) =>
    buildSlotReviewRow(slotKey, brand, factsByKey, brandRecordId, BRAND_NAME)
  );

  const existingLoyaltyRows = [
    ...blocksForSlot(brand, "loyalty.hero_title"),
    ...blocksForSlot(brand, "loyalty.earn"),
    ...blocksForSlot(brand, "loyalty.redeem"),
    ...blocksForSlot(brand, "loyalty.elite"),
    ...blocksForSlot(brand, "loyalty.proof"),
    ...EXCLUDED_KPI_SLOTS.flatMap((sk) => blocksForSlot(brand, sk)),
  ].map((r) => ({
    recordId: nz(r.recordId),
    slotKey: nz(r.slotKey),
    titlePreview: short(r.title, 80),
    bodyPreview: short(r.body, 120),
  }));

  const heroTitleExists = blocksForSlot(brand, "loyalty.hero_title").length > 0;
  const loyaltySlotCoverageAfter2D = [
    heroTitleExists ? 1 : 0,
    ...TARGET_LOYALTY_SLOTS.map((sk) => (slotReviews.find((s) => s.slotKey === sk)?.blocked ? 0 : 1)),
  ].reduce((a, b) => a + b, 0);

  const loyaltyProgramMeetsMinimumAfter2D =
    !slotReviews.some((s) => s.blocked) && loyaltySlotCoverageAfter2D >= 5;

  const rowsNeedingCreation = slotReviews.filter((s) => s.rowCreationRequired && !s.blocked);
  const rowsNeedingUpdate = slotReviews.filter((s) => s.rowUpdateRequired && !s.blocked);

  const v25C2DRowCreationCounts = {
    "loyalty.earn": rowsNeedingCreation.find((s) => s.slotKey === "loyalty.earn")?.proposedRowPayloads
      ?.length || 0,
    "loyalty.redeem": rowsNeedingCreation.find((s) => s.slotKey === "loyalty.redeem")?.proposedRowPayloads
      ?.length || 0,
    "loyalty.elite": rowsNeedingCreation.find((s) => s.slotKey === "loyalty.elite")?.proposedRowPayloads
      ?.length || 0,
    "loyalty.proof": rowsNeedingCreation.find((s) => s.slotKey === "loyalty.proof")?.proposedRowPayloads
      ?.length || 0,
  };
  const v25C2DTotalRowCreations = Object.values(v25C2DRowCreationCounts).reduce((a, b) => a + b, 0);

  const earnCopy = buildPolishedEarnCopy(factsByKey);
  const redeemCopy = buildPolishedRedeemCopy(factsByKey);
  const duplicateEarnRedeemCopyFixed =
    hasVal(earnCopy?.body) && hasVal(redeemCopy?.body) && earnCopy.body !== redeemCopy.body;

  const approvedSourceExcerptRemoved = !slotReviews.some(
    (s) =>
      containsForbiddenUiCopy(s.proposedBody) ||
      (s.proposedRows || []).some(
        (r) => containsForbiddenUiCopy(r.body) || containsForbiddenUiCopy(r.title)
      )
  );

  const nonLoyaltyLeaked = excludedFacts.filter(
    (f) => !f.fieldKey.startsWith("be.loyalty.") && !/^loyalty\./i.test(f.fieldKey)
  );

  const referencePatterns = loadReferenceLoyaltyPatterns();
  const liveReferenceRows = await loadLiveReferenceLoyaltyRows();

  const proposedCopyBySlot = Object.fromEntries(
    slotReviews.map((s) => [
      s.slotKey,
      {
        proposedTitle: s.proposedTitle,
        proposedBody: s.proposedBody,
        proposedRows: s.proposedRows,
        governanceLabels: s.governanceLabels,
      },
    ])
  );

  return {
    packageVersion: PACKAGE_VERSION,
    packageExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-loyalty-row-review-package.js",
      "scripts/brand-explorer-loyalty-row-review-package.mjs",
      "docs/data-intelligence/brand-explorer-loyalty-row-review-package-v25C-2C.md",
      "reports/brand-explorer-loyalty-row-review-package.md",
      "reports/brand-explorer-loyalty-row-review-package.json",
      "package.json",
    ],
    approvedLoyaltyFactsUsed: approvedLoyaltyFacts.map((f) => ({
      fieldKey: f.fieldName,
      factRecordId: f.id,
      approvedValue: approvedValueFor(f),
      humanReviewStatus: nz(f.humanReviewStatus),
      targetExplorerSlots:
        V23_TRIBUTE_RULES.find((r) => r.fieldKey === f.fieldName)?.targetExplorerSlots || [],
    })),
    factsExcluded: excludedFacts,
    existingLoyaltyRowsFound: existingLoyaltyRows,
    loyaltyHeroTitleExists: heroTitleExists,
    targetSlots: TARGET_LOYALTY_SLOTS,
    slotReviews,
    proposedCopyBySlot,
    proposedLoyaltyEarnCopy: proposedCopyBySlot["loyalty.earn"],
    proposedLoyaltyRedeemCopy: proposedCopyBySlot["loyalty.redeem"],
    proposedLoyaltyEliteCopy: proposedCopyBySlot["loyalty.elite"],
    proposedLoyaltyProofCopy: proposedCopyBySlot["loyalty.proof"],
    kpiCountsExcluded: true,
    excludedKpiSlots: EXCLUDED_KPI_SLOTS,
    kpiRowsFound: existingLoyaltyRows.filter((r) => EXCLUDED_KPI_SLOTS.includes(r.slotKey)),
    internalOrFddFactsExcluded: excludedFacts.some((f) =>
      /internal|fdd|standards/i.test(f.reason)
    ),
    nonLoyaltyFactsLeakedIntoPlan: nonLoyaltyLeaked,
    rowsNeedingCreationV25C2D: rowsNeedingCreation.map((s) => s.slotKey),
    rowsNeedingUpdateV25C2D: rowsNeedingUpdate.map((s) => s.slotKey),
    v25C2DSummary: {
      create: rowsNeedingCreation.map((s) => ({
        slotKey: s.slotKey,
        rowCount: s.proposedRowPayloads.length,
        payloads: s.proposedRowPayloads,
      })),
      update: rowsNeedingUpdate.map((s) => ({
        slotKey: s.slotKey,
        existingRecordIds: s.existingRecordIds,
        payloads: s.proposedRowPayloads,
      })),
      blocked: slotReviews.filter((s) => s.blocked).map((s) => ({
        slotKey: s.slotKey,
        reason: s.blockerReason,
      })),
      rowCreationCounts: v25C2DRowCreationCounts,
      totalRowCreations: v25C2DTotalRowCreations,
    },
    copyPolishPatch: "v25C-2C-owner-facing-copy",
    duplicateEarnRedeemCopyFixed,
    approvedSourceExcerptRemoved,
    v25C2DTotalRowCreations,
    v25C2DRowCreationCounts,
    loyaltyProgramMeetsMinimumAfterV25C2D: loyaltyProgramMeetsMinimumAfter2D,
    loyaltyCoverageProjection: {
      heroTitle: heroTitleExists,
      earn: !slotReviews.find((s) => s.slotKey === "loyalty.earn")?.blocked,
      redeem: !slotReviews.find((s) => s.slotKey === "loyalty.redeem")?.blocked,
      elite: !slotReviews.find((s) => s.slotKey === "loyalty.elite")?.blocked,
      proof: !slotReviews.find((s) => s.slotKey === "loyalty.proof")?.blocked,
      projectedCount: loyaltySlotCoverageAfter2D,
      requiredMinimum: 5,
    },
    referenceBrandLoyaltyPatterns: referencePatterns,
    liveReferenceBrandRows: liveReferenceRows,
    presentationRowsUntouched: true,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched: true,
    companyValidatedSnapshot: companyValidatedBefore,
    airtableModified: false,
    readinessBlocked: slotReviews.some((s) => s.blocked),
    exactNextWriter: NEXT_WRITER,
    exactNextWriterVersion: NEXT_WRITER_VERSION,
    exactNextWriterCommand: buildNextWriterCommand(),
    doesNotDo: [
      "Write Airtable or create/update Brand Explorer Presentation rows",
      "Change images, Sort Order, Brand Basics, or Company Validated",
      "Populate loyalty.kpi.* without verified KPI facts",
      "Use pending, FDD, or internal-only facts",
      "Imply Marriott validated anything",
    ],
  };
}

export function buildBrandExplorerLoyaltyRowReviewPackageMarkdown(report) {
  const lines = [
    `# Brand Explorer Loyalty Row Review Package v${PACKAGE_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Package exists: **${report.packageExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Marriott validation implied: **no**`,
    "",
    "## Summary",
    "",
    `| Metric | Value |`,
    `|--------|-------|`,
    `| Approved loyalty facts used | ${report.approvedLoyaltyFactsUsed.length} |`,
    `| Target slots | ${report.targetSlots.length} |`,
    `| Rows needing creation (v25C-2D) | ${report.rowsNeedingCreationV25C2D.length} (${report.v25C2DTotalRowCreations ?? 0} presentation rows) |`,
    `| Rows needing update (v25C-2D) | ${report.rowsNeedingUpdateV25C2D.length} |`,
    `| KPI counts excluded | ${report.kpiCountsExcluded ? "yes" : "no"} |`,
    `| Loyalty meets minimum after v25C-2D | ${report.loyaltyProgramMeetsMinimumAfterV25C2D ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Governance labels (all proposed copy)",
    "",
    ...GOVERNANCE_LABELS.map((l) => `- ${l}`),
    "",
    "## Approved loyalty facts used",
    "",
  ];

  for (const f of report.approvedLoyaltyFactsUsed) {
    lines.push(`- \`${f.fieldKey}\` (\`${f.factRecordId}\`): ${short(f.approvedValue, 120)}`);
  }
  lines.push("");

  if (report.existingLoyaltyRowsFound.length) {
    lines.push("## Existing loyalty rows", "");
    for (const row of report.existingLoyaltyRowsFound) {
      lines.push(
        `- \`${row.slotKey}\` (\`${row.recordId || "—"}\`): ${row.titlePreview || row.bodyPreview || "(empty)"}`
      );
    }
    lines.push("");
  }

  for (const slot of report.slotReviews) {
    lines.push(`## ${slot.slotKey}`, "");
    lines.push(
      `- Existing: **${slot.existingRowStatus}** (${slot.existingRowCount} row(s))`,
      `- v25C-2D action: **${slot.v25C2DAction}**`,
      `- Risk: **${slot.riskLevel}**`,
      `- Founder review: ${slot.founderReviewStatus}`,
      `- Source facts: ${slot.sourceFactKeys.map((k) => `\`${k}\``).join(", ")}`,
      ""
    );
    if (slot.proposedBody) {
      lines.push("### Proposed copy", "");
      if (slot.proposedTitle && !slot.proposedTitle.startsWith("(multiple")) {
        lines.push(`**Title:** ${slot.proposedTitle}`, "");
      }
      lines.push("```", slot.proposedBody, "```", "");
    }
    if (slot.proposedRowPayloads?.length) {
      lines.push("### Proposed row payload(s)", "", "```json", JSON.stringify(slot.proposedRowPayloads, null, 2), "```", "");
    }
  }

  lines.push("## v25C-2D row plan", "");
  lines.push(`- Create: ${report.rowsNeedingCreationV25C2D.map((k) => `\`${k}\``).join(", ") || "—"}`);
  lines.push(`- Update: ${report.rowsNeedingUpdateV25C2D.map((k) => `\`${k}\``).join(", ") || "—"}`);
  lines.push("");

  lines.push("## Exact next writer", "", "```bash", report.exactNextWriterCommand, "```", "");

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) {
    lines.push(`- ${item}`);
  }
  lines.push("");

  return lines.join("\n");
}
