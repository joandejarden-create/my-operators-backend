/**
 * Brand Explorer Bonvoy Loyalty Row Enhancement Writer v25C-2H.
 *
 * Upgrades existing Tribute Portfolio loyalty presentation rows using approved
 * Bonvoy facts (original five + thirteen rich facts from v25C-2G). Dry-run default.
 *
 * @see docs/data-intelligence/brand-explorer-bonvoy-loyalty-row-enhancement-writer-v25C-2H.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import { ELIGIBLE_LOYALTY_FACT_KEYS } from "./brand-explorer-loyalty-fact-approval-writer.js";
import {
  ELIGIBLE_RICH_FACT_KEYS,
  REPORT_JSON_NAME as RICH_FACT_APPROVAL_JSON,
} from "./brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.js";
import {
  EXCLUDED_KPI_SLOTS,
  EXPECTED_LOYALTY_ROW_COUNTS,
  POLISHED_LOYALTY_ROW_COPY,
  TARGET_LOYALTY_SLOTS,
} from "./brand-explorer-loyalty-row-review-package.js";
import { REPORT_JSON_NAME as ENHANCEMENT_PACKAGE_JSON } from "./brand-explorer-bonvoy-loyalty-detail-enhancement-package.js";

export const WRITER_VERSION = "25C-2H";
export const REPORT_JSON_NAME = "brand-explorer-bonvoy-loyalty-row-enhancement-writer.json";
export const REPORT_MD_NAME = "brand-explorer-bonvoy-loyalty-row-enhancement-writer.md";
export const DOC_MD_NAME = "brand-explorer-bonvoy-loyalty-row-enhancement-writer-v25C-2H.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-2H-rich-bonvoy-loyalty-rows";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-rich-bonvoy-loyalty-copy";
export const APPLY_FLAG_FACTS = "--confirm-approved-bonvoy-facts-only";

const HERO_SLOT = "loyalty.hero_title";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const EXISTING_BONVOY_SOURCE_URL = "https://www.marriott.com/loyalty.mi";

const BONVOY_CAVEAT =
  "Illustrative examples only — actual earn/redeem rules vary by market, brand, property, and booking channel.";

const GOVERNANCE_LABELS = [
  "AI-assembled from approved Bonvoy facts",
  "Founder-reviewed rich loyalty package",
  "Not company-validated",
  "Not Marriott-validated",
];

const ELITE_TIER_SPECS = [
  { title: "Member", sort: 0, richFactKey: "be.loyalty.eliteMemberSummary" },
  { title: "Silver Elite", sort: 1, richFactKey: "be.loyalty.eliteSilverSummary" },
  { title: "Gold Elite", sort: 2, richFactKey: "be.loyalty.eliteGoldSummary" },
  { title: "Platinum Elite", sort: 3, richFactKey: "be.loyalty.elitePlatinumSummary" },
  { title: "Titanium Elite", sort: 4, richFactKey: "be.loyalty.eliteTitaniumSummary" },
  { title: "Ambassador Elite", sort: 5, richFactKey: "be.loyalty.eliteAmbassadorSummary" },
];

const RICH_PACKAGE_FACT_KEYS = [...ELIGIBLE_LOYALTY_FACT_KEYS, ...ELIGIBLE_RICH_FACT_KEYS];

const FORBIDDEN_BODY_PATTERNS = [
  /AI-assembled from approved/i,
  /Founder-reviewed rich loyalty package/i,
  /Not company-validated/i,
  /Not Marriott-validated/i,
  /approved source excerpt/i,
  /Pending founder review/i,
];

const THIN_PLACEHOLDER_RE =
  /tier in the Marriott Bonvoy member ladder|Highest named elite tier in the Marriott Bonvoy member ladder/i;

const REFERENCE_BRAND_COPY_PATTERNS = [
  /hilton honors/i,
  /choice privileges/i,
  /\bEQC\b/,
  /diamond reserve/i,
  /ihg one rewards/i,
  /radisson rewards/i,
  /fifth night free/i,
  /milestone rewards/i,
];

const EXCLUDED_FIELD_KEY_PATTERNS = [
  /^be\.standards\./i,
  /^standards\.requirement/i,
  /^loyalty\.kpi\./i,
  /^be\.meta\.fdd/i,
];

const BLOCKED_VALUE_PATTERNS = [
  /item\s*19/i,
  /franchise (agreement|fee|disclosure)/i,
  /\bfdd\b/i,
  /royalt(y|ies)/i,
  /initial franchise fee/i,
  /financial performance/i,
  /company validated/i,
  /marriott validated/i,
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.md",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.json",
  "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json",
  "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.json",
  "reports/brand-explorer-loyalty-row-creation-writer.md",
  "reports/brand-explorer-loyalty-row-creation-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Partner Facts",
  "live Tribute Source Library records",
  "live Tribute Brand Explorer Presentation rows",
  "live Curio/Kimpton/Radisson/Ascend loyalty rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-bonvoy-loyalty-row-enhancement-writer.js",
  "scripts/brand-explorer-bonvoy-loyalty-row-enhancement-writer.mjs",
  "docs/data-intelligence/brand-explorer-bonvoy-loyalty-row-enhancement-writer-v25C-2H.md",
  "reports/brand-explorer-bonvoy-loyalty-row-enhancement-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-row-enhancement-writer.json",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function bulletLines(lines) {
  return lines.map((l) => nz(l)).filter(Boolean).join("\n");
}

function appendCaveat(body) {
  const base = normalizeBody(body);
  if (!base) return BONVOY_CAVEAT;
  if (base.includes("Illustrative examples only")) return base;
  return `${base}\n\n${BONVOY_CAVEAT}`;
}

function factValue(fact) {
  return nz(fact?.extractedValue || fact?.value || fact?.fieldValue);
}

function countBullets(body) {
  const lines = normalizeBody(body).split(/\n+/).filter((l) => nz(l).length > 0);
  return Math.max(1, lines.length);
}

function tierDetailScore(eliteRows) {
  if (!eliteRows.length) return 0;
  const avgLen = eliteRows.reduce((sum, r) => sum + nz(r.body).length, 0) / eliteRows.length;
  const hasQual = eliteRows.filter((r) => /night|spend|qualif|benefit/i.test(r.body)).length;
  return Math.min(5, Math.round(avgLen / 40) + (hasQual >= 2 ? 2 : 0));
}

function analyzeLoyaltyDensity(rows) {
  const bySlot = (sk) => rows.filter((r) => r.slotKey === sk);
  const earn = bySlot("loyalty.earn")[0];
  const redeem = bySlot("loyalty.redeem")[0];
  const elite = bySlot("loyalty.elite");
  const proof = bySlot("loyalty.proof");
  return {
    earnBulletCount: earn ? countBullets(earn.body) : 0,
    redeemBulletCount: redeem ? countBullets(redeem.body) : 0,
    eliteRowCount: elite.length,
    eliteAvgBodyLength: elite.length
      ? Math.round(elite.reduce((s, r) => s + nz(r.body).length, 0) / elite.length)
      : 0,
    eliteDetailScore: tierDetailScore(elite),
    proofRowCount: proof.length,
    proofAvgBodyLength: proof.length
      ? Math.round(proof.reduce((s, r) => s + nz(r.body).length, 0) / proof.length)
      : 0,
  };
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

function isApprovedFact(fact) {
  const st = nz(fact?.humanReviewStatus);
  return st === "Approved" || st === "Edited";
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function bodyHasForbiddenCopy(body) {
  return FORBIDDEN_BODY_PATTERNS.some((re) => re.test(body));
}

function bodyHasReferenceBrandLeak(body) {
  return REFERENCE_BRAND_COPY_PATTERNS.some((re) => re.test(body));
}

function bodyHasThinPlaceholder(body) {
  return THIN_PLACEHOLDER_RE.test(body);
}

function isExcludedFactKey(key) {
  return EXCLUDED_FIELD_KEY_PATTERNS.some((re) => re.test(nz(key)));
}

function factValueBlocked(value) {
  return BLOCKED_VALUE_PATTERNS.some((re) => re.test(nz(value)));
}

function buildRichEarnCopy(factsByKey) {
  const lines = [
    factValue(factsByKey.get("be.loyalty.earnEligibleSpend")),
    factValue(factsByKey.get("be.loyalty.earnMechanics")),
    factValue(factsByKey.get("be.loyalty.memberRatesBenefit")),
    factValue(factsByKey.get("be.loyalty.earnWifiDirectBooking")),
    factValue(factsByKey.get("be.loyalty.earnFreeNightsHeadline")),
    "Actual earning rules vary by brand, property, market, and booking channel.",
  ].filter(Boolean);

  return {
    title: "Earn Examples",
    body: appendCaveat(bulletLines(lines)),
    factsUsed: [
      "be.loyalty.earnEligibleSpend",
      "be.loyalty.earnMechanics",
      "be.loyalty.memberRatesBenefit",
      "be.loyalty.earnWifiDirectBooking",
      "be.loyalty.earnFreeNightsHeadline",
    ],
    sourceUrls: [EXISTING_BONVOY_SOURCE_URL, "https://www.marriott.com/loyalty/earn.mi"],
  };
}

function buildRichRedeemCopy(factsByKey) {
  const lines = [
    factValue(factsByKey.get("be.loyalty.redeemFreeNights")),
    factValue(factsByKey.get("be.loyalty.redeemMechanics")),
    factValue(factsByKey.get("be.loyalty.redeemOnStayExperiences")),
    factValue(factsByKey.get("be.loyalty.redeemParticipatingNetwork")),
    "Actual redemption rules vary by property, market, availability, and booking channel.",
  ].filter(Boolean);

  return {
    title: "Redeem Examples",
    body: appendCaveat(bulletLines(lines)),
    factsUsed: [
      "be.loyalty.redeemFreeNights",
      "be.loyalty.redeemMechanics",
      "be.loyalty.redeemOnStayExperiences",
      "be.loyalty.redeemParticipatingNetwork",
    ],
    sourceUrls: [EXISTING_BONVOY_SOURCE_URL, "https://www.marriott.com/loyalty/redeem.mi"],
  };
}

function buildRichEliteRows(factsByKey) {
  return ELITE_TIER_SPECS.map((tier) => {
    const summary = factValue(factsByKey.get(tier.richFactKey));
    const ladder = factValue(factsByKey.get("be.loyalty.eliteTierLadder"));
    const bodyLines = [summary].filter(Boolean);
    if (tier.sort === 0 && ladder && !summary.includes(ladder.slice(0, 20))) {
      bodyLines.push(
        "Entry point on the published Bonvoy member ladder—confirm current tier names and thresholds for your market."
      );
    }
    return {
      slotKey: "loyalty.elite",
      title: tier.title,
      sort: tier.sort,
      body: appendCaveat(bulletLines(bodyLines)),
      factsUsed: ["be.loyalty.eliteTierLadder", tier.richFactKey],
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL, "https://www.marriott.com/loyalty/member-benefits.mi"],
    };
  });
}

function buildRichProofRows(factsByKey) {
  const scale = factValue(factsByKey.get("be.loyalty.programScaleStatement"));
  const memberRates = factValue(factsByKey.get("be.loyalty.memberRatesBenefit"));
  const directBooking = factValue(factsByKey.get("be.loyalty.proofDirectBookingRelevance"));

  return [
    {
      slotKey: "loyalty.proof",
      title: "Global Program Scale",
      sort: 0,
      body: appendCaveat(
        bulletLines([
          scale || "Marriott Bonvoy spans a large global participating-hotel network.",
          "Use current Marriott Bonvoy materials to confirm scale claims and Tribute Portfolio participation for owner conversations.",
          "Do not treat marketing-scale numbers as property-level performance guarantees.",
        ])
      ),
      factsUsed: ["be.loyalty.programScaleStatement"],
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
    },
    {
      slotKey: "loyalty.proof",
      title: "Member Rate & Direct Booking Relevance",
      sort: 1,
      body: appendCaveat(
        bulletLines(
          [
            memberRates,
            directBooking,
            "For affiliation decisions, pair Bonvoy consumer incentives with property CRS/PMS readiness and rate-parity discipline.",
          ].filter(Boolean)
        )
      ),
      factsUsed: ["be.loyalty.memberRatesBenefit", "be.loyalty.proofDirectBookingRelevance"],
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
    },
  ];
}

function buildConservativeEarnCopy(factsByKey) {
  return {
    title: POLISHED_LOYALTY_ROW_COPY["loyalty.earn"].title,
    body: appendCaveat(
      bulletLines(
        [
          factValue(factsByKey.get("be.loyalty.earnMechanics")),
          factValue(factsByKey.get("be.loyalty.memberRatesBenefit")),
          "Participation, earn rates, and channel rules follow published Bonvoy terms for each property.",
        ].filter(Boolean)
      )
    ),
    factsUsed: ["be.loyalty.earnMechanics", "be.loyalty.memberRatesBenefit"],
    sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
  };
}

function buildConservativeRedeemCopy(factsByKey) {
  return {
    title: POLISHED_LOYALTY_ROW_COPY["loyalty.redeem"].title,
    body: appendCaveat(
      bulletLines(
        [
          factValue(factsByKey.get("be.loyalty.redeemMechanics")),
          "Reward-night availability and point requirements vary by property, date, and brand participation.",
          "Confirm redemption mechanics for a specific Tribute asset against current Bonvoy materials.",
        ].filter(Boolean)
      )
    ),
    factsUsed: ["be.loyalty.redeemMechanics"],
    sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
  };
}

function buildConservativeEliteRows(factsByKey) {
  return POLISHED_LOYALTY_ROW_COPY["loyalty.elite"].map((tier) => ({
    slotKey: "loyalty.elite",
    title: tier.title,
    sort: tier.sort,
    body: appendCaveat(
      `${tier.body} Marriott publishes additional tier benefits on official Bonvoy member materials—confirm thresholds and benefits for your market.`
    ),
    factsUsed: ["be.loyalty.eliteTierLadder"],
    sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
  }));
}

function buildConservativeProofRows(factsByKey) {
  return POLISHED_LOYALTY_ROW_COPY["loyalty.proof"].map((row) => ({
    slotKey: "loyalty.proof",
    title: row.title,
    sort: row.sort,
    body: appendCaveat(
      row.title === "Global Program Scale"
        ? bulletLines([
            factValue(factsByKey.get("be.loyalty.programScaleStatement")) ||
              row.body,
            "Use current Bonvoy materials to confirm applicable participation and terms for a specific asset.",
          ])
        : factValue(factsByKey.get("be.loyalty.memberRatesBenefit")) || row.body
    ),
    factsUsed:
      row.title === "Global Program Scale"
        ? ["be.loyalty.programScaleStatement"]
        : ["be.loyalty.memberRatesBenefit"],
    sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
  }));
}

function buildEnhancementTargets(packageMode, factsByKey) {
  if (packageMode === "conservative") {
    const earn = buildConservativeEarnCopy(factsByKey);
    const redeem = buildConservativeRedeemCopy(factsByKey);
    return [
      { slotKey: "loyalty.earn", title: earn.title, sort: 0, body: earn.body, factsUsed: earn.factsUsed, sourceUrls: earn.sourceUrls },
      { slotKey: "loyalty.redeem", title: redeem.title, sort: 0, body: redeem.body, factsUsed: redeem.factsUsed, sourceUrls: redeem.sourceUrls },
      ...buildConservativeEliteRows(factsByKey),
      ...buildConservativeProofRows(factsByKey),
    ];
  }

  const earn = buildRichEarnCopy(factsByKey);
  const redeem = buildRichRedeemCopy(factsByKey);
  return [
    { slotKey: "loyalty.earn", title: earn.title, sort: 0, body: earn.body, factsUsed: earn.factsUsed, sourceUrls: earn.sourceUrls },
    { slotKey: "loyalty.redeem", title: redeem.title, sort: 0, body: redeem.body, factsUsed: redeem.factsUsed, sourceUrls: redeem.sourceUrls },
    ...buildRichEliteRows(factsByKey),
    ...buildRichProofRows(factsByKey),
  ];
}

function findLiveRow(planned, liveRows) {
  const slotRows = liveRows.filter((r) => r.slotKey === planned.slotKey);
  if (planned.slotKey === "loyalty.earn" || planned.slotKey === "loyalty.redeem") {
    return slotRows[0] || null;
  }
  const bySort = slotRows.find((r) => Number(r.sortOrder ?? -1) === Number(planned.sort));
  if (bySort) return bySort;
  return slotRows.find((r) => nz(r.title).toLowerCase() === nz(planned.title).toLowerCase()) || null;
}

function bodiesMatch(a, b) {
  return normalizeBody(a) === normalizeBody(b);
}

function titlesMatch(a, b) {
  return nz(a) === nz(b);
}

function validatePlannedCopy(planned, packageMode) {
  const issues = [];
  if (bodyHasForbiddenCopy(planned.body)) issues.push("governance_label_in_body");
  if (bodyHasReferenceBrandLeak(planned.body)) issues.push("reference_brand_leak");
  if (bodyHasThinPlaceholder(planned.body)) issues.push("thin_placeholder_copy");
  if (packageMode === "rich" && bodyHasThinPlaceholder(planned.body)) {
    issues.push("rich_package_thin_placeholder");
  }
  for (const key of planned.factsUsed || []) {
    if (isExcludedFactKey(key)) issues.push(`excluded_fact_key:${key}`);
  }
  return issues;
}

export function buildApplyCommand(brandSlug = "tribute-portfolio", packageMode = "rich") {
  return `npm run brand-explorer-bonvoy-loyalty-row-enhancement-writer -- --brand ${brandSlug} --package ${packageMode} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_FACTS}`;
}

export async function buildBrandExplorerBonvoyLoyaltyRowEnhancementWriterReport({
  brandIdOrName = "tribute-portfolio",
  packageMode = "rich",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  approvedFactsOnlyConfirmed = false,
} = {}) {
  const normalizedPackage = nz(packageMode).toLowerCase() || "rich";
  if (!["rich", "conservative"].includes(normalizedPackage)) {
    throw new Error(`Unsupported package mode: ${packageMode} (use rich or conservative)`);
  }

  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-2H pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const allFacts = [];
  let offset = null;
  const { listPartnerFacts } = await import("./airtable-facts.js");
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    allFacts.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);

  const factsByKey = new Map();
  for (const fact of allFacts) {
    const key = nz(fact.fieldName);
    if (key) factsByKey.set(key, fact);
  }

  const requiredFactKeys =
    normalizedPackage === "rich" ? RICH_PACKAGE_FACT_KEYS : ELIGIBLE_LOYALTY_FACT_KEYS;

  const approvedFactsUsed = [];
  const factsExcluded = [];
  const pendingFactsExcluded = [];
  const internalOrFddFactsExcluded = [];

  for (const key of requiredFactKeys) {
    const fact = factsByKey.get(key);
    if (!fact) {
      factsExcluded.push({ fieldKey: key, reason: "missing_fact" });
      continue;
    }
    if (isExcludedFactKey(key)) {
      factsExcluded.push({ fieldKey: key, factRecordId: fact.id, reason: "excluded_field_key" });
      continue;
    }
    if (!isApprovedFact(fact)) {
      if (nz(fact.humanReviewStatus) === "Pending") {
        pendingFactsExcluded.push({ fieldKey: key, factRecordId: fact.id, reason: "pending_fact_excluded" });
      } else {
        factsExcluded.push({
          fieldKey: key,
          factRecordId: fact.id,
          reason: `not_approved:${nz(fact.humanReviewStatus)}`,
        });
      }
      continue;
    }
    const value = factValue(fact);
    if (factValueBlocked(value)) {
      factsExcluded.push({ fieldKey: key, factRecordId: fact.id, reason: "blocked_value_pattern" });
      continue;
    }
    approvedFactsUsed.push({
      fieldKey: key,
      factRecordId: fact.id,
      humanReviewStatus: nz(fact.humanReviewStatus),
      valuePreview: value.slice(0, 160),
    });
  }

  for (const fact of allFacts) {
    const key = nz(fact.fieldName);
    if (/^be\.standards\./i.test(key) || /^be\.meta\.fdd/i.test(key)) {
      internalOrFddFactsExcluded.push({ fieldKey: key, factRecordId: fact.id, reason: "internal_or_fdd_excluded" });
    }
    if (/^loyalty\.kpi\./i.test(key)) {
      factsExcluded.push({ fieldKey: key, factRecordId: fact.id, reason: "kpi_fact_excluded" });
    }
  }

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);

  const loyaltyRows = presentationRows.filter((r) => r.slotKey.startsWith("loyalty."));
  const targetLoyaltyRows = loyaltyRows.filter((r) => TARGET_LOYALTY_SLOTS.includes(r.slotKey));
  const heroRows = loyaltyRows.filter((r) => r.slotKey === HERO_SLOT);
  const nonTargetLoyaltyRows = loyaltyRows.filter(
    (r) => !TARGET_LOYALTY_SLOTS.includes(r.slotKey) && r.slotKey !== HERO_SLOT
  );
  const kpiRowsFound = presentationRows.filter((r) => EXCLUDED_KPI_SLOTS.includes(r.slotKey));

  const beforeDensity = analyzeLoyaltyDensity(targetLoyaltyRows);

  const plannedTargets = buildEnhancementTargets(normalizedPackage, factsByKey);
  const applyBlockers = [];

  if (pendingFactsExcluded.length) {
    applyBlockers.push(
      `pending_facts_in_plan:${pendingFactsExcluded.map((f) => f.fieldKey).join(",")}`
    );
  }
  if (factsExcluded.some((f) => requiredFactKeys.includes(f.fieldKey))) {
    applyBlockers.push(
      `missing_or_unapproved_facts:${factsExcluded
        .filter((f) => requiredFactKeys.includes(f.fieldKey))
        .map((f) => f.fieldKey)
        .join(",")}`
    );
  }

  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const rowsMatched = [];
  const beforeAfterByRow = [];
  const copyValidationIssues = [];

  for (const planned of plannedTargets) {
    const validationIssues = validatePlannedCopy(planned, normalizedPackage);
    if (validationIssues.length) {
      copyValidationIssues.push({ slotKey: planned.slotKey, title: planned.title, issues: validationIssues });
      applyBlockers.push(`copy_validation:${planned.slotKey}:${planned.title}:${validationIssues.join(",")}`);
    }

    const live = findLiveRow(planned, targetLoyaltyRows);
    if (!live) {
      const slotCount = targetLoyaltyRows.filter((r) => r.slotKey === planned.slotKey).length;
      const expected = EXPECTED_LOYALTY_ROW_COUNTS[planned.slotKey] || 0;
      if (slotCount < expected) {
        rowsWouldCreate.push({
          slotKey: planned.slotKey,
          title: planned.title,
          sort: planned.sort,
          body: planned.body,
          action: "create",
        });
      } else {
        applyBlockers.push(`missing_target_row_not_creatable:${planned.slotKey}:${planned.title}`);
      }
      beforeAfterByRow.push({
        slotKey: planned.slotKey,
        title: planned.title,
        sort: planned.sort,
        recordId: null,
        beforeTitle: null,
        beforeBody: null,
        afterTitle: planned.title,
        afterBody: planned.body,
        action: "create",
      });
      continue;
    }

    const needsUpdate =
      !bodiesMatch(live.body, planned.body) || !titlesMatch(live.title, planned.title);

    beforeAfterByRow.push({
      slotKey: planned.slotKey,
      recordId: live.recordId,
      title: planned.title,
      sort: planned.sort,
      beforeTitle: live.title,
      beforeBody: live.body,
      afterTitle: planned.title,
      afterBody: planned.body,
      action: needsUpdate ? "update" : "matched",
    });

    if (needsUpdate) {
      rowsWouldUpdate.push({
        recordId: live.recordId,
        slotKey: planned.slotKey,
        title: planned.title,
        sort: planned.sort,
        currentTitle: live.title,
        currentBody: live.body,
        proposedTitle: planned.title,
        proposedBody: planned.body,
        factsUsed: planned.factsUsed,
        fields: {
          Title: planned.title,
          Body: planned.body,
        },
      });
    } else {
      rowsMatched.push({
        recordId: live.recordId,
        slotKey: planned.slotKey,
        title: planned.title,
        action: "matched",
      });
    }
  }

  for (const slot of TARGET_LOYALTY_SLOTS) {
    const live = targetLoyaltyRows.filter((r) => r.slotKey === slot);
    const expected = EXPECTED_LOYALTY_ROW_COUNTS[slot] || 0;
    if (live.length > expected) {
      applyBlockers.push(`duplicate_rows:${slot}:${live.length}>${expected}`);
    }
  }

  if (kpiRowsFound.length) {
    applyBlockers.push(`kpi_rows_exist:${kpiRowsFound.map((r) => r.recordId).join(",")}`);
  }

  const projectedDensity = analyzeLoyaltyDensity(
    beforeAfterByRow.map((r) => ({
      slotKey: r.slotKey,
      title: r.afterTitle,
      body: r.afterBody,
    }))
  );

  const applyGatesReady =
    apply && approveBatch && founderReviewed && approvedFactsOnlyConfirmed;
  const canApply =
    applyGatesReady &&
    applyBlockers.length === 0 &&
    rowsWouldUpdate.length > 0 &&
    pendingFactsExcluded.length === 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const errors = [];
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
        errors.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          title: row.title,
        });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { updated, errors };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const earnBefore = targetLoyaltyRows.find((r) => r.slotKey === "loyalty.earn");
  const redeemBefore = targetLoyaltyRows.find((r) => r.slotKey === "loyalty.redeem");
  const earnAfter = beforeAfterByRow.find((r) => r.slotKey === "loyalty.earn");
  const redeemAfter = beforeAfterByRow.find((r) => r.slotKey === "loyalty.redeem");

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C2HWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    packageMode: normalizedPackage,
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    sourcePackages: [
      "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.md",
      "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.json",
      ENHANCEMENT_PACKAGE_JSON,
      "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.md",
      "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json",
      "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.md",
      RICH_FACT_APPROVAL_JSON,
      "reports/brand-explorer-loyalty-row-creation-writer.md",
      "reports/brand-explorer-loyalty-row-creation-writer.json",
    ],
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    approvedFactsUsed,
    factsExcluded,
    pendingFactsExcluded,
    internalOrFddFactsExcluded,
    kpiRowsExcluded: true,
    kpiRowsFound: kpiRowsFound.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
    })),
    referenceBrandCopyExcluded: true,
    existingLoyaltyRowsFound: targetLoyaltyRows.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
      bodyPreview: nz(r.body).slice(0, 140),
      sortOrder: r.sortOrder,
    })),
    loyaltyHeroUntouched: true,
    loyaltyHeroRows: heroRows.map((r) => ({
      recordId: r.recordId,
      title: r.title,
      bodyPreview: nz(r.body).slice(0, 80),
    })),
    nonTargetLoyaltyRowsUntouched: nonTargetLoyaltyRows.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
    })),
    rowsWouldUpdate,
    rowsWouldCreate,
    rowsMatched,
    beforeAfterByRow,
    densityComparison: {
      before: beforeDensity,
      after: projectedDensity,
      earnBulletsBefore: earnBefore ? countBullets(earnBefore.body) : 0,
      earnBulletsAfter: earnAfter ? countBullets(earnAfter.afterBody) : 0,
      redeemBulletsBefore: redeemBefore ? countBullets(redeemBefore.body) : 0,
      redeemBulletsAfter: redeemAfter ? countBullets(redeemAfter.afterBody) : 0,
      eliteDetailScoreBefore: beforeDensity.eliteDetailScore,
      eliteDetailScoreAfter: projectedDensity.eliteDetailScore,
    },
    proposedEnhancedEarnCopy: earnAfter?.afterBody || "",
    proposedEnhancedRedeemCopy: redeemAfter?.afterBody || "",
    proposedEnhancedEliteCopy: beforeAfterByRow
      .filter((r) => r.slotKey === "loyalty.elite")
      .map((r) => ({ title: r.afterTitle, body: r.afterBody, sort: r.sort })),
    proposedEnhancedProofCopy: beforeAfterByRow
      .filter((r) => r.slotKey === "loyalty.proof")
      .map((r) => ({ title: r.afterTitle, body: r.afterBody, sort: r.sort })),
    copyValidationIssues,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    partnerFactsUntouched: true,
    presentationRowsTouchedOnly: rowsWouldUpdate.map((r) => r.recordId),
    nonTargetSectionsModified: false,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      approvedFactsOnlyConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio", normalizedPackage),
    idempotentAfterApply: rowsWouldUpdate.length === 0 && rowsWouldCreate.length === 0,
    doesNotDo: [
      "Create loyalty.kpi.* rows",
      "Use pending or FDD/internal-only facts",
      "Change images or Sort Order",
      "Change Brand Basics or Company Validated",
      "Imply Marriott validated anything",
      "Write governance labels into UI body copy",
    ],
  };
}

export function buildBrandExplorerBonvoyLoyaltyRowEnhancementWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Bonvoy Loyalty Row Enhancement Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Package: **${report.packageMode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-2H exists: **${report.v25C2HWriterExists ? "yes" : "no"}**`,
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Approved facts used | ${report.approvedFactsUsed.length} |`,
    `| Facts excluded | ${report.factsExcluded.length} |`,
    `| Pending facts excluded | ${report.pendingFactsExcluded.length} |`,
    `| Existing loyalty target rows | ${report.existingLoyaltyRowsFound.length} |`,
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| KPI rows excluded | ${report.kpiRowsExcluded ? "yes" : "no"} |`,
    `| Reference-brand copy excluded | ${report.referenceBrandCopyExcluded ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    "",
    "## Density comparison",
    "",
    "| Metric | Before | After |",
    "|--------|--------|-------|",
    `| Earn bullets | ${report.densityComparison.earnBulletsBefore} | ${report.densityComparison.earnBulletsAfter} |`,
    `| Redeem bullets | ${report.densityComparison.redeemBulletsBefore} | ${report.densityComparison.redeemBulletsAfter} |`,
    `| Elite detail score | ${report.densityComparison.eliteDetailScoreBefore} | ${report.densityComparison.eliteDetailScoreAfter} |`,
    "",
    "## Proposed earn copy",
    "",
    "```",
    report.proposedEnhancedEarnCopy,
    "```",
    "",
    "## Proposed redeem copy",
    "",
    "```",
    report.proposedEnhancedRedeemCopy,
    "```",
    "",
    "## Before / after by row",
    "",
  ];

  for (const row of report.beforeAfterByRow) {
    lines.push(
      `### ${row.slotKey} — ${row.title || row.afterTitle}`,
      "",
      `- Record: \`${row.recordId || "create"}\` · action: **${row.action}**`,
      "",
      "**Before title:**",
      "",
      row.beforeTitle || "—",
      "",
      "**Before body:**",
      "",
      "```",
      row.beforeBody || "—",
      "```",
      "",
      "**After title:**",
      "",
      row.afterTitle,
      "",
      "**After body:**",
      "",
      "```",
      row.afterBody,
      "```",
      ""
    );
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
      `- Updated: ${report.applyResults.updated?.length || 0}`,
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
  lines.push("");

  return lines.join("\n");
}
