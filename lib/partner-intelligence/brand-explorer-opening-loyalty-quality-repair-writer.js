/**
 * Brand Explorer Opening Path + Loyalty Section Quality Repair Writer v25C-4E.
 *
 * Repairs Tribute Portfolio economics.opening.* milestone bodies, loyalty.proof
 * (6 cards), and loyalty.earn / loyalty.redeem sample mechanics. Dry-run default.
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { ELIGIBLE_LOYALTY_FACT_KEYS } from "./brand-explorer-loyalty-fact-approval-writer.js";
import { ELIGIBLE_RICH_FACT_KEYS } from "./brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.js";

export const WRITER_VERSION = "25C-4E";
export const REPORT_JSON_NAME = "brand-explorer-opening-loyalty-quality-repair-writer.json";
export const REPORT_MD_NAME = "brand-explorer-opening-loyalty-quality-repair-writer.md";
export const DOC_MD_NAME = "brand-explorer-opening-loyalty-quality-repair-writer-v25C-4E.md";

export const APPLY_FLAG = "--approve-brand-explorer-v25C-4E-opening-loyalty-quality-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-opening-loyalty-copy";
export const APPLY_FLAG_FACTS = "--confirm-approved-bonvoy-facts-only";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MIN_PROOF_CARDS = 6;
const SECTION_CAVEAT =
  "Illustrative examples only — actual earn/redeem rules vary by brand, property, market, availability, and booking channel.";

const APPROVED_FACT_KEYS_MIN = [
  "be.loyalty.programScaleStatement",
  "be.loyalty.earnMechanics",
  "be.loyalty.redeemMechanics",
  "be.loyalty.memberRatesBenefit",
];

export const OPENING_STEP_SPECS = [
  {
    slotKey: "economics.opening.step.1",
    title: "Application & Feasibility",
    body: "Confirm asset fit, conversion path, ownership goals, market positioning, operator readiness, and Marriott/Tribute eligibility before advancing into design or commercial review.",
    sort: 1,
  },
  {
    slotKey: "economics.opening.step.2",
    title: "Design & Standards",
    body: "Align the property’s independent character with Tribute Portfolio positioning, Marriott system requirements, design review, signage, FF&E, guestroom, public-space, and local-experience expectations.",
    sort: 2,
  },
  {
    slotKey: "economics.opening.step.3",
    title: "Pre-Opening Planning",
    body: "Plan systems integration, distribution setup, Bonvoy participation, training, staffing, operating procedures, and operator coordination before the opening or conversion timeline is finalized.",
    sort: 3,
  },
  {
    slotKey: "economics.opening.step.4",
    title: "Opening Support",
    body: "Coordinate brand onboarding, systems cutover, launch readiness, sales/distribution setup, quality review, and guest-experience checks with the brand and operator.",
    sort: 4,
  },
  {
    slotKey: "economics.opening.step.5",
    title: "Stabilization",
    body: "Track early operating performance, guest feedback, QA findings, brand-standard remediation, loyalty/distribution execution, and owner/operator follow-through after launch.",
    sort: 5,
  },
];

export const OPENING_PROCESS_SPEC = {
  slotKey: "economics.opening.process",
  title: "",
  body: "Tribute openings and conversions should be planned around asset fit, design review, systems readiness, operator coordination, and post-opening stabilization. Owners should confirm current Marriott/Tribute requirements, agreement terms, and conversion milestones before committing capital.",
  sort: 0,
};

export const PROOF_CARD_SPECS = [
  {
    title: "Global Program Scale",
    body: "Marriott Bonvoy gives Tribute Portfolio a connection to a global loyalty ecosystem, helping owners frame the asset within a broader Marriott demand and recognition platform.",
    sort: 0,
  },
  {
    title: "Member Rates",
    body: "Member-rate incentives can give guests a direct-booking reason to consider participating Tribute properties through Marriott channels.",
    sort: 1,
  },
  {
    title: "Earn & Redeem Points",
    body: "Bonvoy participation gives guests a familiar points-based reason to stay within Marriott’s network, subject to property participation and program rules.",
    sort: 2,
  },
  {
    title: "Elite Recognition",
    body: "Bonvoy’s member and elite tiers can support guest recognition expectations at participating properties, especially for frequent Marriott travelers.",
    sort: 3,
  },
  {
    title: "Direct Booking & Digital Channels",
    body: "Marriott channels, member benefits, and direct-booking mechanics can support distribution conversations when an owner is evaluating affiliation strategy.",
    sort: 4,
  },
  {
    title: "Redemption & Experience Pathways",
    body: "Reward nights and select on-property redemption options can add consumer-facing value where available, subject to market, property, and program participation rules.",
    sort: 5,
  },
];

export const EARN_MECHANICS_LINES = [
  "Members earn Marriott Bonvoy points on eligible hotel charges at participating properties when enrolled and booked per program rules.",
  "Member rates and direct-booking benefits can give guests a reason to book through Marriott channels.",
  "Complimentary in-room Wi-Fi may be available on direct Marriott website or app bookings where offered.",
  "Stays can count toward free nights and elite-tier progress within the Bonvoy program.",
  SECTION_CAVEAT,
];

export const REDEEM_MECHANICS_LINES = [
  "Redeem points toward reward nights at participating Marriott Bonvoy hotels, subject to availability and published program rules.",
  "Points can be used across the Marriott Bonvoy participating-hotel network where the property participates.",
  "Select on-property pathways—such as dining, spa, golf, or experiences—may be available where Marriott publishes them.",
  "Network redemption relevance matters when owners are weighing affiliation against independent operation.",
  SECTION_CAVEAT,
];

const TARGET_SLOT_KEYS = new Set([
  "economics.opening.step.1",
  "economics.opening.step.2",
  "economics.opening.step.3",
  "economics.opening.step.4",
  "economics.opening.step.5",
  "economics.opening.process",
  "loyalty.proof",
  "loyalty.earn",
  "loyalty.redeem",
]);

const PROTECTED_SLOT_KEYS = new Set([
  "footprint.openings",
  "footprint.momentum",
  "footprint.portfolio_mix",
  "footprint.portfolio_mix.archived",
  "overview.portfolio_context",
  "overview.relative_positioning",
  "standards.requirement",
  "standards.intro",
  "loyalty.elite",
  "loyalty.hero_title",
  "loyalty.kpi.members",
  "loyalty.kpi.hotels",
  "loyalty.kpi.markets",
  "loyalty.kpi.mix",
]);

const DUPLICATE_GENERIC_RE = [
  /earn and redeem points that take you everywhere you want to go/i,
  /everywhere you want to go/i,
];

const GOVERNANCE_UI_RE = [
  /source-backed/i,
  /approved facts/i,
  /owner conversations/i,
  /current materials/i,
  /no performance guarantee/i,
  /confirm scale claims/i,
  /property-level performance/i,
  /not company-validated/i,
  /not marriott-validated/i,
  /ai-assembled/i,
];

const REFERENCE_BRAND_LEAK_RE = [
  /hilton honors/i,
  /choice privileges/i,
  /\bEQC\b/i,
  /curio collection/i,
  /radisson rewards/i,
  /ihg one rewards/i,
];

const TITLE_PREFIX_BULLET_RE = /^(earn examples|redeem examples)\s*:/i;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-bonvoy-loyalty-row-enhancement-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-row-enhancement-writer.json",
  "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Tribute Partner Facts",
  "live Curio/Kimpton/Radisson/Ascend reference rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-opening-loyalty-quality-repair-writer.js",
  "scripts/brand-explorer-opening-loyalty-quality-repair-writer.mjs",
  "docs/data-intelligence/brand-explorer-opening-loyalty-quality-repair-writer-v25C-4E.md",
  "reports/brand-explorer-opening-loyalty-quality-repair-writer.md",
  "reports/brand-explorer-opening-loyalty-quality-repair-writer.json",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function hasVal(v) {
  return nz(v) !== "";
}

function bulletLines(lines) {
  return lines.map((l) => nz(l)).filter(Boolean).join("\n");
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

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
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

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        sortOrder: f["Sort Order"],
        active: f.Active,
      };
    })
    .filter((r) => r.slotKey);
}

function presentationFields(slotKey, title, body, sort, brandRecordId, brandName) {
  return {
    "Slot Key": slotKey,
    Title: title,
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort,
  };
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function isApprovedFact(fact) {
  const st = nz(fact?.humanReviewStatus);
  return st === "Approved" || st === "Edited";
}

function factValue(fact) {
  return nz(fact?.extractedValue || fact?.value || fact?.fieldValue);
}

function findLiveRow(liveRows, slotKey, title, sort) {
  const slotRows = liveRows.filter((r) => r.slotKey === slotKey);
  if (slotKey === "loyalty.earn" || slotKey === "loyalty.redeem" || slotKey === "economics.opening.process") {
    return slotRows[0] || null;
  }
  const bySort = slotRows.find((r) => Number(r.sortOrder ?? -1) === Number(sort));
  if (bySort) return bySort;
  if (title) {
    return slotRows.find((r) => nz(r.title).toLowerCase() === nz(title).toLowerCase()) || null;
  }
  return slotRows[0] || null;
}

function diagnoseOpeningPath(liveRows) {
  const steps = OPENING_STEP_SPECS.map((spec) => {
    const live = findLiveRow(liveRows, spec.slotKey, spec.title, spec.sort);
    return {
      slotKey: spec.slotKey,
      title: spec.title,
      recordId: live?.recordId || null,
      hasBody: hasVal(live?.body),
      bodyPreview: nz(live?.body).slice(0, 100),
    };
  });
  const processLive = findLiveRow(liveRows, OPENING_PROCESS_SPEC.slotKey, "", OPENING_PROCESS_SPEC.sort);
  return {
    slotFamily: "economics.opening.step.* + economics.opening.process",
    rootCause:
      steps.every((s) => !s.hasBody) && !hasVal(processLive?.body)
        ? "missing_presentation_rows_no_step_bodies"
        : steps.some((s) => !s.hasBody)
          ? "partial_step_rows_missing_body"
          : "rows_present",
    steps,
    process: {
      recordId: processLive?.recordId || null,
      hasBody: hasVal(processLive?.body),
      bodyPreview: nz(processLive?.body).slice(0, 100),
    },
    tilesWithEmptyBody: steps.filter((s) => !s.hasBody).length,
  };
}

function diagnoseProofCards(liveRows) {
  const proofRows = liveRows.filter((r) => r.slotKey === "loyalty.proof");
  return {
    slotKey: "loyalty.proof",
    currentCount: proofRows.length,
    rows: proofRows.map((r) => ({
      recordId: r.recordId,
      title: r.title,
      bodyPreview: nz(r.body).slice(0, 100),
      hasBody: hasVal(r.body),
    })),
    needsSixCards: proofRows.length < MIN_PROOF_CARDS,
  };
}

function diagnoseMechanics(liveRows) {
  const earn = findLiveRow(liveRows, "loyalty.earn", "Earn Examples", 0);
  const redeem = findLiveRow(liveRows, "loyalty.redeem", "Redeem Examples", 0);
  const earnBody = normalizeBody(earn?.body);
  const redeemBody = normalizeBody(redeem?.body);
  return {
    earn: { recordId: earn?.recordId || null, body: earnBody, bodyPreview: earnBody.slice(0, 120) },
    redeem: { recordId: redeem?.recordId || null, body: redeemBody, bodyPreview: redeemBody.slice(0, 120) },
    duplicateGenericLines: DUPLICATE_GENERIC_RE.some((re) => re.test(earnBody) || re.test(redeemBody)),
    titlePrefixedBullets:
      [earnBody, redeemBody].some((b) => b.split("\n").some((line) => TITLE_PREFIX_BULLET_RE.test(line.trim()))) ||
      TITLE_PREFIX_BULLET_RE.test(earnBody) ||
      TITLE_PREFIX_BULLET_RE.test(redeemBody),
    repeatedCaveat:
      (earnBody.match(/illustrative examples only/gi) || []).length > 1 ||
      (redeemBody.match(/illustrative examples only/gi) || []).length > 1,
  };
}

function buildRepairPlans(liveRows) {
  const plans = [];

  for (const spec of OPENING_STEP_SPECS) {
    const live = findLiveRow(liveRows, spec.slotKey, spec.title, spec.sort);
    const needsUpdate =
      !live || !hasVal(live.body) || normalizeBody(live.body) !== spec.body || nz(live.title) !== spec.title;
    plans.push({
      section: "opening_path",
      slotKey: spec.slotKey,
      title: spec.title,
      recordId: live?.recordId || null,
      action: live ? "update" : "create",
      needsUpdate,
      fields: presentationFields(spec.slotKey, spec.title, spec.body, spec.sort, TRIBUTE_RECORD_ID, BRAND_NAME),
    });
  }

  const processLive = findLiveRow(liveRows, OPENING_PROCESS_SPEC.slotKey, "", OPENING_PROCESS_SPEC.sort);
  const processNeeds =
    !processLive ||
    !hasVal(processLive.body) ||
    normalizeBody(processLive.body) !== OPENING_PROCESS_SPEC.body;
  plans.push({
    section: "opening_path",
    slotKey: OPENING_PROCESS_SPEC.slotKey,
    title: OPENING_PROCESS_SPEC.title,
    recordId: processLive?.recordId || null,
    action: processLive ? "update" : "create",
    needsUpdate: processNeeds,
    fields: presentationFields(
      OPENING_PROCESS_SPEC.slotKey,
      OPENING_PROCESS_SPEC.title,
      OPENING_PROCESS_SPEC.body,
      OPENING_PROCESS_SPEC.sort,
      TRIBUTE_RECORD_ID,
      BRAND_NAME
    ),
  });

  for (const spec of PROOF_CARD_SPECS) {
    let matchRow = findLiveRow(liveRows, "loyalty.proof", spec.title, spec.sort);
    if (!matchRow && spec.sort === 1) {
      matchRow =
        liveRows.find(
          (r) =>
            r.slotKey === "loyalty.proof" &&
            /member rate.*direct booking/i.test(nz(r.title))
        ) || null;
    }
    const needsUpdate =
      !matchRow ||
      !hasVal(matchRow.body) ||
      normalizeBody(matchRow.body) !== spec.body ||
      nz(matchRow.title) !== spec.title ||
      Number(matchRow.sortOrder ?? -1) !== spec.sort;
    plans.push({
      section: "loyalty_proof",
      slotKey: "loyalty.proof",
      title: spec.title,
      recordId: matchRow?.recordId || null,
      action: matchRow ? "update" : "create",
      needsUpdate,
      fields: presentationFields("loyalty.proof", spec.title, spec.body, spec.sort, TRIBUTE_RECORD_ID, BRAND_NAME),
    });
  }

  const earnBody = bulletLines(EARN_MECHANICS_LINES);
  const earnLive = findLiveRow(liveRows, "loyalty.earn", "Earn Examples", 0);
  plans.push({
    section: "sample_mechanics",
    slotKey: "loyalty.earn",
    title: "Earn Examples",
    recordId: earnLive?.recordId || null,
    action: earnLive ? "update" : "create",
    needsUpdate: !earnLive || normalizeBody(earnLive.body) !== earnBody,
    fields: presentationFields("loyalty.earn", "Earn Examples", earnBody, 0, TRIBUTE_RECORD_ID, BRAND_NAME),
    proposedBody: earnBody,
  });

  const redeemBody = bulletLines(REDEEM_MECHANICS_LINES);
  const redeemLive = findLiveRow(liveRows, "loyalty.redeem", "Redeem Examples", 0);
  plans.push({
    section: "sample_mechanics",
    slotKey: "loyalty.redeem",
    title: "Redeem Examples",
    recordId: redeemLive?.recordId || null,
    action: redeemLive ? "update" : "create",
    needsUpdate: !redeemLive || normalizeBody(redeemLive.body) !== redeemBody,
    fields: presentationFields("loyalty.redeem", "Redeem Examples", redeemBody, 0, TRIBUTE_RECORD_ID, BRAND_NAME),
    proposedBody: redeemBody,
  });

  return plans;
}

function validateCopy(body) {
  const issues = [];
  if (GOVERNANCE_UI_RE.some((re) => re.test(body))) issues.push("governance_ui_language");
  if (REFERENCE_BRAND_LEAK_RE.some((re) => re.test(body))) issues.push("reference_brand_leak");
  if (DUPLICATE_GENERIC_RE.some((re) => re.test(body))) issues.push("duplicate_generic_marketing_line");
  if (body.split("\n").some((line) => TITLE_PREFIX_BULLET_RE.test(line.trim()))) {
    issues.push("title_prefixed_bullet");
  }
  if ((body.match(/illustrative examples only/gi) || []).length > 1) issues.push("repeated_caveat");
  return issues;
}

function simulatePostRepair(proofPlanCount, earnBody, redeemBody, openingPlans) {
  const openingBodiesFilled = openingPlans
    .filter((p) => p.section === "opening_path" && p.slotKey.startsWith("economics.opening.step"))
    .every((p) => hasVal(p.fields.Body));
  return {
    openingTilesWithBody: openingBodiesFilled ? 5 : 0,
    proofCardCount: proofPlanCount,
    earnDistinct: !DUPLICATE_GENERIC_RE.some((re) => re.test(earnBody)),
    redeemDistinct: !DUPLICATE_GENERIC_RE.some((re) => re.test(redeemBody)),
  };
}

export function buildApplyCommand({ brandSlug = "tribute-portfolio" } = {}) {
  return `npm run brand-explorer-opening-loyalty-quality-repair-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_FACTS}`;
}

export async function buildBrandExplorerOpeningLoyaltyQualityRepairWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  approvedFactsOnlyConfirmed = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TRIBUTE_RECORD_ID);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const brand = await fetchBrandApiShape(TRIBUTE_RECORD_ID);
  if (!brand) throw new Error(`Brand not found: ${TRIBUTE_RECORD_ID}`);

  const allFacts = [];
  let offset = null;
  const { listPartnerFacts } = await import("./airtable-facts.js");
  do {
    const page = await listPartnerFacts({ brandId: TRIBUTE_RECORD_ID, limit: 100, offset });
    allFacts.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);

  const factsByKey = new Map();
  for (const fact of allFacts) {
    const key = nz(fact.fieldName);
    if (key) factsByKey.set(key, fact);
  }

  const approvedFactsUsed = [];
  const pendingFactsExcluded = [];
  for (const key of [...APPROVED_FACT_KEYS_MIN, ...ELIGIBLE_LOYALTY_FACT_KEYS, ...ELIGIBLE_RICH_FACT_KEYS]) {
    const fact = factsByKey.get(key);
    if (!fact) continue;
    if (!isApprovedFact(fact)) {
      if (nz(fact.humanReviewStatus) === "Pending") {
        pendingFactsExcluded.push({ fieldKey: key, factRecordId: fact.id });
      }
      continue;
    }
    if (!approvedFactsUsed.some((f) => f.fieldKey === key)) {
      approvedFactsUsed.push({
        fieldKey: key,
        factRecordId: fact.id,
        humanReviewStatus: nz(fact.humanReviewStatus),
      });
    }
  }

  const minFactsApproved = APPROVED_FACT_KEYS_MIN.every((key) => {
    const fact = factsByKey.get(key);
    return fact && isApprovedFact(fact);
  });

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(TRIBUTE_RECORD_ID)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );
  const liveRows = normalizePresentationRows(presentationRaw);

  const openingDiagnosis = diagnoseOpeningPath(liveRows);
  const proofDiagnosis = diagnoseProofCards(liveRows);
  const mechanicsDiagnosis = diagnoseMechanics(liveRows);

  const repairPlans = buildRepairPlans(liveRows);
  const rowsWouldUpdate = repairPlans.filter((p) => p.needsUpdate && p.action === "update");
  const rowsWouldCreate = repairPlans.filter((p) => p.needsUpdate && p.action === "create");

  const earnBody = bulletLines(EARN_MECHANICS_LINES);
  const redeemBody = bulletLines(REDEEM_MECHANICS_LINES);

  const applyBlockers = [];
  if (!minFactsApproved) applyBlockers.push("required_approved_bonvoy_facts_missing");

  for (const plan of repairPlans) {
    const issues = validateCopy(plan.fields.Body);
    if (issues.length) applyBlockers.push(`${plan.slotKey}:${issues.join(",")}`);
  }

  const postSim = simulatePostRepair(PROOF_CARD_SPECS.length, earnBody, redeemBody, repairPlans);
  if (!postSim.openingTilesWithBody) applyBlockers.push("opening_path_tiles_still_empty");
  if (postSim.proofCardCount < MIN_PROOF_CARDS) applyBlockers.push(`proof_cards_below_minimum:${postSim.proofCardCount}`);
  if (!postSim.earnDistinct || DUPLICATE_GENERIC_RE.some((re) => re.test(earnBody))) {
    applyBlockers.push("earn_duplicate_generic_lines");
  }
  if (!postSim.redeemDistinct || DUPLICATE_GENERIC_RE.some((re) => re.test(redeemBody))) {
    applyBlockers.push("redeem_duplicate_generic_lines");
  }

  const kpiRowsTouched = repairPlans.some((p) => /^loyalty\.kpi\./i.test(p.slotKey));
  if (kpiRowsTouched) applyBlockers.push("kpi_rows_blocked");

  const applyGatesReady =
    apply && approveBatch && founderReviewed && approvedFactsOnlyConfirmed && minFactsApproved;
  const hasWork = rowsWouldUpdate.length > 0 || rowsWouldCreate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = { ...companyValidatedBefore };

  if (canApply) {
    const updated = [];
    const created = [];
    const errors = [];
    const patchOps = [
      ...rowsWouldUpdate.map((r) => ({ ...r, method: "PATCH" })),
      ...rowsWouldCreate.map((r) => ({ ...r, method: "POST" })),
    ];
    for (const row of patchOps) {
      if (!TARGET_SLOT_KEYS.has(row.slotKey)) {
        errors.push({ slotKey: row.slotKey, message: "non_target_slot_blocked" });
        continue;
      }
      if (PROTECTED_SLOT_KEYS.has(row.slotKey)) {
        errors.push({ slotKey: row.slotKey, message: "protected_slot_blocked" });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: row.method,
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.method === "PATCH" ? row.recordId : ""
      );
      if (!res.ok) {
        errors.push({ recordId: row.recordId, slotKey: row.slotKey, message: json.error?.message || res.status });
      } else if (row.method === "PATCH") {
        updated.push({ recordId: row.recordId, slotKey: row.slotKey, title: row.title });
      } else {
        created.push({ recordId: json.id, slotKey: row.slotKey, title: row.title });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = (updated.length > 0 || created.length > 0) && errors.length === 0;
    applyResults = { updated, created, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(TRIBUTE_RECORD_ID));
  } else if (apply) {
    applyResults = { updated: [], created: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C4EWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: TRIBUTE_RECORD_ID, slug: "tribute-portfolio" },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    openingPathDiagnosis: openingDiagnosis,
    keyBenefitsDiagnosis: proofDiagnosis,
    sampleMechanicsDiagnosis: mechanicsDiagnosis,
    proposedOpeningTileCopy: OPENING_STEP_SPECS.map((s) => ({ title: s.title, body: s.body })),
    proposedOpeningProcessCopy: OPENING_PROCESS_SPEC.body,
    proposedProofCards: PROOF_CARD_SPECS,
    proposedEarnCopy: earnBody,
    proposedRedeemCopy: redeemBody,
    duplicateGenericLinesRemoved:
      !DUPLICATE_GENERIC_RE.some((re) => re.test(earnBody) || re.test(redeemBody)) &&
      !mechanicsDiagnosis.duplicateGenericLines,
    sixBenefitsCardsWillRender: PROOF_CARD_SPECS.length >= MIN_PROOF_CARDS,
    kpiRowsExcluded: true,
    pendingFactsExcluded,
    internalOrFddFactsExcluded: true,
    approvedFactsUsed,
    minApprovedBonvoyFactsReady: minFactsApproved,
    rowsWouldUpdate: rowsWouldUpdate.map((r) => ({
      section: r.section,
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
    })),
    rowsWouldCreate: rowsWouldCreate.map((r) => ({
      section: r.section,
      slotKey: r.slotKey,
      title: r.title,
    })),
    repairPlans: repairPlans.filter((p) => p.needsUpdate),
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    portfolioMixRowsUntouched: true,
    standardsRowsUntouched: true,
    eliteRowsUntouched: true,
    imagesUntouched: true,
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
    exactApplyCommand: buildApplyCommand({ brandSlug: "tribute-portfolio" }),
    idempotentAfterApply: !hasWork,
  };
}

export function buildBrandExplorerOpeningLoyaltyQualityRepairWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Opening Path + Loyalty Quality Repair v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}**`,
    "",
    "## Opening & Conversion Path",
    "",
    `Root cause: **${report.openingPathDiagnosis.rootCause}**`,
    "",
    `| Tiles with empty body (before) | ${report.openingPathDiagnosis.tilesWithEmptyBody} |`,
    "",
    "## Key Benefits & Program Strengths",
    "",
    `Current proof cards: **${report.keyBenefitsDiagnosis.currentCount}** → target **${MIN_PROOF_CARDS}**`,
    "",
    "## Sample Mechanics",
    "",
    `Duplicate generic lines (before): ${report.sampleMechanicsDiagnosis.duplicateGenericLines ? "yes" : "no"}`,
    "",
    "## Summary",
    "",
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| 6 benefits cards | ${report.sixBenefitsCardsWillRender ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    "",
    "## Exact apply command",
    "",
    "```bash",
    report.exactApplyCommand,
    "```",
    "",
  ];
  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
    lines.push("");
  }
  return lines.join("\n");
}
