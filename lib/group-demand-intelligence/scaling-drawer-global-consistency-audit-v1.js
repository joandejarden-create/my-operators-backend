/**
 * Global Scaling Companies drawer consistency audit V1
 *
 * Permanent reusable contract checker: inventory → sanitize → render → score.
 * Structural / presentation only — commercial research quality is NOT assessed.
 */

import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { sanitizeOpportunityForCustomerPresentation } from "./customer-presentation-sanitize-v1.js";
import {
  hasAssociationProcurementBoilerplate,
  isScalingCompaniesSegment,
  SCALING_DRAWER_PRESENTATION_CONTRACT_V1,
} from "./scaling-drawer-presentation-contract-v1.js";
import {
  SCALING_CANONICAL_SECTION_IDS,
  SCALING_CANONICAL_SECTION_TITLES,
  extractScalingSectionIdsFromHtml,
  assertCanonicalSectionOrder,
} from "./scaling-drawer-normalized-model-v1.js";

export const SCALING_DRAWER_GLOBAL_AUDIT_V1 =
  "gdi-scaling-drawer-global-consistency-audit-v1";

/**
 * Master section contract V2 — aligned to SCALING_CANONICAL_SECTION_IDS.
 * Legacy optional headings (Commercial context / Why Dealality selected /
 * Company background) are FORBIDDEN as top-level sections.
 */
export const SCALING_DRAWER_MASTER_SECTION_CONTRACT = Object.freeze({
  version: "gdi-scaling-drawer-master-contract-v2",
  canonicalSectionIds: [...SCALING_CANONICAL_SECTION_IDS],
  requiredHeadingsInOrder: SCALING_CANONICAL_SECTION_IDS.map(
    (id) => SCALING_CANONICAL_SECTION_TITLES[id],
  ),
  /** No conditional omissions — all ten sections are mandatory. */
  conditionalHeadingsInOrder: [],
  /** Nested labels allowed inside canonical sections; never top-level alternatives. */
  allowedOptionalHeadings: [],
  /** Forbidden as top-level H3 (legacy Scaling branches + Association). */
  forbiddenTopLevelHeadings: [
    "Commercial context",
    "Why Dealality selected this company",
    "Company background and local presence",
    "Advantages",
    "Limitations / trade-offs",
    "Uncertainties",
    "Why act now / timing",
  ],
  forbiddenAssociationHeadings: [
    "Account / occasion",
    "Who could travel / why hotel business",
    "Why investigate now",
    "Suggested action",
    "Limitations / competing arrangements",
    "Early-warning lodging status",
    "Event cycle",
    "Attendance",
    "Peak rooms",
  ],
  forbiddenAssociationPatterns: [
    /Peak rooms:/i,
    /Attendance:/i,
    /association staff\s*vs\s*PCO/i,
    /host\/overflow/i,
    /Account \/ occasion/i,
    /Delegate lodging/i,
    /Scientific programme/i,
    /Event edition/i,
  ],
  corporateFrameworkClass: "gdi-detail-framework--corporate-account",
  scalingCanonicalClass: "gdi-detail-framework--scaling-canonical",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const HOTELS_DIR = path.join(ROOT, "data/group-demand-intelligence/hotels");
const UI_PATH = path.join(
  ROOT,
  "public/js/group-demand-intelligence/dealality-gdi-ui.js",
);

let _uiCache = null;

export function loadDealalityGdiUi() {
  if (_uiCache) return _uiCache;
  const code = fs.readFileSync(UI_PATH, "utf8");
  const sandbox = { window: {}, console };
  sandbox.window = sandbox;
  vm.runInNewContext(code, sandbox);
  _uiCache = sandbox.DealalityGdiUi;
  return _uiCache;
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function extractSectionHeadings(html) {
  const headings = [];
  const re = /<h3[^>]*>([\s\S]*?)<\/h3>/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    headings.push(
      String(m[1] || "")
        .replace(/<[^>]+>/g, "")
        .replace(/\s+/g, " ")
        .trim(),
    );
  }
  return headings;
}

/** Top-level Scaling section titles only (direct child h3 of data-section-id sections). */
export function extractTopLevelScalingHeadings(html) {
  const headings = [];
  const re =
    /<section[^>]*data-section-id=["'][a-z_]+["'][^>]*>\s*<h3[^>]*>([\s\S]*?)<\/h3>/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    headings.push(
      String(m[1] || "")
        .replace(/<[^>]+>/g, "")
        .replace(/\s+/g, " ")
        .trim(),
    );
  }
  return headings;
}

function orderOk(actual, requiredSubset) {
  let last = -1;
  for (const h of requiredSubset) {
    const i = actual.indexOf(h);
    if (i < 0) return { ok: false, missing: h };
    if (i < last) return { ok: false, outOfOrder: h };
    last = i;
  }
  return { ok: true };
}

function resolveHotelName(hotelId, configLoader) {
  try {
    const c = configLoader?.(hotelId);
    return c?.displayName || c?.hotelName || hotelId;
  } catch {
    return hotelId;
  }
}

/**
 * Discover all Scaling Companies opportunities from canonical hotel stores.
 */
export function inventoryScalingOpportunities({
  hotelsDir = HOTELS_DIR,
  loadHotelConfig = null,
} = {}) {
  if (!fs.existsSync(hotelsDir)) {
    return { hotels: [], opportunities: [], at: new Date().toISOString() };
  }
  const hotelIds = fs
    .readdirSync(hotelsDir, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name);

  const opportunities = [];
  const hotels = [];

  for (const hotelId of hotelIds) {
    const storePath = path.join(hotelsDir, hotelId, "opportunities.json");
    if (!fs.existsSync(storePath)) {
      hotels.push({
        hotelId,
        hotelName: resolveHotelName(hotelId, loadHotelConfig),
        storeMissing: true,
        total: 0,
        scalingCount: 0,
      });
      continue;
    }
    const doc = JSON.parse(fs.readFileSync(storePath, "utf8"));
    const ops = Array.isArray(doc.opportunities)
      ? doc.opportunities
      : Array.isArray(doc)
        ? doc
        : [];
    const hotelName = resolveHotelName(hotelId, loadHotelConfig);
    const scaling = [];
    for (const o of ops) {
      const segScaling = isScalingCompaniesSegment(o);
      const titleScaling =
        /scaling corporate account|scaling companies/i.test(text(o.title)) &&
        String(o.opportunityType || "").toUpperCase() ===
          "CORPORATE_ACCOUNT_RELATIONSHIP";
      if (!segScaling && !titleScaling) continue;
      const row = {
        hotelId,
        hotelName,
        opportunityId: o.opportunityId || o.id,
        title: o.title || null,
        organizationName: o.organizationName || o.company || o.companyName || null,
        researchSegment: o.researchSegment || o.segment || null,
        opportunityType: o.opportunityType || null,
        detailLayout: o.detailLayout || null,
        qualification:
          o.opportunityQualification ||
          o.scalingQualificationStatus ||
          o.qualification ||
          null,
        customerVisibility:
          o.customerFacingState ||
          o.publicationStatus ||
          o.visibility ||
          o.customerVisibility ||
          null,
        hasCIB: Boolean(o.commercialIntelligenceBrief),
        hasAEGB: Boolean(o.associationEventsGoldBrief),
        hasSB: Boolean(o.salesBrief),
        hasVerifiedNamedRelevantContact: o.hasVerifiedNamedRelevantContact === true,
        storePath,
        storeMtime: fs.statSync(storePath).mtime.toISOString(),
        classificationBasis: segScaling
          ? "researchSegment"
          : "title_and_corporate_type",
        _raw: o,
      };
      scaling.push(row);
      opportunities.push(row);
    }
    hotels.push({
      hotelId,
      hotelName,
      storePath,
      storeMtime: fs.statSync(storePath).mtime.toISOString(),
      total: ops.length,
      scalingCount: scaling.length,
      storeMissing: false,
    });
  }

  return {
    at: new Date().toISOString(),
    auditVersion: SCALING_DRAWER_GLOBAL_AUDIT_V1,
    contractVersion: SCALING_DRAWER_MASTER_SECTION_CONTRACT.version,
    presentationContract: SCALING_DRAWER_PRESENTATION_CONTRACT_V1,
    hotels,
    opportunities,
    opportunityCount: opportunities.length,
    hotelsWithScaling: hotels.filter((h) => h.scalingCount > 0).length,
  };
}

/**
 * Audit one opportunity: sanitize → render → contract checks.
 */
export function auditOneScalingOpportunity(row, { ui = null, hotelDisplayName = null } = {}) {
  const UI = ui || loadDealalityGdiUi();
  const raw = row._raw || row;
  const hotelName = hotelDisplayName || row.hotelName || row.hotelId;

  let sanitized = null;
  let sanitizeError = null;
  try {
    sanitized = sanitizeOpportunityForCustomerPresentation({
      ...raw,
      hotelId: row.hotelId,
      hotelDisplayName: hotelName,
    });
  } catch (e) {
    sanitizeError = e?.message || String(e);
  }

  let html = null;
  let renderError = null;
  if (sanitized && UI?.intelligenceDetailHtml) {
    try {
      html = UI.intelligenceDetailHtml(
        sanitized,
        {},
        {
          whoContactHtml: "",
          validationHtml:
            '<div class="gdi-validation-panel">Hotel Validation / Action / Outcome</div>',
        },
      );
    } catch (e) {
      renderError = e?.message || String(e);
    }
  }

  if (!sanitized || !html) {
    return {
      hotelId: row.hotelId,
      hotelName,
      opportunityId: row.opportunityId,
      title: row.title,
      organizationName: row.organizationName,
      customerVisibility: row.customerVisibility,
      STRUCTURE: "NOT_VERIFIED",
      LAYOUT: "NOT_VERIFIED",
      FIELD_FIDELITY: "NOT_VERIFIED",
      CONTACT_UI: "NOT_VERIFIED",
      FEEDBACK_UI: "NOT_VERIFIED",
      COMMERCIAL_CONTENT_QUALITY: "NOT_ASSESSED",
      overall: "NOT_VERIFIED",
      sanitizeError,
      renderError,
      discrepancies: [
        {
          severity: "P0",
          issue: sanitizeError || renderError || "Could not sanitize/render",
          rootCause: "render_or_sanitize_failure",
        },
      ],
    };
  }

  const headingsAll = extractSectionHeadings(html);
  const headings =
    extractTopLevelScalingHeadings(html).length >=
    SCALING_CANONICAL_SECTION_IDS.length
      ? extractTopLevelScalingHeadings(html)
      : headingsAll;
  const contract = SCALING_DRAWER_MASTER_SECTION_CONTRACT;
  const discrepancies = [];

  const hasCorporate = html.includes(contract.corporateFrameworkClass);
  const hasCanonical = html.includes(contract.scalingCanonicalClass);
  if (!hasCorporate) {
    discrepancies.push({
      severity: "P0",
      issue: "Not using corporate-account drawer framework",
      rootCause: "wrong_template_selection",
      evidence: headings.slice(0, 8),
    });
  }
  if (!hasCanonical) {
    discrepancies.push({
      severity: "P0",
      issue: "Missing scaling-canonical framework class",
      rootCause: "wrong_template_selection",
    });
  }

  // Strict: data-section-id array must equal canonical 10 (not heading-subset).
  const sectionIds = extractScalingSectionIdsFromHtml(html).filter((id) =>
    SCALING_CANONICAL_SECTION_IDS.includes(id),
  );
  // Prefer unique top-level sequence: first occurrence order of each canonical id
  const topIds = [];
  for (const id of extractScalingSectionIdsFromHtml(html)) {
    if (SCALING_CANONICAL_SECTION_IDS.includes(id) && !topIds.includes(id)) {
      topIds.push(id);
    }
  }
  const idOrder = assertCanonicalSectionOrder(topIds);
  if (!idOrder.ok) {
    discrepancies.push({
      severity: "P0",
      issue: "Canonical section-ID order mismatch",
      rootCause: "section_contract_violation",
      expected: idOrder.expected,
      actual: idOrder.actual,
    });
  }

  // Duplicate top-level headings
  const h3Counts = {};
  for (const h of headings) {
    h3Counts[h] = (h3Counts[h] || 0) + 1;
  }
  for (const [h, n] of Object.entries(h3Counts)) {
    if (n > 1 && contract.requiredHeadingsInOrder.includes(h)) {
      discrepancies.push({
        severity: "P0",
        issue: `Duplicate top-level heading: ${h} (×${n})`,
        rootCause: "duplicate_heading",
      });
    }
  }

  // Empty bullets
  if (/<li>\s*<\/li>/i.test(html) || /<li>\s*—\s*<\/li>/i.test(html)) {
    discrepancies.push({
      severity: "P0",
      issue: "Empty or placeholder list bullets rendered",
      rootCause: "empty_bullets",
    });
  }

  for (const bad of [
    ...contract.forbiddenAssociationHeadings,
    ...(contract.forbiddenTopLevelHeadings || []),
  ]) {
    if (headings.includes(bad)) {
      discrepancies.push({
        severity: "P0",
        issue: `Forbidden top-level heading present: ${bad}`,
        rootCause: bad.startsWith("Account") || /occasion|investigate|Suggested|Event|Attendance|Peak/i.test(bad)
          ? "association_template_or_contamination"
          : "section_contract_violation",
      });
    }
  }
  for (const re of contract.forbiddenAssociationPatterns) {
    if (re.test(html)) {
      discrepancies.push({
        severity: "P0",
        issue: `Association pattern in HTML: ${re}`,
        rootCause: "association_boilerplate_leak",
      });
    }
  }

  const requiredOrder = orderOk(headings, contract.requiredHeadingsInOrder);
  if (!requiredOrder.ok) {
    discrepancies.push({
      severity: "P0",
      issue: requiredOrder.missing
        ? `Missing required section: ${requiredOrder.missing}`
        : `Required section out of order: ${requiredOrder.outOfOrder}`,
      rootCause: "section_contract_violation",
      headings,
    });
  }

  // Nested-only labels (Advantages / Limitations) may appear as <strong>, not H3 —
  // if they appear as H3 they are already caught via forbiddenTopLevelHeadings.

  // Unknown top-level H3s outside the canonical ten
  const allowed = new Set([...contract.requiredHeadingsInOrder]);
  for (const h of headings) {
    if (!allowed.has(h) && !/^Pursuit$/i.test(h)) {
      // Nested validation subheads inside hotel_validation are still H3 in some UIs
      if (/^Validation$|^Action$|^Outcome$/i.test(h)) continue;
      discrepancies.push({
        severity: "P0",
        issue: `Unexpected section heading: ${h}`,
        rootCause: "unexpected_heading",
      });
    }
  }

  // Field fidelity / contamination
  if (hasAssociationProcurementBoilerplate(sanitized.recommendedAction)) {
    discrepancies.push({
      severity: "P0",
      issue: "Association procurement language in recommendedAction",
      rootCause: "gold_brief_sot_or_unscrubbed_action",
    });
  }
  if (sanitized.detailLayout !== "CORPORATE_ACCOUNT_RELATIONSHIP") {
    discrepancies.push({
      severity: "P0",
      issue: `detailLayout=${sanitized.detailLayout}`,
      rootCause: "layout_fields_not_normalized",
    });
  }
  if (/OPEN[ _]?ENDED/i.test(text(sanitized.whyNow))) {
    discrepancies.push({
      severity: "P1",
      issue: "Internal OPEN_ENDED timing leaked",
      rootCause: "enum_leakage",
    });
  }
  if (/#{1,6}\s|Skip to main content/i.test(text(sanitized.summaryWhat))) {
    discrepancies.push({
      severity: "P0",
      issue: "Scraped webpage markers in summary",
      rootCause: "scrape_contamination",
    });
  }

  // Contact UI — structural presence only
  const contactSection = /Who to contact/i.test(html);
  const contactUiPass = contactSection;
  if (!contactUiPass) {
    discrepancies.push({
      severity: "P0",
      issue: "Who to contact section missing",
      rootCause: "contact_section_missing",
    });
  }

  // Feedback UI — validation panel must remain
  const feedbackPass = /Hotel Validation \/ Action \/ Outcome|gdi-validation-panel/i.test(
    html,
  );
  if (!feedbackPass) {
    discrepancies.push({
      severity: "P0",
      issue: "Validation / Action / Outcome panel missing from render extras",
      rootCause: "feedback_controls_not_wired_in_test_harness_or_template",
    });
  }

  const structureFail = discrepancies.some(
    (d) =>
      d.severity === "P0" &&
      /template|heading|section_contract|Association|layout_fields|procurement|scrape|empty_bullets|duplicate|unexpected|boilerplate_leak/i.test(
        `${d.rootCause} ${d.issue}`,
      ),
  );
  const layoutFail =
    !hasCorporate ||
    !hasCanonical ||
    discrepancies.some((d) =>
      /wrong_template|Association heading|Forbidden top-level/i.test(
        `${d.rootCause} ${d.issue}`,
      ),
    );
  const fieldFail = discrepancies.some((d) =>
    /sot|boilerplate|enum|scrape|fidelity/i.test(`${d.rootCause} ${d.issue}`),
  );

  const STRUCTURE =
    structureFail || !idOrder.ok || !requiredOrder.ok ? "FAIL" : "PASS";
  const LAYOUT = layoutFail ? "FAIL" : "PASS";
  const FIELD_FIDELITY = fieldFail ? "FAIL" : "PASS";
  const CONTACT_UI = contactUiPass ? "PASS" : "FAIL";
  const FEEDBACK_UI = feedbackPass ? "PASS" : "FAIL";

  const hardFail = [STRUCTURE, LAYOUT, FIELD_FIDELITY, CONTACT_UI, FEEDBACK_UI].some(
    (v) => v === "FAIL",
  );

  return {
    hotelId: row.hotelId,
    hotelName,
    opportunityId: row.opportunityId,
    title: row.title,
    organizationName: row.organizationName,
    researchSegment: sanitized.researchSegment || row.researchSegment,
    customerVisibility: row.customerVisibility,
    drawerTemplate: hasCorporate
      ? "CORPORATE_ACCOUNT_RELATIONSHIP"
      : "OTHER_OR_ASSOCIATION",
    sectionIds: topIds,
    sectionSequence: headings,
    presentationSourceOfTruth: sanitized.presentationSourceOfTruth || null,
    detailLayout: sanitized.detailLayout || null,
    hasCIB: Boolean(sanitized.commercialIntelligenceBrief),
    hasAEGB: Boolean(sanitized.associationEventsGoldBrief),
    recommendedActionPreview: text(sanitized.recommendedAction).slice(0, 220),
    whyThisHotelPreview: text(sanitized.whyThisHotel).slice(0, 220),
    STRUCTURE,
    LAYOUT,
    FIELD_FIDELITY,
    CONTACT_UI,
    FEEDBACK_UI,
    COMMERCIAL_CONTENT_QUALITY: "NOT_ASSESSED",
    overall: hardFail ? "FAIL" : "STRUCTURE_PASS",
    discrepancies,
    htmlLength: html.length,
  };
}

export function runGlobalScalingDrawerConsistencyAudit(opts = {}) {
  let loadHotelConfig = opts.loadHotelConfig || null;
  if (!loadHotelConfig) {
    try {
      // Lazy — avoid hard fail if profile module changes
      const mod = awaitableHotelConfig();
      loadHotelConfig = mod;
    } catch {
      loadHotelConfig = null;
    }
  }

  const inventory = inventoryScalingOpportunities({
    hotelsDir: opts.hotelsDir || HOTELS_DIR,
    loadHotelConfig,
  });
  const ui = loadDealalityGdiUi();
  const results = [];
  for (const row of inventory.opportunities) {
    // Drop heavy raw from inventory result copies later
    results.push(
      auditOneScalingOpportunity(row, {
        ui,
        hotelDisplayName: row.hotelName,
      }),
    );
  }

  const byHotel = {};
  for (const r of results) {
    if (!byHotel[r.hotelId]) {
      byHotel[r.hotelId] = {
        hotelId: r.hotelId,
        hotelName: r.hotelName,
        total: 0,
        structurePass: 0,
        fail: 0,
        notVerified: 0,
      };
    }
    const h = byHotel[r.hotelId];
    h.total += 1;
    if (r.overall === "STRUCTURE_PASS") h.structurePass += 1;
    else if (r.overall === "NOT_VERIFIED") h.notVerified += 1;
    else h.fail += 1;
  }

  const rootCauses = {};
  for (const r of results) {
    for (const d of r.discrepancies || []) {
      const k = d.rootCause || "unspecified";
      if (!rootCauses[k]) rootCauses[k] = { rootCause: k, count: 0, opportunityIds: [] };
      rootCauses[k].count += 1;
      rootCauses[k].opportunityIds.push(r.opportunityId);
    }
  }

  // Strip _raw from inventory for JSON export
  const inventoryClean = {
    ...inventory,
    opportunities: inventory.opportunities.map(({ _raw, ...rest }) => rest),
  };

  return {
    at: new Date().toISOString(),
    auditVersion: SCALING_DRAWER_GLOBAL_AUDIT_V1,
    masterContract: SCALING_DRAWER_MASTER_SECTION_CONTRACT,
    inventory: inventoryClean,
    results,
    summary: {
      audited: results.length,
      structurePass: results.filter((r) => r.overall === "STRUCTURE_PASS").length,
      fail: results.filter((r) => r.overall === "FAIL").length,
      notVerified: results.filter((r) => r.overall === "NOT_VERIFIED").length,
      byHotel: Object.values(byHotel),
      rootCauses: Object.values(rootCauses).sort((a, b) => b.count - a.count),
    },
  };
}

function awaitableHotelConfig() {
  // sync dynamic import alternative: read config JSON files
  return (hotelId) => {
    const p = path.join(
      ROOT,
      "config/group-demand-intelligence/hotels",
      `${hotelId}.json`,
    );
    if (!fs.existsSync(p)) return { displayName: hotelId };
    try {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      return { displayName: j.displayName || j.hotelName || hotelId };
    } catch {
      return { displayName: hotelId };
    }
  };
}
