/**
 * GDI Full Customer Consistency Audit V1
 *
 * Portfolio-wide deterministic + cross-field commercial validation for every
 * customer-facing opportunity surface. Does not rewrite evidence or change
 * eligibility/publication policy — report-only unless a caller remediates.
 *
 * Layers:
 *   A) Deterministic presentation (titles, narratives, UI contracts)
 *   B) Cross-field commercial (competitor vs target sourcing, actions)
 *   C) Semantic review-required flags (ambiguous wording for founder review)
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  customerFacingOpportunityTitle,
  GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1,
  GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_REVISION,
} from "../customer-facing-opportunity-title-contract-v1.js";
import { toOpportunityListDto } from "../opportunity-list-dto.js";
import {
  annotateInventoryPopulations,
  applyCmiLedgerRemediation,
} from "../bethesda-67-sales-baseline-cmi-v1.js";
import { sanitizeCommercialPresentation } from "../commercial-consistency/commercial-presentation-sanitizer-v1.js";
import { composeCustomerFacingHotelSourcingLabel } from "../commercial-consistency/commercial-status-dimensions-v1.js";
import {
  detectOpportunityContradictions,
  CONTRADICTION_SEVERITY,
} from "../commercial-consistency/commercial-contradiction-detector-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../opportunity-type-filter-v1.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";

export const GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1 =
  "gdi-full-customer-consistency-audit-v1";

export const ISSUE_SEVERITY = Object.freeze({
  P0: "P0",
  P1: "P1",
  P2: "P2",
  P3: "P3",
  REVIEW: "REVIEW_REQUIRED",
});

export const ISSUE_CATEGORY = Object.freeze({
  TITLE: "TITLE",
  SOURCING: "SOURCING",
  NARRATIVE: "NARRATIVE",
  CLASSIFICATION: "CLASSIFICATION",
  COMMERCIAL: "COMMERCIAL",
  COMPETITIVE: "COMPETITIVE",
  CONTACT: "CONTACT",
  UI_CONTRACT: "UI_CONTRACT",
  FEEDBACK: "FEEDBACK",
  DATES: "DATES",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const TITLE_DIRTY_RE =
  /—\s*.*\b(Host|Competitor Room Block|Marriott|Hilton|Hyatt|WEWCC|Overflow Watch|Early Watch)\b/i;
const MALFORMED_NARRATIVE_RE =
  /Qualify Overflow\s*[—–-]\s*Qualify Overflow|dated in Host hotel contracted|\bis Host hotel contracted\b/i;
const OPEN_RFP_RE = /request\s+rfp|send\s+.*\s*rfp|rfp\s*\/\s*site/i;
const VENUE_STATUS_AS_LOCATION_RE =
  /host\s+hotel\s+contracted|competitor\s+(?:venue|hotel|booking)\b/i;

/** Canonical customer-facing field → source-of-truth map (Phase 1). */
export const CUSTOMER_FACING_FIELD_SOT = Object.freeze([
  {
    field: "displayTitle",
    source: "customer-facing-opportunity-title-contract-v1 + CMI ledger rem.title",
    transform: "applyCustomerFacingOpportunityTitleContract / toOpportunityListDto",
    ui: "card name, drawer header, overview identity",
    export: "CSV Opportunity Name / displayTitle",
    tests: "test:gdi-customer-facing-opportunity-title-contract-v1; test:gdi-title-sourcing-commercial-consistency-v1",
  },
  {
    field: "hotelSourcingStatusLabel",
    source: "composeCustomerFacingHotelSourcingLabel (competitorVenueStatus × targetHotelSourcingStatus)",
    transform: "sanitizeCommercialPresentation → DTO → overview.hotelSourcing",
    ui: "card meta, overview Hotel Sourcing",
    export: "CSV Hotel Sourcing",
    tests: "test:gdi-title-sourcing-commercial-consistency-v1",
  },
  {
    field: "demandSegment / opportunityType",
    source: "demandSegmentId + opportunity-type-filter-v1 (14 segments)",
    transform: "normalizeOpportunityDemandClassification; never CMI as type",
    ui: "Opportunity Type filter + segment labels",
    export: "CSV Demand Segment",
    tests: "test:gdi-opportunity-type-classification-v1",
  },
  {
    field: "marketScope / inventoryPopulations",
    source: "annotateInventoryPopulations (HOTEL / COMPETITIVE / BOTH)",
    transform: "Market Scope radiogroup only — not Opportunity Type",
    ui: "Market Scope All/Hotel/Competitive",
    export: "population flags when present",
    tests: "test:gdi-market-scope-toggle-v1",
  },
  {
    field: "recommendedAction",
    source: "recommendCommercialAction + ledger remediation + drawer model",
    transform: "sanitizeRecommendedActionForSourcing; strip action-prefix dupes",
    ui: "sales-action pill + Recommended Next Step",
    export: "CSV Recommended Action",
    tests: "test:gdi-title-sourcing-commercial-consistency-v1; commercial-contradiction-detector",
  },
  {
    field: "primaryContact / tileContactPreview",
    source: "sanitizeOpportunityForCustomerPresentation → universal-tile-contact-preview",
    transform: "scalingTwoContactStandard + Surfe on-demand reveal",
    ui: "card contact footer + Who to Contact",
    export: "CSV contact columns",
    tests: "test:gdi-surfe-on-demand-contact-reveal-v1; test:gdi-compact-tile-contact-v1",
  },
  {
    field: "hotelFeedback (Validation/Action/Outcome)",
    source: "feedback.json / Airtable hotel feedback persistence",
    transform: "presentation-only; never rewritten by research sanitize",
    ui: "drawer Hotel Validation / Action / Outcome",
    export: "not commercial CSV overwrite",
    tests: "hotel-feedback persistence suites",
  },
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function issue(partial) {
  return {
    issueId: partial.issueId,
    severity: partial.severity,
    category: partial.category,
    hotelId: partial.hotelId || null,
    opportunityId: partial.opportunityId || null,
    field: partial.field || null,
    message: partial.message,
    evidence: partial.evidence || {},
    remediationStatus: partial.remediationStatus || "OPEN",
    layer: partial.layer || "A",
  };
}

function mapContradictionSeverity(sev) {
  if (sev === CONTRADICTION_SEVERITY.CRITICAL) return ISSUE_SEVERITY.P0;
  if (sev === CONTRADICTION_SEVERITY.HIGH) return ISSUE_SEVERITY.P1;
  if (sev === CONTRADICTION_SEVERITY.MEDIUM) return ISSUE_SEVERITY.P2;
  return ISSUE_SEVERITY.P3;
}

function presentOpportunity(raw, hotelId) {
  const rem = sanitizeCommercialPresentation(applyCmiLedgerRemediation(raw));
  const annotated = annotateInventoryPopulations([rem], { hotelId })[0] || rem;
  const dto = toOpportunityListDto(annotated, { hotelId });
  return { rem: annotated, dto };
}

function auditOpportunity(raw, hotelId, seq) {
  const issues = [];
  const { rem, dto } = presentOpportunity(raw, hotelId);
  const id = String(dto.id || rem.id || raw.id || `anon_${seq}`);
  const prefix = `GDI-CCA-${hotelId || "hotel"}-${id}`;

  const displayTitle = text(dto.displayTitle || rem.displayTitle || "");
  const sourceTitle = text(
    rem.originalSourceTitle || rem.researchTitlePreserved || raw.title || ""
  );
  const contractTitle = customerFacingOpportunityTitle(
    rem.title || raw.title || displayTitle,
    {
      competitorHotel:
        rem.competitorHotel || rem.cmiCompetitorHotel || raw.competitorHotel,
    }
  );

  if (TITLE_DIRTY_RE.test(displayTitle)) {
    issues.push(
      issue({
        issueId: `${prefix}-TITLE-DIRTY`,
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.TITLE,
        hotelId,
        opportunityId: id,
        field: "displayTitle",
        message: "Customer-facing title still carries competitor/host/venue suffix.",
        evidence: { displayTitle, contractTitle, sourceTitle },
        layer: "A",
      })
    );
  }
  if (displayTitle && contractTitle && displayTitle !== contractTitle) {
    // Soft — may be ledger-preferred official name vs contract strip of raw
    if (TITLE_DIRTY_RE.test(displayTitle) || /\(overflow\)/i.test(displayTitle)) {
      issues.push(
        issue({
          issueId: `${prefix}-TITLE-CONTRACT-DRIFT`,
          severity: ISSUE_SEVERITY.P1,
          category: ISSUE_CATEGORY.TITLE,
          hotelId,
          opportunityId: id,
          field: "displayTitle",
          message: "DTO displayTitle drifts from title contract output.",
          evidence: { displayTitle, contractTitle },
          layer: "A",
        })
      );
    }
  }

  const sourcingLabel = text(dto.hotelSourcingStatusLabel || "");
  const ovSourcing = text(dto.eventsOpportunityOverview?.hotelSourcing || "");
  if (
    sourcingLabel &&
    ovSourcing &&
    sourcingLabel !== ovSourcing &&
    (/^Venue Confirmed$/i.test(ovSourcing) ||
      /^Hotel Sourcing Unconfirmed$/i.test(ovSourcing))
  ) {
    issues.push(
      issue({
        issueId: `${prefix}-SOURCING-COLLAPSE`,
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.SOURCING,
        hotelId,
        opportunityId: id,
        field: "eventsOpportunityOverview.hotelSourcing",
        message:
          "Overview hotelSourcing collapses competitor-vs-target dimensions vs composed label.",
        evidence: { sourcingLabel, ovSourcing },
        layer: "A",
      })
    );
  }

  const location = text(dto.eventsOpportunityOverview?.location || rem.eventLocationSummary || "");
  if (VENUE_STATUS_AS_LOCATION_RE.test(location)) {
    issues.push(
      issue({
        issueId: `${prefix}-LOCATION-VENUE-STATUS`,
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.SOURCING,
        hotelId,
        opportunityId: id,
        field: "location",
        message: "Location row uses venue-commitment prose instead of geography.",
        evidence: { location },
        layer: "A",
      })
    );
  }

  const narrativeBlob = [
    rem.whyNow,
    rem.summaryWhat,
    rem.whatIsHappening,
    rem.recommendedAction,
    rem.suggestedAction,
    rem.cardWhyNowLine,
    dto.recommendedAction,
  ]
    .map(text)
    .join(" | ");
  if (MALFORMED_NARRATIVE_RE.test(narrativeBlob)) {
    issues.push(
      issue({
        issueId: `${prefix}-NARRATIVE-MALFORMED`,
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.NARRATIVE,
        hotelId,
        opportunityId: id,
        field: "narrative",
        message: "Malformed duplicated action prefix or Host-hotel-contracted template glue.",
        evidence: { snippet: narrativeBlob.slice(0, 280) },
        layer: "A",
      })
    );
  }

  const typeKey = classifyOpportunityTypeFilter(rem);
  if (typeKey === OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED) {
    // Customer filter already hides Other — store-level unclassified is a research
    // queue gap, not a presentation defect. Flag for research, not as UI P1.
    issues.push(
      issue({
        issueId: `${prefix}-TYPE-UNCLASSIFIED`,
        severity: ISSUE_SEVERITY.REVIEW,
        category: ISSUE_CATEGORY.CLASSIFICATION,
        hotelId,
        opportunityId: id,
        field: "opportunityType",
        message:
          "Research-queue classification gap: maps to Other / Unclassified internally (customer filter hides Other). Needs demand-segment research — do not treat as clean presentation.",
        evidence: {
          typeKey,
          label: OPPORTUNITY_TYPE_FILTER_LABEL[typeKey],
          demandSegmentId: rem.demandSegmentId || null,
          opportunityType: rem.opportunityType || null,
        },
        layer: "C",
        remediationStatus: "RESEARCH_REQUIRED",
      })
    );
  }
  if (
    String(rem.opportunityType || "").toUpperCase() ===
    "COMPETITIVE_MARKET_INTELLIGENCE"
  ) {
    issues.push(
      issue({
        issueId: `${prefix}-TYPE-CMI-AS-DEMAND`,
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.CLASSIFICATION,
        hotelId,
        opportunityId: id,
        field: "opportunityType",
        message: "Competitive Market Intelligence stored as demand opportunityType (must stay Market Scope).",
        evidence: { opportunityType: rem.opportunityType },
        layer: "B",
      })
    );
  }

  const composed = composeCustomerFacingHotelSourcingLabel(rem);
  if (
    /Competitor venue confirmed/i.test(composed) &&
    OPEN_RFP_RE.test(text(rem.recommendedAction || dto.recommendedAction || ""))
  ) {
    issues.push(
      issue({
        issueId: `${prefix}-COMMERCIAL-RFP-VS-COMPETITOR`,
        severity: ISSUE_SEVERITY.P0,
        category: ISSUE_CATEGORY.COMMERCIAL,
        hotelId,
        opportunityId: id,
        field: "recommendedAction",
        message: "Competitor venue confirmed but recommended action still requests RFP as host.",
        evidence: {
          composed,
          recommendedAction: text(rem.recommendedAction || "").slice(0, 220),
        },
        layer: "B",
      })
    );
  }

  const contradictions = detectOpportunityContradictions(rem);
  for (const f of contradictions.findings || []) {
    issues.push(
      issue({
        issueId: `${prefix}-CONTRA-${f.code}`,
        severity: mapContradictionSeverity(f.severity),
        category: ISSUE_CATEGORY.COMMERCIAL,
        hotelId,
        opportunityId: id,
        field: f.code,
        message: f.message,
        evidence: f.evidence || {},
        layer: "B",
      })
    );
  }

  // Semantic / review-required: Venue Confirmed alone on customer label after presentation
  if (/^Venue Confirmed$/i.test(sourcingLabel)) {
    issues.push(
      issue({
        issueId: `${prefix}-SEMANTIC-VENUE-CONFIRMED`,
        severity: ISSUE_SEVERITY.REVIEW,
        category: ISSUE_CATEGORY.SOURCING,
        hotelId,
        opportunityId: id,
        field: "hotelSourcingStatusLabel",
        message:
          "Customer sourcing label is generic Venue Confirmed — confirm competitor vs target hotel dimension is intentional.",
        evidence: { sourcingLabel, composed },
        layer: "C",
      })
    );
  }

  // Date conflict: start/end vs overview timing year mismatch (deterministic)
  const start = text(rem.eventStartDate || dto.eventStartDate || "");
  const end = text(rem.eventEndDate || dto.eventEndDate || "");
  if (start && end && start > end) {
    issues.push(
      issue({
        issueId: `${prefix}-DATES-INVERTED`,
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.DATES,
        hotelId,
        opportunityId: id,
        field: "eventStartDate/eventEndDate",
        message: "Event end date precedes start date.",
        evidence: { start, end },
        layer: "A",
      })
    );
  }

  return {
    opportunityId: id,
    hotelId,
    displayTitle,
    sourcingLabel: sourcingLabel || composed,
    marketScope: rem.inventoryPopulations || dto.inventoryPopulations || null,
    demandType: rem.demandSegmentId || rem.opportunityType || null,
    opportunityTypeFilter: typeKey,
    issueCount: issues.length,
    issues,
    dtoSnapshot: {
      displayTitle: dto.displayTitle || null,
      hotelSourcingStatusLabel: dto.hotelSourcingStatusLabel || null,
      overviewSourcing: dto.eventsOpportunityOverview?.hotelSourcing || null,
      overviewLocation: dto.eventsOpportunityOverview?.location || null,
      recommendedAction: text(dto.recommendedAction || rem.recommendedAction || "").slice(
        0,
        200
      ),
    },
  };
}

function auditUiContracts() {
  const issues = [];
  const uiPath = path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js");
  const cssPath = path.join(ROOT, "public/css/group-demand-intelligence.css");
  const htmlPath = path.join(ROOT, "public/group-demand-intelligence.html");
  const ui = fs.readFileSync(uiPath, "utf8");
  const css = fs.readFileSync(cssPath, "utf8");
  const html = fs.readFileSync(htmlPath, "utf8");

  if (/data-gdi-competitive-status/.test(ui)) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-COMPETITIVE-STATUS-SELECTOR",
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "Obsolete Competitive Status selector still present in UI.",
        layer: "A",
      })
    );
  }
  if (!/data-gdi-market-scope/.test(ui)) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-MARKET-SCOPE-MISSING",
        severity: ISSUE_SEVERITY.P0,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "Market Scope control missing from UI.",
        layer: "A",
      })
    );
  }
  if (!/Group & Demand\\nOpportunities/.test(ui)) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-NAV-TITLE",
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "Group & Demand Opportunities navigation title missing.",
        layer: "A",
      })
    );
  }
  if (!/function resetViewButtonHtml/.test(ui) || !/filter-count-badge/.test(ui)) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-RESET-VIEW-BADGE",
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "Reset View active-filter badge helpers missing.",
        layer: "A",
      })
    );
  }
  // Card must not render verification pills in opportunityTileHtml path
  if (
    /tilePriorityPill[\s\S]{0,200}competitiveStatusPillHtml/.test(ui) ||
    /gdi-tile-pills[\s\S]{0,120}competitiveStatusPillHtml/.test(ui)
  ) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-CARD-VERIFICATION-PILLS",
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "Verification status pills appear re-wired onto opportunity cards.",
        layer: "A",
      })
    );
  }
  if (!/--gdi-toolbar-control-height:\s*3rem/.test(css)) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-TOOLBAR-HEIGHT",
        severity: ISSUE_SEVERITY.P2,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "Market Scope toolbar height token missing/changed.",
        layer: "A",
      })
    );
  }
  if (!/gdi-guided-report-tour\.js/.test(html)) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-GUIDED-TOUR",
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "Guided report tour script not referenced in GDI HTML.",
        layer: "A",
      })
    );
  }
  if (!/gdi-regression-recovery-v1/.test(html)) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-CACHE-TOKEN",
        severity: ISSUE_SEVERITY.P2,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message: "HTML cache-bust token does not include consolidated recovery baseline.",
        evidence: { hint: "expected gdi-regression-recovery-v1" },
        layer: "A",
      })
    );
  }
  // Customer Opportunity Type must never push OTHER_UNCLASSIFIED into the option list.
  // Label maps may still define the internal enum — that is not a customer option.
  const hidesOther =
    /Never expose Other \/ Unclassified as a customer-facing filter option/.test(ui) &&
    /if\s*\(\s*value\s*===\s*["']OTHER_UNCLASSIFIED["']\s*\)\s*return/.test(ui);
  if (!hidesOther) {
    issues.push(
      issue({
        issueId: "GDI-CCA-UI-OTHER-OPTION",
        severity: ISSUE_SEVERITY.P1,
        category: ISSUE_CATEGORY.UI_CONTRACT,
        message:
          "Opportunity Type filter no longer explicitly excludes OTHER_UNCLASSIFIED from customer options.",
        layer: "A",
      })
    );
  }

  return issues;
}

export async function listAuditHotelIds({ includeFixtures = false } = {}) {
  const hotelsRoot = path.join(ROOT, "data/group-demand-intelligence/hotels");
  const dirs = fs
    .readdirSync(hotelsRoot, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .filter((id) => {
      if (!includeFixtures && /^(dec_|recTEST)/i.test(id)) return false;
      return fs.existsSync(path.join(hotelsRoot, id, "opportunities.json"));
    });
  return dirs.sort();
}

/**
 * Run full portfolio audit.
 */
export async function runFullCustomerConsistencyAudit(opts = {}) {
  const hotelIds = opts.hotelIds || (await listAuditHotelIds(opts));
  const allIssues = [];
  const hotelSummaries = [];
  const opportunityRows = [];
  let totalOpportunities = 0;

  allIssues.push(...auditUiContracts());

  for (const hotelId of hotelIds) {
    let doc;
    try {
      doc = await loadOpportunitiesCanonical(hotelId);
    } catch (err) {
      allIssues.push(
        issue({
          issueId: `GDI-CCA-${hotelId}-LOAD-FAIL`,
          severity: ISSUE_SEVERITY.P2,
          category: ISSUE_CATEGORY.UI_CONTRACT,
          hotelId,
          message: `Failed to load opportunities: ${err.message}`,
          layer: "A",
        })
      );
      continue;
    }
    const opps = doc.opportunities || [];
    totalOpportunities += opps.length;
    let hotelIssueCount = 0;
    const bySeverity = { P0: 0, P1: 0, P2: 0, P3: 0, REVIEW_REQUIRED: 0 };

    for (let i = 0; i < opps.length; i++) {
      const row = auditOpportunity(opps[i], hotelId, i);
      opportunityRows.push({
        hotelId,
        opportunityId: row.opportunityId,
        displayTitle: row.displayTitle,
        sourcingLabel: row.sourcingLabel,
        opportunityTypeFilter: row.opportunityTypeFilter,
        issueCount: row.issueCount,
        dtoSnapshot: row.dtoSnapshot,
      });
      for (const iss of row.issues) {
        allIssues.push(iss);
        hotelIssueCount += 1;
        bySeverity[iss.severity] = (bySeverity[iss.severity] || 0) + 1;
      }
    }

    hotelSummaries.push({
      hotelId,
      opportunityCount: opps.length,
      issueCount: hotelIssueCount,
      bySeverity,
    });
  }

  const bySeverity = { P0: 0, P1: 0, P2: 0, P3: 0, REVIEW_REQUIRED: 0 };
  const byCategory = {};
  for (const iss of allIssues) {
    bySeverity[iss.severity] = (bySeverity[iss.severity] || 0) + 1;
    byCategory[iss.category] = (byCategory[iss.category] || 0) + 1;
  }

  const confirmedDefects = allIssues.filter(
    (i) => i.severity === "P0" || i.severity === "P1"
  );
  const reviewRequired = allIssues.filter((i) => i.severity === "REVIEW_REQUIRED");

  // Presentation score ignores research-queue REVIEW items so hidden filter gaps
  // cannot be used to claim a perfect product while still reporting those gaps.
  let presentationScore = 100;
  presentationScore -= bySeverity.P0 * 15;
  presentationScore -= bySeverity.P1 * 5;
  presentationScore -= bySeverity.P2 * 1;
  presentationScore -= bySeverity.P3 * 0.25;
  presentationScore = Math.max(0, Math.round(presentationScore * 10) / 10);

  let researchQueueScore = 100;
  researchQueueScore -= bySeverity.REVIEW_REQUIRED * 0.75;
  researchQueueScore = Math.max(0, Math.round(researchQueueScore * 10) / 10);

  const overall = Math.round(
    (presentationScore * 0.7 + researchQueueScore * 0.3) * 10
  ) / 10;

  return {
    schema: GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1,
    generatedAt: new Date().toISOString(),
    baseline: {
      branchHint: "fix/gdi-regression-recovery-v1",
      titleContract: GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1,
      titleContractRevision: GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_REVISION,
      consolidatedPr: "https://github.com/joandejarden-create/my-operators-backend/pull/233",
      fieldSourceOfTruth: CUSTOMER_FACING_FIELD_SOT,
    },
    scope: {
      hotelsScanned: hotelIds.length,
      hotelIds,
      totalOpportunities,
    },
    score: {
      overall,
      presentation: presentationScore,
      researchQueue: researchQueueScore,
      note:
        bySeverity.P0 || bySeverity.P1
          ? "Overall reduced by confirmed P0/P1 presentation/commercial defects — not a perfect score."
          : bySeverity.REVIEW_REQUIRED
            ? "No P0/P1 presentation defects; research-queue classification gaps remain and prevent a perfect overall score."
            : "No P0/P1 confirmed defects and no research-queue review flags.",
    },
    totals: {
      issues: allIssues.length,
      bySeverity,
      byCategory,
      confirmedDefects: confirmedDefects.length,
      reviewRequired: reviewRequired.length,
    },
    hotelSummaries,
    issues: allIssues,
    opportunitySample: opportunityRows.filter((r) => r.issueCount > 0).slice(0, 200),
    opportunityRowsCount: opportunityRows.length,
  };
}

export function writeAuditArtifacts(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "00-audit-summary.json"),
    JSON.stringify(
      {
        schema: report.schema,
        generatedAt: report.generatedAt,
        baseline: report.baseline,
        scope: report.scope,
        score: report.score,
        totals: report.totals,
        hotelSummaries: report.hotelSummaries,
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(outDir, "01-findings.json"),
    JSON.stringify(
      {
        schema: `${GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1}/findings`,
        generatedAt: report.generatedAt,
        count: report.issues.length,
        issues: report.issues,
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(outDir, "02-field-source-of-truth.json"),
    JSON.stringify(
      {
        schema: `${GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1}/field-sot`,
        fields: CUSTOMER_FACING_FIELD_SOT,
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(outDir, "03-opportunities-with-issues.json"),
    JSON.stringify(
      {
        schema: `${GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1}/opportunity-issues`,
        count: report.opportunitySample.length,
        rows: report.opportunitySample,
      },
      null,
      2
    )
  );
  return outDir;
}
