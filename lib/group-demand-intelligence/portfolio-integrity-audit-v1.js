/**
 * Run-052 portfolio addendum — Level A deterministic integrity audit.
 * Read-only: never mutates opportunity records.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { assertRejectedContactAbsent } from "./research/association-events-buyer-integrity-v1.js";
import {
  isContaminatedWebScrapeNarrative,
  sanitizeOpportunityForCustomerPresentation,
} from "./customer-presentation-sanitize-v1.js";
import { isGenericFacilitiesHotelFitNarrative } from "./hotel-fit-presentation-honesty-v1.js";
import { evaluateCustomerDrawerAcceptance } from "./customer-drawer-acceptance-harness-v1.js";

export const PORTFOLIO_INTEGRITY_AUDIT_V1 = "gdi-portfolio-integrity-audit-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const HOTELS_ROOT = path.join(ROOT, "data/group-demand-intelligence/hotels");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function sha256File(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function listId(o) {
  return text(o.opportunityId || o.id || o.canonicalOpportunityId);
}

function segmentOf(o) {
  return text(
    o.researchSegment ||
      o.segment ||
      o.demandFamily ||
      o.opportunityType ||
      "UNKNOWN",
  );
}

function briefTypeOf(o) {
  if (o.associationEventsGoldBrief) return "associationEventsGoldBrief";
  if (o.commercialIntelligenceBrief) return "commercialIntelligenceBrief";
  if (o.salesBrief) return "salesBrief";
  return "none";
}

function lifecycleOf(o) {
  return text(
    o.commercialResearchCompletionState ||
      o.opportunityQualification ||
      o.associationEventsGoldBrief?.qualification?.status ||
      o.salesBrief?.qualification?.status ||
      o.customerFacingState ||
      "UNKNOWN",
  );
}

/** Extract candidate human names that appear in action strings as Contact X. */
function contactNamesInActions(o) {
  const blob = [
    o.recommendedAction,
    o.salesActionRecommended,
    o.suggestedAction,
    o.associationEventsGoldBrief?.recommendedSalesApproach,
  ]
    .map((v) => (typeof v === "object" ? text(v?.nextAction || v?.openingApproach) : text(v)))
    .join("\n");
  const names = [];
  const re = /\bContact\s+([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ'’.-]+(?:\s+[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ'’.-]+){0,3})/g;
  let m;
  while ((m = re.exec(blob))) names.push(m[1].trim());
  return [...new Set(names)];
}

/**
 * Level A checks against one opportunity (raw store projection).
 * Also evaluates sanitized customer DTO when opts.includeSanitize !== false.
 */
export function auditOpportunityLevelA(opportunity = {}, opts = {}) {
  const hotelId = text(opts.hotelId || opportunity.hotelId);
  const id = listId(opportunity);
  const findings = [];
  const push = (family, severity, field, actual, expected, detail = {}) => {
    findings.push({
      defectFamily: family,
      severity,
      field,
      actual: actual == null ? null : String(actual).slice(0, 240),
      expected,
      customerVisible: detail.customerVisible !== false,
      ...detail,
    });
  };

  const goldQual =
    opportunity.associationEventsGoldBrief?.qualification ||
    opportunity.salesBrief?.qualification ||
    opportunity.commercialIntelligenceBrief?.qualification ||
    null;
  const goldStatus = text(goldQual?.status);
  const topQual = text(opportunity.opportunityQualification);
  const conf = Number(opportunity.evidenceConfidence);

  // RC-01 / RC-03 — rejected/stale named buyers in presentation surfaces
  const actionNames = contactNamesInActions(opportunity);
  for (const name of actionNames) {
    if (opportunity.hasVerifiedNamedRelevantContact === false) {
      push(
        "RC-01",
        "HIGH",
        "recommendedAction",
        `Contact ${name}`,
        "no Contact-<person> when verified buyer absent",
        { person: name },
      );
    }
  }
  if (
    opportunity.personEvidence?.verified === true &&
    opportunity.hasVerifiedNamedRelevantContact === false
  ) {
    push(
      "RC-03",
      "HIGH",
      "personEvidence",
      JSON.stringify({
        name: opportunity.personEvidence?.name,
        verified: true,
      }).slice(0, 200),
      "personEvidence.verified must not stay true when hasVerifiedNamedRelevantContact is false",
    );
  }
  if (
    opportunity.hasVerifiedNamedRelevantContact === false &&
    (text(opportunity.salesBrief?.whoToContact?.primary?.name) ||
      text(opportunity.salesBrief?.whoToContact?.primary))
  ) {
    const n =
      text(opportunity.salesBrief?.whoToContact?.primary?.name) ||
      text(opportunity.salesBrief?.whoToContact?.primary);
    push(
      "RC-01",
      "HIGH",
      "salesBrief.whoToContact.primary",
      n,
      "primary contact cleared when verified buyer absent",
    );
  }

  // RC-04 — qualification disagreement (sales-ready stamps vs gold continuation)
  if (
    goldQual &&
    (goldQual.qualified === false || /RESEARCH_CONTINUATION/i.test(goldStatus)) &&
    /^(QUALIFIED|STRONG|MODERATE|ACTIONABLE|VERIFIED_OPEN)$/i.test(topQual)
  ) {
    push(
      "RC-04",
      "HIGH",
      "opportunityQualification",
      topQual,
      `align with gold ${goldStatus || "RESEARCH_CONTINUATION"}`,
    );
  }

  // RC-05 — scraped narrative
  for (const [field, val] of [
    ["summaryWhat", opportunity.summaryWhat],
    ["whatIsHappening", opportunity.whatIsHappening],
    ["salesBriefActivity", opportunity.salesBriefActivity],
    [
      "associationEventsGoldBrief.eventHistoryAndActivity",
      opportunity.associationEventsGoldBrief?.eventHistoryAndActivity,
    ],
  ]) {
    if (isContaminatedWebScrapeNarrative(val)) {
      push("RC-05", "HIGH", field, val, "customer narrative free of scrape markup");
    }
  }

  // RC-06 / RC-07 — hotel fit
  if (isGenericFacilitiesHotelFitNarrative(opportunity.whyThisHotel)) {
    push(
      "RC-06",
      "MEDIUM",
      "whyThisHotel",
      opportunity.whyThisHotel,
      "opportunity-specific fit, not facilities inventory alone",
    );
  }
  const dims =
    opportunity.hotelFitDimensionAssessments ||
    opportunity.hotelFitQualitativeComponents;
  const emptyShell =
    dims &&
    typeof dims === "object" &&
    Object.keys(dims).length === 0 &&
    opportunity.hotelFitShowDimensionGrid !== false;
  if (emptyShell && opportunity.hotelFitShowDimensionGrid === true) {
    push(
      "RC-07",
      "MEDIUM",
      "hotelFitShowDimensionGrid",
      "true with empty dimensions",
      "false when no assessed dimensions",
    );
  }

  // RC-08 — inflated confidence
  if (
    (goldQual?.qualified === false ||
      /RESEARCH_CONTINUATION/i.test(goldStatus) ||
      opportunity.hasVerifiedNamedRelevantContact === false) &&
    Number.isFinite(conf) &&
    conf >= 80
  ) {
    push(
      "RC-08",
      "HIGH",
      "evidenceConfidence",
      conf,
      "<=60 when continuation / missing verified buyer",
    );
  }

  // Chronology: PAST recommended as current pursue
  if (
    /PAST_COMPLETED|PAST\b/i.test(text(goldQual?.chronologyStatus)) &&
    /Contact\s+/i.test(text(opportunity.recommendedAction)) &&
    !/next|future|202[7-9]|cycle|edition/i.test(text(opportunity.recommendedAction))
  ) {
    push(
      "CHRONOLOGY_PAST_AS_CURRENT",
      "MEDIUM",
      "recommendedAction",
      opportunity.recommendedAction,
      "past edition must not be recommended as current demand without future-cycle thesis",
    );
  }

  // Duplicate source URLs
  const urls = (opportunity.sources || [])
    .map((s) => text(typeof s === "string" ? s : s?.url).toLowerCase())
    .filter(Boolean);
  const dup = urls.filter((u, i) => urls.indexOf(u) !== i);
  if (dup.length) {
    push(
      "DUPLICATE_SOURCES",
      "LOW",
      "sources",
      [...new Set(dup)].slice(0, 5).join(" | "),
      "unique source URLs",
      { customerVisible: false },
    );
  }

  // Segment vs brief mismatch
  const seg = segmentOf(opportunity).toLowerCase();
  if (
    /scaling/.test(seg) &&
    opportunity.associationEventsGoldBrief &&
    !opportunity.commercialIntelligenceBrief
  ) {
    push(
      "SEGMENT_BRIEF_MISMATCH",
      "MEDIUM",
      "researchSegment",
      seg,
      "scaling segment should not only carry association gold brief",
    );
  }
  if (
    /association|medical_academic|conference/.test(seg) &&
    opportunity.commercialIntelligenceBrief &&
    !opportunity.associationEventsGoldBrief &&
    /scaling_companies/i.test(text(opportunity.researchSegment))
  ) {
    push(
      "SEGMENT_IDENTITY",
      "HIGH",
      "researchSegment",
      opportunity.researchSegment,
      "association identity must not be stamped scaling_companies",
    );
  }

  let sanitizeResult = null;
  let acceptance = null;
  if (opts.includeSanitize !== false) {
    try {
      sanitizeResult = sanitizeOpportunityForCustomerPresentation({
        ...opportunity,
        hotelId,
      });
      // Only enforce segment family when classification is unambiguous.
      // Do not treat "Private Events" / "team events" as Association (false /event/ match).
      let expectedSegment;
      if (/scaling_companies|scaling company/i.test(seg)) {
        expectedSegment = "scaling";
      } else if (
        /association|medical_academic|associations?_events|associations?_government|ASSOCIATION_CONFERENCE/i.test(
          seg,
        )
      ) {
        expectedSegment = "association";
      }
      acceptance = evaluateCustomerDrawerAcceptance(sanitizeResult, {
        expectedSegment,
      });
      if (acceptance && acceptance.ok === false) {
        for (const f of acceptance.failures || []) {
          push(
            "RC-09_ACCEPTANCE",
            "HIGH",
            "customerDrawerAcceptance",
            f,
            "sanitize DTO must pass drawer acceptance harness",
          );
        }
      }
      // Post-sanitize residual RC checks (should be clean after Run-052)
      if (isContaminatedWebScrapeNarrative(sanitizeResult.summaryWhat)) {
        push(
          "RC-05",
          "HIGH",
          "sanitize.summaryWhat",
          sanitizeResult.summaryWhat,
          "clean after sanitize",
        );
      }
      if (
        /^QUALIFIED$/i.test(text(sanitizeResult.opportunityQualification)) &&
        (sanitizeResult.associationEventsGoldBrief?.qualification?.qualified ===
          false ||
          /RESEARCH_CONTINUATION/i.test(
            text(sanitizeResult.associationEventsGoldBrief?.qualification?.status),
          ))
      ) {
        push(
          "RC-04",
          "HIGH",
          "sanitize.opportunityQualification",
          sanitizeResult.opportunityQualification,
          "demoted after sanitize",
        );
      }
      if (
        Number(sanitizeResult.evidenceConfidence) >= 80 &&
        (sanitizeResult.hasVerifiedNamedRelevantContact === false ||
          sanitizeResult.associationEventsGoldBrief?.qualification?.qualified ===
            false)
      ) {
        push(
          "RC-08",
          "HIGH",
          "sanitize.evidenceConfidence",
          sanitizeResult.evidenceConfidence,
          "capped after sanitize",
        );
      }
    } catch (err) {
      push(
        "SANITIZE_ERROR",
        "HIGH",
        "sanitizeOpportunityForCustomerPresentation",
        err.message,
        "sanitize must not throw",
      );
    }
  }

  // Cross-hotel name in why (light)
  const why = text(opportunity.whyThisHotel);
  if (
    why &&
    hotelId === "recterKmNPdntCm0v" &&
    /\b(Waterstone|Mayflower|Bethesda Marriott|Renaissance Times Square)\b/i.test(why)
  ) {
    push(
      "CROSS_HOTEL_CONTAMINATION",
      "HIGH",
      "whyThisHotel",
      why,
      "why must not name foreign hotel for Chapultepec",
    );
  }

  return {
    hotelId,
    opportunityId: id,
    title: text(opportunity.title || opportunity.organizationName || opportunity.companyName).slice(
      0,
      120,
    ),
    segment: segmentOf(opportunity),
    briefType: briefTypeOf(opportunity),
    lifecycle: lifecycleOf(opportunity),
    findingCount: findings.length,
    findings,
    sanitizeOk: acceptance ? acceptance.ok === true : null,
    acceptanceFailures: acceptance?.failures || null,
    postSanitizeConfidence: sanitizeResult
      ? Number(sanitizeResult.evidenceConfidence)
      : null,
    postSanitizeQualification: sanitizeResult
      ? text(sanitizeResult.opportunityQualification)
      : null,
  };
}

export function discoverHotelOpportunityStores(root = HOTELS_ROOT) {
  if (!fs.existsSync(root)) return [];
  return fs
    .readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => {
      const hotelId = d.name;
      const filePath = path.join(root, hotelId, "opportunities.json");
      return {
        hotelId,
        filePath,
        exists: fs.existsSync(filePath),
      };
    });
}

export function loadHotelOpportunities(hotelId, root = HOTELS_ROOT) {
  const filePath = path.join(root, hotelId, "opportunities.json");
  if (!fs.existsSync(filePath)) {
    return { hotelId, filePath, opportunities: [], sha256: null, error: "missing" };
  }
  const raw = fs.readFileSync(filePath);
  const sha256 = crypto.createHash("sha256").update(raw).digest("hex");
  const doc = JSON.parse(raw.toString("utf8"));
  const opportunities = Array.isArray(doc) ? doc : doc.opportunities || [];
  return {
    hotelId,
    filePath,
    opportunities,
    sha256,
    byteLength: raw.length,
    updatedAt: doc.updatedAt || null,
  };
}

/**
 * Full Level A portfolio scan (read-only).
 */
export function runPortfolioLevelAAudit(opts = {}) {
  const root = opts.root || HOTELS_ROOT;
  const stores = discoverHotelOpportunityStores(root);
  const at = new Date().toISOString();
  const inventory = [];
  const audits = [];
  const coverage = {
    hotelDirs: stores.length,
    hotelsWithOpportunityFiles: 0,
    opportunitiesStored: 0,
    opportunitiesAudited: 0,
    sanitizeEvaluated: 0,
    airtableRead: false,
    airtableNote:
      "Airtable not queried (read-only FS audit; production Airtable excluded per Run-052 authorization).",
    shadowPortfolioPathsChecked: [],
    apiDetailSampled: 0,
    browserDrawersInspected: 0,
    aiCommercialReviews: 0,
    deepForensicTraces: 0,
  };

  const shadowCandidates = [
    path.join(ROOT, "data/group-demand-intelligence/local-shadow/shadow-portfolios.json"),
    path.join(
      ROOT,
      "reports/gdi-2-review-loop/waterstone-chapultepec-expansion/shadow-portfolios.json",
    ),
  ];
  for (const sp of shadowCandidates) {
    coverage.shadowPortfolioPathsChecked.push({
      path: sp,
      exists: fs.existsSync(sp),
      sha256: fs.existsSync(sp) ? sha256File(sp) : null,
    });
  }

  for (const store of stores) {
    if (!store.exists) {
      inventory.push({
        hotelId: store.hotelId,
        filePath: store.filePath,
        opportunityCount: 0,
        accessible: false,
        reason: "no_opportunities_json",
      });
      continue;
    }
    coverage.hotelsWithOpportunityFiles += 1;
    const loaded = loadHotelOpportunities(store.hotelId, root);
    coverage.opportunitiesStored += loaded.opportunities.length;
    inventory.push({
      hotelId: store.hotelId,
      filePath: loaded.filePath,
      opportunityCount: loaded.opportunities.length,
      sha256: loaded.sha256,
      byteLength: loaded.byteLength,
      accessible: true,
      segments: summarizeSegments(loaded.opportunities),
    });

    for (const opp of loaded.opportunities) {
      const row = auditOpportunityLevelA(opp, {
        hotelId: store.hotelId,
        includeSanitize: opts.includeSanitize !== false,
      });
      audits.push(row);
      coverage.opportunitiesAudited += 1;
      if (row.sanitizeOk != null) coverage.sanitizeEvaluated += 1;
    }
  }

  const defects = [];
  let defectSeq = 0;
  for (const row of audits) {
    for (const f of row.findings) {
      defectSeq += 1;
      defects.push({
        defectId: `PF-${String(defectSeq).padStart(5, "0")}`,
        hotelId: row.hotelId,
        opportunityId: row.opportunityId,
        title: row.title,
        segment: row.segment,
        defectFamily: f.defectFamily,
        severity: f.severity,
        field: f.field,
        actual: f.actual,
        expected: f.expected,
        customerVisible: f.customerVisible !== false,
        repairStatus: "OPEN",
        suspectedCodePath: suspectPath(f.defectFamily),
      });
    }
  }

  const clusters = clusterDefects(defects);
  const byHotel = {};
  for (const d of defects) {
    byHotel[d.hotelId] = byHotel[d.hotelId] || { total: 0, byFamily: {} };
    byHotel[d.hotelId].total += 1;
    byHotel[d.hotelId].byFamily[d.defectFamily] =
      (byHotel[d.hotelId].byFamily[d.defectFamily] || 0) + 1;
  }

  return {
    version: PORTFOLIO_INTEGRITY_AUDIT_V1,
    at,
    root,
    coverage,
    inventory,
    audits,
    defects,
    clusters,
    summary: {
      hotelsAccessible: coverage.hotelsWithOpportunityFiles,
      opportunities: coverage.opportunitiesAudited,
      defectCount: defects.length,
      opportunitiesWithDefects: audits.filter((a) => a.findingCount > 0).length,
      opportunitiesClean: audits.filter((a) => a.findingCount === 0).length,
      sanitizePass: audits.filter((a) => a.sanitizeOk === true).length,
      sanitizeFail: audits.filter((a) => a.sanitizeOk === false).length,
      byFamily: Object.fromEntries(
        Object.entries(
          defects.reduce((acc, d) => {
            acc[d.defectFamily] = (acc[d.defectFamily] || 0) + 1;
            return acc;
          }, {}),
        ).sort((a, b) => b[1] - a[1]),
      ),
      byHotel,
    },
  };
}

function summarizeSegments(opps = []) {
  const m = {};
  for (const o of opps) {
    const s = segmentOf(o) || "UNKNOWN";
    m[s] = (m[s] || 0) + 1;
  }
  return m;
}

function suspectPath(family) {
  const map = {
    "RC-01": "association-events-buyer-integrity-v1 / portfolio-commercial-integrity-v1",
    "RC-02": "customer-presentation-sanitize-v1 applyAuthoritativeGoldBriefPresentation / dealality-gdi-ui",
    "RC-03": "association-events-buyer-integrity-v1 personEvidence",
    "RC-04": "portfolio-commercial-integrity-v1 reconcileOpportunityQualificationDisplay",
    "RC-05": "customer-presentation-sanitize-v1 scrubCustomerOccasionNarrative",
    "RC-06": "hotel-fit-presentation-honesty-v1 / applyAuthoritativeGoldBriefPresentation",
    "RC-07": "hotel-fit-presentation-honesty-v1 / dealality-gdi-ui hotelFitAssessmentSectionHtml",
    "RC-08": "portfolio-commercial-integrity-v1 recalibrateEvidenceConfidence",
    "RC-09_ACCEPTANCE": "customer-drawer-acceptance-harness-v1",
    "RC-10": "research-action-contract-v1 classifyResearchActionBriefKind",
    SEGMENT_IDENTITY: "scaling-companies-full-e2e-publication-v1 resolvePublicationSegmentStamp",
    CHRONOLOGY_PAST_AS_CURRENT: "association chronology + recommendedAction generation",
    DUPLICATE_SOURCES: "evidence merge / sources attach",
    CROSS_HOTEL_CONTAMINATION: "portfolio-commercial-integrity-v1 quarantineCrossHotelContent",
    SEGMENT_BRIEF_MISMATCH: "segment classification / brief attachment",
    SANITIZE_ERROR: "customer-presentation-sanitize-v1",
  };
  return map[family] || "unknown";
}

function clusterDefects(defects = []) {
  const byFamily = {};
  for (const d of defects) {
    const key = d.defectFamily;
    if (!byFamily[key]) {
      byFamily[key] = {
        defectFamily: key,
        count: 0,
        severityMax: "LOW",
        customerVisibleCount: 0,
        opportunityIds: new Set(),
        hotelIds: new Set(),
        suspectedCodePath: d.suspectedCodePath,
        examples: [],
      };
    }
    const c = byFamily[key];
    c.count += 1;
    c.opportunityIds.add(d.opportunityId);
    c.hotelIds.add(d.hotelId);
    if (d.customerVisible) c.customerVisibleCount += 1;
    if (severityRank(d.severity) > severityRank(c.severityMax)) c.severityMax = d.severity;
    if (c.examples.length < 5) {
      c.examples.push({
        defectId: d.defectId,
        hotelId: d.hotelId,
        opportunityId: d.opportunityId,
        field: d.field,
        actual: d.actual,
      });
    }
  }
  return Object.values(byFamily)
    .map((c) => ({
      ...c,
      affectedOpportunities: c.opportunityIds.size,
      affectedHotels: c.hotelIds.size,
      opportunityIds: [...c.opportunityIds].slice(0, 40),
      hotelIds: [...c.hotelIds],
    }))
    .sort((a, b) => {
      const sev = severityRank(b.severityMax) - severityRank(a.severityMax);
      if (sev) return sev;
      return b.customerVisibleCount - a.customerVisibleCount || b.count - a.count;
    });
}

function severityRank(s) {
  return { CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 }[s] || 0;
}

export function writePortfolioAuditArtifacts(result, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const baseline = {
    version: result.version,
    at: result.at,
    inventory: result.inventory,
    summary: {
      hotelsAccessible: result.summary.hotelsAccessible,
      opportunities: result.summary.opportunities,
    },
    medicosBenchmarkId: "gdi_e2d_adb3cac9c5109ba4",
    medicosHotelId: "recterKmNPdntCm0v",
  };
  const coverage = { ...result.coverage, at: result.at };
  const defects = { at: result.at, version: result.version, defects: result.defects };
  const clusters = { at: result.at, clusters: result.clusters };

  fs.writeFileSync(
    path.join(outDir, "portfolio-audit-baseline.json"),
    `${JSON.stringify(baseline, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(outDir, "portfolio-audit-coverage.json"),
    `${JSON.stringify(coverage, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(outDir, "portfolio-defects.json"),
    `${JSON.stringify(defects, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(outDir, "root-cause-clusters.json"),
    `${JSON.stringify(clusters, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(outDir, "portfolio-level-a-full.json"),
    `${JSON.stringify({ summary: result.summary, audits: result.audits }, null, 2)}\n`,
  );
  return {
    baseline: path.join(outDir, "portfolio-audit-baseline.json"),
    coverage: path.join(outDir, "portfolio-audit-coverage.json"),
    defects: path.join(outDir, "portfolio-defects.json"),
    clusters: path.join(outDir, "root-cause-clusters.json"),
  };
}
