/**
 * Shared specialist-hunter execution harness.
 * Cache-first, dry-run default, bounded budget, fail-closed paid gates.
 */

import fs from "node:fs";
import path from "node:path";
import {
  SPECIALIST_HUNTER_HORIZON_MONTHS,
  getSpecialistHunterMeta,
  isSpecialistHunterId,
  HUNTER_HYPOTHESIS_STATUS,
} from "./specialist-hunter-contract-v1.js";
import { resolveHunterProviderPlan } from "./hunter-provider-routing-v1.js";
import {
  createEcosystemGraph,
  mergeEcosystemDelta,
  buildHotelDemandHypothesis,
  filterIncrementalHypotheses,
  upsertEcosystemNode,
  addEcosystemEdge,
  ECOSYSTEM_NODE_KIND,
  ECOSYSTEM_EDGE_KIND,
  canonicalOrganizationId,
} from "./organization-demand-ecosystem-v1.js";
import { commercialRelevanceScreen } from "../research/apify/apify-cross-provider-dedup-v1.js";
import { resolveHunterConfig } from "./shared/hunter-config-registry-v1.js";
import { buildQualificationHandoffPacket } from "./shared/qualification-handoff-v1.js";
import {
  DISCOVERY_EVIDENCE_CLASS,
  resolveDiscoveryEvidenceClass,
} from "./shared/discovery-evidence-class-v1.js";
import {
  customerFacingOpportunityTitle,
  normalizeCustomerFacingOpportunityTitle,
} from "../customer-facing-opportunity-title-contract-v1.js";

/** Nightlife / leisure organizers retained from V7 Party Bus rejection. */
const LEISURE_REJECTION =
  /\b(party bus|nightclub|club euro|strip|drag queen|ladies night|sip.?n.?paint|yoga|sound bath|booze cruise)\b/i;

export function parseHunterCliArgs(argv = process.argv.slice(2)) {
  const out = {
    hotelId: "",
    hotelName: "",
    dryRun: true,
    cacheFirst: true,
    budgetUsd: 0,
    allowPaid: false,
    authorizeMeasurement: false,
    horizonMonths: SPECIALIST_HUNTER_HORIZON_MONTHS,
    reportDir: "",
    existingKeys: [],
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--hotel-id" && next) {
      out.hotelId = next;
      i += 1;
    } else if (a === "--hotel-name" && next) {
      out.hotelName = next;
      i += 1;
    } else if (a === "--budget-usd" && next) {
      out.budgetUsd = Number(next) || 0;
      i += 1;
    } else if (a === "--horizon-months" && next) {
      out.horizonMonths = Number(next) || SPECIALIST_HUNTER_HORIZON_MONTHS;
      i += 1;
    } else if (a === "--report-dir" && next) {
      out.reportDir = next;
      i += 1;
    } else if (a === "--execute") {
      out.dryRun = false;
    } else if (a === "--dry-run") {
      out.dryRun = true;
    } else if (a === "--no-cache") {
      out.cacheFirst = false;
    } else if (a === "--allow-paid") {
      out.allowPaid = true;
    } else if (a === "--authorize-measurement") {
      out.authorizeMeasurement = true;
    }
  }
  return out;
}

export function assertHunterRunSafe(opts = {}) {
  const errors = [];
  if (!opts.hotelId || !/^rec[a-zA-Z0-9]+$/.test(String(opts.hotelId))) {
    errors.push("canonical hotelId (Airtable rec…) required");
  }
  if (!isSpecialistHunterId(opts.hunterId)) {
    errors.push(`unknown hunterId: ${opts.hunterId}`);
  }
  const budget = Number(opts.budgetUsd) || 0;
  const wantsPaid = opts.allowPaid === true || opts.authorizeMeasurement === true;
  if (wantsPaid && budget <= 0) {
    errors.push("paid research requires --budget-usd > 0 plus explicit authorization");
  }
  // This architecture wave does not authorize paid spend even with flags.
  if (wantsPaid) {
    errors.push(
      "paid Apify/Exa not authorized by nextgen discovery wave — cache/fixture only"
    );
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Screen raw discoveries before hypothesis formation.
 */
export function screenHunterCandidate(raw = {}) {
  const blob = [raw.organizationName, raw.activityTitle, raw.notes]
    .map((x) => String(x || ""))
    .join(" ");
  if (LEISURE_REJECTION.test(blob)) {
    return {
      accepted: false,
      reason: "V7 leisure/nightlife organizer gate (Party Bus class)",
      commercialScreen: "REJECT_LEISURE",
    };
  }
  const screened = commercialRelevanceScreen({
    title: raw.activityTitle,
    organizationName: raw.organizationName,
    startDate: raw.activityDate,
    relationshipTrackOnly: raw.relationshipTrackOnly === true,
    recordKind: raw.recordKind || "DEMAND_TRIGGER",
  });
  if (screened.commercialScreen === "DEMOTE_COMPETITOR_HOST_RISK") {
    return {
      accepted: false,
      reason: screened.commercialScreenReasons?.join("; ") || "competitor host",
      commercialScreen: screened.commercialScreen,
    };
  }
  return {
    accepted: true,
    commercialScreen: screened.commercialScreen,
    reasons: screened.commercialScreenReasons || [],
  };
}

/**
 * Convert accepted raw rows into ecosystem delta + hypotheses.
 */
export function materializeHunterDiscoveries({
  hunterId,
  hotelId,
  territory = "",
  discoveries = [],
  existingKeys = [],
} = {}) {
  const graph = createEcosystemGraph({ hotelId });
  const hypotheses = [];
  const rejected = [];
  const excluded = [];

  for (const raw of discoveries) {
    const screen = screenHunterCandidate(raw);
    if (!screen.accepted) {
      rejected.push({ ...raw, rejectReason: screen.reason });
      continue;
    }
    if (raw.exclude === true) {
      excluded.push(raw);
      continue;
    }

    const orgId = canonicalOrganizationId({
      name: raw.organizationName,
      domain: raw.domain,
      externalId: raw.externalOrgId,
    });
    upsertEcosystemNode(graph, {
      id: orgId,
      kind: ECOSYSTEM_NODE_KIND.ORGANIZATION,
      name: raw.organizationName,
      canonicalName: raw.organizationName,
      evidenceUrls: raw.evidenceUrls || [],
      confidence: raw.confidence ?? 0.55,
      hotelDemandRelevance: raw.hotelDemandRelevance ?? 0.5,
      sourceHunter: hunterId,
      geographicScope: raw.geographicScope || territory,
      verifiedAt: raw.verifiedAt || new Date().toISOString().slice(0, 10),
    });

    const sourceActivityTitle = String(raw.activityTitle || "").trim();
    const activityTitleNorm = sourceActivityTitle
      ? normalizeCustomerFacingOpportunityTitle(sourceActivityTitle)
      : null;
    const activityTitle =
      activityTitleNorm?.displayTitle || sourceActivityTitle || "";

    if (activityTitle) {
      const actId = `act_${orgId}_${String(activityTitle).slice(0, 24)}`;
      upsertEcosystemNode(graph, {
        id: actId,
        kind: ECOSYSTEM_NODE_KIND.EVENT_ACTIVITY,
        name: activityTitle,
        canonicalName: activityTitle,
        originalSourceTitle:
          activityTitleNorm?.originalSourceTitle || sourceActivityTitle || null,
        activityDate: raw.activityDate || null,
        evidenceUrls: raw.evidenceUrls || [],
        sourceHunter: hunterId,
        confidence: raw.confidence ?? 0.5,
      });
      addEcosystemEdge(graph, {
        kind: ECOSYSTEM_EDGE_KIND.ACTIVITY_OF,
        fromId: orgId,
        toId: actId,
        sourceHunter: hunterId,
        evidenceUrls: raw.evidenceUrls || [],
      });
    }

    if (raw.contactName) {
      const contactId = `ctc_${orgId}_${raw.contactName}`;
      upsertEcosystemNode(graph, {
        id: contactId,
        kind: ECOSYSTEM_NODE_KIND.COMMERCIAL_CONTACT,
        name: raw.contactName,
        canonicalName: raw.contactName,
        role: raw.contactRole || null,
        evidenceUrls: raw.evidenceUrls || [],
        sourceHunter: hunterId,
        confidence: raw.contactConfidence ?? 0.45,
      });
      addEcosystemEdge(graph, {
        kind: ECOSYSTEM_EDGE_KIND.CONTACT_AT,
        fromId: contactId,
        toId: orgId,
        sourceHunter: hunterId,
      });
    }

    const evidenceClass = resolveDiscoveryEvidenceClass(
      raw,
      DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE
    );
    const hyp = buildHotelDemandHypothesis({
      hotelId,
      organizationName: raw.organizationName,
      organizationId: orgId,
      activityTitle:
        activityTitle ||
        customerFacingOpportunityTitle(raw.activityTitle) ||
        raw.activityTitle,
      activityDate: raw.activityDate,
      demandTrigger: raw.demandTrigger,
      territory: territory || raw.territory,
      contactName: raw.contactName,
      contactRole: raw.contactRole,
      evidenceUrls: raw.evidenceUrls,
      sourceHunter: hunterId,
      confidence: raw.confidence ?? 0.5,
      hotelDemandRelevance: raw.hotelDemandRelevance ?? 0.55,
      pursuitThesis: raw.pursuitThesis,
      hasCredibleContact: Boolean(raw.contactName && raw.contactRole),
      hasRealisticHotelThesis: Boolean(raw.pursuitThesis),
    });
    hyp.discoveryEvidenceClass = evidenceClass;
    hyp.activityDateVerified = raw.activityDateVerified === true;
    hyp.customerInventoryEligible =
      evidenceClass !== DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE;
    hyp.publicationEligible = false;
    if (sourceActivityTitle && sourceActivityTitle !== activityTitle) {
      hyp.originalSourceTitle = sourceActivityTitle;
      hyp.researchTitlePreserved = sourceActivityTitle;
    }
    if (activityTitle) {
      hyp.displayTitle = activityTitle;
      hyp.title = activityTitle;
    }
    upsertEcosystemNode(graph, hyp);
    addEcosystemEdge(graph, {
      kind: ECOSYSTEM_EDGE_KIND.DEMAND_FOR_HOTEL,
      fromId: orgId,
      toId: hyp.id,
      sourceHunter: hunterId,
      confidence: hyp.confidence,
    });
    hypotheses.push(hyp);
  }

  const keySet = new Set(existingKeys);
  const { incremental, duplicates } = filterIncrementalHypotheses(hypotheses, keySet);

  return {
    graph,
    hypotheses,
    incremental,
    duplicates,
    rejected,
    excluded,
  };
}

export async function runSpecialistHunter({
  hunterId,
  discoverFn,
  hotelId,
  hotelName = "",
  territory = "",
  commercialPriorities = null,
  dryRun = true,
  cacheFirst = true,
  budgetUsd = 0,
  horizonMonths = SPECIALIST_HUNTER_HORIZON_MONTHS,
  existingKeys = [],
  reportDir = "",
  rootDir = process.cwd(),
} = {}) {
  const meta = getSpecialistHunterMeta(hunterId);
  const hunterConfig = resolveHunterConfig(hunterId, {
    hotelId,
    budgetUsd,
  });
  const safety = assertHunterRunSafe({
    hunterId,
    hotelId,
    budgetUsd,
    allowPaid: false,
    authorizeMeasurement: false,
  });
  const providerPlan = resolveHunterProviderPlan(hunterId, {
    dryRun,
    cacheFirst,
    allowPaidApify: false,
    allowPaidExa: false,
  });

  const startedAt = new Date().toISOString();
  let discoveries = [];
  let discoverMeta = {};

  if (
    safety.ok &&
    hunterConfig?.enabled !== false &&
    typeof discoverFn === "function"
  ) {
    const result = await discoverFn({
      hotelId,
      hotelName,
      territory,
      commercialPriorities,
      dryRun,
      cacheFirst,
      budgetUsd: 0,
      horizonMonths,
      providerPlan,
      rootDir,
      hunterConfig,
    });
    discoveries = Array.isArray(result?.discoveries) ? result.discoveries : [];
    discoverMeta = result?.meta || {};
  } else if (hunterConfig?.enabled === false) {
    discoverMeta = { mode: "DISABLED", reason: "hunter_disabled_by_config" };
  }

  const materialized = materializeHunterDiscoveries({
    hunterId,
    hotelId,
    territory,
    discoveries,
    existingKeys,
  });

  const evidenceClass =
    discoverMeta.discoveryEvidenceClass ||
    resolveDiscoveryEvidenceClass(
      discoveries[0] || {},
      DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE
    );

  const qualificationHandoff = buildQualificationHandoffPacket({
    hunterId,
    hotelId,
    hypotheses: materialized.incremental,
    graph: materialized.graph,
    discoveryEvidenceClass: evidenceClass,
    dryRun,
  });

  const report = {
    schema: "gdi-specialist-hunter-run-report-v1",
    hunterId,
    label: meta?.label || hunterId,
    hotelId,
    hotelName: hotelName || null,
    territory: territory || null,
    startedAt,
    finishedAt: new Date().toISOString(),
    dryRun,
    cacheFirst,
    budgetUsdSpent: 0,
    budgetUsdCeiling: Number(budgetUsd) || 0,
    paidAuthorized: false,
    horizonMonths,
    safety,
    hunterConfig,
    providerPlan,
    providerLiveCertified: false,
    discoveryEvidenceClass: evidenceClass,
    commercialPrioritiesApplied: Boolean(commercialPriorities),
    discoverMeta,
    qualificationHandoff,
    metrics: {
      rawDiscoveries: discoveries.length,
      hypotheses: materialized.hypotheses.length,
      incrementalHypotheses: materialized.incremental.length,
      duplicates: materialized.duplicates.length,
      rejected: materialized.rejected.length,
      excluded: materialized.excluded.length,
      organizations: materialized.graph.stats.organizations,
      activities: materialized.graph.stats.activities,
      contacts: materialized.graph.stats.contacts,
      readyForOrs: materialized.incremental.filter(
        (h) => h.hypothesisStatus === HUNTER_HYPOTHESIS_STATUS.READY_FOR_ORS
      ).length,
      hotelThesisFormed: materialized.incremental.filter(
        (h) =>
          h.hypothesisStatus === HUNTER_HYPOTHESIS_STATUS.HOTEL_THESIS_FORMED ||
          h.hypothesisStatus === HUNTER_HYPOTHESIS_STATUS.READY_FOR_ORS
      ).length,
      orsRouted: qualificationHandoff.summary.routedToOrs,
      fixturesExcludedFromOrs: qualificationHandoff.summary.fixturesExcluded,
      salesReadyEmitted: 0,
      autoPublished: false,
    },
    incremental: materialized.incremental,
    duplicates: materialized.duplicates,
    rejected: materialized.rejected,
    ecosystemStats: materialized.graph.stats,
    publicationSafeguard: {
      autoPublish: false,
      salesReadyFromHunter: false,
      entersExistingQualification: true,
      speculativeStaysResearchContinuation: true,
      syntheticFixturesNotCustomerInventory: true,
    },
  };

  if (reportDir) {
    fs.mkdirSync(reportDir, { recursive: true });
    const outPath = path.join(
      reportDir,
      `${hunterId}-${hotelId}-run.json`
    );
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
    fs.writeFileSync(
      path.join(reportDir, `${hunterId}-${hotelId}-ecosystem.json`),
      JSON.stringify(materialized.graph, null, 2),
      "utf8"
    );
    report.reportPath = outPath;
  }

  return report;
}

export { mergeEcosystemDelta };
