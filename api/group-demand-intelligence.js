/**
 * Group Demand Intelligence API — standalone; does not alter ADP.
 */

import { listDiscoveryCandidates } from "../lib/group-demand-intelligence/research/discovery-to-decision/discovery-candidate-ledger-v1.js";
import {
  aggregateResearchFunnel,
  funnelReconciles,
} from "../lib/group-demand-intelligence/research/discovery-to-decision/research-funnel-aggregate-v1.js";
import { whyIsntThisShowing } from "../lib/group-demand-intelligence/research/discovery-to-decision/why-isnt-this-showing-v1.js";
import { applyApprovedBethesdaCustomerProjection } from "../lib/group-demand-intelligence/workflow/gdi-bethesda-publication-v1.js";
import {
  isBethesdaInternalUnionHotel,
  mergeBethesdaInternalResearchOpportunities,
  BETHESDA_INTERNAL_LIST_SCOPE,
} from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  RESEARCH_PREVIEW_DISCLAIMER,
  RESEARCH_PREVIEW_SCOPE,
  RESEARCH_PREVIEW_TITLE,
  isResearchPreviewClaim,
  researchPreviewForClaim,
  researchPreviewManifestForHotel,
} from "../lib/group-demand-intelligence/research-preview-v1.js";
import {
  assertFounderCanarySession,
  loadTrustedConsumedFounderIdentity,
  requestFounderApproval,
} from "../lib/group-demand-intelligence/workflow/gdi-founder-approval-v1.js";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
} from "../lib/dealality/property-display-label.js";
import {
  isGroupDemandIntelligenceEnabled,
  isGroupDemandIntelligencePilotReadAllowed,
  getGroupDemandIntelligenceFlagState,
  runGroupDemandResearch,
  listRegisteredHotels,
  listGdiSelectableHotels,
  loadHotelProfile,
  loadLatestSummary,
  listResearchRuns,
  loadResearchRun,
  buildWeeklyBrief,
  filterSalespersonView,
  filterCustomerFacingOpportunities,
  isCustomerFacingOpportunity,
  isGdiTestOrFixtureOpportunity,
  saveFeedbackItem,
  loadFeedback,
  FEEDBACK_QUALITY,
  SALES_OUTCOME,
  HOTEL_FAMILIARITY,
  HOTEL_COMMERCIAL_STATUS,
  HOTEL_VALUE_FEEDBACK,
  SOURCING_STATUS,
  INCREMENTAL_VALUE_STATUS,
  QUALIFICATION_FAILURE_REASON,
  reconstructScoreAudit,
  buildPilotMetrics,
  PILOT_HOTEL_ID,
  getWebhoundHardCapUsd,
  resolveAmpfyAdapterStatus,
  listGdiResearchMethods,
  GDI_PRODUCT_VERSION,
  extractGdiShareCapabilityFromRequest,
  verifyGdiShareCapability,
  sanitizeOpportunityForShare,
  issueGdiShareCapability,
  revokeGdiShareCapability,
  applyCanonicalOverlaysToOpportunities,
  loadShareValidation,
  saveShareValidationItem,
  getShareValidationForOpportunity,
  restoreShareValidationItemAfterFailedPersist,
  buildShareValidationSummary,
  SHARE_FAMILIARITY_STATUS,
  SHARE_FAMILIARITY_LABEL,
  SHARE_COMMERCIAL_VALUE,
  SHARE_COMMERCIAL_VALUE_LABEL,
  SHARE_CONTACT_PERSON_ASSESSMENT,
  SHARE_CONTACT_PERSON_LABEL,
  SHARE_EMAIL_ASSESSMENT,
  SHARE_EMAIL_ASSESSMENT_LABEL,
  SHARE_PHONE_ASSESSMENT,
  SHARE_PHONE_ASSESSMENT_LABEL,
  mapOpportunitiesToListDto,
  toOpportunityListDto,
  GDI_OPPORTUNITY_LIST_SCHEMA,
  applyLiveCommercialQuality,
  buildRelatedOpportunityGroups,
  buildExportCsv,
  customerCsvContentDisposition,
  HOTEL_VALIDATION_REASON,
  HOTEL_VALIDATION_REASON_LABEL,
} from "../lib/group-demand-intelligence/index.js";
import {
  loadOpportunitiesCanonical,
  getGdiOpportunityPersistenceMode,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { applyCanonicalCustomerFacingTitle } from "../lib/group-demand-intelligence/demand-segments/universal-opportunity-identity-v1.js";
import { prepareCustomerFacingOpportunity } from "../lib/group-demand-intelligence/share/gdi-customer-facing-opportunity-contract-v1.js";
import {
  mapOpportunitiesToPresentationDtos,
  projectOpportunityForPresentation,
} from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import {
  annotateInventoryPopulations,
  buildInventoryPopulationCounts,
  isBethesda67CustomerPublicationApproved,
  selectBethesdaSalesOpportunityInventory,
  resolveGdiInventoryAudience,
  assertOpportunityReadableForInventoryAudience,
  assertHotelScopedSalesTeamInventoryAccess,
  GDI_INVENTORY_AUDIENCE,
  BETHESDA_HOTEL_ID,
  BETHESDA_SALES_TEAM_LIST_SCOPE,
  BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import {
  getCachedOpportunityDoc,
  invalidateGdiHotelReadCache,
} from "../lib/group-demand-intelligence/read-cache.js";
import {
  REVIEWED_PROSPECT_PUBLICATION_BASIS,
  isReviewedProspectPublicationEnabled,
  isReviewedProspectHotelEnabled,
} from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { enrichPrivateEventOpportunityDetail } from "../lib/group-demand-intelligence/private-events/customer-detail-enrichment.js";
import { buildHotelActivityHistory } from "../lib/group-demand-intelligence/hotel-activity-history-v1.js";
import { opportunityMatchesTypeFilter } from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import {
  buildShareOpportunityDetailDto,
  buildShareOpportunityListDto,
} from "../lib/group-demand-intelligence/share/share-customer-presentation-parity-v1.js";
import { redactCustomerFacingGdiSummary } from "../lib/group-demand-intelligence/customer-safe-run-status-label-v1.js";

function enrichSummaryWithHotelActivity(hotelId, summary, opportunityDoc) {
  const opportunities = opportunityDoc?.opportunities || [];
  const history = buildHotelActivityHistory(hotelId, {
    opportunities,
    opportunityDoc,
  });
  // Customer history is authoritative from eligible activity only — do not
  // fall back to loadLatestSummary stamps that mix verification / dry-runs.
  return {
    ...summary,
    lastResearchAt: history.lastResearchAt || null,
    lastSourceVerificationAt: history.lastSourceVerificationAt || null,
    lastSourceVerificationLabel: history.lastSourceVerificationLabel || null,
    runStatus: history.runStatus || "HISTORY_UNAVAILABLE",
    runStatusLabel: history.runStatusLabel || "History unavailable",
    latestRunId: history.latestRunId || null,
    runHistoryProvenance: history.runHistoryProvenance || null,
    lastReviewedPackageImportAt: history.lastReviewedPackageImportAt || null,
    lastPublishedUpdateAt: history.lastPublishedUpdateAt || null,
    activityHistoryVersion: history.version,
    excludedFromCustomerHistory: history.excludedFromCustomerHistory || null,
  };
}
import {
  ingestGdiAuthFeedback,
  ingestGdiShareValidation,
  ensureGdiOpportunityDecision,
  recordAction,
  recordOutcome,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
} from "../lib/decision-outcomes/index.js";
import {
  loadGdiCommercialProgressionBySubject,
  toCustomerCommercialProgressionDto,
} from "../lib/decision-outcomes/gdi-commercial-progression.js";
import {
  saveFounderReview,
  loadFounderReviews,
} from "../lib/group-demand-intelligence/local-shadow-commercial-portfolio-v1.js";
import { withTimeout, gdiReadTimeoutMs } from "../lib/http/with-timeout.js";
import {
  listVisibleDemandCampaigns,
  loadDemandCampaigns,
  isGdiDemandGeneratorVisible,
} from "../lib/group-demand-intelligence/demand-campaigns/index.js";
import {
  listPursuits,
  applyFollowUpDue,
  canStartPursuitFromOpportunity,
} from "../lib/group-demand-intelligence/pursuit/index.js";
import {
  loadPublicationMonitors,
  CUSTOMER_MONITORING_LABEL,
} from "../lib/group-demand-intelligence/publication-monitor/index.js";

/** Canonical opportunity load (Airtable primary when configured). Short-TTL cached. */
async function loadOppDoc(hotelId) {
  const t0 = Date.now();
  try {
    const doc = await withTimeout(
      loadOpportunitiesCanonical(hotelId),
      gdiReadTimeoutMs(),
      "gdi_load_opportunities"
    );
    let opportunities = doc?.opportunities || [];
    let publishedInventoryDiagnostic = null;
    // Durable published inventory registry (allowlist) — no local-shadow merges.
    // Allowlisted hotels use published inventory only (e.g. Spice approved seven).
    try {
      const {
        isGdiPublishedInventoryRegistryEnabledForHotel,
        diagnoseGdiPublishedInventoryRead,
      } = await import(
        "../lib/group-demand-intelligence/durable-publication/resolve-gdi-published-registry-root-v1.js"
      );
      if (isGdiPublishedInventoryRegistryEnabledForHotel(hotelId)) {
        publishedInventoryDiagnostic =
          diagnoseGdiPublishedInventoryRead(hotelId);
        // Allowlisted hotels fail closed. A missing or invalid registry must
        // not fall through to research-stage rows or the older approved pack.
        if (!publishedInventoryDiagnostic.ok) {
          opportunities = [];
        } else {
          const { mergePublishedInventoryIntoOpportunities } = await import(
            "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js"
          );
          opportunities = mergePublishedInventoryIntoOpportunities(
            hotelId,
            opportunities,
            { strictPublishedOnly: true }
          );
          if (!Array.isArray(opportunities) || opportunities.length === 0) {
            opportunities = [];
            publishedInventoryDiagnostic = {
              ...(publishedInventoryDiagnostic || {}),
              ok: false,
              code: "PUBLISHED_SNAPSHOTS_UNAVAILABLE",
            };
          }
        }
      }
    } catch (pubErr) {
      console.error(
        "[gdi] published inventory registry merge failed",
        pubErr?.message || pubErr
      );
      opportunities = [];
      publishedInventoryDiagnostic = {
        ok: false,
        code: "REGISTRY_MERGE_ERROR",
        message: String(pubErr?.message || pubErr),
      };
    }
    // Waterstone clean-28 share merge — peer regression lock for existing client share.
    // Idempotent; does not touch Spice registry hotels or rotate share tokens.
    try {
      const { mergeWaterstoneClean28ForShare } = await import(
        "../lib/group-demand-intelligence/waterstone-clean-28-share-merge-v1.js"
      );
      opportunities = mergeWaterstoneClean28ForShare(hotelId, opportunities);
    } catch (wsErr) {
      console.error(
        "[gdi] waterstone clean-28 share merge failed",
        wsErr?.message || wsErr
      );
    }
    const merged = {
      ...doc,
      opportunities,
      ...(publishedInventoryDiagnostic
        ? { publishedInventoryDiagnostic }
        : {}),
    };
    const ms = Date.now() - t0;
    if (ms >= 1000 || process.env.GDI_PERF_LOG === "1") {
      console.info(
        JSON.stringify({
          gdi_perf: true,
          route: "loadOppDoc",
          hotelId,
          durationMs: ms,
          cacheHit: doc?.cacheHit === true,
          recordCount: Array.isArray(merged?.opportunities)
            ? merged.opportunities.length
            : 0,
        })
      );
    }
    return merged;
  } catch (err) {
    if (err?.code === "UPSTREAM_TIMEOUT") {
      console.error(
        JSON.stringify({
          gdi_perf: true,
          route: "loadOppDoc",
          hotelId,
          error: "UPSTREAM_TIMEOUT",
          timeoutMs: err.timeoutMs,
          durationMs: Date.now() - t0,
        })
      );
      const timeoutErr = new Error(
        "Group Demand data is taking too long to load. Please retry in a moment."
      );
      timeoutErr.code = "UPSTREAM_TIMEOUT";
      timeoutErr.status = 504;
      throw timeoutErr;
    }
    throw err;
  }
}

/**
 * Prefer cached hotel doc; on miss load single Airtable row when possible
 * instead of re-listing the full hotel set for View Details.
 * Filesystem persistence mode never falls through to Airtable (local/import roots).
 * Bethesda: same store + internal-union identity system as the list endpoint.
 */
async function loadOpportunityForDetail(hotelId, opportunityId) {
  const t0 = Date.now();
  // Always use loadOppDoc so local shadow merge + founder reviews apply.
  // (Cached canonical docs alone omit shadow association rows / tier overlays.)
  const doc = await loadOppDoc(hotelId);
  const { resolveHotelOpportunityById } = await import(
    "../lib/group-demand-intelligence/gdi-opportunity-id-resolution-v1.js"
  );
  const resolved = resolveHotelOpportunityById(
    hotelId,
    opportunityId,
    doc.opportunities || [],
  );
  let source = resolved.source;
  if (resolved.opportunity && source === "hotel_store") {
    source = doc?.cacheHit ? "cache_with_shadow" : "full_doc";
  }
  return {
    opportunity: resolved.opportunity || null,
    source,
    resolvedId: resolved.resolvedId || null,
    durationMs: Date.now() - t0,
  };
}

function asOfToday() {
  return new Date().toISOString().slice(0, 10);
}

/** Apply live commercial quality + related-opportunity grouping (read path). */
function projectOpportunitiesCommercialQuality(opportunities, { nowDate } = {}) {
  const related = buildRelatedOpportunityGroups(opportunities || []);
  return (opportunities || []).map((o) => {
    const relatedIds = related[o.id] || [];
    // Published snapshots are authoritative. Hydrate/stamp only — never rebuild
    // titles, classifications, or drawer commercial copy via live sanitize.
    if (
      o?.publishedPresentationLocked === true ||
      o?.gdiPublishedInventoryRegistry === true
    ) {
      return prepareCustomerFacingOpportunity({
        ...o,
        relatedOpportunityIds: relatedIds,
        relatedOpportunityCount: relatedIds.length,
      });
    }
    // Bethesda internal research-stage reconstructions already carry honest
    // list/detail fields — do not run full customer sanitize that would strip
    // research diagnostics. Still apply the permanent title contract so
    // customer-visible list/tile/drawer titles stay short and professional.
    if (
      o?.bethesdaInternalResearchInventory === true ||
      o?.gdiInternalUnionOnly === true
    ) {
      return applyCanonicalCustomerFacingTitle({
        ...o,
        relatedOpportunityIds: relatedIds,
        relatedOpportunityCount: relatedIds.length,
        salesReady: false,
        customerFacing: false,
        customerVisible: false,
      });
    }
    const cq = applyLiveCommercialQuality(o, { nowDate: nowDate || asOfToday() });
    // Never leak discovery vertical slugs into customer segment
    let segment = cq.segment;
    if (
      segment &&
      /^[a-z][a-z0-9]*(?:_[a-z0-9]+){1,8}$/.test(String(segment).trim()) &&
      !/\s/.test(String(segment))
    ) {
      segment =
        cq.demandSignalTypeLabel ||
        cq.opportunityTypeLabel ||
        cq.demandFamilyLabel ||
        "Discovered";
    }
    // Shared list+detail customer presentation: scrub assessment prose, provider
    // brands, and withheld-score fallbacks. Hotel-agnostic — no property bypasses.
    return sanitizeOpportunityForCustomerPresentation({
      ...cq,
      segment,
      relatedOpportunityIds: relatedIds,
      relatedOpportunityCount: relatedIds.length,
    });
  });
}

async function attachCommercialProgression(hotelId, opportunities) {
  try {
    const bySubject = await withTimeout(
      loadGdiCommercialProgressionBySubject(hotelId),
      gdiReadTimeoutMs(),
      "gdi_commercial_progression"
    );
    return (opportunities || []).map((o) => {
      const dto = toCustomerCommercialProgressionDto(bySubject.get(o.id));
      return dto ? { ...o, commercialProgression: dto } : o;
    });
  } catch (err) {
    // Non-fatal for browse: return list without progression rather than hang/fail the page.
    console.error(
      JSON.stringify({
        gdi_perf: true,
        route: "attachCommercialProgression",
        hotelId,
        error: err?.code || err?.message || String(err),
        degraded: true,
      })
    );
    return opportunities || [];
  }
}

/** Apply list/export filters (ids + browse facets). */
function filterOpportunitiesForExport(opportunities, query = {}) {
  let rows = opportunities || [];
  const idsFilter = String(query.ids || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (idsFilter.length) {
    const set = new Set(idsFilter);
    rows = rows.filter((o) => set.has(o.id) || set.has(o.opportunityId));
  }
  const weekly = String(query.weekly || "").trim().toUpperCase();
  const priority = String(query.priority || "").trim().toUpperCase();
  const booking = String(query.booking || "").trim().toUpperCase();
  const segment = String(query.segment || "").trim();
  const territory = String(query.territory || "").trim().toUpperCase();
  const opportunityType = String(query.opportunityType || "").trim();
  if (weekly && weekly !== "ALL") {
    rows = rows.filter((o) => {
      if (weekly === "NEW") return o.isNewThisWeek === true || o.weeklyDeltaState === "NEW";
      return String(o.weeklyDeltaState || "").toUpperCase() === weekly;
    });
  }
  if (priority && priority !== "ALL") {
    rows = rows.filter((o) => String(o.priority || "").toUpperCase() === priority);
  }
  if (booking && booking !== "ALL") {
    rows = rows.filter(
      (o) => String(o.bookingWindowStatus || "").toUpperCase() === booking
    );
  }
  if (segment) {
    rows = rows.filter((o) => String(o.segment || "") === segment);
  }
  if (territory && territory !== "ALL") {
    rows = rows.filter(
      (o) => String(o.demandTerritoryFit || "").toUpperCase() === territory
    );
  }
  if (opportunityType && opportunityType.toUpperCase() !== "ALL") {
    rows = rows.filter((o) => opportunityMatchesTypeFilter(o, opportunityType));
  }
  return rows;
}

function sendCustomerCsv(res, opportunities, hotel) {
  const t0 = Date.now();
  const csv = buildExportCsv(opportunities, hotel, {});
  const name =
    hotel?.name ||
    hotel?.displayName ||
    hotel?.identity?.hotelName ||
    hotel?.hotelName ||
    "Hotel";
  const durationMs = Date.now() - t0;
  if (durationMs >= 500 || process.env.GDI_PERF_LOG === "1") {
    console.info(
      JSON.stringify({
        gdi_perf: true,
        route: "sendCustomerCsv",
        hotelId: hotel?.hotelId || null,
        opportunityCount: Array.isArray(opportunities)
          ? opportunities.length
          : 0,
        durationMs,
        bytes: Buffer.byteLength(csv, "utf8"),
      })
    );
  }
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", customerCsvContentDisposition(name));
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-GDI-Csv-Contract", "gdi_customer_csv_full_detail_v1");
  return res.status(200).send(csv);
}

function flagGate(req, res) {
  if (
    isGroupDemandIntelligenceEnabled() ||
    isGroupDemandIntelligencePilotReadAllowed()
  ) {
    return true;
  }
  res.status(403).json({
    ok: false,
    error: "feature_disabled",
    message:
      "Group Demand Intelligence is disabled. Local servers auto-enable pilot read; set GROUP_DEMAND_INTELLIGENCE_V1=1 or GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1, or remove PILOT_READ=0.",
    flag: getGroupDemandIntelligenceFlagState(),
  });
  return false;
}

function sendGdiUpstreamError(res, err, route) {
  const code = err?.code || "server_error";
  const status =
    err?.status ||
    (code === "UPSTREAM_TIMEOUT" ? 504 : code === "hotel_boundary_violation" ? 403 : 500);
  console.error(
    JSON.stringify({
      gdi_error: true,
      route,
      code,
      status,
      message: err?.message || String(err),
    })
  );
  return res.status(status).json({
    ok: false,
    error: code === "UPSTREAM_TIMEOUT" ? "upstream_timeout" : code,
    message:
      err?.message ||
      (status === 504
        ? "Upstream data timed out. Please retry."
        : "Failed to load Group Demand Intelligence."),
  });
}

export function getGdiFlag(req, res) {
  return res.json({
    ok: true,
    flag: getGroupDemandIntelligenceFlagState(),
    productVersion: GDI_PRODUCT_VERSION,
    webhoundHardCapUsd: getWebhoundHardCapUsd(),
    ampfy: resolveAmpfyAdapterStatus(),
    pilotHotelId: PILOT_HOTEL_ID,
  });
}

export function getGdiHotels(req, res) {
  if (!flagGate(req, res)) return;
  const hotels = listGdiSelectableHotels();
  if (hotels.length === 0) {
    // Fallback: materialized profiles only (legacy)
    const registered = listRegisteredHotels();
    if (registered.length === 0) {
      return res.json({
        ok: true,
        hotels: [],
        experimentalLabel: "EXPERIMENTAL / PILOT",
        note: "No GDI-enabled hotels discovered yet. Add config + run research.",
      });
    }
    return res.json({
      ok: true,
      hotels: registered.map((h) => ({
        hotelId: h.hotelId,
        hotelName: h.identity?.hotelName || h.hotelId,
        displayName: h.identity?.hotelName || h.hotelId,
        brand: h.identity?.brand,
        city: h.identity?.city,
        state: h.identity?.state,
        locationLine: formatPropertyLocationLine({
          city: h.identity?.city,
          state: h.identity?.state,
          country: h.identity?.country,
        }),
        optionLabel: formatPropertySelectorLabel({
          hotelName: h.identity?.hotelName || h.hotelId,
          city: h.identity?.city,
          state: h.identity?.state,
          country: h.identity?.country,
        }),
        hasProfile: true,
        hasOpportunities: false,
        identityStatus: h.canonicalIds?.censusRecordId
          ? "canonical_census"
          : "provisional_gdi_key",
      })),
      experimentalLabel: "EXPERIMENTAL / PILOT",
    });
  }
  return res.json({
    ok: true,
    hotels,
    experimentalLabel: "EXPERIMENTAL / PILOT",
  });
}

export async function getGdiSummary(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    const profile = loadHotelProfile(hotelId);
    const doc = await loadOppDoc(hotelId);
    let summaryOpps = doc.opportunities || [];
    let bethesdaUnionSummary = null;
    if (isBethesdaInternalUnionHotel(hotelId)) {
      const union = mergeBethesdaInternalResearchOpportunities(
        hotelId,
        summaryOpps
      );
      summaryOpps = union.opportunities;
      bethesdaUnionSummary = {
        applied: true,
        counts: union.counts,
        listScope: union.scope,
      };
    }
    const summary = loadLatestSummary(hotelId, {
      opportunities: summaryOpps,
      opportunityDoc: doc,
    });
    if (bethesdaUnionSummary?.counts?.union) {
      summary.qualifiedCount = bethesdaUnionSummary.counts.union;
      summary.opportunityCount = bethesdaUnionSummary.counts.union;
      summary.bethesdaInternalUnion = bethesdaUnionSummary;
    }
    const feedback = loadFeedback(hotelId);
    const pilotMetrics = buildPilotMetrics(summaryOpps, feedback);
    const campaignsVisible = listVisibleDemandCampaigns(hotelId, {
      nowDate: asOfToday(),
    });
    // Research runs ≠ reviewed imports ≠ canonical publication. NEVER_RUN only
    // when history proves no run; otherwise prefer HISTORY_UNAVAILABLE / recovered runs.
    const enrichedSummary = {
      ...enrichSummaryWithHotelActivity(hotelId, summary, doc),
      automatedResearchRunStatus: null, // prefer runStatusLabel for customers
    };
    enrichedSummary.automatedResearchRunStatus = enrichedSummary.runStatus;
    // Run IDs / costs / provenance are admin-only (AI Demand Admin → GDI Research Audit).
    const customerSummary = redactCustomerFacingGdiSummary(enrichedSummary);
    return res.json({
      ok: true,
      experimentalLabel: "EXPERIMENTAL / PILOT",
      hotelId,
      profileReady: Boolean(profile),
      summary: customerSummary,
      competitiveContext: profile
        ? {
            strCompSet: profile.strCompSet || [],
            relevantGroupDemandAlternatives: profile.relevantGroupDemandAlternatives || [],
            demandTerritory: profile.demandTerritory || null,
          }
        : null,
      pilotMetrics,
      demandCampaigns: {
        visibleCount: campaignsVisible.count,
        storedCount: campaignsVisible.totalStored,
      },
      flag: getGroupDemandIntelligenceFlagState(),
    });
  } catch (err) {
    return sendGdiUpstreamError(res, err, "getGdiSummary");
  }
}

/**
 * Demand campaigns / generators — research universe layer.
 * Visibility uses isGdiDemandGeneratorVisible — NOT customer-ready gate.
 */
export function getGdiDemandCampaigns(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    const includeHidden = String(req.query.includeHidden || "") === "1";
    const nowDate = asOfToday();
    if (includeHidden) {
      const doc = loadDemandCampaigns(hotelId);
      const rows = (doc.campaigns || []).map((c) => ({
        ...c,
        visibility: isGdiDemandGeneratorVisible(c, { nowDate }),
      }));
      return res.json({
        ok: true,
        hotelId,
        updatedAt: doc.updatedAt,
        count: rows.length,
        campaigns: rows,
        gate: "isGdiDemandGeneratorVisible",
        note: "Campaigns are not customer-ready opportunities.",
      });
    }
    const visible = listVisibleDemandCampaigns(hotelId, { nowDate });
    return res.json({
      ok: true,
      hotelId,
      updatedAt: visible.updatedAt,
      count: visible.count,
      storedCount: visible.totalStored,
      campaigns: visible.campaigns,
      gate: "isGdiDemandGeneratorVisible",
      note: "Campaigns are not customer-ready opportunities.",
    });
  } catch (err) {
    return sendGdiUpstreamError(res, err, "getGdiDemandCampaigns");
  }
}

/**
 * Publication-trigger monitors — internal research ops surface.
 * Customer UI never sees crawler hashes; only watch-card safe fields on opportunities.
 */
export function getGdiPublicationMonitors(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    const customerSafe = String(req.query.customerSafe || "") === "1";
    const doc = loadPublicationMonitors(hotelId);
    const monitors = (doc.monitors || []).map((m) => {
      if (!customerSafe) return m;
      return {
        monitorId: m.monitorId,
        campaignId: m.campaignId,
        campaignKey: m.campaignKey,
        monitoringStatus: m.monitoringStatus,
        customerMonitoringFor: m.customerMonitoringFor || [],
        lastCheckedAt: m.lastCheckedAt || null,
        nextCheckAt: m.nextCheckAt || null,
        expectedPublicationWindowStart: m.expectedPublicationWindowStart || null,
        expectedPublicationWindowEnd: m.expectedPublicationWindowEnd || null,
        triggerLabels: (m.watchForTypes || []).map(
          (t) => CUSTOMER_MONITORING_LABEL[t] || t
        ),
      };
    });
    return res.json({
      ok: true,
      hotelId,
      updatedAt: doc.updatedAt,
      count: monitors.length,
      monitors,
      note: customerSafe
        ? "Customer-safe monitor summary — no content hashes or crawler debug."
        : "Internal publication monitors (known official sources only).",
    });
  } catch (err) {
    return sendGdiUpstreamError(res, err, "getGdiPublicationMonitors");
  }
}

export function getGdiProfile(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const profile = loadHotelProfile(hotelId);
  if (!profile) {
    return res.status(404).json({
      ok: false,
      error: "profile_not_found",
      message: "Run research to build the Hotel Group Demand Profile.",
    });
  }
  return res.json({ ok: true, profile });
}

export async function getGdiOpportunities(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    const includeDisqualified = String(req.query.includeDisqualified || "") === "1";
    const view = String(req.query.view || "list").trim().toLowerCase();
    const format = String(req.query.format || "").trim().toLowerCase();
    const wantFull = view === "full" || view === "complete";

    // Optional immutable publication preview (same contract as external share).
    // Use ?publicationPackage=1 (or surface=customer) so founder UI can match
    // production without live commercial/need-date recompute. Research browse
    // remains the default unless a package pointer exists and is requested.
    const wantPublicationPackage =
      String(req.query.publicationPackage || "") === "1" ||
      String(req.query.surface || "").toLowerCase() === "customer";
    if (wantPublicationPackage && hotelId) {
      const {
        resolvePreviewOpportunitiesFromPublicationPackage,
        mergeShareFeedbackOntoPackageOpportunities,
      } = await import(
        "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js"
      );
      const pkgResolved = resolvePreviewOpportunitiesFromPublicationPackage({
        hotelId,
        forceLive: String(req.query.live || "") === "1",
      });
      if (pkgResolved.ok && pkgResolved.mode === "PACKAGE") {
        const validationDoc = loadShareValidation(hotelId);
        let withFeedback = mergeShareFeedbackOntoPackageOpportunities(
          pkgResolved.opportunities,
          validationDoc,
        );
        // Read-time presentation attach (tile contact + revenue icons). Does
        // not alter package fingerprint — same resolvers as live sanitize.
        try {
          const { attachPackageCustomerPresentation } = await import(
            "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js"
          );
          withFeedback = attachPackageCustomerPresentation(withFeedback);
        } catch (tilePreviewErr) {
          console.error(
            "[gdi] auth publication-package customer presentation attach failed",
            tilePreviewErr?.message || tilePreviewErr,
          );
        }
        // Match share PACKAGE: hotel Need Dates status is live hotel-config
        // metadata (not package fingerprint). Without it, auth
        // ?publicationPackage=1 falls back to NOT_ESTABLISHED while share
        // correctly shows HOTEL_CONFIRMED for the same fingerprint.
        let hotelNeedDateConfigV1;
        try {
          const {
            resolveHotelNeedDateConfiguration,
            toCustomerSafeHotelNeedDateStatus,
          } = await import(
            "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js"
          );
          hotelNeedDateConfigV1 = toCustomerSafeHotelNeedDateStatus(
            resolveHotelNeedDateConfiguration(hotelId),
            { includeUnpublishedEstimates: false },
          );
        } catch (needDateCfgErr) {
          console.error(
            "[gdi] auth publication-package hotel need-date config resolve failed",
            needDateCfgErr?.message || needDateCfgErr,
          );
          hotelNeedDateConfigV1 = {
            version: "GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1",
            hotelId,
            status: "NOT_ESTABLISHED",
            statusLabel: "Need dates not established",
            accessibleLabel: "Need dates not established",
            icon: "calendar-minus",
            colorClass: "gdi-hotel-need-date-status--not-established",
            confirmedPeriodCount: 0,
            estimatedPeriodCount: 0,
            internalOnly: false,
          };
        }
        return res.json({
          ok: true,
          count: withFeedback.length,
          opportunities: withFeedback,
          publication: pkgResolved.meta || null,
          inventoryPopulationCounts:
            pkgResolved.meta?.inventoryPopulationCounts || null,
          listScope: "PUBLICATION_PACKAGE",
          hotelNeedDateConfigV1,
        });
      }
    }

    const doc = await loadOppDoc(hotelId);
    // Visibility must run on canonical fields before presentation sanitize.
    // Sanitizer may fill summaryWhat from summaryWhyMatters/thesis, which
    // incorrectly satisfies travel_or_recurring_evidence and widens the list.
    let opportunities = doc.opportunities || [];
    let listScope = "CUSTOMER_FACING";
    let bethesdaUnionMeta = null;
    const { isInventoryVisibilityHotelEnabled } = await import(
      "../lib/group-demand-intelligence/inventory-visibility-v1.js"
    );
    if (isBethesdaInternalUnionHotel(hotelId)) {
      // INTERNAL unified inventory: retain all canonical Bethesda IDs + 29
      // research-stage reconstructions (88). Do not apply customer-facing
      // publication filters here — share APIs remain separately gated.
      const union = mergeBethesdaInternalResearchOpportunities(
        hotelId,
        opportunities
      );
      opportunities = union.opportunities;
      listScope = union.scope || BETHESDA_INTERNAL_LIST_SCOPE;
      const {
        filterOpportunitiesByCommercialSurface,
        attachCommercialSurfaceDisposition,
        GDI_LIST_SURFACE_VIEW,
        summarizeCommercialSurfaceBatch,
        resolveRequestedCommercialSurfaceView,
        assertPrimaryCommercialHasNoForbiddenIds,
      } = await import(
        "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js"
      );
      const requestedSurface = resolveRequestedCommercialSurfaceView(req.query || {});
      const surfaceView = requestedSurface;
      const requireFailClosed =
        surfaceView === GDI_LIST_SURFACE_VIEW.COMMERCIAL;
      // Presentation-only commercial-family collapse: one primary card per
      // underlying event; linked/quarantined research IDs retained for detail.
      let familyMeta = null;
      try {
        const { applyBethesdaCommercialEventFamilyPresentation } = await import(
          "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js"
        );
        const family = applyBethesdaCommercialEventFamilyPresentation(opportunities, {
          hideLinkedFromList: surfaceView !== GDI_LIST_SURFACE_VIEW.FULL,
        });
        opportunities = family.opportunities;
        familyMeta = family.counts;
      } catch (familyErr) {
        console.error(
          JSON.stringify({
            gdi_error: true,
            route: "getGdiOpportunities",
            code: "BETHESDA_FAMILY_COLLAPSE_FAILED",
            hotelId,
            requestedSurface: surfaceView,
            message: familyErr?.message || String(familyErr),
          }),
        );
        if (requireFailClosed) {
          return res.status(503).json({
            ok: false,
            error: "BETHESDA_FAMILY_COLLAPSE_FAILED",
            hotelId,
            requestedSurface: surfaceView,
            effectiveSurface: null,
            listScope: null,
            message:
              "Commercial opportunity list unavailable: event-family presentation failed closed.",
            opportunities: [],
            count: 0,
          });
        }
      }
      // PRIMARY commercial list (default): exclude DQ, out-of-market archive,
      // market-intelligence themes, research-pipeline-only, linked/quarantined.
      // Research/admin ledger requires explicit ?view=full|complete or ?surface=.
      // NEVER fail open to the unfiltered union on default commercial browse.
      try {
        if (
          requireFailClosed &&
          String(process.env.GDI_FORCE_COMMERCIAL_SURFACE_FILTER_FAILURE || "") ===
            "1"
        ) {
          throw new Error("FORCED_COMMERCIAL_SURFACE_FILTER_FAILURE");
        }
        const beforeSurfaceRows = opportunities;
        const beforeSurface = beforeSurfaceRows.length;
        const surfaceSummary = summarizeCommercialSurfaceBatch(beforeSurfaceRows);
        opportunities = filterOpportunitiesByCommercialSurface(
          beforeSurfaceRows,
          surfaceView,
        ).map((o) => attachCommercialSurfaceDisposition(o));
        if (requireFailClosed) {
          const leakCheck = assertPrimaryCommercialHasNoForbiddenIds(opportunities);
          if (!leakCheck.ok) {
            console.error(
              JSON.stringify({
                gdi_error: true,
                route: "getGdiOpportunities",
                code: "COMMERCIAL_SURFACE_FORBIDDEN_LEAK",
                hotelId,
                requestedSurface: surfaceView,
                leakedIds: leakCheck.leakedIds,
              }),
            );
            return res.status(503).json({
              ok: false,
              error: "COMMERCIAL_SURFACE_FORBIDDEN_LEAK",
              hotelId,
              requestedSurface: surfaceView,
              effectiveSurface: null,
              listScope: null,
              leakedIds: leakCheck.leakedIds,
              message:
                "Commercial opportunity list unavailable: forbidden research/archive IDs leaked after filter.",
              opportunities: [],
              count: 0,
            });
          }
        }
        // Primary commercial (default): sales-team inventory ≠ published feed.
        // Research/FULL surfaces keep research inventory for admin tools only.
        let afterSurfaceCount = opportunities.length;
        let inventoryAudience = GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
        let salesSelectionMeta = null;
        if (surfaceView === GDI_LIST_SURFACE_VIEW.COMMERCIAL) {
          const beforeFacing = opportunities.length;
          inventoryAudience = resolveGdiInventoryAudience(req, { hotelId });
          if (
            inventoryAudience === GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY
          ) {
            // Do NOT apply public customer-publication eligibility to the
            // authorized sales-team baseline. Select exact frozen 67 IDs.
            const sel = selectBethesdaSalesOpportunityInventory(opportunities, {
              salesTeamInventory: true,
              founderReview: true,
            });
            opportunities = sel.selected;
            afterSurfaceCount = opportunities.length;
            salesSelectionMeta = {
              expectedCount: sel.expectedCount,
              selectedCount: sel.selected.length,
              missingIds: sel.missingIds,
              publishedCount: sel.publishedCount,
              stagedPresentCount: sel.stagedPresentCount,
            };
            listScope = BETHESDA_SALES_TEAM_LIST_SCOPE;
          } else {
            // Exact frozen published 19 — do not widen via live customerFacing
            // heuristics (Airtable thin projections can inflate beyond the
            // protected publication boundary).
            const sel = selectBethesdaSalesOpportunityInventory(opportunities, {
              salesTeamInventory: false,
              founderReview: false,
            });
            opportunities = sel.selected;
            afterSurfaceCount = opportunities.length;
            salesSelectionMeta = {
              expectedCount: sel.expectedCount,
              selectedCount: sel.selected.length,
              missingIds: sel.missingIds,
              publishedCount: sel.publishedCount,
              stagedPresentCount: sel.stagedPresentCount,
            };
            listScope = BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE;
          }
          bethesdaUnionMeta = {
            applied: true,
            counts: union.counts,
            commercialEventFamily: familyMeta,
            inventoryAudience,
            salesTeamInventory: salesSelectionMeta,
            commercialSurface: {
              requestedSurface: surfaceView,
              effectiveSurface: surfaceView,
              view: surfaceView,
              beforeCount: beforeSurface,
              afterSurfaceCount: beforeFacing,
              afterCustomerFacingCount:
                inventoryAudience ===
                GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED
                  ? afterSurfaceCount
                  : null,
              afterSalesTeamInventoryCount:
                inventoryAudience ===
                GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY
                  ? afterSurfaceCount
                  : null,
              afterCount: afterSurfaceCount,
              eligibleCount: afterSurfaceCount,
              excludedCount: Math.max(0, beforeSurface - afterSurfaceCount),
              customerFacingExcluded:
                inventoryAudience ===
                GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED
                  ? Math.max(0, beforeFacing - afterSurfaceCount)
                  : 0,
              summary: surfaceSummary,
              // Local auth sales-team page intentionally exceeds published 19.
              localPublishedParity:
                inventoryAudience ===
                GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED,
            },
            storeWrites: false,
            customerShare: false,
          };
        } else {
          bethesdaUnionMeta = {
            applied: true,
            counts: union.counts,
            commercialEventFamily: familyMeta,
            inventoryAudience: null,
            commercialSurface: {
              requestedSurface: surfaceView,
              effectiveSurface: surfaceView,
              view: surfaceView,
              beforeCount: beforeSurface,
              afterCount: opportunities.length,
              eligibleCount: opportunities.length,
              excludedCount: Math.max(0, beforeSurface - opportunities.length),
              summary: surfaceSummary,
              localPublishedParity: false,
            },
            storeWrites: false,
            customerShare: false,
          };
          listScope =
            surfaceView === GDI_LIST_SURFACE_VIEW.FULL
              ? listScope
              : `BETHESDA_SURFACE_${String(surfaceView).toUpperCase()}`;
        }
      } catch (surfaceErr) {
        console.error(
          JSON.stringify({
            gdi_error: true,
            route: "getGdiOpportunities",
            code: "COMMERCIAL_SURFACE_FILTER_FAILED",
            hotelId,
            requestedSurface: surfaceView,
            message: surfaceErr?.message || String(surfaceErr),
          }),
        );
        if (requireFailClosed) {
          return res.status(503).json({
            ok: false,
            error: "COMMERCIAL_SURFACE_FILTER_FAILED",
            hotelId,
            requestedSurface: surfaceView,
            effectiveSurface: null,
            listScope: null,
            message:
              "Commercial opportunity list unavailable: surface filter failed closed.",
            detail: String(surfaceErr?.message || surfaceErr),
            opportunities: [],
            count: 0,
          });
        }
        bethesdaUnionMeta = {
          applied: true,
          counts: union.counts,
          commercialEventFamily: familyMeta,
          storeWrites: false,
          customerShare: false,
          surfaceFilterError: String(surfaceErr?.message || surfaceErr),
        };
      }
    } else {
      // Inventory visibility (Waterstone): keep DISQUALIFIED for Excluded/history lanes.
      if (!includeDisqualified && !isInventoryVisibilityHotelEnabled(hotelId)) {
        opportunities = filterSalespersonView(opportunities);
      }
      const visibilityOpts = { hotelId };
      opportunities = filterCustomerFacingOpportunities(
        opportunities,
        visibilityOpts
      );
    }
    opportunities = applyApprovedBethesdaCustomerProjection(opportunities);
    opportunities = projectOpportunitiesCommercialQuality(opportunities);
    // Hotel-scoped Need-Date tile icons — one period load per request (no N+1).
    try {
      const { attachHotelNeedDateTilePreviews } = await import(
        "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js"
      );
      const hotels = listRegisteredHotels();
      const hotelMeta =
        hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
      opportunities = await attachHotelNeedDateTilePreviews(hotelId, opportunities, {
        hotelName: hotelMeta.name || hotelMeta.hotelName || null,
      });
    } catch (needDateTileErr) {
      console.error(
        "[gdi] need-date tile preview attach failed",
        needDateTileErr?.message || needDateTileErr,
      );
    }

    // CSV via ?format=csv (compat path; dedicated /export.csv also exists)
    if (format === "csv") {
      const hotels = listRegisteredHotels();
      const hotel = hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
      opportunities = filterOpportunitiesForExport(opportunities, req.query);
      opportunities = await attachCommercialProgression(hotelId, opportunities);
      return sendCustomerCsv(res, opportunities, hotel);
    }

    // Annotate populations on canonical rows BEFORE presentation DTO trim so
    // CMI / baseline membership survive field allowlisting.
    opportunities = annotateInventoryPopulations(opportunities, { hotelId });
    const populationAnnotationsById = new Map(
      opportunities.map((o) => [
        String(o.id || o.opportunityId || ""),
        {
          inventoryPopulations: o.inventoryPopulations,
          inventoryPopulationPrimary: o.inventoryPopulationPrimary,
          salesBaselineMembership: o.salesBaselineMembership,
          salesBaselineMaturityKey: o.salesBaselineMaturityKey,
          salesBaselineMaturityLabel: o.salesBaselineMaturityLabel,
          stagedCustomerReadiness: o.stagedCustomerReadiness,
          cmiClass: o.cmiClass,
          cmiVerified: o.cmiVerified,
          cmiCompetitorHotel: o.cmiCompetitorHotel,
          cmiClassificationReason: o.cmiClassificationReason,
          competitiveStatus: o.competitiveStatus,
          competitiveStatusLabel: o.competitiveStatusLabel,
          competitorVenueStatus: o.competitorVenueStatus,
          competitorVenueStatusLabel: o.competitorVenueStatusLabel,
          targetHotelSourcingStatus: o.targetHotelSourcingStatus,
          targetHotelSourcingStatusLabel: o.targetHotelSourcingStatusLabel,
          commercialActionability: o.commercialActionability,
          hotelMarketScopeEligible: o.hotelMarketScopeEligible,
          competitiveMarketScopeEligible: o.competitiveMarketScopeEligible,
          competitiveFutureCyclePresentation: o.competitiveFutureCyclePresentation,
          competitiveCommercialActionKey: o.competitiveCommercialActionKey,
          competitiveCommercialActionLabel: o.competitiveCommercialActionLabel,
          futureCycleOpportunityStatus: o.futureCycleOpportunityStatus,
          recommendedAction: o.recommendedAction,
          salesActionKey: o.salesActionKey,
          salesActionPillLabel: o.salesActionPillLabel,
        },
      ])
    );
    if (!wantFull) {
      // Single universal list projection (same prepare → DTO path as share).
      opportunities = mapOpportunitiesToPresentationDtos(opportunities, {
        hotelId,
        surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
      });
      opportunities = opportunities.map((dto) => {
        const ann = populationAnnotationsById.get(String(dto.id || ""));
        return ann ? { ...dto, ...ann } : dto;
      });
    }
    opportunities = await attachCommercialProgression(hotelId, opportunities);
    // Overlay pursuit workflow (sales status — independent of Ready/Watch)
    try {
      applyFollowUpDue(hotelId);
      const byOpp = new Map(
        listPursuits(hotelId).map((p) => [p.opportunityId, p])
      );
      opportunities = opportunities.map((o) => {
        const p = byOpp.get(o.id);
        return {
          ...o,
          pursuitId: p?.pursuitId || o.pursuitId || null,
          pursuitStatus: p?.pursuitStatus || o.pursuitStatus || null,
          canStartPursuit:
            !p &&
            canStartPursuitFromOpportunity({
              outreachReadiness: o.outreachReadiness,
            }),
        };
      });
    } catch {
      /* pursuit overlay optional */
    }
    res.setHeader("X-GDI-Opportunity-View", wantFull ? "full" : "list");
    res.setHeader("X-GDI-List-Scope", listScope);
    res.setHeader("X-GDI-Already-Filtered", "1");
    if (bethesdaUnionMeta?.applied) {
      res.setHeader("X-GDI-Bethesda-Internal-Union", "88");
    }
    const reviewedInStore = (doc.opportunities || []).filter(
      (o) =>
        String(o?.prospectPublicationBasis || "") ===
          REVIEWED_PROSPECT_PUBLICATION_BASIS &&
        o?.prospectHotelFacing === true
    ).length;
    let visibilityWarning = null;
    if (opportunities.length === 0 && reviewedInStore > 0) {
      const publicationEnabled = isReviewedProspectPublicationEnabled();
      const hotelEnabled = isReviewedProspectHotelEnabled(hotelId);
      visibilityWarning = {
        code: "REVIEWED_PROSPECTS_FILTERED",
        reviewedInStore,
        publicationEnabled,
        hotelEnabled,
        message: !publicationEnabled
          ? "Reviewed prospects exist for this property but GDI_REVIEWED_PROSPECT_PUBLICATION_V1 is not enabled in this process."
          : !hotelEnabled
            ? "Reviewed prospects exist for this property but it is not on the reviewed-prospect hotel allowlist."
            : "Reviewed prospects exist in storage but none passed customer-facing visibility filters.",
      };
    }
    // Prefer explicit publication-data diagnostics over a false "no opportunities" empty state.
    const pubDiag = doc.publishedInventoryDiagnostic || null;
    if (
      opportunities.length === 0 &&
      pubDiag &&
      pubDiag.ok === false &&
      !["OK", "APPROVED_PACK_FALLBACK", "MISSING_PUBLICATION_POINTER"].includes(
        String(pubDiag.code || "")
      )
    ) {
      visibilityWarning = {
        code: String(pubDiag.code || "REGISTRY_UNAVAILABLE"),
        publicationDataConnection: false,
        message:
          "Unable to load approved opportunities. Check publication data connection.",
        diagnostic: pubDiag,
      };
    } else if (
      opportunities.length === 0 &&
      pubDiag &&
      String(pubDiag.code || "") === "MISSING_PUBLICATION_POINTER"
    ) {
      visibilityWarning = {
        code: "MISSING_PUBLICATION_POINTER",
        publicationDataConnection: true,
        message:
          "Publication registry is configured, but this hotel has no current approved publication pointer.",
        diagnostic: pubDiag,
      };
    }
    let hotelNeedDateConfigV1;
    try {
      const {
        resolveHotelNeedDateConfiguration,
        toCustomerSafeHotelNeedDateStatus,
      } = await import(
        "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js"
      );
      hotelNeedDateConfigV1 = toCustomerSafeHotelNeedDateStatus(
        resolveHotelNeedDateConfiguration(hotelId),
        { includeUnpublishedEstimates: true }
      );
    } catch (needDateCfgErr) {
      console.error(
        "[gdi] hotel need-date config resolve failed",
        needDateCfgErr?.message || needDateCfgErr
      );
      // Never omit the DTO — UI defaults require an explicit status object.
      hotelNeedDateConfigV1 = {
        version: "GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1",
        hotelId,
        status: "NOT_ESTABLISHED",
        statusLabel: "Need dates not established",
        accessibleLabel: "Need dates not established",
        icon: "calendar-minus",
        colorClass: "gdi-hotel-need-date-status--not-established",
        confirmedPeriodCount: 0,
        estimatedPeriodCount: 0,
        internalOnly: false,
      };
    }

    const serverBuildSha =
      process.env.RAILWAY_GIT_COMMIT_SHA ||
      process.env.GITHUB_SHA ||
      process.env.COMMIT_SHA ||
      process.env.GDI_SERVER_BUILD_SHA ||
      null;
    const surfaceMeta = bethesdaUnionMeta?.commercialSurface || null;
    const requestedSurface =
      surfaceMeta?.requestedSurface ||
      surfaceMeta?.view ||
      (wantFull ? "full" : "commercial");
    const effectiveSurface =
      surfaceMeta?.effectiveSurface || surfaceMeta?.view || requestedSurface;
    if (surfaceMeta?.view) {
      res.setHeader("X-GDI-Effective-Surface", String(effectiveSurface));
    }
    if (serverBuildSha) {
      res.setHeader("X-GDI-Server-Build-Sha", String(serverBuildSha));
    }
    // Re-assert annotation after progression overlays (idempotent).
    opportunities = annotateInventoryPopulations(opportunities, { hotelId });
    const inventoryPopulationCounts =
      buildInventoryPopulationCounts(opportunities);
    const bethesda67PublicationApproved =
      hotelId === BETHESDA_HOTEL_ID
        ? isBethesda67CustomerPublicationApproved()
        : null;
    const bethesda67Baseline =
      hotelId === BETHESDA_HOTEL_ID
        ? {
            baselineCount: 67,
            publishedCount: 19,
            stagedCount: 48,
            customerPublicationApproved: bethesda67PublicationApproved,
            headline: inventoryPopulationCounts.headline,
          }
        : null;
    const inventoryAudience =
      bethesdaUnionMeta?.inventoryAudience ||
      (hotelId === BETHESDA_HOTEL_ID
        ? resolveGdiInventoryAudience(req, { hotelId })
        : null);
    return res.json({
      ok: true,
      hotelId,
      updatedAt: doc.updatedAt,
      runId: doc.runId || null,
      count: opportunities.length,
      view: wantFull ? "full" : "list",
      schemaVersion: wantFull ? null : GDI_OPPORTUNITY_LIST_SCHEMA,
      listScope,
      inventoryAudience,
      requestedSurface,
      effectiveSurface,
      inventoryPopulationCounts,
      bethesda67Baseline,
      eligibleCount:
        surfaceMeta?.eligibleCount != null
          ? surfaceMeta.eligibleCount
          : opportunities.length,
      excludedCount:
        surfaceMeta?.excludedCount != null ? surfaceMeta.excludedCount : null,
      serverBuildSha,
      sourceOfTruth: isBethesdaInternalUnionHotel(hotelId)
        ? "BETHESDA_INTERNAL_UNION_88_FS"
        : "HOTEL_OPPORTUNITY_STORE",
      storeIdentity: {
        hotelId,
        updatedAt: doc.updatedAt || null,
        opportunityCountInStore: Array.isArray(doc.opportunities)
          ? doc.opportunities.length
          : null,
      },
      opportunities,
      hotelNeedDateConfigV1,
      ...(bethesdaUnionMeta ? { bethesdaInternalUnion: bethesdaUnionMeta } : {}),
      ...(visibilityWarning ? { visibilityWarning } : {}),
      ...(doc.publishedInventoryDiagnostic
        ? { publishedInventoryDiagnostic: doc.publishedInventoryDiagnostic }
        : {}),
      reviewedProspectImport: doc.reviewedProspectImport || null,
    });
  } catch (err) {
    return sendGdiUpstreamError(res, err, "getGdiOpportunities");
  }
}

export async function getGdiOpportunityDetail(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    const opportunityId = String(req.params.opportunityId || "").trim();
    const wantPublicationPackage =
      String(req.query.publicationPackage || "") === "1" ||
      String(req.query.surface || "").toLowerCase() === "customer";
    if (wantPublicationPackage && hotelId && opportunityId) {
      const {
        resolveOpportunityFromApprovedPackage,
        mergeShareFeedbackOntoPackageOpportunities,
      } = await import(
        "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js"
      );
      const pkgResolved = resolveOpportunityFromApprovedPackage({
        hotelId,
        opportunityId,
        preview: true,
      });
      if (
        pkgResolved.ok &&
        pkgResolved.mode === "PACKAGE" &&
        pkgResolved.opportunity
      ) {
        const validationDoc = loadShareValidation(hotelId);
        let [withFeedback] = mergeShareFeedbackOntoPackageOpportunities(
          [pkgResolved.opportunity],
          validationDoc,
        );
        try {
          const { attachPackageCustomerPresentation } = await import(
            "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js"
          );
          withFeedback = attachPackageCustomerPresentation(withFeedback);
        } catch (tilePreviewErr) {
          console.error(
            "[gdi] auth publication-package detail customer presentation attach failed",
            tilePreviewErr?.message || tilePreviewErr,
          );
        }
        res.setHeader("X-GDI-Detail-Source", "PUBLICATION_PACKAGE");
        return res.json({
          ok: true,
          opportunity: withFeedback,
          publication: pkgResolved.meta || null,
          listScope: "PUBLICATION_PACKAGE",
          scoreAudit: null,
        });
      }
    }
    const t0 = Date.now();
    const loaded = await withTimeout(
      loadOpportunityForDetail(hotelId, opportunityId),
      gdiReadTimeoutMs(),
      "gdi_opportunity_detail"
    );
    if (!loaded.opportunity) {
      console.error(
        JSON.stringify({
          gdi_error: true,
          route: "getGdiOpportunityDetail",
          code: "opportunity_not_found",
          hotelId,
          opportunityId,
          loadSource: loaded.source,
        }),
      );
      return res.status(404).json({
        ok: false,
        error: "opportunity_not_found",
        message:
          "This opportunity could not be opened. The detail record was not found for this property.",
        hotelId,
        opportunityId,
      });
    }
    const projected = projectOpportunitiesCommercialQuality([loaded.opportunity]);
    let opportunity = projected[0];
    try {
      const { attachHotelNeedDateTilePreviews } = await import(
        "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js"
      );
      const hotels = listRegisteredHotels();
      const hotelMeta =
        hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
      opportunity = (
        await attachHotelNeedDateTilePreviews(hotelId, [opportunity], {
          hotelName: hotelMeta.name || hotelMeta.hotelName || null,
        })
      )[0];
    } catch (needDateTileErr) {
      console.error(
        "[gdi] need-date tile preview detail attach failed",
        needDateTileErr?.message || needDateTileErr,
      );
    }
    // Local-only certification fixtures may open in founder GDI UI for subtype
    // drawer FullE2E — never share/production (doesNotAffectProduction + localDev).
    const allowLocalCertFixture =
      opportunity?.certificationFixtureOnly === true &&
      opportunity?.doesNotAffectProduction === true &&
      (opportunity?.localShadowOnly === true ||
        opportunity?.localShadowPortfolio === true) &&
      process.env.NODE_ENV !== "production" &&
      String(process.env.GDI_BLOCK_LOCAL_CERT_FIXTURE_DETAIL || "").trim() !== "1";
    // Auth detail: block TRUE synthetics only. customerVisible=false means
    // unpublished-to-customer, not "fixture" — those rows are list-eligible on
    // Bethesda internal commercial browse and must open in the drawer.
    if (!allowLocalCertFixture) {
      const { mayOpenAuthOpportunityDetail } = await import(
        "../lib/group-demand-intelligence/gdi-opportunity-id-resolution-v1.js"
      );
      const authGate = mayOpenAuthOpportunityDetail(opportunity, { hotelId });
      if (!authGate.ok) {
        console.error(
          JSON.stringify({
            gdi_error: true,
            route: "getGdiOpportunityDetail",
            code: "opportunity_not_found",
            reason: authGate.reason || "BLOCKED",
            hotelId,
            opportunityId,
            customerVisible: opportunity?.customerVisible,
          }),
        );
        return res.status(404).json({
          ok: false,
          error: "opportunity_not_found",
          message:
            "This opportunity could not be opened. It is not available as a detail record in the current view.",
          hotelId,
          opportunityId,
        });
      }
    }
    // Inventory audience parity with list/CSV: unpublished Bethesda rows require
    // hotel-scoped sales-team inventory authorization (not pilot-unauth alone
    // for non-allowlisted hotels / cross-hotel-scoped users / share surfaces).
    {
      const inventoryAudience = resolveGdiInventoryAudience(req, { hotelId });
      const readable = assertOpportunityReadableForInventoryAudience(
        req,
        opportunity,
        {
          hotelId,
          opportunityId,
          inventoryAudience,
          isCustomerFacing: isCustomerFacingOpportunity(opportunity, {
            hotelId,
          }),
        }
      );
      if (!readable.ok) {
        console.error(
          JSON.stringify({
            gdi_error: true,
            route: "getGdiOpportunityDetail",
            code: "opportunity_not_found",
            reason: readable.reason || "INVENTORY_AUDIENCE_DENIED",
            hotelId,
            opportunityId,
            inventoryAudience: readable.audience,
            salesTeamAccess: assertHotelScopedSalesTeamInventoryAccess(req, {
              hotelId,
            }),
          }),
        );
        return res.status(404).json({
          ok: false,
          error: "opportunity_not_found",
          message:
            "This opportunity could not be opened. It is not available as a detail record in the current view.",
          hotelId,
          opportunityId,
        });
      }
    }
    try {
      const pe = await withTimeout(
        enrichPrivateEventOpportunityDetail(opportunity),
        gdiReadTimeoutMs(),
        "gdi_pe_detail_enrich"
      );
      if (pe?.opportunity) opportunity = pe.opportunity;
      if (pe?.peEnriched && !pe.hasOfficialSource) {
        console.error(
          JSON.stringify({
            gdi_pe_detail: true,
            warning: "TRUE_PE_OPPORTUNITY_MISSING_SOURCES",
            opportunityId,
            hotelId,
          })
        );
      }
    } catch (peErr) {
      console.error(
        "[gdi] PE detail enrichment failed",
        peErr?.message || peErr
      );
    }
    let commercialProgression = null;
    try {
      const bySubject = await withTimeout(
        loadGdiCommercialProgressionBySubject(hotelId),
        gdiReadTimeoutMs(),
        "gdi_detail_progression"
      );
      commercialProgression = toCustomerCommercialProgressionDto(
        bySubject.get(opportunityId)
      );
    } catch (progErr) {
      console.error(
        JSON.stringify({
          gdi_perf: true,
          route: "getGdiOpportunityDetail.progression",
          hotelId,
          error: progErr?.code || progErr?.message || String(progErr),
          degraded: true,
        })
      );
    }
    const durationMs = Date.now() - t0;
    if (durationMs >= 1000 || process.env.GDI_PERF_LOG === "1") {
      console.info(
        JSON.stringify({
          gdi_perf: true,
          route: "getGdiOpportunityDetail",
          hotelId,
          durationMs,
          loadSource: loaded.source,
          loadMs: loaded.durationMs,
        })
      );
    }
    const shareValidation = getShareValidationForOpportunity(
      hotelId,
      opportunityId
    );
    const founderReviews = loadFounderReviews(hotelId);
    const founderReview =
      (founderReviews.items || []).find(
        (r) => r.opportunityId === opportunityId
      ) || null;
    // Annotate Market Scope + competitive/future-cycle presentation before DTO
    // so Confirmed Competitor Business vs Potential Future Opportunity survive.
    opportunity = annotateInventoryPopulations([opportunity], { hotelId })[0];
    const authProjected = projectOpportunityForPresentation(opportunity, {
      hotelId,
      surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
      view: "detail",
    });
    const progressionWithValidationTs =
      commercialProgression || shareValidation?.validatedAt
        ? {
            ...(commercialProgression || {}),
            hotelValidationUpdatedAt:
              shareValidation?.validatedAt ||
              shareValidation?.hotelValidationUpdatedAt ||
              commercialProgression?.hotelValidationUpdatedAt ||
              null,
          }
        : null;
    const opportunityPayload = {
      ...authProjected.dto,
      // Re-attach annotation fields the detail DTO may trim (list parity).
      inventoryPopulations: opportunity.inventoryPopulations,
      inventoryPopulationPrimary: opportunity.inventoryPopulationPrimary,
      cmiVerified: opportunity.cmiVerified,
      cmiCompetitorHotel: opportunity.cmiCompetitorHotel,
      cmiClass: opportunity.cmiClass,
      competitiveStatus: opportunity.competitiveStatus,
      competitiveStatusLabel: opportunity.competitiveStatusLabel,
      competitorVenueStatus: opportunity.competitorVenueStatus,
      competitorVenueStatusLabel: opportunity.competitorVenueStatusLabel,
      targetHotelSourcingStatus: opportunity.targetHotelSourcingStatus,
      targetHotelSourcingStatusLabel: opportunity.targetHotelSourcingStatusLabel,
      commercialActionability: opportunity.commercialActionability,
      hotelMarketScopeEligible: opportunity.hotelMarketScopeEligible,
      competitiveMarketScopeEligible: opportunity.competitiveMarketScopeEligible,
      competitiveFutureCyclePresentation:
        opportunity.competitiveFutureCyclePresentation,
      competitiveCommercialActionKey:
        opportunity.competitiveCommercialActionKey,
      competitiveCommercialActionLabel:
        opportunity.competitiveCommercialActionLabel,
      futureCycleOpportunityStatus: opportunity.futureCycleOpportunityStatus,
      recommendedAction:
        opportunity.recommendedAction || authProjected.dto?.recommendedAction,
      salesActionKey: opportunity.salesActionKey || authProjected.dto?.salesActionKey,
      salesActionPillLabel:
        opportunity.salesActionPillLabel ||
        authProjected.dto?.salesActionPillLabel,
      ...(progressionWithValidationTs
        ? { commercialProgression: progressionWithValidationTs }
        : {}),
      ...(shareValidation
        ? {
            shareValidation: {
              familiarityStatus: shareValidation.familiarityStatus,
              commercialValue: shareValidation.commercialValue,
              contactPersonAssessment: shareValidation.contactPersonAssessment,
              emailAssessment: shareValidation.emailAssessment,
              phoneAssessment: shareValidation.phoneAssessment,
              note: shareValidation.note || "",
              validatedAt: shareValidation.validatedAt,
              hotelValidationUpdatedAt:
                shareValidation.hotelValidationUpdatedAt ||
                shareValidation.validatedAt ||
                null,
            },
          }
        : {}),
      ...(founderReview
        ? {
            founderReviewDecision: founderReview.decision,
            founderReviewComment: founderReview.comment || "",
            founderReviewUpdatedAt: founderReview.updatedAt || null,
            founderReviewLocalOnly: true,
          }
        : {}),
    };
    res.setHeader("X-GDI-Detail-Source", loaded.source);
    res.setHeader("X-GDI-Universal-Presentation", "v1");
    return res.json({
      ok: true,
      opportunity: opportunityPayload,
      scoreAudit: reconstructScoreAudit(opportunity),
      validationReasons: HOTEL_VALIDATION_REASON,
      validationReasonLabels: HOTEL_VALIDATION_REASON_LABEL,
    });
  } catch (err) {
    return sendGdiUpstreamError(res, err, "getGdiOpportunityDetail");
  }
}

/**
 * CSV export of current hotel opportunities (honors includeDisqualified + ids filter).
 * Query: ids=comma (optional), includeDisqualified=1, weekly=, priority=, booking=, segment=, territory=
 */
export async function getGdiOpportunitiesExport(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const includeDisqualified = String(req.query.includeDisqualified || "") === "1";

  const doc = await loadOppDoc(hotelId);
  // Align Bethesda export with Opportunities list audience (sales-team 67 vs
  // published 19). Share/export routes that force published stay gated.
  let opportunities = doc.opportunities || [];
  const inventoryAudience = resolveGdiInventoryAudience(req, { hotelId });
  if (isBethesdaInternalUnionHotel(hotelId)) {
    const union = mergeBethesdaInternalResearchOpportunities(
      hotelId,
      opportunities
    );
    opportunities = union.opportunities;
    // Same commercial-surface gate as the Opportunities list — never export
    // research/archive fixture IDs (Airtable thin projections included).
    try {
      const {
        filterOpportunitiesByCommercialSurface,
        attachCommercialSurfaceDisposition,
        GDI_LIST_SURFACE_VIEW,
        assertPrimaryCommercialHasNoForbiddenIds,
      } = await import(
        "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js"
      );
      opportunities = filterOpportunitiesByCommercialSurface(
        opportunities,
        includeDisqualified
          ? GDI_LIST_SURFACE_VIEW.FULL
          : GDI_LIST_SURFACE_VIEW.COMMERCIAL,
      ).map((o) => attachCommercialSurfaceDisposition(o));
      if (!includeDisqualified) {
        const leakCheck = assertPrimaryCommercialHasNoForbiddenIds(opportunities);
        if (!leakCheck.ok) {
          console.error(
            JSON.stringify({
              gdi_error: true,
              route: "getGdiOpportunitiesExport",
              code: "COMMERCIAL_SURFACE_FORBIDDEN_LEAK",
              hotelId,
              leakedIds: leakCheck.leakedIds,
            }),
          );
          return res.status(503).json({
            ok: false,
            error: "COMMERCIAL_SURFACE_FORBIDDEN_LEAK",
            hotelId,
            leakedIds: leakCheck.leakedIds,
            message:
              "Commercial CSV unavailable: forbidden research/archive IDs leaked after filter.",
          });
        }
      }
    } catch (surfaceErr) {
      console.error(
        "[gdi] export commercial surface filter failed",
        surfaceErr?.message || surfaceErr,
      );
      return res.status(503).json({
        ok: false,
        error: "COMMERCIAL_SURFACE_FILTER_FAILED",
        hotelId,
        message:
          "Commercial CSV unavailable: commercial surface filter failed closed.",
      });
    }
    if (inventoryAudience === GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY) {
      opportunities = selectBethesdaSalesOpportunityInventory(opportunities, {
        salesTeamInventory: true,
        founderReview: true,
      }).selected;
    } else {
      opportunities = selectBethesdaSalesOpportunityInventory(opportunities, {
        salesTeamInventory: false,
        founderReview: false,
      }).selected;
    }
  } else {
    if (!includeDisqualified) {
      opportunities = filterSalespersonView(opportunities);
    }
    opportunities = filterCustomerFacingOpportunities(opportunities, {
      hotelId,
    });
  }
  opportunities = applyApprovedBethesdaCustomerProjection(opportunities);
  opportunities = projectOpportunitiesCommercialQuality(opportunities);
  opportunities = annotateInventoryPopulations(opportunities, { hotelId });
  opportunities = filterOpportunitiesForExport(opportunities, req.query);
  opportunities = await attachCommercialProgression(hotelId, opportunities);

  const hotels = listRegisteredHotels();
  const hotel =
    hotels.find((h) => h.hotelId === hotelId) ||
    { hotelId, name: hotelId };

  return sendCustomerCsv(res, opportunities, hotel);
}

export async function getGdiWeeklyBrief(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const doc = await loadOppDoc(hotelId);
  const brief = buildWeeklyBrief(
    applyApprovedBethesdaCustomerProjection(
      filterCustomerFacingOpportunities(doc.opportunities || [], { hotelId })
    )
  );
  const bySubject = await loadGdiCommercialProgressionBySubject(hotelId);
  const attachItems = (items) =>
    (items || []).map((it) => {
      const dto = toCustomerCommercialProgressionDto(
        bySubject.get(it.opportunityId || it.id)
      );
      return dto ? { ...it, commercialProgression: dto } : it;
    });
  const enriched = {
    ...brief,
    items: attachItems(brief.items),
    highPriority: attachItems(brief.highPriority),
    mediumPriority: attachItems(brief.mediumPriority),
    watchlist: attachItems(brief.watchlist),
  };
  return res.json({ ok: true, hotelId, brief: enriched });
}

export function getGdiResearchRuns(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  return res.json({
    ok: true,
    hotelId,
    runs: listResearchRuns(hotelId),
    methods: listGdiResearchMethods(),
  });
}

export function getGdiResearchRun(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const runId = String(req.params.runId || "").trim();
  const run = loadResearchRun(hotelId, runId);
  if (!run) {
    return res.status(404).json({ ok: false, error: "run_not_found" });
  }
  return res.json({ ok: true, run });
}

/**
 * Internal Research Audit — discovery funnel (V37).
 * Never expose via share capability routes.
 */
export function getGdiResearchFunnel(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const campaignId =
    String(req.query.campaignId || "").trim() ||
    "four-hotel-14-workflow-full-e2e-v1";
  try {
    const { candidates, document } = listDiscoveryCandidates(hotelId, {
      campaignId,
      workflowId: req.query.workflowId || undefined,
      researchStatus: req.query.status || undefined,
      dispositionCode: req.query.dispositionReason || undefined,
      organization: req.query.organization || undefined,
      language: req.query.language || undefined,
      from: req.query.from || undefined,
      to: req.query.to || undefined,
      q: req.query.q || undefined,
    });
    const funnel = aggregateResearchFunnel(candidates);
    return res.json({
      ok: true,
      hotelId,
      campaignId,
      internalOnly: true,
      documentMeta: {
        hotelName: document.hotelName || null,
        candidateCount: document.candidateCount || candidates.length,
        generatedAt: document.generatedAt || null,
      },
      funnel,
      reconcile: funnelReconciles(funnel),
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "research_funnel_failed",
      message: String(err?.message || err),
    });
  }
}

export function getGdiDiscoveryCandidates(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const campaignId =
    String(req.query.campaignId || "").trim() ||
    "four-hotel-14-workflow-full-e2e-v1";
  const limit = Math.min(500, Math.max(1, Number(req.query.limit) || 100));
  try {
    const { candidates, document } = listDiscoveryCandidates(hotelId, {
      campaignId,
      workflowId: req.query.workflowId || undefined,
      researchStatus: req.query.status || undefined,
      dispositionCode: req.query.dispositionReason || undefined,
      organization: req.query.organization || undefined,
      language: req.query.language || undefined,
      from: req.query.from || undefined,
      to: req.query.to || undefined,
      q: req.query.q || undefined,
    });
    // Sanitize: never return raw provider envelopes / prompts
    const rows = candidates.slice(0, limit).map((c) => ({
      discoveryId: c.discoveryId,
      hotelId: c.hotelId,
      workflowId: c.workflowId,
      powershell: c.powershell,
      organizationName: c.organizationName,
      programmeName: c.programmeName,
      opportunityId: c.opportunityId,
      researchStatus: c.researchStatus,
      disposition: c.disposition,
      originalSourceUrls: c.originalSourceUrls,
      exaRunId: c.exaRunId,
      queryTimestamp: c.queryTimestamp,
      deepArtifactPath: c.deepArtifactPath || null,
      staged: Boolean(c.staged),
      customerFacing: Boolean(c.customerFacing),
      awaitingFounderPromotion: Boolean(c.awaitingFounderPromotion),
      researchCostUsd:
        (Number(c.researchCostUsd) || 0) + (Number(c.deepResearchCostUsd) || 0),
      provenance: {
        available: c.provenance?.available !== false,
        gaps: c.provenance?.gaps || [],
        artifactPath: c.provenance?.artifactPath || null,
        rawProviderEnvelopePersisted: false,
      },
    }));
    return res.json({
      ok: true,
      hotelId,
      campaignId,
      internalOnly: true,
      total: candidates.length,
      returned: rows.length,
      candidates: rows,
      generatedAt: document.generatedAt || null,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "discovery_candidates_failed",
      message: String(err?.message || err),
    });
  }
}

export function getGdiWhyIsntThisShowing(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const q = String(req.query.q || "").trim();
  const campaignId =
    String(req.query.campaignId || "").trim() ||
    "four-hotel-14-workflow-full-e2e-v1";
  try {
    const result = whyIsntThisShowing({ hotelId, q, campaignId });
    return res.json({ ...result, internalOnly: true });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "why_isnt_this_showing_failed",
      message: String(err?.message || err),
    });
  }
}

export async function postGdiRunResearch(req, res) {
  if (!isGroupDemandIntelligenceEnabled() && !isGroupDemandIntelligencePilotReadAllowed()) {
    return res.status(403).json({
      ok: false,
      error: "feature_disabled",
      message: "Enable GROUP_DEMAND_INTELLIGENCE_V1 to run research.",
    });
  }
  const hotelId = String(req.params.hotelId || "").trim();
  const body = req.body || {};
  try {
    const result = await runGroupDemandResearch({
      hotelId,
      trigger: "admin_api",
      webhoundSessionId: body.webhoundSessionId || null,
      webhoundCostUsd: body.webhoundCostUsd,
      webhoundUrl: body.webhoundUrl || null,
      webhoundQuestion: body.webhoundQuestion || null,
      webhoundMergedOpportunityIds: body.webhoundMergedOpportunityIds || null,
      allowWebhoundWithoutFlag: body.allowWebhoundWithoutFlag === true,
      dryRun: body.dryRun === true,
    });
    return res.json({
      ok: true,
      experimentalLabel: "EXPERIMENTAL / PILOT",
      ...result,
    });
  } catch (err) {
    const status = err.code === "hotel_not_onboarded_for_gdi_pilot" ? 400 : 500;
    console.error("[gdi] run research failed", err);
    return res.status(status).json({
      ok: false,
      error: err.code || "run_failed",
      message: err.message || "Research run failed",
    });
  }
}

export async function postGdiFeedback(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const body = req.body || {};
  const quality = body.quality || body.value || null;
  const salesOutcome = body.salesOutcome || body.commercialStatus || "Not Reviewed";
  const familiarity = body.familiarity || null;
  const commercialStatus = body.commercialStatus || null;
  const value = body.value || null;
  const sourcingStatus = body.sourcingStatus || null;
  const incrementalValueStatus = body.incrementalValueStatus || null;
  const qualificationFailureReason = body.qualificationFailureReason || null;

  if (quality && !FEEDBACK_QUALITY.includes(quality)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_quality",
      allowed: FEEDBACK_QUALITY,
    });
  }
  if (salesOutcome && !SALES_OUTCOME.includes(salesOutcome)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_sales_outcome",
      allowed: SALES_OUTCOME,
    });
  }
  if (familiarity && !HOTEL_FAMILIARITY.includes(familiarity)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_familiarity",
      allowed: HOTEL_FAMILIARITY,
    });
  }
  if (commercialStatus && !HOTEL_COMMERCIAL_STATUS.includes(commercialStatus)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_commercial_status",
      allowed: HOTEL_COMMERCIAL_STATUS,
    });
  }
  if (value && !HOTEL_VALUE_FEEDBACK.includes(value) && !FEEDBACK_QUALITY.includes(value)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_value",
      allowed: HOTEL_VALUE_FEEDBACK,
    });
  }
  if (sourcingStatus && !Object.values(SOURCING_STATUS).includes(sourcingStatus)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_sourcing_status",
      allowed: Object.values(SOURCING_STATUS),
    });
  }
  if (
    incrementalValueStatus &&
    !Object.values(INCREMENTAL_VALUE_STATUS).includes(incrementalValueStatus)
  ) {
    return res.status(400).json({
      ok: false,
      error: "invalid_incremental_value_status",
      allowed: Object.values(INCREMENTAL_VALUE_STATUS),
    });
  }
  if (
    qualificationFailureReason &&
    !Object.values(QUALIFICATION_FAILURE_REASON).includes(qualificationFailureReason)
  ) {
    return res.status(400).json({
      ok: false,
      error: "invalid_qualification_failure_reason",
      allowed: Object.values(QUALIFICATION_FAILURE_REASON),
    });
  }

  const saved = saveFeedbackItem(hotelId, {
    opportunityId,
    quality,
    salesOutcome,
    familiarity,
    commercialStatus,
    value,
    sourcingStatus,
    incrementalValueStatus,
    qualificationFailureReason,
    comment: body.comment || "",
    actor: req.dealalityUser?.email || req.dealalityUser?.id || "unknown",
    // Hotel validation must never rewrite canonical opportunity facts
    doesNotOverwriteCanonicalFacts: true,
  });

  // Dual-write into canonical Decision & Outcome layer (does not replace legacy store)
  let decisionOutcome = null;
  try {
    const oppDoc = await loadOppDoc(hotelId);
    const opportunity = (oppDoc.opportunities || []).find(
      (o) => o.id === opportunityId
    );
    decisionOutcome = await ingestGdiAuthFeedback({
      hotelId,
      opportunityId,
      opportunity: opportunity || null,
      feedback: saved,
      actor: {
        userId: req.dealalityUser?.email || req.dealalityUser?.id || "unknown",
        role: req.dealalityUser?.role || "OTHER",
      },
    });
  } catch (err) {
    console.error("[gdi] decision-outcome dual-write failed", err?.message || err);
    return res.status(502).json({
      ok: false,
      error: "decision_outcome_persist_failed",
      message: err?.message || "Canonical Decision & Outcome write failed",
      feedback: saved,
      note: "Legacy feedback was stored, but durable Decision & Outcome persistence failed.",
    });
  }

  return res.json({
    ok: true,
    feedback: saved,
    decisionOutcome: decisionOutcome
      ? {
          decisionId:
            decisionOutcome.decision?.decision?.decisionId ||
            decisionOutcome.decision?.decisionId ||
            null,
          eventCount: (decisionOutcome.events || []).length,
        }
      : null,
    note: "Your feedback helps track this opportunity and improve future recommendations.",
  });
}

/**
 * Authenticated customer validation — same field contract as share validation.
 * Writes share-validation store + canonical Decision Events.
 */
export async function postGdiCustomerValidation(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  try {
    const body = req.body || {};
    const founderDecision = String(
      body.founderReviewDecision || body.dosGrade || ""
    ).trim();
    const hasHotelValidation = Boolean(
      body.familiarityStatus ||
        body.commercialValue ||
        body.contactPersonAssessment ||
        body.emailAssessment ||
        body.phoneAssessment ||
        (body.note != null && String(body.note).trim() !== "")
    );

    // Local-only founder DOS review — never dual-written to customer share surfaces.
    let founderReview = null;
    if (founderDecision) {
      founderReview = saveFounderReview(hotelId, {
        opportunityId,
        decision: founderDecision,
        comment: body.note || body.founderReviewComment || "",
        actor:
          req.dealalityUser?.email ||
          req.dealalityUser?.id ||
          body.validator ||
          "founder_local",
      });
    }

    let item = null;
    let validationUnchanged = false;
    if (hasHotelValidation) {
      const previousValidation = getShareValidationForOpportunity(
        hotelId,
        opportunityId
      );
      const saved = saveShareValidationItem(
        hotelId,
        {
          opportunityId,
          validator:
            body.validator ||
            req.dealalityUser?.email ||
            req.dealalityUser?.id ||
            "AUTH_REVIEWER",
          familiarityStatus: body.familiarityStatus || null,
          commercialValue: body.commercialValue || null,
          contactPersonAssessment: body.contactPersonAssessment || null,
          emailAssessment: body.emailAssessment || null,
          phoneAssessment: body.phoneAssessment || null,
          note: body.note || "",
        },
        {
          expectedHotelValidationUpdatedAt:
            body.expectedHotelValidationUpdatedAt || null,
        }
      );
      item = saved.item;
      validationUnchanged = saved.unchanged === true;
      if (!validationUnchanged) {
        try {
          await ingestGdiShareValidation({
            hotelId,
            opportunityId,
            opportunity,
            validation: item,
            actor: {
              userId:
                req.dealalityUser?.email ||
                req.dealalityUser?.id ||
                "AUTH_REVIEWER",
              role: req.dealalityUser?.role || "OTHER",
            },
          });
        } catch (dualErr) {
          restoreShareValidationItemAfterFailedPersist(
            hotelId,
            opportunityId,
            previousValidation
          );
          console.error(
            "[gdi] auth decision-outcome dual-write failed",
            dualErr?.message || dualErr
          );
          return res.status(502).json({
            ok: false,
            error: "decision_outcome_persist_failed",
            message:
              dualErr?.message ||
              "Canonical Decision & Outcome write failed",
            hotelValidationUpdatedAt:
              previousValidation?.validatedAt || null,
          });
        }
      }
      invalidateGdiHotelReadCache(hotelId);
    } else if (!founderReview) {
      return res.status(400).json({
        ok: false,
        error: "validation_or_founder_review_required",
      });
    }

    const hotelValidationUpdatedAt =
      item?.hotelValidationUpdatedAt || item?.validatedAt || null;

    return res.json({
      ok: true,
      created: hasHotelValidation ? !validationUnchanged : Boolean(founderReview),
      unchanged: validationUnchanged,
      validation: item
        ? {
            familiarityStatus: item.familiarityStatus,
            commercialValue: item.commercialValue,
            contactPersonAssessment: item.contactPersonAssessment,
            emailAssessment: item.emailAssessment,
            phoneAssessment: item.phoneAssessment,
            note: item.note || "",
            validatedAt: item.validatedAt,
            hotelValidationUpdatedAt,
            validator: item.validator || null,
          }
        : null,
      hotelValidationUpdatedAt,
      ...(founderReview
        ? {
            founderReview: {
              decision: founderReview.decision,
              comment: founderReview.comment || "",
              updatedAt: founderReview.updatedAt || null,
              localOnly: true,
            },
          }
        : {}),
    });
  } catch (err) {
    if (err.code === "hotel_validation_conflict") {
      return res.status(409).json({
        ok: false,
        error: err.code,
        message:
          "Hotel Validation was updated by another session. Reload and try again.",
        hotelValidationUpdatedAt: err.hotelValidationUpdatedAt || null,
        current: err.current
          ? {
              familiarityStatus: err.current.familiarityStatus,
              commercialValue: err.current.commercialValue,
              contactPersonAssessment: err.current.contactPersonAssessment,
              emailAssessment: err.current.emailAssessment,
              phoneAssessment: err.current.phoneAssessment,
              note: err.current.note || "",
              validatedAt: err.current.validatedAt,
              hotelValidationUpdatedAt:
                err.current.hotelValidationUpdatedAt ||
                err.current.validatedAt ||
                null,
            }
          : null,
      });
    }
    if (err.code === "invalid_share_validation") {
      return res.status(400).json({ ok: false, error: err.code, errors: err.errors });
    }
    if (err.code === "invalid_founder_review") {
      return res.status(400).json({
        ok: false,
        error: err.code,
        message: err.message,
        allowed: err.allowed || null,
      });
    }
    console.error("[gdi] auth customer validation failed", err);
    return res.status(500).json({ ok: false, error: "customer_validation_failed" });
  }
}

/**
 * User-initiated contact email/phone reveal (ephemeral).
 * Never persists Surfe/provider PII. Live provider requires body.authorizeLiveProvider=true
 * AND an injected/live adapter (default returns LIVE_PROVIDER_NOT_AUTHORIZED / NOT_WIRED).
 *
 * POST .../opportunities/:opportunityId/contacts/reveal
 * body: { channel: 'email'|'phone'|'both', contactName?, authorizeLiveProvider?, useMock?: boolean }
 */
export async function postGdiContactReveal(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const body = req.body || {};
  const channel = String(body.channel || "both").toLowerCase();

  let doc;
  try {
    doc = await loadOppDoc(hotelId);
  } catch (err) {
    return res.status(500).json({ ok: false, error: "load_failed" });
  }
  const opportunity = (doc.opportunities || []).find(
    (o) =>
      o.id === opportunityId ||
      o.opportunityId === opportunityId ||
      String(o.opportunityId || "") === opportunityId,
  );
  if (!opportunity) {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }

  const { revealGdiContactOnDemand, createMockGdiContactRevealProvider } =
    await import(
      "../lib/group-demand-intelligence/gdi-contact-on-demand-reveal-v1.js"
    );
  const { createGdiSurfeContactRevealProvider } = await import(
    "../lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js"
  );

  const contactName =
    String(body.contactName || "").trim() ||
    opportunity.primaryContact?.name ||
    opportunity.contactName ||
    null;

  // Resolve selected person from primary/secondary/buyerCandidates so Surfe uses the clicked card.
  const secondary = opportunity.secondaryContact || null;
  const primary = opportunity.primaryContact || null;
  const buyers = [
    ...(opportunity.buyerCandidates || []),
    ...(opportunity.contactPackage?.buyerCandidates || []),
  ];
  const buyerMatch = contactName
    ? buyers.find(
        (b) =>
          String(b.fullName || b.name || "")
            .trim()
            .toLowerCase() === contactName.toLowerCase(),
      )
    : null;
  const selectedContact =
    (secondary &&
      contactName &&
      String(secondary.name || "").trim().toLowerCase() ===
        contactName.toLowerCase() &&
      secondary) ||
    (primary &&
      contactName &&
      String(primary.name || "").trim().toLowerCase() ===
        contactName.toLowerCase() &&
      primary) ||
    (buyerMatch
      ? {
          name: buyerMatch.fullName || buyerMatch.name,
          role: buyerMatch.role || buyerMatch.title,
          title: buyerMatch.title || buyerMatch.role,
          linkedinUrl: buyerMatch.sourceUrl || buyerMatch.linkedinUrl || null,
          profileUrl: buyerMatch.sourceUrl || buyerMatch.linkedinUrl || null,
          sourceUrl: buyerMatch.sourceUrl || null,
        }
      : null) ||
    (primary &&
      (!contactName ||
        String(primary.name || "").trim().toLowerCase() ===
          contactName.toLowerCase()) &&
      primary) ||
    primary ||
    secondary ||
    null;

  const useMock =
    (body.useMock === true || process.env.GDI_CONTACT_REVEAL_MOCK === "1") &&
    process.env.GDI_CONTACT_REVEAL_MOCK === "1";
  // Live Surfe requires an explicit button authorize flag AND a signed-in user.
  // Mock mode is env-gated (GDI_CONTACT_REVEAL_MOCK=1) for local certification only.
  const authorizeLive =
    body.authorizeLiveProvider === true &&
    Boolean(req.dealalityUser || req.memberstackMemberId);

  if (!useMock && !authorizeLive) {
    return res.status(403).json({
      ok: false,
      ephemeral: true,
      persisted: false,
      surfePiiPersisted: false,
      error: "LIVE_PROVIDER_NOT_AUTHORIZED",
      message:
        "Contact lookup requires a signed-in Dealality user. Live Surfe is never called for anonymous or share viewers.",
      contactDetails: null,
      hotelId,
      opportunityId,
    });
  }

  const {
    contactRevealIdentityKey,
    preflightSurfeCredits,
    beginSurfeInFlight,
    endSurfeInFlight,
    appendSurfeLiveAccounting,
  } = await import(
    "../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js"
  );

  const resolvedName = contactName || selectedContact?.name || null;
  const identityKey = contactRevealIdentityKey({
    hotelId,
    opportunityId,
    contactName: resolvedName,
    channel,
  });

  let creditsBefore = null;
  let provider = null;
  if (useMock) {
    provider = createMockGdiContactRevealProvider(String(body.mockScenario || "hit"));
  } else if (authorizeLive) {
    const pre = await preflightSurfeCredits(channel);
    if (!pre.ok) {
      return res.status(pre.error === "INSUFFICIENT_CREDITS" ? 402 : 503).json({
        ok: false,
        ephemeral: true,
        persisted: false,
        surfePiiPersisted: false,
        error: pre.error,
        message: pre.message,
        contactDetails: null,
        credits: pre.credits,
        hotelId,
        opportunityId,
      });
    }
    creditsBefore = pre.credits;
    const flight = beginSurfeInFlight(identityKey);
    if (!flight.ok) {
      return res.status(409).json({
        ok: false,
        ephemeral: true,
        persisted: false,
        surfePiiPersisted: false,
        error: flight.error,
        message: flight.message,
        contactDetails: null,
        hotelId,
        opportunityId,
      });
    }
    provider = createGdiSurfeContactRevealProvider({
      pollOnEnrich: true,
      maxPollWaitMs: Number(process.env.GDI_SURFE_POLL_MS || 90000),
    });
  }

  let result;
  try {
    result = await revealGdiContactOnDemand({
      channel,
      contact: {
        name: resolvedName,
        role:
          selectedContact?.role ||
          selectedContact?.title ||
          opportunity.contactRole ||
          null,
        linkedinUrl:
          selectedContact?.linkedinUrl ||
          selectedContact?.profileUrl ||
          selectedContact?.sourceUrl ||
          null,
        profileUrl:
          selectedContact?.profileUrl ||
          selectedContact?.linkedinUrl ||
          selectedContact?.sourceUrl ||
          null,
      },
      organization: {
        name:
          opportunity.displayCompanyName ||
          opportunity.organizationName ||
          opportunity.company ||
          null,
      },
      provider,
      authorizeLiveProvider: authorizeLive,
    });
  } finally {
    if (authorizeLive && !useMock) endSurfeInFlight(identityKey);
  }

  let creditsAfter = null;
  if (authorizeLive && !useMock) {
    try {
      const after = await preflightSurfeCredits(channel);
      creditsAfter = after.credits;
    } catch {
      creditsAfter = null;
    }
    appendSurfeLiveAccounting({
      hotelId,
      opportunityId,
      identityKey,
      channel,
      enrichmentId: result?.enrichmentId || null,
      ok: result?.ok === true,
      miss: result?.miss === true,
      error: result?.error || null,
      creditsBefore,
      creditsAfter,
      emailReturned: Boolean(result?.contactDetails?.email),
      phoneReturned: Boolean(result?.contactDetails?.phone),
    });
  }

  const status =
    result.error === "NO_NAMED_CONTACT_SELECTED"
      ? 400
      : result.error === "LIVE_PROVIDER_NOT_AUTHORIZED" ||
          result.error === "PROVIDER_ADAPTER_NOT_WIRED" ||
          result.error === "CONTACT_ENRICHMENT_DISABLED"
        ? 403
        : result.error === "INSUFFICIENT_CREDITS"
          ? 402
          : result.ok
            ? 200
            : 502;

  // Never echo Surfe PII into logs — only return in this response body for the active session.
  return res.status(status).json({
    ...result,
    hotelId,
    opportunityId,
    contactName: resolvedName,
    // Explicit contract: never claim persistence
    persisted: false,
    surfePiiPersisted: false,
    creditsBefore: creditsBefore || undefined,
    creditsAfter: creditsAfter || undefined,
  });
}


/**
 * One-click hotel publication (permanent architecture).
 * Default dryRun=true. Apply requires explicit founder approval payload.
 * Never mutates other hotels. Never rotates share tokens.
 * Admin-only at the route layer (requireAdminAccess).
 */
export async function postGdiPublishHotelInventory(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  try {
    const body = req.body || {};
    const dryRun = body.dryRun !== false; // safe default

    // Local preview set (may be incomplete in production research docs).
    const doc = await loadOppDoc(hotelId);
    let localOpportunities = filterCustomerFacingOpportunities(
      doc.opportunities || [],
      { hotelId },
    );
    try {
      // CANONICAL_DISPLAY: one opportunity list — display eligibility == publication candidate set.
      const {
        selectDisplayEligibleOpportunities,
        GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
      } = await import(
        "../lib/group-demand-intelligence/commercial-inventory/gdi-canonical-display-eligibility-contract-v1.js"
      );
      void GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1;
      localOpportunities = selectDisplayEligibleOpportunities(
        hotelId,
        localOpportunities,
      ).opportunities;
    } catch (eligErr) {
      console.warn("[gdi] display eligibility unavailable for publish", eligErr?.message || eligErr);
    }

    const opportunityIds = Array.isArray(body.opportunityIds)
      ? body.opportunityIds.map(String)
      : null;

    const {
      resolveApprovedPublicationOpportunities,
      assertApprovedEventTitlesPreserved,
    } = await import(
      "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js"
    );
    const resolved = resolveApprovedPublicationOpportunities({
      hotelId,
      opportunities: localOpportunities,
      opportunityIds: opportunityIds || undefined,
    });
    if (!resolved.ok) {
      return res.status(422).json({
        ok: false,
        error: "APPROVED_PUBLICATION_SOURCE_INCOMPLETE",
        hotelId,
        missing: resolved.missing,
        sources: resolved.sources,
        packDir: resolved.packDir,
      });
    }
    const titleGate = assertApprovedEventTitlesPreserved(resolved.opportunities);
    if (!titleGate.ok) {
      return res.status(422).json({
        ok: false,
        error: "APPROVED_EVENT_TITLE_MISMATCH",
        fails: titleGate.fails,
      });
    }

    const { publishHotelInventoryOneClick } = await import(
      "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js"
    );

    const approval =
      body.approval && typeof body.approval === "object"
        ? {
            ...body.approval,
            authorized: body.approval.authorized === true,
            authorizedBy:
              body.approval.authorizedBy ||
              req.dealalityUser?.email ||
              req.dealalityUser?.id ||
              "founder",
            authorizedAt: body.approval.authorizedAt || new Date().toISOString(),
          }
        : null;

    const result = publishHotelInventoryOneClick({
      hotelId,
      opportunities: resolved.opportunities,
      opportunityIds: opportunityIds || resolved.opportunityIds,
      approval,
      dryRun,
      revokeMissingPublishedIds: body.revokeMissingPublishedIds === true,
      sourceResearchReference: body.sourceResearchReference || null,
    });

    const status = result.ok ? 200 : dryRun ? 422 : 409;
    return res.status(status).json({
      ok: result.ok === true,
      oneClickPublish: "gdi-one-click-hotel-publish-v1",
      hotelId,
      dryRun,
      approvedSource: {
        contract: resolved.contract,
        sources: resolved.sources,
        packDir: resolved.packDir,
      },
      result,
    });
  } catch (err) {
    return sendGdiUpstreamError(res, err, "postGdiPublishHotelInventory");
  }
}

/** Local-only founder review — disabled in production release packaging. */
export async function postGdiFounderReview(req, res) {
  if (!flagGate(req, res)) return;
  return res.status(501).json({
    ok: false,
    error: "founder_review_not_in_production_release",
    message:
      "Local shadow founder review is not part of the GDI one-click production release.",
  });
}

export function getGdiFeedback(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  return res.json({ ok: true, ...loadFeedback(hotelId) });
}

function requireGdiShare(req, res, requiredSurface) {
  const token = extractGdiShareCapabilityFromRequest(req);
  const verified = verifyGdiShareCapability(token, {
    expectedHotelId: req.params.hotelId ? String(req.params.hotelId).trim() : null,
    requiredSurface,
  });
  if (!verified.ok) {
    res.status(403).json({
      ok: false,
      error: verified.code || verified.error,
      code: verified.code || verified.error,
      message:
        verified.customerMessage ||
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
    return null;
  }
  return verified;
}

function researchPreviewDocument(verified) {
  return isResearchPreviewClaim(verified?.claims) ? researchPreviewForClaim(verified.claims) : null;
}

function rejectResearchPreviewWrite(verified, res) {
  if (!isResearchPreviewClaim(verified?.claims)) return false;
  res.status(403).json({
    ok: false,
    error: "RESEARCH_PREVIEW_READ_ONLY",
    message: "A research preview is read-only and cannot record validation, actions, or outcomes.",
  });
  return true;
}

/** Public resolve — no Memberstack; token is the auth. */
export async function getGdiShareResolve(req, res) {
  const token = extractGdiShareCapabilityFromRequest(req);
  const verified = verifyGdiShareCapability(token);
  if (!verified.ok) {
    return res.status(403).json({
      ok: false,
      error: verified.code || verified.error,
      code: verified.code || verified.error,
      message:
        verified.customerMessage ||
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const hotelId = verified.claims.hotelId;
  const preview = researchPreviewDocument(verified);
  if (isResearchPreviewClaim(verified.claims)) {
    if (!preview || preview.hotelId !== hotelId) {
      return res.status(403).json({ ok: false, error: "RESEARCH_PREVIEW_SCOPE" });
    }
    const profile = loadHotelProfile(hotelId);
    return res.json({
      ok: true,
      mode: "read_only",
      accessScope: RESEARCH_PREVIEW_SCOPE,
      researchPreview: true,
      pageTitle: preview.title || RESEARCH_PREVIEW_TITLE,
      disclaimer: preview.disclaimer || RESEARCH_PREVIEW_DISCLAIMER,
      commerciallyReady: false,
      customerReady: false,
      publicationStatus: "not_published",
      hotelId,
      hotelName: profile?.identity?.hotelName || hotelId,
      city: profile?.identity?.city || null,
      state: profile?.identity?.state || null,
      opportunityCount: preview.opportunities.length,
      capabilities: verified.claims.capabilities || [],
      canValidate: false,
      canRecordAction: false,
      canRecordOutcome: false,
    });
  }
  const profile = loadHotelProfile(hotelId);
  let hotelConfig = null;
  try {
    const { loadHotelDemandConfig } = await import(
      "../lib/group-demand-intelligence/hotel-profile.js"
    );
    hotelConfig = loadHotelDemandConfig(hotelId);
  } catch {
    hotelConfig = null;
  }
  const oppDoc = await loadOppDoc(hotelId);
  const summary = enrichSummaryWithHotelActivity(
    hotelId,
    loadLatestSummary(hotelId, {
      opportunities: oppDoc.opportunities || [],
      opportunityDoc: oppDoc,
    }),
    oppDoc
  );
  const { isInventoryVisibilityHotelEnabled: invHotelShareSummary } =
    await import("../lib/group-demand-intelligence/inventory-visibility-v1.js");
  const shareSummaryBase = invHotelShareSummary(hotelId)
    ? oppDoc.opportunities || []
    : filterSalespersonView(oppDoc.opportunities || []);
  const active = applyApprovedBethesdaCustomerProjection(
    filterCustomerFacingOpportunities(shareSummaryBase, { hotelId })
  );
  const validationDoc = loadShareValidation(hotelId);
  const validationSummary = buildShareValidationSummary({
    opportunities: active,
    validationItems: validationDoc.items || [],
  });
  // Prefer customer-facing inventory length over filesystem summary counts
  // so Waterstone inventory mode does not show a stale ~11 KPI.
  const inventoryFacingCount = active.length;
  return res.json({
    ok: true,
    mode: "read_only",
    capabilities: verified.capabilities || verified.claims.capabilities || [],
    canValidate: (verified.capabilities || []).includes("CAN_VALIDATE"),
    canRecordAction: (verified.capabilities || []).includes("CAN_RECORD_ACTION"),
    canRecordOutcome: (verified.capabilities || []).includes(
      "CAN_RECORD_OUTCOME"
    ),
    pilotLabel: "Pilot",
    hotelId,
    hotelName:
      profile?.identity?.hotelName ||
      hotelConfig?.displayName ||
      "Hotel",
    city: profile?.identity?.city || hotelConfig?.city || null,
    state: profile?.identity?.state || null,
    surfaces: verified.claims.surfaces,
    summary: redactCustomerFacingGdiSummary({
      lastResearchAt: summary.lastResearchAt,
      runStatus: summary.runStatus,
      runStatusLabel: summary.runStatusLabel,
      lastReviewedPackageImportAt: summary.lastReviewedPackageImportAt,
      lastPublishedUpdateAt: summary.lastPublishedUpdateAt,
      qualifiedCount: inventoryFacingCount,
      highPriorityCount: active.filter((o) => o.priority === "HIGH_PRIORITY")
        .length,
      mediumPriorityCount: active.filter((o) => o.priority === "MEDIUM_PRIORITY")
        .length,
      watchlistCount: active.filter((o) => o.priority === "WATCHLIST").length,
      // hotel-confirmed validation KPIs only
      validated: validationSummary.validated,
      pendingValidation: validationSummary.pendingValidation,
      confirmedNew: validationSummary.confirmedNew,
      alreadyKnown: validationSummary.alreadyKnown,
      worthPursuingNow: validationSummary.worthPursuingNow,
      notRelevant: validationSummary.notRelevant,
      // intentionally omit latestRunId / researchCostUsd / admin cost internals
    }),
    validationSummary,
    expiresAt: verified.meta?.expiresAt || null,
  });
}

/** Public weekly brief — share token auth. */
export async function getGdiShareBrief(req, res) {
  const verified = requireGdiShare(req, res, "brief");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const doc = await loadOppDoc(hotelId);
  const overlaid = applyCanonicalOverlaysToOpportunities(
    hotelId,
    doc.opportunities || []
  );
  const brief = buildWeeklyBrief(overlaid);
  const validationDoc = loadShareValidation(hotelId);
  return res.json({
    ok: true,
    mode: "read_only",
    hotelId,
    brief: {
      ...brief,
      items: (brief.items || []).map((it) => {
        const full = overlaid.find((o) => o.id === it.opportunityId);
        return {
          ...it,
          primaryContact: full?.primaryContact
            ? {
                name: full.primaryContact.name,
                role: full.primaryContact.role,
                email: full.primaryContact.email,
                phone: full.primaryContact.phone || null,
                phoneTypeLabel: full.primaryContact.phoneTypeLabel || null,
              }
            : it.primaryContact,
          validation: (validationDoc.items || []).find(
            (v) => v.opportunityId === it.opportunityId
          )
            ? {
                familiarityStatus: validationDoc.items.find(
                  (v) => v.opportunityId === it.opportunityId
                ).familiarityStatus,
                commercialValue: validationDoc.items.find(
                  (v) => v.opportunityId === it.opportunityId
                ).commercialValue,
              }
            : null,
        };
      }),
    },
  });
}

export async function getGdiShareOpportunities(req, res) {
  const verified = requireGdiShare(req, res, "opportunities");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const preview = researchPreviewDocument(verified);
  if (isResearchPreviewClaim(verified.claims)) {
    if (!preview) return res.status(403).json({ ok: false, error: "RESEARCH_PREVIEW_SCOPE" });
    return res.json({
      ok: true,
      mode: "read_only",
      accessScope: RESEARCH_PREVIEW_SCOPE,
      researchPreview: true,
      pageTitle: preview.title || RESEARCH_PREVIEW_TITLE,
      disclaimer: preview.disclaimer || RESEARCH_PREVIEW_DISCLAIMER,
      commerciallyReady: false,
      customerReady: false,
      count: preview.opportunities.length,
      opportunities: preview.opportunities,
    });
  }
  const validationDoc = loadShareValidation(hotelId);
  const byVal = new Map((validationDoc.items || []).map((v) => [v.opportunityId, v]));
  const view = String(req.query.view || "list").trim().toLowerCase();
  const format = String(req.query.format || "").trim().toLowerCase();
  const wantFull = view === "full" || view === "complete";

  if (format === "csv") {
    return getGdiShareOpportunitiesExport(req, res);
  }

  // Canonical publication package (immutable) — preferred when CUSTOMER_PUBLISHED.
  // Fail closed when pointer is broken or GDI_REQUIRE_APPROVED_PUBLICATION_PACKAGE=1.
  {
    const {
      resolveShareOpportunitiesFromApprovedPackage,
      mergeShareFeedbackOntoPackageOpportunities,
    } = await import(
      "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js"
    );
    const pkgResolved = resolveShareOpportunitiesFromApprovedPackage({ hotelId });
    if (!pkgResolved.ok && pkgResolved.mode === "FAIL_CLOSED") {
      return res.status(503).json({
        ok: false,
        error: "APPROVED_PUBLICATION_PACKAGE_UNAVAILABLE",
        detail: pkgResolved.error || null,
        publication: pkgResolved.detail || null,
      });
    }
    if (pkgResolved.ok && pkgResolved.mode === "PACKAGE") {
      let withFeedback = mergeShareFeedbackOntoPackageOpportunities(
        pkgResolved.opportunities,
        validationDoc,
      );
      try {
        const { attachPackageCustomerPresentation } = await import(
          "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js"
        );
        withFeedback = attachPackageCustomerPresentation(withFeedback);
      } catch (tilePreviewErr) {
        console.error(
          "[gdi] share publication-package customer presentation attach failed",
          tilePreviewErr?.message || tilePreviewErr,
        );
      }
      // Hotel-level Need Dates status is live hotel-config metadata (not package
      // fingerprint content). Must be present on PACKAGE responses so share UI
      // does not fall back to NOT_ESTABLISHED while auth shows HOTEL_CONFIRMED.
      let hotelNeedDateConfigV1;
      try {
        const {
          resolveHotelNeedDateConfiguration,
          toCustomerSafeHotelNeedDateStatus,
        } = await import(
          "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js"
        );
        hotelNeedDateConfigV1 = toCustomerSafeHotelNeedDateStatus(
          resolveHotelNeedDateConfiguration(hotelId),
          { includeUnpublishedEstimates: false },
        );
      } catch (needDateCfgErr) {
        console.error(
          "[gdi] package share hotel need-date config resolve failed",
          needDateCfgErr?.message || needDateCfgErr,
        );
        hotelNeedDateConfigV1 = {
          version: "GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1",
          hotelId,
          status: "NOT_ESTABLISHED",
          statusLabel: "Need dates not established",
          accessibleLabel: "Need dates not established",
          icon: "calendar-minus",
          colorClass: "gdi-hotel-need-date-status--not-established",
          confirmedPeriodCount: 0,
          estimatedPeriodCount: 0,
          internalOnly: false,
        };
      }
      return res.json({
        ok: true,
        mode: "read_only",
        count: withFeedback.length,
        opportunities: withFeedback,
        publication: pkgResolved.meta || null,
        inventoryPopulationCounts:
          pkgResolved.meta?.inventoryPopulationCounts || null,
        listScope: "PUBLICATION_PACKAGE",
        hotelNeedDateConfigV1,
      });
    }
  }

  const doc = await loadOppDoc(hotelId);
  const { isInventoryVisibilityHotelEnabled } = await import(
    "../lib/group-demand-intelligence/inventory-visibility-v1.js"
  );
  let salesView = isInventoryVisibilityHotelEnabled(hotelId)
    ? doc.opportunities || []
    : filterSalespersonView(doc.opportunities || []);
  // Bethesda share: frozen published-19 (or 67 after founder approval receipt).
  // Never widen via live customerFacing heuristics; always annotate Market Scope.
  let shareListScope = "CUSTOMER_FACING";
  let shareInventoryPopulationCounts = null;
  let facing;
  if (hotelId === BETHESDA_HOTEL_ID) {
    const { selectBethesdaExternalShareOpportunities } = await import(
      "../lib/group-demand-intelligence/bethesda-share-inventory-v1.js"
    );
    const selected = selectBethesdaExternalShareOpportunities(salesView, {
      hotelId,
    });
    facing = applyApprovedBethesdaCustomerProjection(selected.opportunities);
    shareListScope = selected.listScope;
    shareInventoryPopulationCounts = selected.inventoryPopulationCounts;
  } else {
    facing = applyApprovedBethesdaCustomerProjection(
      filterCustomerFacingOpportunities(salesView, { hotelId })
    );
    facing = annotateInventoryPopulations(facing, { hotelId });
    shareInventoryPopulationCounts = buildInventoryPopulationCounts(facing);
  }
  try {
    const { attachHotelNeedDateTilePreviews } = await import(
      "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js"
    );
    const hotels = listRegisteredHotels();
    const hotelMeta =
      hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
    facing = await attachHotelNeedDateTilePreviews(hotelId, facing, {
      hotelName: hotelMeta.name || hotelMeta.hotelName || null,
    });
  } catch (needDateTileErr) {
    console.error(
      "[gdi] share need-date tile preview attach failed",
      needDateTileErr?.message || needDateTileErr,
    );
  }
  // Same commercial projection as auth list — locked snaps stay frozen.
  facing = projectOpportunitiesCommercialQuality(facing);
  const opportunities = facing.map((o) => {
    const projected = projectOpportunityForPresentation(o, {
      hotelId,
      surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
      view: wantFull ? "detail" : "list",
    });
    const v = byVal.get(o.id);
    return {
      ...projected.dto,
      shareValidation: v
        ? {
            familiarityStatus: v.familiarityStatus,
            commercialValue: v.commercialValue,
            contactPersonAssessment: v.contactPersonAssessment,
            emailAssessment: v.emailAssessment,
            phoneAssessment: v.phoneAssessment,
            note: v.note || "",
            validatedAt: v.validatedAt,
            hotelValidationUpdatedAt:
              v.hotelValidationUpdatedAt || v.validatedAt || null,
          }
        : null,
    };
  });
  const withProgression = (
    await attachCommercialProgression(hotelId, opportunities)
  ).map((row) => {
    const ts =
      row.shareValidation?.hotelValidationUpdatedAt ||
      row.shareValidation?.validatedAt ||
      null;
    if (!ts) return row;
    return {
      ...row,
      commercialProgression: {
        ...(row.commercialProgression || {}),
        hotelValidationUpdatedAt: ts,
      },
    };
  });
  let hotelNeedDateConfigV1;
  try {
    const {
      resolveHotelNeedDateConfiguration,
      toCustomerSafeHotelNeedDateStatus,
    } = await import(
      "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js"
    );
    // Share: hide unpublished market estimates (NOT_ESTABLISHED unless confirmed).
    // Live compute — no snapshot republish required for hotel-config status icons.
    hotelNeedDateConfigV1 = toCustomerSafeHotelNeedDateStatus(
      resolveHotelNeedDateConfiguration(hotelId),
      { includeUnpublishedEstimates: false }
    );
  } catch (needDateCfgErr) {
    console.error(
      "[gdi] share hotel need-date config resolve failed",
      needDateCfgErr?.message || needDateCfgErr
    );
    hotelNeedDateConfigV1 = {
      version: "GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1",
      hotelId,
      status: "NOT_ESTABLISHED",
      statusLabel: "Need dates not established",
      accessibleLabel: "Need dates not established",
      icon: "calendar-minus",
      colorClass: "gdi-hotel-need-date-status--not-established",
      confirmedPeriodCount: 0,
      estimatedPeriodCount: 0,
      internalOnly: false,
    };
  }
  res.setHeader("X-GDI-Opportunity-View", wantFull ? "full" : "list");
  res.setHeader("X-GDI-Share-Presentation-Parity", "v1");
  if (shareListScope) {
    res.setHeader("X-GDI-Share-List-Scope", String(shareListScope));
  }
  return res.json({
    ok: true,
    mode: "read_only",
    hotelId,
    count: withProgression.length,
    view: wantFull ? "full" : "list",
    schemaVersion: wantFull ? null : GDI_OPPORTUNITY_LIST_SCHEMA,
    listScope: shareListScope,
    inventoryPopulationCounts: shareInventoryPopulationCounts,
    opportunities: withProgression,
    hotelNeedDateConfigV1,
    validationEnums: {
      familiarityStatus: SHARE_FAMILIARITY_STATUS,
      familiarityLabels: SHARE_FAMILIARITY_LABEL,
      commercialValue: SHARE_COMMERCIAL_VALUE,
      commercialValueLabels: SHARE_COMMERCIAL_VALUE_LABEL,
      contactPersonAssessment: SHARE_CONTACT_PERSON_ASSESSMENT,
      contactPersonLabels: SHARE_CONTACT_PERSON_LABEL,
      emailAssessment: SHARE_EMAIL_ASSESSMENT,
      emailAssessmentLabels: SHARE_EMAIL_ASSESSMENT_LABEL,
      phoneAssessment: SHARE_PHONE_ASSESSMENT,
      phoneAssessmentLabels: SHARE_PHONE_ASSESSMENT_LABEL,
      validationReason: Object.values(HOTEL_VALIDATION_REASON),
      validationReasonLabels: HOTEL_VALIDATION_REASON_LABEL,
    },
  });
}

/** Share-scoped CSV export (same capability gate as list). */
export async function getGdiShareOpportunitiesExport(req, res) {
  const verified = requireGdiShare(req, res, "opportunities");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  if (isResearchPreviewClaim(verified.claims)) {
    return res.status(403).json({ ok: false, error: "RESEARCH_PREVIEW_READ_ONLY" });
  }
  const doc = await loadOppDoc(hotelId);
  const { isInventoryVisibilityHotelEnabled: invHotelShareExport } =
    await import("../lib/group-demand-intelligence/inventory-visibility-v1.js");
  const shareExportBase = invHotelShareExport(hotelId)
    ? doc.opportunities || []
    : filterSalespersonView(doc.opportunities || []);
  let shareExportFacing;
  if (hotelId === BETHESDA_HOTEL_ID) {
    const { selectBethesdaExternalShareOpportunities } = await import(
      "../lib/group-demand-intelligence/bethesda-share-inventory-v1.js"
    );
    shareExportFacing = selectBethesdaExternalShareOpportunities(
      shareExportBase,
      { hotelId }
    ).opportunities;
  } else {
    shareExportFacing = filterCustomerFacingOpportunities(shareExportBase, {
      hotelId,
    });
  }
  let opportunities = projectOpportunitiesCommercialQuality(
    applyApprovedBethesdaCustomerProjection(shareExportFacing)
  );
  opportunities = filterOpportunitiesForExport(opportunities, req.query);
  opportunities = await attachCommercialProgression(hotelId, opportunities);
  const hotels = listRegisteredHotels();
  const hotel = hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
  return sendCustomerCsv(res, opportunities, hotel);
}

export async function getGdiShareOpportunityDetail(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const preview = researchPreviewDocument(verified);
  if (isResearchPreviewClaim(verified.claims)) {
    const card = preview?.opportunities?.find((row) => row.id === opportunityId);
    if (!card) return res.status(404).json({ ok: false, error: "opportunity_not_found" });
    return res.json({
      ok: true,
      mode: "read_only",
      researchPreview: true,
      disclaimer: preview.disclaimer || RESEARCH_PREVIEW_DISCLAIMER,
      commerciallyReady: false,
      customerReady: false,
      opportunity: card,
    });
  }

  // Immutable publication package must own DETAIL as well as LIST.
  // Prior bug: list served PACKAGE (with needDateMatchV1) but detail fell
  // through to live reconstruct/sanitize, so drawers omitted Hotel Need-Date
  // Match and other frozen sections while cards still showed package badges.
  {
    const {
      resolveOpportunityFromApprovedPackage,
      mergeShareFeedbackOntoPackageOpportunities,
    } = await import(
      "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js"
    );
    const pkgResolved = resolveOpportunityFromApprovedPackage({
      hotelId,
      opportunityId,
    });
    if (!pkgResolved.ok && pkgResolved.mode === "FAIL_CLOSED") {
      return res.status(503).json({
        ok: false,
        error: "APPROVED_PUBLICATION_PACKAGE_UNAVAILABLE",
        detail: pkgResolved.error || null,
      });
    }
    if (
      pkgResolved.ok &&
      pkgResolved.mode === "PACKAGE" &&
      pkgResolved.opportunity
    ) {
      const validationDoc = loadShareValidation(hotelId);
      let [withFeedback] = mergeShareFeedbackOntoPackageOpportunities(
        [pkgResolved.opportunity],
        validationDoc,
      );
      try {
        const { attachPackageCustomerPresentation } = await import(
          "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js"
        );
        withFeedback = attachPackageCustomerPresentation(withFeedback);
      } catch (tilePreviewErr) {
        console.error(
          "[gdi] share publication-package detail customer presentation attach failed",
          tilePreviewErr?.message || tilePreviewErr,
        );
      }
      const validation = getShareValidationForOpportunity(
        hotelId,
        opportunityId,
      );
      const bySubject = await loadGdiCommercialProgressionBySubject(hotelId);
      const commercialProgression = toCustomerCommercialProgressionDto(
        bySubject.get(opportunityId),
      );
      const hotelValidationUpdatedAt =
        validation?.hotelValidationUpdatedAt || validation?.validatedAt || null;
      const progressionMerged =
        commercialProgression || hotelValidationUpdatedAt
          ? {
              ...(commercialProgression || {}),
              hotelValidationUpdatedAt:
                hotelValidationUpdatedAt ||
                commercialProgression?.hotelValidationUpdatedAt ||
                null,
            }
          : null;
      res.setHeader("X-GDI-Detail-Source", "PUBLICATION_PACKAGE");
      res.setHeader("X-GDI-Share-Presentation-Parity", "v1");
      res.setHeader("X-GDI-Universal-Presentation", "v1");
      return res.json({
        ok: true,
        mode: "read_only",
        listScope: "PUBLICATION_PACKAGE",
        publication: pkgResolved.meta || null,
        opportunity: {
          ...withFeedback,
          ...(progressionMerged
            ? { commercialProgression: progressionMerged }
            : {}),
          shareValidation: validation
            ? {
                familiarityStatus: validation.familiarityStatus,
                commercialValue: validation.commercialValue,
                contactPersonAssessment: validation.contactPersonAssessment,
                emailAssessment: validation.emailAssessment,
                phoneAssessment: validation.phoneAssessment,
                note: validation.note || "",
                validatedAt: validation.validatedAt,
                hotelValidationUpdatedAt,
              }
            : withFeedback?.shareValidation || null,
        },
        scoreAudit: null,
      });
    }
    if (
      pkgResolved.mode === "PACKAGE" &&
      pkgResolved.error === "OPPORTUNITY_NOT_IN_PUBLICATION_PACKAGE"
    ) {
      return res.status(404).json({ ok: false, error: "opportunity_not_found" });
    }
  }

  const t0 = Date.now();
  const loaded = await loadOpportunityForDetail(hotelId, opportunityId);
  let opportunity = loaded.opportunity;
  if (
    !opportunity ||
    opportunity.priority === "DISQUALIFIED" ||
    isGdiTestOrFixtureOpportunity(opportunity)
  ) {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  try {
    const pe = await enrichPrivateEventOpportunityDetail(opportunity);
    if (pe?.opportunity) opportunity = pe.opportunity;
  } catch (peErr) {
    console.error("[gdi] share PE detail enrichment failed", peErr?.message || peErr);
  }
  // Prefer clean-28 merged row from loadOppDoc (same source as share list).
  try {
    const mergedDoc = await loadOppDoc(hotelId);
    const fromMerged = (mergedDoc?.opportunities || []).find(
      (row) => row?.id === opportunityId,
    );
    if (fromMerged) opportunity = fromMerged;
  } catch (mergeReadErr) {
    console.error(
      "[gdi] share detail merge reload failed",
      mergeReadErr?.message || mergeReadErr,
    );
  }
  // Universal presentation pipeline — same object path as auth + share list.
  const [projected] = projectOpportunitiesCommercialQuality([opportunity]);
  const shareProjected = projectOpportunityForPresentation(projected, {
    hotelId,
    surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
    view: "detail",
  });
  const shareDetail = shareProjected.dto;
  const validation = getShareValidationForOpportunity(hotelId, opportunityId);
  const bySubject = await loadGdiCommercialProgressionBySubject(hotelId);
  const commercialProgression = toCustomerCommercialProgressionDto(
    bySubject.get(opportunityId)
  );
  const durationMs = Date.now() - t0;
  if (durationMs >= 1000 || process.env.GDI_PERF_LOG === "1") {
    console.info(
      JSON.stringify({
        gdi_perf: true,
        route: "getGdiShareOpportunityDetail",
        hotelId,
        durationMs,
        loadSource: loaded.source,
        loadMs: loaded.durationMs,
      })
    );
  }
  res.setHeader("X-GDI-Detail-Source", loaded.source);
  res.setHeader("X-GDI-Share-Presentation-Parity", "v1");
  res.setHeader("X-GDI-Universal-Presentation", "v1");
  const hotelValidationUpdatedAt =
    validation?.hotelValidationUpdatedAt || validation?.validatedAt || null;
  const progressionMerged =
    commercialProgression || hotelValidationUpdatedAt
      ? {
          ...(commercialProgression || {}),
          hotelValidationUpdatedAt:
            hotelValidationUpdatedAt ||
            commercialProgression?.hotelValidationUpdatedAt ||
            null,
        }
      : null;
  return res.json({
    ok: true,
    mode: "read_only",
    opportunity: {
      ...shareDetail,
      ...(progressionMerged ? { commercialProgression: progressionMerged } : {}),
      shareValidation: validation
        ? {
            familiarityStatus: validation.familiarityStatus,
            commercialValue: validation.commercialValue,
            contactPersonAssessment: validation.contactPersonAssessment,
            emailAssessment: validation.emailAssessment,
            phoneAssessment: validation.phoneAssessment,
            note: validation.note || "",
            validatedAt: validation.validatedAt,
            hotelValidationUpdatedAt,
          }
        : null,
    },
    scoreAudit: reconstructScoreAudit(opportunity),
  });
}

/**
 * Share-token validation write — stores separately from research/canonical.
 * Requires CAN_VALIDATE (legacy tokens with opportunity_detail surface qualify).
 */
export async function postGdiShareValidation(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  if (rejectResearchPreviewWrite(verified, res)) return;
  const caps = verified.capabilities || [];
  if (!caps.includes("CAN_VALIDATE")) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_SURFACE",
      code: "SHARE_SURFACE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_HOTEL_SCOPE",
      code: "SHARE_HOTEL_SCOPE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  try {
    const body = req.body || {};
    const previousValidation = getShareValidationForOpportunity(
      hotelId,
      opportunityId
    );
    const saved = saveShareValidationItem(
      hotelId,
      {
        opportunityId,
        validator: body.validator || "SHARE_REVIEWER",
        familiarityStatus: body.familiarityStatus || null,
        commercialValue: body.commercialValue || null,
        contactPersonAssessment: body.contactPersonAssessment || null,
        emailAssessment: body.emailAssessment || null,
        phoneAssessment: body.phoneAssessment || null,
        note: body.note || "",
      },
      {
        expectedHotelValidationUpdatedAt:
          body.expectedHotelValidationUpdatedAt || null,
      }
    );
    const item = saved.item;
    const unchanged = saved.unchanged === true;

    if (!unchanged) {
      try {
        await ingestGdiShareValidation({
          hotelId,
          opportunityId,
          opportunity,
          validation: item,
          actor: {
            userId: body.validator || "SHARE_REVIEWER",
            role: "OTHER",
          },
        });
      } catch (dualErr) {
        restoreShareValidationItemAfterFailedPersist(
          hotelId,
          opportunityId,
          previousValidation
        );
        console.error(
          "[gdi] share decision-outcome dual-write failed",
          dualErr?.message || dualErr
        );
        return res.status(502).json({
          ok: false,
          error: "decision_outcome_persist_failed",
          message:
            dualErr?.message ||
            "Canonical Decision & Outcome write failed",
          hotelValidationUpdatedAt: previousValidation?.validatedAt || null,
          note: "Saved is not confirmed until Airtable persistence succeeds.",
        });
      }
      invalidateGdiHotelReadCache(hotelId);
    }

    const validationDoc = loadShareValidation(hotelId);
    const summary = buildShareValidationSummary({
      opportunities: filterSalespersonView(doc.opportunities || []),
      validationItems: validationDoc.items || [],
    });
    const hotelValidationUpdatedAt =
      item.hotelValidationUpdatedAt || item.validatedAt || null;
    return res.json({
      ok: true,
      created: !unchanged,
      unchanged,
      validation: {
        familiarityStatus: item.familiarityStatus,
        commercialValue: item.commercialValue,
        contactPersonAssessment: item.contactPersonAssessment,
        emailAssessment: item.emailAssessment,
        phoneAssessment: item.phoneAssessment,
        note: item.note || "",
        validatedAt: item.validatedAt,
        hotelValidationUpdatedAt,
        validator: item.validator || null,
      },
      hotelValidationUpdatedAt,
      summary,
      note: "Validation stored separately. Does not overwrite research facts or mutate canonical contacts. Dual-written to canonical Decision layer.",
    });
  } catch (err) {
    if (err.code === "hotel_validation_conflict") {
      return res.status(409).json({
        ok: false,
        error: err.code,
        message:
          "Hotel Validation was updated by another session. Reload and try again.",
        hotelValidationUpdatedAt: err.hotelValidationUpdatedAt || null,
      });
    }
    if (err.code === "invalid_share_validation") {
      return res.status(400).json({ ok: false, error: err.code, errors: err.errors });
    }
    console.error("[gdi] share validation failed", err);
    return res.status(500).json({ ok: false, error: "share_validation_failed" });
  }
}

export async function getGdiShareValidation(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  if (!requireShareCapability(verified, res, "CAN_VALIDATE")) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const doc = await loadOppDoc(hotelId);
  const validationDoc = loadShareValidation(hotelId);
  const summary = buildShareValidationSummary({
    opportunities: filterSalespersonView(doc.opportunities || []),
    validationItems: validationDoc.items || [],
  });
  return res.json({
    ok: true,
    hotelId,
    items: (validationDoc.items || []).map((v) => ({
      opportunityId: v.opportunityId,
      familiarityStatus: v.familiarityStatus,
      commercialValue: v.commercialValue,
      contactPersonAssessment: v.contactPersonAssessment,
      emailAssessment: v.emailAssessment,
      phoneAssessment: v.phoneAssessment,
      note: v.note || "",
      validatedAt: v.validatedAt,
      hotelValidationUpdatedAt:
        v.hotelValidationUpdatedAt || v.validatedAt || null,
      validator: v.validator || null,
    })),
    summary,
  });
}

function requireShareCapability(verified, res, capability) {
  const caps = verified.capabilities || [];
  if (!caps.includes(capability)) {
    res.status(403).json({
      ok: false,
      error: "SHARE_SURFACE",
      code: "SHARE_SURFACE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
    return false;
  }
  return true;
}

/**
 * Share-token ACTION write → canonical Decision Events.
 * Requires CAN_RECORD_ACTION (registry grant or explicit token capability).
 */
export async function postGdiShareAction(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  if (rejectResearchPreviewWrite(verified, res)) return;
  if (!requireShareCapability(verified, res, "CAN_RECORD_ACTION")) return;

  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_HOTEL_SCOPE",
      code: "SHARE_HOTEL_SCOPE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  const actionType = String(req.body?.actionType || "").trim();
  if (!actionType || !Object.values(GDI_ACTION_TYPE).includes(actionType)) {
    return res.status(400).json({ ok: false, error: "invalid_action_type" });
  }
  try {
    const ensured = await ensureGdiOpportunityDecision({ hotelId, opportunity });
    const decision = ensured.decision;
    if (!decision) {
      return res.status(500).json({ ok: false, error: "decision_ensure_failed" });
    }
    const result = await recordAction(hotelId, decision.decisionId, {
      actionType,
      actionDate: req.body?.actionDate || new Date().toISOString(),
      sourceSurface: "gdi_share",
      displayName: req.body?.displayName || "Share reviewer",
      userId: req.body?.validator || "SHARE_REVIEWER",
      expectedSalesActionUpdatedAt:
        req.body?.expectedSalesActionUpdatedAt || null,
      provenance: {
        tokenId: verified.claims.tid,
        opportunityId,
        source: "CUSTOMER_SHARE",
      },
    });
    if (result.created !== false) {
      invalidateGdiHotelReadCache(hotelId);
    }
    const progression = toCustomerCommercialProgressionDto(
      result.current?.commercialProgression || null
    );
    return res.status(result.created === false ? 200 : 201).json({
      ok: true,
      decisionId: decision.decisionId,
      event: result.event,
      created: result.created !== false,
      unchanged: result.unchanged === true,
      current: result.current,
      commercialProgression: progression,
      salesActionUpdatedAt:
        result.salesActionUpdatedAt ||
        progression?.salesActionUpdatedAt ||
        null,
    });
  } catch (err) {
    if (err.code === "sales_action_conflict") {
      return res.status(409).json({
        ok: false,
        error: err.code,
        message:
          "Action was updated by another session. Reload and try again.",
        salesActionUpdatedAt: err.salesActionUpdatedAt || null,
      });
    }
    console.error("[gdi] share action failed", err);
    return res.status(500).json({
      ok: false,
      error: err.code || "share_action_failed",
      message: err.message,
    });
  }
}

/**
 * Share-token OUTCOME write → canonical Decision Events.
 * Requires CAN_RECORD_OUTCOME.
 */
export async function postGdiShareOutcome(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  if (rejectResearchPreviewWrite(verified, res)) return;
  if (!requireShareCapability(verified, res, "CAN_RECORD_OUTCOME")) return;

  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_HOTEL_SCOPE",
      code: "SHARE_HOTEL_SCOPE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  const outcomeType = String(req.body?.outcomeType || "").trim();
  if (!outcomeType || !Object.values(GDI_OUTCOME_TYPE).includes(outcomeType)) {
    return res.status(400).json({ ok: false, error: "invalid_outcome_type" });
  }
  try {
    const ensured = await ensureGdiOpportunityDecision({ hotelId, opportunity });
    const decision = ensured.decision;
    if (!decision) {
      return res.status(500).json({ ok: false, error: "decision_ensure_failed" });
    }
    const result = await recordOutcome(hotelId, decision.decisionId, {
      outcomeType,
      outcomeReason: req.body?.outcomeReason || req.body?.lossReason || null,
      outcomeNote: req.body?.note || null,
      outcomeDate: req.body?.outcomeDate || new Date().toISOString(),
      causalConfidence: "UNKNOWN",
      sourceSurface: "gdi_share",
      displayName: req.body?.displayName || "Share reviewer",
      userId: req.body?.validator || "SHARE_REVIEWER",
      expectedSalesOutcomeUpdatedAt:
        req.body?.expectedSalesOutcomeUpdatedAt || null,
      provenance: {
        tokenId: verified.claims.tid,
        opportunityId,
        source: "CUSTOMER_SHARE",
      },
    });
    if (result.created !== false) {
      invalidateGdiHotelReadCache(hotelId);
    }
    const progression = toCustomerCommercialProgressionDto(
      result.current?.commercialProgression || null
    );
    return res.status(result.created === false ? 200 : 201).json({
      ok: true,
      decisionId: decision.decisionId,
      event: result.event,
      created: result.created !== false,
      unchanged: result.unchanged === true,
      current: result.current,
      commercialProgression: progression,
      salesOutcomeUpdatedAt:
        result.salesOutcomeUpdatedAt ||
        progression?.salesOutcomeUpdatedAt ||
        null,
    });
  } catch (err) {
    if (err.code === "sales_outcome_conflict") {
      return res.status(409).json({
        ok: false,
        error: err.code,
        message:
          "Outcome was updated by another session. Reload and try again.",
        salesOutcomeUpdatedAt: err.salesOutcomeUpdatedAt || null,
      });
    }
    console.error("[gdi] share outcome failed", err);
    return res.status(500).json({
      ok: false,
      error: err.code || "share_outcome_failed",
      message: err.message,
    });
  }
}

/** Admin-only: issue share token */
export function postGdiIssueShare(req, res) {
  if (!flagGate(req, res)) return;
  try {
    const body = req.body || {};
    const hotelId = String(body.hotelId || req.params.hotelId || "").trim();
    const issued = issueGdiShareCapability({
      hotelId,
      label: body.label || null,
      expiresAt: body.expiresAt || null,
    });
    return res.json({
      ok: true,
      tokenId: issued.tokenId,
      hotelId: issued.hotelId,
      sharePath: issued.sharePath,
      expiresAt: issued.meta.expiresAt,
      mode: "read_only",
    });
  } catch (err) {
    console.error("[gdi] issue share failed", err);
    return res.status(500).json({
      ok: false,
      error: "issue_share_failed",
      message: err.message,
    });
  }
}

const researchPreviewLedger = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "data/group-demand-intelligence/gdi-founder-approval-v1/approval-ledger-v1.json"
);

export async function postGdiIssueResearchPreview(req, res) {
  const hotelId = String(req.params.hotelId || "").trim();
  const manifest = researchPreviewManifestForHotel(hotelId);
  if (!manifest) return res.status(404).json({ ok: false, reason: "NO_RESEARCH_PREVIEW_MANIFEST", written: false });
  const trusted = loadTrustedConsumedFounderIdentity();
  const gate = assertFounderCanarySession({
    verifiedVia: req.memberstackVerifiedVia,
    email: req.memberstackEmail,
    trustedEmail: trusted.ok ? trusted.email : "",
  });
  if (!gate.ok) return res.status(403).json({ ok: false, reason: gate.reason, written: false });
  const snapshot = researchPreviewForClaim({
    accessScope: RESEARCH_PREVIEW_SCOPE,
    previewManifestId: manifest.manifestId,
    hotelId,
  });
  if (!snapshot || snapshot.opportunities.length !== manifest.opportunityIds.length) {
    return res.status(409).json({ ok: false, reason: "RESEARCH_PREVIEW_SNAPSHOT_MISSING", written: false });
  }
  const sourceHash = createHash("sha256").update(JSON.stringify(manifest.opportunityIds)).digest("hex");
  const match = String(req.headers.authorization || req.headers.Authorization || "").match(/^Bearer\s+(\S+)$/i);
  const approval = await requestFounderApproval({
    token: match ? match[1] : "",
    operation: "RESEARCH_PREVIEW",
    hotelIds: [hotelId],
    sourceHash,
    opportunityIds: [...manifest.opportunityIds],
    customerAccessScope: RESEARCH_PREVIEW_SCOPE,
    restrictions: ["read_only", "not_customer_publication", "no_customer_visible_change", "no_commercial_ready_change"],
    expiresAt: new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString(),
    ledgerPath: researchPreviewLedger,
  });
  if (!approval.ok) return res.status(401).json(approval);
  try {
    const issued = issueGdiShareCapability({
      hotelId,
      label: `Research Preview ${manifest.manifestId}`,
      accessScope: RESEARCH_PREVIEW_SCOPE,
      previewManifestId: manifest.manifestId,
    });
    return res.json({
      ok: true,
      researchPreview: true,
      commerciallyReady: false,
      customerReady: false,
      publicationStatus: "not_published",
      approvalId: approval.approvalId,
      tokenId: issued.tokenId,
      sharePath: issued.sharePath,
      opportunityCount: snapshot.opportunities.length,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, reason: "PREVIEW_ISSUE_FAILED", message: err.message });
  }
}

export function postGdiRevokeShare(req, res) {
  if (!flagGate(req, res)) return;
  const tokenId = String(req.body?.tokenId || req.params.tokenId || "").trim();
  const result = revokeGdiShareCapability(tokenId, req.body?.reason || "admin_revoke");
  if (!result.ok) {
    return res.status(404).json(result);
  }
  return res.json(result);
}
