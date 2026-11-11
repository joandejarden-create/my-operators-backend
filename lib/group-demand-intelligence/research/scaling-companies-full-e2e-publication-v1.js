/**
 * Scaling-companies Full E2E — gated publication + customer-share verification.
 *
 * Reuses:
 *   - authorizeProspectPublicationPromotion / evaluateProspectPublicationEligibility
 *   - persistPromotedOpportunityCanonical
 *   - upsertOpportunity (Airtable) when production writes authorized
 *   - isReviewedProspectCustomerVisible / filterCustomerFacingOpportunities
 *
 * Independent gates:
 *   authorizePublication  — customer-facing promotion + canonical persist
 *   authorizeProductionWrites — Airtable upsert (defaults true when publication authorized)
 *   applyProductionWrites — explicit execute flag (PS1 FullE2E sets when publishing)
 *
 * Never treats Airtable write alone as proof of external customer visibility.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateProspectPublicationEligibility,
  authorizeProspectPublicationPromotion,
} from "../prospect-publication-eligibility-v1.js";
import {
  isReviewedProspectCustomerVisible,
  isReviewedProspectHotelEnabled,
  evaluateReviewedProspectProductionListPreflight,
  REVIEWED_PROSPECT_PUBLICATION_BASIS,
} from "../reviewed-prospect-publication-v1.js";
import { filterCustomerFacingOpportunities } from "../customer-visibility.js";
import { persistPromotedOpportunityCanonical } from "./maturation-canonical-persist-v1.js";
import {
  loadOpportunitiesCanonical,
} from "../opportunity-persistence.js";
import { upsertOpportunity } from "../airtable-opportunity-store.js";
import { applyScalingSalesPresentation } from "../scaling-companies-sales-presentation-v1.js";
import { applyScalingTwoContactStandard } from "../scaling-two-contact-standard-v1.js";
import { classifyScalingCompanyEntityFitness } from "../workflow/scaling-companies-research-v1.js";
import { scoreScalingGoldStandardOpportunity } from "../scaling-gold-standard-drawer-contract-v1.js";
import {
  evaluateScalingPublicationQualification,
  applyScalingPublicationGateToEligibility,
  SCALING_PUBLICATION_QUALIFICATION_V1,
} from "../scaling-publication-qualification-v1.js";
import { enrichResearchContinuationWithBuyerStrategy } from "../scaling-buyer-research-strategies-v1.js";
import { attachGdi2ShadowEvaluation } from "../gdi2-shadow-publication-evaluation-v1.js";
import { planBuyerRecovery } from "../buyer-recovery-planner-v1.js";

export const SCALING_FULL_E2E_PUBLICATION_V1 =
  "gdi-scaling-companies-full-e2e-publication-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * run-052 Issue 8: Association / medical_academic records must never be rewritten
 * as scaling_companies during shared publication staging.
 */
export function isAssociationSegmentOpportunity(opportunity = {}, opts = {}) {
  if (
    opts.segmentFamily === "associations_events" ||
    opts.preserveResearchSegment === true ||
    opts.segmentFamily === "association_events"
  ) {
    return true;
  }
  const blob = [
    opportunity.researchSegment,
    opportunity.segment,
    opportunity.demandFamily,
    opportunity.opportunityType,
    opportunity.associationEventsGoldBrief ? "associationEventsGoldBrief" : "",
    opportunity.discoverySource,
  ]
    .map(text)
    .join(" ");
  return /association|medical_academic|conference_group|associations?_events|event_to_demand/i.test(
    blob,
  );
}

export function resolvePublicationSegmentStamp(opportunity = {}, opts = {}) {
  if (isAssociationSegmentOpportunity(opportunity, opts)) {
    const existing = text(opportunity.researchSegment);
    return {
      researchSegment:
        existing && !/^scaling_companies$/i.test(existing)
          ? existing
          : "association_events",
      segment:
        text(opportunity.segment) ||
        "Association / Conference Group",
      preserveAssociationIdentity: true,
    };
  }
  return {
    researchSegment: "scaling_companies",
    segment:
      text(opportunity.segment) ||
      "Scaling Companies / Emerging Corporate Accounts",
    preserveAssociationIdentity: false,
  };
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

/**
 * Select matured handoff rows that are publication candidates (not HOLD/EXCLUDE).
 * Entity misclassification (DMC / attraction / individual planner) is not
 * hotel-facing for the Scaling Companies segment.
 */
export function selectPublicationCandidates(maturedRows = [], opts = {}) {
  return (maturedRows || [])
    .filter((r) => r && r.opportunityPayload)
    .map((r) => {
      const label = String(r.publicationAuditLabel || "").toUpperCase();
      const opp = r.opportunityPayload;
      const baseElig = evaluateProspectPublicationEligibility(opp, {
        hotelId: opp.hotelId,
      });
      // Founder rule: Scaling publication requires one named relevant buyer +
      // demand thesis + activity + usable route. Second buyer / RFP / volume ≠ gates.
      const elig = applyScalingPublicationGateToEligibility(baseElig, opp);
      const scalingQual =
        elig.scalingQualification ||
        evaluateScalingPublicationQualification(opp);
      const entityFitness = classifyScalingCompanyEntityFitness(opp);
      const holdOrExclude =
        label === "EXCLUDE" ||
        label.startsWith("HOLD_") ||
        label === "HOLD" ||
        entityFitness.ok !== true;
      const hotelFacingEligible =
        elig.hotelFacing === true &&
        entityFitness.ok === true &&
        scalingQual.publicationQualified === true;
      const researchContinuation =
        scalingQual.researchContinuation === true && !holdOrExclude;
      const row = {
        candidateId: r.candidateId,
        organizationName: r.organizationName || opp.organizationName,
        publicationAuditLabel: r.publicationAuditLabel,
        holdOrExclude,
        hotelFacingEligible,
        researchContinuation,
        eligibility: elig,
        scalingQualification: scalingQual,
        scalingQualificationVersion: SCALING_PUBLICATION_QUALIFICATION_V1,
        entityFitness,
        opportunity: opp,
        salesBrief: r.salesBrief || null,
      };
      // Phase 2B: shadow GDI 2.0 evaluation — does not change hotelFacingEligible.
      if (opts.skipGdi2Shadow === true) return row;
      const shadowed = attachGdi2ShadowEvaluation(row, {
        hotel: opts.hotel,
        segmentFamily: "scaling_companies",
        existingSalesReady: hotelFacingEligible,
        existingResearchContinuation: researchContinuation,
        existingEligibility: elig,
        authorizePublication: opts.authorizePublication === true,
        env: opts.env,
      });
      if (opts.includeBuyerRecoveryPlan === true && !hotelFacingEligible && !holdOrExclude) {
        shadowed.gdi2BuyerRecoveryPlanV1 = planBuyerRecovery(opp, {
          hotel: opts.hotel,
          attempts: opp.buyerResearchAttempts || [],
          paidResearchAuthorized: false,
        });
      }
      return shadowed;
    });
}

/**
 * Resolve optional client share token for a hotel from known catalogs.
 * Never fabricates tokens — only returns previously issued capability strings.
 */
export function resolveHotelShareToken(hotelId, { reportsRoot = ROOT } = {}) {
  const id = text(hotelId);
  if (!id) return null;
  const candidates = [
    "reports/group-demand-intelligence/gdi-client-share-urls-20260922.json",
    "reports/group-demand-intelligence/gdi-client-share-urls.json",
    "reports/group-demand-intelligence/gdi-client-share-urls-mexico-20261027.json",
    "reports/group-demand-intelligence/gdi-client-share-urls-tampa-20261027.json",
    "config/client-share/gdi-issued-share-urls.json",
    // Gitignored sealed production URLs (local Admin / ProbeLiveShare resolution)
    "config/client-share/production-share-sealed.local.json",
  ];
  for (const rel of candidates) {
    const p = path.join(reportsRoot, rel);
    if (!fs.existsSync(p)) continue;
    try {
      const doc = JSON.parse(fs.readFileSync(p, "utf8"));
      const rows = Array.isArray(doc)
        ? doc
        : doc.hotels ||
          doc.shares ||
          doc.urls ||
          doc.entries ||
          doc.tokens ||
          [];
      for (const row of rows) {
        const hid = row.hotelId || row.hotel_id || row.id;
        if (String(hid) !== id) continue;
        if (row.status && String(row.status).toUpperCase() === "REVOKED") continue;
        const token =
          row.shareToken ||
          row.share ||
          row.token ||
          row.capability ||
          (typeof row.url === "string"
            ? (row.url.match(/[?&]share=([^&]+)/) || [])[1]
            : null);
        if (token) {
          let base = doc.base || null;
          if (!base && row.url) {
            try {
              base = new URL(row.url).origin;
            } catch {
              /* ignore */
            }
          }
          if (!base && row.pagePath) {
            base = "https://my-operators-backend-production.up.railway.app";
          }
          return {
            token: decodeURIComponent(token),
            tokenId: row.tokenId || null,
            base,
            source: rel,
            hotelId: id,
          };
        }
      }
    } catch {
      /* skip */
    }
  }
  return null;
}

/**
 * Local (canonical) customer-visibility verification — does not prove Railway share.
 */
export async function verifyCustomerShareLocal({
  hotelId,
  publishedIds = [],
} = {}) {
  const doc = await loadOpportunitiesCanonical(hotelId, { bypassCache: true });
  const ops = doc?.opportunities || [];
  const byId = new Map(ops.map((o) => [o.id || o.opportunityId, o]));
  const customerFacing = filterCustomerFacingOpportunities(ops, { hotelId });
  const facingIds = new Set(
    customerFacing.map((o) => o.id || o.opportunityId).filter(Boolean),
  );
  const details = (publishedIds || []).map((id) => {
    const opp = byId.get(id);
    const brief = opp?.salesBrief || opp?.commercialIntelligenceBrief || null;
    const substantiveBrief =
      Boolean(brief?.companyAndLocalActivity || brief?.opportunityOverview) &&
      Boolean(brief?.potentialBusinessForThisHotel || brief?.whyThisHotel) &&
      Boolean(brief?.recommendedSalesApproach?.nextAction);
    const gold = opp ? scoreScalingGoldStandardOpportunity(opp) : null;
    return {
      id,
      found: Boolean(opp),
      customerVisible: opp?.customerVisible === true,
      prospectHotelFacing: opp?.prospectHotelFacing === true,
      basis: opp?.prospectPublicationBasis || null,
      reviewedProspectVisible: opp
        ? isReviewedProspectCustomerVisible(opp, { hotelId })
        : false,
      inCustomerFacingFilter: facingIds.has(id),
      hasSalesBrief: Boolean(brief),
      substantiveBrief,
      growthEvidenceDate: opp?.growthEvidenceDate || null,
      // Presentation score is diagnostic only — never flips commercial eligibility.
      goldStandardScore: gold?.score ?? null,
      goldStandardSubstantivePass: gold?.substantivePass ?? null,
      hasWhySelected: Boolean(
        brief?.whyDealalitySelectedThisCompany || opp?.whySelected,
      ),
    };
  });
  const briefOk =
    details.length === 0 || details.every((d) => d.substantiveBrief === true);
  return {
    kind: "canonical_local_filter",
    hotelId,
    reviewedProspectHotelEnabled: isReviewedProspectHotelEnabled(hotelId),
    customerFacingCount: customerFacing.length,
    details,
    allFound: details.every((d) => d.found),
    allFacing: details.every(
      (d) => d.inCustomerFacingFilter && d.reviewedProspectVisible,
    ),
    commercialBriefPersisted: briefOk,
    provesExternalShare: false,
    note: "Local canonical filter only — not Railway production share proof.",
  };
}

/**
 * Optional live share probe (network). Skipped when no token or probeLive=false.
 */
export async function verifyCustomerShareLive({
  hotelId,
  publishedIds = [],
  shareToken = null,
  baseUrl = process.env.GDI_SHARE_PROBE_BASE_URL ||
    process.env.RAILWAY_PUBLIC_DOMAIN ||
    null,
  probeLive = false,
} = {}) {
  if (!probeLive) {
    return {
      kind: "live_share_probe",
      attempted: false,
      skippedReason: "probeLive_false",
      provesExternalShare: false,
    };
  }
  const shareMeta = resolveHotelShareToken(hotelId);
  const resolved = shareToken || shareMeta?.token || null;
  if (!resolved) {
    return {
      kind: "live_share_probe",
      attempted: false,
      skippedReason: "share_token_unavailable",
      provesExternalShare: false,
    };
  }
  const resolvedBase = baseUrl || shareMeta?.base || null;
  if (!resolvedBase) {
    return {
      kind: "live_share_probe",
      attempted: false,
      skippedReason: "base_url_unavailable",
      provesExternalShare: false,
    };
  }
  const origin = String(resolvedBase).startsWith("http")
    ? String(resolvedBase).replace(/\/$/, "")
    : `https://${String(resolvedBase).replace(/\/$/, "")}`;
  const listUrl = `${origin}/api/group-demand-intelligence/share/hotels/${hotelId}/opportunities?share=${encodeURIComponent(resolved)}`;
  try {
    const res = await fetch(listUrl, { method: "GET" });
    const body = await res.json().catch(() => null);
    const list = Array.isArray(body)
      ? body
      : body?.opportunities || body?.items || [];
    const ids = new Set(list.map((o) => o.id || o.opportunityId).filter(Boolean));
    const detailChecks = [];
    for (const id of publishedIds) {
      const detailUrl = `${origin}/api/group-demand-intelligence/share/hotels/${hotelId}/opportunities/${encodeURIComponent(id)}?share=${encodeURIComponent(resolved)}`;
      const dRes = await fetch(detailUrl, { method: "GET" });
      const dBody = await dRes.json().catch(() => ({}));
      const opp = dBody.opportunity || dBody || {};
      const brief = opp.salesBrief || opp.commercialIntelligenceBrief || null;
      const substantiveBrief =
        Boolean(brief?.companyAndLocalActivity || brief?.opportunityOverview) &&
        Boolean(brief?.potentialBusinessForThisHotel || brief?.whyThisHotel) &&
        Boolean(brief?.recommendedSalesApproach?.nextAction);
      detailChecks.push({
        id,
        inList: ids.has(id),
        detailStatus: dRes.status,
        detailOk: dRes.status === 200,
        hasSalesBrief: Boolean(brief),
        substantiveBrief,
        growthEvidenceDate: opp.growthEvidenceDate || null,
      });
    }
    const briefOk =
      detailChecks.length === 0 ||
      detailChecks.every((d) => d.substantiveBrief === true);
    return {
      kind: "live_share_probe",
      attempted: true,
      ok: res.status === 200,
      listStatus: res.status,
      listCount: list.length,
      shareTokenId: resolved,
      detailChecks,
      commercialBriefVisible: briefOk,
      allVisible:
        detailChecks.length > 0 &&
        detailChecks.every((d) => d.inList && d.detailOk && d.substantiveBrief),
      provesExternalShare:
        detailChecks.length > 0 &&
        detailChecks.every((d) => d.inList && d.detailOk && d.substantiveBrief),
    };
  } catch (err) {
    return {
      kind: "live_share_probe",
      attempted: true,
      ok: false,
      error: err.message,
      provesExternalShare: false,
    };
  }
}

/**
 * Execute gated publication for matured Full E2E candidates.
 */
export async function runScalingFullE2ePublicationStage({
  hotelId,
  maturedRows = [],
  authorizePublication = false,
  authorizeProductionWrites = false,
  applyProductionWrites = false,
  reportDir = null,
  probeLiveShare = false,
  authorizedBy = "gdi-scaling-companies-hotel-runner-v1",
  // run-052 Issue 8: Association FullE2E must pass associations_events so
  // shared publication staging cannot rewrite segment identity to Scaling.
  segmentFamily = "scaling_companies",
  preserveResearchSegment = false,
} = {}) {
  const segmentOpts = { segmentFamily, preserveResearchSegment };
  const selected = selectPublicationCandidates(maturedRows);
  const qualifying = selected.filter(
    (s) => !s.holdOrExclude && s.hotelFacingEligible,
  );
  const held = selected.filter((s) => s.holdOrExclude || !s.hotelFacingEligible);

  const stage = {
    version: SCALING_FULL_E2E_PUBLICATION_V1,
    hotelId,
    segmentFamily,
    preserveResearchSegment: preserveResearchSegment === true,
    authorizePublication: authorizePublication === true,
    authorizeProductionWrites: authorizeProductionWrites === true,
    applyProductionWrites: applyProductionWrites === true,
    reviewedProspectHotelEnabled: isReviewedProspectHotelEnabled(hotelId),
    candidateCount: selected.length,
    qualifyingCount: qualifying.length,
    heldOrExcludedCount: held.length,
    publishedIds: [],
    upserts: [],
    promotions: [],
    skipped: [],
    beforeSnapshotPath: null,
    rollbackManifestPath: null,
    customerShareLocal: null,
    customerShareLive: null,
    status: "NOT_RUN",
    fullE2eComplete: false,
  };

  if (!authorizePublication) {
    stage.status = "GATED_AWAITING_AUTHORIZE_PUBLICATION";
    stage.wouldPublish = qualifying.map((q) => ({
      id: q.opportunity?.id || q.candidateId,
      organizationName: q.organizationName,
      label: q.publicationAuditLabel,
    }));
    stage.held = held.map((h) => ({
      organizationName: h.organizationName,
      label: h.publicationAuditLabel,
      hotelFacing: h.hotelFacingEligible,
    }));
    stage.note =
      "Publication not authorized. Research/qualification may still complete. Independent -AuthorizePublication required.";
    return stage;
  }

  if (!isReviewedProspectHotelEnabled(hotelId)) {
    stage.status = "HOTEL_NOT_ON_REVIEWED_PROSPECT_ALLOWLIST";
    stage.note =
      "Hotel must be enabled for REVIEWED_PACKAGE_V1 (GDI_REVIEWED_PROSPECT_HOTEL_IDS or built-in allowlist).";
    return stage;
  }

  // Fail-closed: live production share claims require the hotel on the explicit
  // env allowlist (Railway). Built-in local fallback alone is not enough.
  const requireExplicit =
    probeLiveShare === true &&
    (applyProductionWrites === true || authorizeProductionWrites === true);
  const listPreflight = evaluateReviewedProspectProductionListPreflight(hotelId, {
    requireExplicitEnvAllowlist: requireExplicit,
    targetEnvironmentLabel: requireExplicit ? "production (Railway)" : "current",
  });
  stage.reviewedProspectListPreflight = listPreflight;
  if (requireExplicit && listPreflight.ok !== true) {
    stage.status = "BLOCKED_PRODUCTION_REVIEWED_PROSPECT_ALLOWLIST";
    stage.fullE2eComplete = false;
    stage.productionConfigBlocker = {
      code: listPreflight.blockerCode,
      requiredAction: listPreflight.requiredAction,
      hotelId,
    };
    stage.note =
      `Cannot claim customer-facing publication: ${listPreflight.blockerCode}. ` +
      `${listPreflight.requiredAction}`;
    return stage;
  }

  if (qualifying.length === 0) {
    stage.status = "ZERO_QUALIFYING_RECORDS";
    stage.publishedIds = [];
    stage.note = "No HOLD/EXCLUDE-free hotel-facing records — publish zero.";
    stage.fullE2eComplete = true; // complete sequence with zero publishes is valid
    return stage;
  }

  // Immutable before snapshot
  const beforeDoc = await loadOpportunitiesCanonical(hotelId, {
    bypassCache: true,
  });
  if (reportDir) {
    stage.beforeSnapshotPath = path.join(
      reportDir,
      "14-publication-before-snapshot.json",
    );
    writeJson(stage.beforeSnapshotPath, {
      at: new Date().toISOString(),
      hotelId,
      opportunityCount: (beforeDoc?.opportunities || []).length,
      opportunityIds: (beforeDoc?.opportunities || [])
        .map((o) => o.id || o.opportunityId)
        .filter(Boolean),
    });
  }

  for (const row of qualifying) {
    const segmentStamp = resolvePublicationSegmentStamp(
      row.opportunity,
      segmentOpts,
    );
    // Last choke point before canonical/Airtable publish: hotel-sales narrative
    // + secondary usable-route gate (hotel-generic; no Waterstone shortcuts).
    // Association records keep Association gold briefs — do not force Scaling CIB.
    let salesReady;
    if (segmentStamp.preserveAssociationIdentity) {
      salesReady = {
        ...row.opportunity,
        hotelId,
        researchSegment: segmentStamp.researchSegment,
        segment: segmentStamp.segment,
      };
    } else {
      salesReady = applyScalingTwoContactStandard(
        applyScalingSalesPresentation(
          {
            ...row.opportunity,
            hotelId,
            researchSegment: segmentStamp.researchSegment,
            segment: segmentStamp.segment,
          },
          {
            hotelDisplayName: row.opportunity?.hotelDisplayName || null,
            applyCommercialIntelligenceBrief: true,
          },
        ),
        { force: true },
      );
    }
    const opp = {
      ...salesReady,
      hotelId,
      researchSegment: segmentStamp.researchSegment,
      segment: segmentStamp.segment || salesReady.segment,
    };
    // Final choke: Scaling uses scaling publication qualification; Association
    // uses associationEvents qualification and must not be forced through Scaling.
    let finalQual;
    if (segmentStamp.preserveAssociationIdentity) {
      const assocQual =
        opp.associationEventsQualification ||
        opp.associationEventsGoldBrief?.qualification ||
        null;
      finalQual = {
        publicationQualified: assocQual?.qualified === true,
        status: assocQual?.status || "ASSOCIATION_NOT_QUALIFIED",
        gaps: assocQual?.reasons || [],
        researchContinuation: assocQual?.researchContinuation === true,
        segmentFamily: "associations_events",
      };
      if (!finalQual.publicationQualified) {
        stage.skipped.push({
          id: opp.id,
          organizationName: opp.organizationName,
          reason: `association_qualification_${finalQual.status}`,
          gaps: finalQual.gaps,
          researchContinuation: true,
          preserveOpportunity: true,
          researchSegmentPreserved: opp.researchSegment,
        });
        continue;
      }
    } else {
      finalQual = evaluateScalingPublicationQualification(opp);
      if (!finalQual.publicationQualified) {
        stage.skipped.push({
          id: opp.id,
          organizationName: opp.organizationName,
          reason: `scaling_qualification_${finalQual.status}`,
          gaps: finalQual.gaps,
          researchContinuation: true,
          preserveOpportunity: true,
        });
        stage.researchContinuation = stage.researchContinuation || [];
        stage.researchContinuation.push(
          enrichResearchContinuationWithBuyerStrategy({
            id: opp.id,
            organizationName: opp.organizationName,
            status: finalQual.status,
            gaps: finalQual.gaps,
            buyerResearchAttempts: opp.buyerResearchAttempts || [],
            preserveOpportunity: true,
            doNotSilentWithdraw: true,
          }),
        );
        continue;
      }
    }
    const promo = authorizeProspectPublicationPromotion(opp, {
      authorizePublication: true,
      authorizedBy,
      reason: segmentStamp.preserveAssociationIdentity
        ? "association_events_full_e2e_publication"
        : "scaling_companies_full_e2e_publication",
      hotelId,
    });
    stage.promotions.push({
      id: opp.id,
      organizationName: opp.organizationName,
      ok: promo.ok === true,
      reason: promo.reason || null,
      scalingQualification: finalQual.status,
      researchSegment: opp.researchSegment,
    });
    if (!promo.ok) {
      stage.skipped.push({
        id: opp.id,
        organizationName: opp.organizationName,
        reason: promo.reason || "promotion_refused",
      });
      continue;
    }
    const patched = {
      ...promo.patched,
      prospectPublicationBasis: REVIEWED_PROSPECT_PUBLICATION_BASIS,
      prospectHotelFacing: true,
      customerVisible: true,
      customerFacingState: "CUSTOMER_VISIBLE",
    };

    const persist = await persistPromotedOpportunityCanonical({
      hotelId,
      patched,
      outDir: reportDir,
      authorizePublication: true,
    });
    if (!persist.ok) {
      stage.skipped.push({
        id: patched.id,
        organizationName: patched.organizationName,
        reason: persist.blockingGate || persist.status || "persist_failed",
      });
      continue;
    }

    let upsert = { attempted: false, applied: false };
    if (authorizeProductionWrites && applyProductionWrites) {
      try {
        const result = await upsertOpportunity(patched, { hotelId });
        upsert = {
          attempted: true,
          applied: true,
          created: result.created === true,
          airtableRecordId: result.airtableRecordId || null,
        };
      } catch (err) {
        upsert = {
          attempted: true,
          applied: false,
          error: err.message,
          code: err.code || null,
        };
        stage.skipped.push({
          id: patched.id,
          organizationName: patched.organizationName,
          reason: `airtable_upsert_failed:${err.message}`,
        });
        continue;
      }
    } else {
      upsert = {
        attempted: false,
        applied: false,
        note: "Airtable upsert gated — set applyProductionWrites with authorizeProductionWrites",
      };
    }

    stage.upserts.push({
      id: patched.id,
      organizationName: patched.organizationName,
      upsert,
      persistStatus: persist.status,
    });
    if (upsert.applied || persist.ok) {
      stage.publishedIds.push(patched.id);
    }
  }

  stage.customerShareLocal = await verifyCustomerShareLocal({
    hotelId,
    publishedIds: stage.publishedIds,
  });
  stage.customerShareLive = await verifyCustomerShareLive({
    hotelId,
    publishedIds: stage.publishedIds,
    probeLive: probeLiveShare === true && stage.publishedIds.length > 0,
  });

  if (reportDir) {
    stage.rollbackManifestPath = path.join(
      reportDir,
      "15-publication-rollback-manifest.json",
    );
    writeJson(stage.rollbackManifestPath, {
      at: new Date().toISOString(),
      hotelId,
      beforeSnapshotPath: stage.beforeSnapshotPath,
      publishedIds: stage.publishedIds,
      rollbackNote:
        "Restore customerVisible/prospectHotelFacing from before snapshot or withdraw via prospectPublicationWithdrawn.",
      upserts: stage.upserts,
    });
    writeJson(path.join(reportDir, "16-customer-share-verification.json"), {
      local: stage.customerShareLocal,
      live: stage.customerShareLive,
    });
  }

  const airtableApplied = stage.upserts.some((u) => u.upsert?.applied);
  const localOk =
    stage.publishedIds.length === 0 ||
    stage.customerShareLocal?.allFacing === true;
  const briefPersisted =
    stage.publishedIds.length === 0 ||
    stage.customerShareLocal?.commercialBriefPersisted === true;
  const liveOk =
    !probeLiveShare ||
    stage.customerShareLive?.provesExternalShare === true ||
    stage.publishedIds.length === 0;

  if (!briefPersisted && stage.publishedIds.length > 0) {
    stage.commercialBriefGap = {
      code: "COMMERCIAL_INTELLIGENCE_BRIEF_MISSING_AFTER_PERSIST",
      note: "Published scaling rows lack substantive salesBrief in canonical payload — incomplete publication.",
      details: stage.customerShareLocal?.details || [],
    };
  }

  stage.status =
    stage.publishedIds.length === 0 && qualifying.length > 0
      ? "PUBLICATION_ATTEMPTED_NONE_APPLIED"
      : airtableApplied
        ? localOk && liveOk && briefPersisted
          ? "PUBLISHED_AND_VERIFIED"
          : briefPersisted
            ? "PUBLISHED_AWAITING_SHARE_PROOF"
            : "PUBLISHED_INCOMPLETE_COMMERCIAL_BRIEF"
        : "CANONICAL_PROMOTED_AIRTABLE_GATED";

  // Full E2E COMPLETE only when publication authorized AND
  // (zero qualifying OR local facing verified) AND brief survived AND
  // (live probe not required OR proved).
  stage.fullE2eComplete =
    authorizePublication &&
    (qualifying.length === 0 ||
      (stage.publishedIds.length > 0 && localOk && briefPersisted)) &&
    liveOk &&
    stage.status !== "PUBLICATION_ATTEMPTED_NONE_APPLIED" &&
    stage.status !== "PUBLISHED_INCOMPLETE_COMMERCIAL_BRIEF";

  if (airtableApplied && !stage.customerShareLive?.provesExternalShare) {
    stage.fullE2eComplete = false;
    stage.note =
      (stage.note ? `${stage.note} ` : "") +
      "Airtable write is not proof of external customer share visibility.";

    const checks = stage.customerShareLive?.detailChecks || [];
    const detailOkNotInList =
      checks.length > 0 &&
      checks.every((d) => d.detailOk === true && d.inList !== true);
    if (detailOkNotInList) {
      stage.productionConfigBlocker = {
        code: "REVIEWED_PROSPECT_HOTEL_NOT_ON_PRODUCTION_ALLOWLIST",
        hotelId,
        requiredAction: `Append ${hotelId} to Railway GDI_REVIEWED_PROSPECT_HOTEL_IDS while preserving every existing hotel ID; keep GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1; redeploy/restart; re-probe share list.`,
        evidence:
          "Production detail endpoints return 200 but opportunity IDs are absent from the share list — classic reviewed-prospect allowlist miss.",
      };
      stage.status = "PUBLISHED_AWAITING_SHARE_PROOF";
      stage.note +=
        " Production list omission with detail-OK strongly indicates Railway GDI_REVIEWED_PROSPECT_HOTEL_IDS excludes this hotel.";
    }
  }

  return stage;
}
