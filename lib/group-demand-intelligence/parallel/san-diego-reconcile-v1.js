/**
 * Dry-run San Diego reconciliation: lab/fixture candidates vs authoritative baseline.
 * Does not write opportunities. Novelty UNKNOWN when baseline unreadable.
 */

import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import {
  evaluateProspectPublicationEligibility,
} from "../prospect-publication-eligibility-v1.js";
import { isCustomerFacingOpportunity } from "../customer-visibility.js";
import { normalizeOrgIdentityKey } from "../workflow/reuse-prior-intelligence-v1.js";
import { assessIndependentHotelFit } from "../workflow/independent-hotel-fit-v1.js";
import { loadSanDiegoFixtureCards, SAN_DIEGO_HOTEL_ID } from "./fixture-pack-v1.js";
import { mapLabCardToDiscoveryCandidate } from "../workflow/parallel-discovery-adapter-v1.js";
import {
  dedupeAccountTripCandidates,
} from "./identity-dedupe-v1.js";
import {
  evaluateContactChannels,
  buildSurfeFallbackQueue,
  ROUTE_CLASS,
} from "./contact-channel-policy-v1.js";
import { attachClaimEvidence } from "./claim-evidence-v1.js";
import { isGdiParallelHotelAllowed } from "./feature-flag.js";

export const SAN_DIEGO_RECONCILE_VERSION = "gdi-san-diego-reconcile-v1";

const LOCAL_RECOVERY_ORGS = ["sol-ark", "spargo", "esri"];

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function orgMatch(a, b) {
  const na = normalizeOrgIdentityKey(a);
  const nb = normalizeOrgIdentityKey(b);
  if (!na || !nb) return false;
  return na === nb || na.includes(nb) || nb.includes(na);
}

/**
 * @param {object} opts
 * @param {boolean} [opts.skipBaseline] — force novelty UNKNOWN
 */
export async function reconcileSanDiegoReviewSet(opts = {}) {
  const hotelId = opts.hotelId || SAN_DIEGO_HOTEL_ID;
  const pack = loadSanDiegoFixtureCards(opts);
  const flagAllowed = isGdiParallelHotelAllowed(hotelId, opts.env || process.env);

  let baselineOps = [];
  let baselineReadable = false;
  let baselineError = null;
  if (!opts.skipBaseline) {
    try {
      const doc = await loadOpportunitiesCanonical(hotelId);
      baselineOps = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      baselineReadable = true;
    } catch (err) {
      baselineError = String(err?.message || err);
      baselineReadable = false;
    }
  }

  const noveltyPolicy = baselineReadable
    ? "compare_against_baseline"
    : "UNKNOWN_baseline_unreadable_block_auto_publication";

  const mapped = (pack.cards || []).map((card) => {
    let cand = mapLabCardToDiscoveryCandidate(card, hotelId);
    cand = attachClaimEvidence(cand, card.claimBuckets || {}, {
      evidenceLinks: card.evidenceLinks,
      retrievedAt: card.completedAt || pack.reviewPackage?.completedAt,
    });
    cand.travelingCohort = card.travelingCohort || null;
    cand.dedupeKey = card.dedupeKey || null;
    cand.labPriority = card.priority || null;
    cand.contact = card.contact || {};
    const channels = evaluateContactChannels(card.contact || {}, {
      organizationName: card.account,
    });
    cand.contactChannelEvaluation = channels;
    cand.independentHotelFit = assessIndependentHotelFit(cand, hotelId);
    const elig = evaluateProspectPublicationEligibility({
      ...cand,
      primaryContact: cand.primaryContact,
      organizationName: cand.organizationName,
      prospectHotelFacing: false,
    });
    cand.gdiEligibility = elig;
    cand.gdiClassifierResult = "RAN_ELIGIBILITY_ONLY_NOT_FULL_ORCHESTRATOR";
    cand.publicationEligible = false;
    cand.publicationBlockReasons = [
      "dry_run_preview_only",
      ...(baselineReadable ? [] : ["baseline_novelty_UNKNOWN"]),
      "lab_acceptance_is_not_customer_publication",
    ];
    return cand;
  });

  const deduped = dedupeAccountTripCandidates(mapped);

  const existingMatches = [];
  const proposedNew = [];
  const proposedEnrichments = [];

  for (const cand of deduped.unique) {
    const match = baselineOps.find((o) =>
      orgMatch(o.organizationName || o.company, cand.organizationName)
    );
    if (match) {
      const row = {
        candidateId: cand.candidateId,
        organizationName: cand.organizationName,
        existingOpportunityId: match.id || match.opportunityId,
        disposition: "existing_match",
        proposedAction: "enrich_if_stronger_evidence_only",
        overwriteStrongerEvidence: false,
      };
      existingMatches.push(row);
      proposedEnrichments.push(row);
    } else {
      proposedNew.push({
        candidateId: cand.candidateId,
        organizationName: cand.organizationName,
        disposition: baselineReadable ? "proposed_new_prospect" : "proposed_new_novelty_UNKNOWN",
        labPriority: cand.labPriority,
        routeClass: cand.contactChannelEvaluation?.routeClass,
        fitOutcome: cand.independentHotelFit?.outcome,
      });
    }
  }

  const contactBuckets = {
    email_or_phone: 0,
    routing_only: 0,
    neither_or_quarantined: 0,
  };
  const evaluatedForSurfe = [];
  for (const cand of deduped.unique) {
    const ch = cand.contactChannelEvaluation;
    if (!ch) continue;
    if (ch.hasUsableChannel) contactBuckets.email_or_phone += 1;
    else if (ch.routeClass === ROUTE_CLASS.ROUTING_ONLY) contactBuckets.routing_only += 1;
    else contactBuckets.neither_or_quarantined += 1;
    evaluatedForSurfe.push({
      ...ch,
      personName: cand.contact?.name,
      organizationName: cand.organizationName,
      cardId: cand.candidateId,
      contact: cand.contact,
    });
  }

  const unsupportedQuarantined = deduped.unique
    .filter(
      (c) =>
        c.contactChannelEvaluation?.routeClass === ROUTE_CLASS.QUARANTINED ||
        (c.contactChannelEvaluation?.quarantinedEmails || []).length
    )
    .map((c) => ({
      candidateId: c.candidateId,
      organizationName: c.organizationName,
      quarantined: c.contactChannelEvaluation.quarantinedEmails,
      routeClass: c.contactChannelEvaluation.routeClass,
    }));

  const localRecovery = LOCAL_RECOVERY_ORGS.map((key) => {
    const cand = deduped.unique.find((c) =>
      normalizeOrgIdentityKey(c.organizationName).includes(key.replace("-", " "))
    );
    if (!cand) {
      return {
        orgKey: key,
        found: false,
        plan: "identity_research_task",
        surfe: "not_queued_missing_card",
      };
    }
    const ch = cand.contactChannelEvaluation;
    const plan = ch?.surfeEligible
      ? "queue_surfe_after_local_if_authorized"
      : ch?.hasUsableChannel
        ? "local_channel_present_verify_variants_only"
        : "local_first_identity_research_task";
    return {
      orgKey: key,
      found: true,
      candidateId: cand.candidateId,
      organizationName: cand.organizationName,
      routeClass: ch?.routeClass,
      hasUsableChannel: ch?.hasUsableChannel,
      surfeEligible: ch?.surfeEligible === true,
      surfeSkipReason: ch?.surfeSkipReason,
      plan,
      estimatedSurfeEmailCredits: ch?.surfeEligible ? 1 : 0,
    };
  });

  const surfeQueue = buildSurfeFallbackQueue(evaluatedForSurfe);

  const customerVisibleProposed = [];
  const internalOnly = deduped.unique.map((c) => ({
    candidateId: c.candidateId,
    organizationName: c.organizationName,
    reason: "preview_internal_only_pending_explicit_publication_approval",
    eligibility: c.gdiEligibility?.eligibility,
    hotelFacing: c.gdiEligibility?.hotelFacing === true,
  }));

  // Existing baseline facing (if any) — do not disturb
  const baselineFacing = baselineOps.filter((o) =>
    isCustomerFacingOpportunity(o, { hotelId })
  );

  return {
    version: SAN_DIEGO_RECONCILE_VERSION,
    asOf: new Date().toISOString(),
    hotelId,
    parallelFlagAllowed: flagAllowed,
    fixturePack: {
      available: pack.available,
      root: pack.root,
      cardCount: (pack.cards || []).length,
      reviewPackageAccepted: pack.reviewPackage?.counts?.acceptedProspects ?? null,
      labBaselineNovelty: pack.reviewPackage?.baselineNovelty || "UNKNOWN",
    },
    baseline: {
      readable: baselineReadable,
      error: baselineError,
      opportunityCount: baselineOps.length,
      customerFacingCount: baselineFacing.length,
      noveltyPolicy,
      autoPublicationBlocked: !baselineReadable,
    },
    reconciliation: {
      fixtureCandidates: mapped.length,
      afterAccountTripDedupe: deduped.unique.length,
      duplicateCount: deduped.duplicateCount,
      existingMatches: existingMatches.length,
      proposedNew: proposedNew.length,
      proposedEnrichments: proposedEnrichments.length,
      sharedHousingPathways: deduped.sharedHousingPathways,
      contactBuckets,
      unsupportedQuarantined: unsupportedQuarantined.length,
      customerVisibleProposed: customerVisibleProposed.length,
      internalOnlyResearch: internalOnly.length,
      note: "Do not force 15 publishable cards. Lab acceptance ≠ GDI publication.",
    },
    existingMatches,
    proposedNew,
    proposedEnrichments,
    unsupportedQuarantined,
    localRecovery,
    surfeFallbackQueue: surfeQueue,
    sharedHousingPathways: deduped.sharedHousingPathways,
    customerVisibleProposed,
    internalOnly,
    gdiClassifierNote:
      "Eligibility helper ran per candidate; full research-orchestrator qualify not claimed as PASS",
    confirmedOpenDemand: 0,
    confirmedOpenDemandNote:
      "Experiments established no confirmed open demand — sourcing remains unconfirmed",
  };
}
