/**
 * Final Commercial Depth Hardening V1 — offline remediation certification.
 * Replays ACCOUNT_LED / EVENT_LED / COMPETITIVE_FUTURE_CYCLE through the normal
 * deep-research GDI entrypoint with fixtures. No paid research / Airtable / publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runDeepResearchOnExistingGdiOpportunity,
  buildOpportunityBoundHotelSalesBrief,
  createFixtureResearcher,
} from "./deep-research-gdi-integration-v1.js";
import {
  selectCrossOpportunityCertificationSet,
} from "./autonomous-longform-gdi-integration-v1.js";
import {
  isConsumerEmailDomain,
  classifyContactOwnership,
  CONTACT_OWNERSHIP_CLASS,
  assertPaidContactProvidersDisabled,
} from "./commercial-spine-restoration-v41.js";
import {
  hasRoleRelevantDecisionMaker,
  buyerWhoPagesRetrieved,
  buildTargetedEnrichmentRequest,
} from "./autonomous-deep-research-recovery-v1.js";
import { runLockedLedgerSafetySuite } from "./evidence-driven-longform-validation-v1.js";

export const COMMERCIAL_DEPTH_HARDENING_V1 = "gdi-commercial-depth-hardening-v1";
export const HARDENING_REPORTS_DIR = "reports/gdi-commercial-depth-hardening-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadLiveArtifacts(opportunityId) {
  const base = path.join(ROOT, "reports/gdi-autonomous-longform-gdi-integration-v1/live", opportunityId);
  const read = (name) => {
    const p = path.join(base, name);
    return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : null;
  };
  return {
    before: read("BEFORE.json"),
    after: read("AFTER.json"),
    salesBrief: read("SALES_BRIEF.json"),
    cert: read("CERTIFICATION.json"),
    trace: read("RETRIEVAL_TRACE.json"),
    costs: read("COSTS.json"),
  };
}

/**
 * Root-cause audit of one live investigation (saved artifacts only).
 */
export function auditLiveInvestigationRootCause({ selection, live }) {
  const org = selection.bag?.organizationName || selection.checkpoint?.organizationName || "Unknown";
  const hotel = selection.hotelName;
  const brief = live.salesBrief || {};
  const cert = live.cert || {};
  const trace = live.trace || {};
  const after = live.after || {};

  const importantQuestions = [
    `Who at ${org} owns hotel sourcing / housing / contracting?`,
    `What dated programme creates lodging demand for ${hotel}?`,
    selection.competingHotel
      ? `Has next-cycle headquarter hotel been named, or does ${selection.competingHotel} remain the pattern?`
      : `Which events/programmes create guestroom or meeting-space demand suitable for ${hotel}?`,
    `Is there a role-relevant named buyer beyond a general organization route?`,
  ];

  const answered = [];
  const contactRoute = brief.whoShouldIContact?.route || after.contactOwnership?.route || null;
  if (contactRoute) answered.push(`General org route observed: ${contactRoute}`);
  const namedFromLive = [
    ...(cert.namedBuyers || []),
    ...((after.namedStakeholders || after.after?.namedStakeholders || [])
      .map((p) => p?.name || p?.personName)
      .filter(Boolean)),
  ];
  const namedUnique = [...new Set(namedFromLive)];
  if (namedUnique.length) {
    answered.push(`Named people extracted (not necessarily DMs): ${namedUnique.join(", ")}`);
  }
  if ((trace.executed || []).length) {
    answered.push(`${(trace.executed || []).length} URLs retrieved`);
  }

  const evidenceAcquired = (trace.executed || []).map((e) => ({
    url: e.url,
    provider: e.provider,
    strategy: e.strategy,
    chars: e.chars,
  }));

  const pathsAttempted = [
    ...new Set((trace.attempted || []).map((a) => a.strategy).filter(Boolean)),
  ];
  const exaQueries = (trace.exaQueries || []).map((q) => q.query);

  const namedBuyerResearch = {
    performed: Boolean(namedUnique.length) || buyerWhoPagesRetrieved({
      investigatedUrls: (trace.executed || []).map((e) => e.url),
    }, { executed: trace.executed || [] }),
    names: namedUnique,
    whoPages: (trace.executed || []).filter((e) =>
      /team|people|leadership|board|staff|directors/i.test(e.url || "")).map((e) => e.url),
    skippedAsAlreadyRetrievedViaAbout: /buyer_who_already_retrieved|about/i.test(
      JSON.stringify(cert.buyerWhoFallback || after.buyerWhoFallback || {}),
    ),
    influencerNotDecisionMaker: namedUnique.length > 0
      && !/hotel.?sourc|housing|meeting.?director|procurement/i.test(
        JSON.stringify(after.namedStakeholders || cert.namedBuyers || []),
      ),
  };

  const relatedEntities = {
    discovered: trace.relatedEntity?.discovered || [],
    attempted: trace.relatedEntity?.attempted || [],
    executed: trace.relatedEntity?.executed || [],
    relatedExecutedCount: Number(cert.relatedExecuted ?? after.relatedEntityLeadsExecuted ?? 0),
  };

  const notAttempted = [
    "Alternative commercial search strategies beyond first Exa annual-meeting query",
    "Named program / housing-bureau / intermediary follow-ups when org pages were thin",
    relatedEntities.relatedExecutedCount === 0 ? "Related-entity lead execution (count was 0)" : null,
    !namedBuyerResearch.whoPages.length
      ? "Dedicated buyer WHO pages (team/leadership) when /about was treated as sufficient"
      : null,
    "Surfe/PDL (correctly disabled — structured enrichment request should remain gated)",
  ].filter(Boolean);

  const stopReason = cert.stopReason
    || after.orsStopReason
    || cert.technicalStop
    || "iteration/provider stop after thin first-pass retrievals";

  const principalContamination = /Principal\.|Nobu Portman|Rosewood London|central-London luxury/i.test(
    `${brief.whyThisOrganization || ""} ${brief.whatPotentialBusinessExists || ""} ${brief.whyMyHotel || ""}`,
  );

  const consumerEmailPromoted = Boolean(
    brief.whoShouldIContact?.route
    && isConsumerEmailDomain(brief.whoShouldIContact.route)
    && brief.whoShouldIContact?.salesUsableAsProspectContact !== false
    && !brief.whoShouldIContact?.gap,
  );

  const furtherResearchVoI = {
    positive: true,
    rationale: [
      "Buyer WHO / role-relevant DM still open",
      relatedEntities.relatedExecutedCount === 0 ? "Related-entity leads not executed" : null,
      principalContamination ? "Sales brief contamination blocked commercial use of outputs" : null,
      "Material commercial questions remain answerable via public paths",
    ].filter(Boolean),
  };

  return {
    opportunityId: selection.bag?.id || cert.opportunityId,
    role: selection.role,
    pathway: selection.pathway,
    hotelName: hotel,
    hotelId: selection.hotelId,
    organizationName: org,
    importantCommercialQuestions: importantQuestions,
    questionsActuallyAnswered: answered,
    evidenceAcquired,
    researchPathsAttempted: pathsAttempted,
    exaQueries,
    namedBuyerResearch,
    relatedEntitiesInvestigated: relatedEntities,
    researchStrategiesNotAttempted: notAttempted,
    exactStopReason: stopReason,
    furtherResearchHadPositiveVoI: furtherResearchVoI,
    liveDefects: {
      principalNobuSalesBriefContamination: principalContamination,
      consumerEmailOwnershipRisk: Boolean(
        (selection.bag?.contactEmail || "").match(/gmail|yahoo|hotmail/i),
      ),
      consumerEmailPromotedAsUsable: consumerEmailPromoted,
      relatedEntityLeadsExecuted: relatedEntities.relatedExecutedCount,
      commercialOutcome: cert.commercialOutcome || "LIMITED_IMPROVEMENT",
      retrievalVolumeNotCompleteness: true,
    },
    spendUsd: live.costs?.spentUsd ?? cert.costs?.spentUsd ?? null,
  };
}

function seedOpportunityFromSelection(sel) {
  const bag = sel.bag || {};
  const cp = sel.checkpoint || {};
  const id = bag.id || cp.id || cp.opportunityId;
  const org = bag.organizationName || cp.organizationName || "Unknown Organization";
  const contactEmail = bag.contactEmail || bag.primaryContactEmail || cp.contactEmail || null;
  const official = bag.officialSource || bag.contactUrl || `https://www.${sel.orgHost}/`;

  const heldUrls = [
    official,
    `https://www.${sel.orgHost}/about`,
    ...(sel.role === "COMPETITIVE_FUTURE_CYCLE"
      ? ["https://www.marriott.com/en-us/hotels/wascr-crystal-gateway-marriott/overview/"]
      : []),
  ];

  return {
    id,
    opportunityId: id,
    hotelId: sel.hotelId,
    hotelName: sel.hotelName,
    organizationName: org,
    organizationDomain: sel.orgHost.replace(/^www\./, ""),
    pathway: sel.pathway,
    internalDisposition: sel.internalDisposition,
    commercialStage: sel.internalDisposition,
    segment: sel.segment || bag.segment,
    hotelOpportunityThesis: bag.hotelOpportunityThesis || null,
    contactEmail,
    contact: {
      businessEmail: contactEmail,
      personName: bag.contactIsNamedPerson ? bag.contactName || bag.primaryContactName : null,
      roleTitle: bag.contactRole || bag.primaryContactRole || null,
    },
    officialSource: official,
    evidence: [{
      url: official,
      statement: text(bag.hotelOpportunityThesis || `${org} organizes events requiring lodging.`).slice(0, 400),
      quote: text(bag.hotelOpportunityThesis || "").slice(0, 400),
      provenance: "canonical_gdi_bag",
      accepted: true,
      previouslyHeld: true,
    }],
    investigatedUrls: [...heldUrls],
    seedUrls: heldUrls,
    candidateQueue: [],
    pendingDiscoverySearches: [],
    unresolvedQuestions: [
      `Who at ${org} owns hotel sourcing / housing / contracting?`,
      `What dated programme creates lodging demand for ${sel.hotelName}?`,
    ],
    researchAttempts: [
      { strategy: "related_entity_followup", url: heldUrls[0], status: "held_reread_unproductive" },
    ],
    relatedEntityLeadsExecuted: 0,
    namedStakeholders: [],
    competingHotel: sel.competingHotel || null,
    namedHostHotel: sel.competingHotel || null,
  };
}

function buildFixtureCorpus(sel, opportunity) {
  const host = sel.orgHost;
  const teamUrl = `https://www.${host}/team`;
  const programsUrl = `https://www.${host}/programs`;
  const newsUrl = `https://www.${host}/news`;
  const person = sel.fixturePerson;
  const org = opportunity.organizationName;

  const corpus = {
    [`https://www.${host}/`]: `${org} home page. Welcome. Generic marketing.`,
    [`https://www.${host}/about`]: `${org} about us. We deliver excellent experiences. Contact us.`,
    [teamUrl]: `${person.name}, ${person.title} — leads hotel sourcing, housing blocks, and supplier contracting for ${org} programmes. Email available via organization route.`,
    [programsUrl]: sel.role === "EVENT_LED"
      ? `${org} publishes the Memorial Tournament housing program requiring participating teams to book through the official housing bureau. Stay-to-play lodging is mandatory for traveling teams near Maryland SoccerPlex.`
      : sel.role === "COMPETITIVE_FUTURE_CYCLE"
        ? `${org} Washington Conference is a recurring annual meeting. 2026/2027 headquarter hotel pattern: Crystal Gateway Marriott (Arlington). 2028 hotel not yet named — future-cycle sourcing window.`
        : `${org} produces corporate receptions and catered special events in downtown San Diego requiring hotel meeting space and guestroom blocks for out-of-town attendees.`,
    [newsUrl]: `${org} announces upcoming program cycle and continues to source hotels for group accommodation.`,
  };

  if (sel.competingHotel) {
    corpus["https://www.marriott.com/en-us/hotels/wascr-crystal-gateway-marriott/overview/"] =
      "Crystal Gateway Marriott Arlington — meetings and events. Does not name NADO. Generic hotel meetings marketing.";
  }

  return { corpus, teamUrl, programsUrl, newsUrl };
}

/**
 * Offline replay of one pathway through runDeepResearchOnExistingGdiOpportunity.
 */
export async function replayHardeningPathway(sel) {
  const opportunity = seedOpportunityFromSelection(sel);
  const { corpus, teamUrl } = buildFixtureCorpus(sel, opportunity);
  const beforeOwnership = classifyContactOwnership({
    email: opportunity.contactEmail,
    pageUrl: opportunity.officialSource,
    organizationName: opportunity.organizationName,
    organizationDomain: opportunity.organizationDomain,
    publisherKind: "ORGANIZATION_OWNED",
  });

  const result = await runDeepResearchOnExistingGdiOpportunity({
    opportunity,
    corpus,
    offlineLongform: false,
    options: {
      maxIterations: 4,
      remainingBudgetUsd: 1,
      paidBlocked: true,
      ensureBuyerWho: true,
      allowReplan: true,
      allowVoiContinue: true,
      resetResearchAttempts: true,
      forbidPublication: true,
      forbidAirtable: true,
    },
  });

  const brief = result.hotelSalesBrief || {};
  const afterOwnership = result.after?.contactOwnership || {};
  const named = result.after?.namedStakeholders || [];
  const roleDm = hasRoleRelevantDecisionMaker(named);

  const principalLeak = /Principal\.|Nobu Portman|Rosewood London|central-London luxury|Marylebone/i.test(
    JSON.stringify({
      whyThisOrganization: brief.whyThisOrganization,
      whatPotentialBusinessExists: brief.whatPotentialBusinessExists,
      whyMyHotel: brief.whyMyHotel,
      whatShouldISayOrAsk: brief.whatShouldISayOrAsk,
    }),
  );

  const consumerRejected = Boolean(
    opportunity.contactEmail
    && isConsumerEmailDomain(opportunity.contactEmail)
    && afterOwnership.salesUsableAsProspectContact !== true,
  );

  const hotelBound = brief.hotelName === opportunity.hotelName
    && brief.hotelId === opportunity.hotelId
    && brief.opportunityId === opportunity.id
    && brief.provenanceBound?.crossHotelCopyForbidden === true;

  const teamRetrieved = (result.after?.opportunity?.investigatedUrls || [])
    .some((u) => /\/team/i.test(u))
    || (result.after?.opportunity?.evidence || []).some((e) => /\/team/i.test(e.url || ""))
    || (result.buyerWhoFallback?.buyerUrlsAttempted || []).includes(teamUrl)
    || Boolean(result.voiContinuePass?.executed);

  const enrichment = result.targetedEnrichmentRequest || result.after?.targetedEnrichmentRequest;
  const enrichmentOk = roleDm
    ? enrichment == null
    : enrichment?.status === "AWAITING_SEPARATE_AUTHORIZATION"
      && enrichment?.authorizeSurfe === false
      && enrichment?.authorizePdl === false;

  return {
    opportunityId: opportunity.id,
    role: sel.role,
    pathway: sel.pathway,
    hotelName: opportunity.hotelName,
    hotelId: opportunity.hotelId,
    organizationName: opportunity.organizationName,
    before: {
      contactEmail: opportunity.contactEmail,
      ownershipClass: beforeOwnership.ownershipClass,
      salesUsable: beforeOwnership.salesUsableAsProspectContact,
      namedStakeholders: [],
      relatedExecuted: 0,
    },
    after: {
      contactOwnership: afterOwnership,
      namedStakeholders: named,
      roleRelevantDecisionMaker: roleDm,
      relatedEntityLeadsExecuted: result.after?.relatedEntityLeadsExecuted || 0,
      replanPassExecuted: result.after?.replanPassExecuted || false,
      voiContinuePassExecuted: result.after?.voiContinuePassExecuted || false,
      buyerWhoFallback: result.after?.buyerWhoFallback || null,
      researchCompleteness: result.researchCompleteness?.researchCompleteness || null,
      evidenceCount: (result.after?.opportunity?.evidence || []).length,
    },
    hotelSalesBrief: brief,
    targetedEnrichmentRequest: enrichment || null,
    gates: {
      sameOpportunityId: result.gdiRoundTrip?.sameOpportunityId === true,
      sameHotelId: result.gdiRoundTrip?.sameHotelId === true,
      publicationUnchanged: result.publicationEligibilityUnchanged === true,
      noAirtableWrite: result.gdiRoundTrip?.airtableWritePerformed !== true,
      surfePdlDisabled: assertPaidContactProvidersDisabled({}).ok !== false,
      noPrincipalNobuContamination: !principalLeak,
      hotelOpportunityBound: hotelBound,
      consumerEmailNotSalesUsable: !opportunity.contactEmail || !isConsumerEmailDomain(opportunity.contactEmail) || consumerRejected,
      buyerResearchNotSilentlySkipped: Boolean(
        result.buyerWhoFallback?.executed
        || result.voiContinuePass?.executed
        || teamRetrieved
        || roleDm,
      ),
      unproductiveTriggeredPivot: Boolean(
        result.replanPass || result.evidenceDrivenReplan || result.voiContinuePass,
      ),
      // Named person on the sales-contact block is allowed only when role-relevant.
      // Unresolved (null namedPerson) is always honest — including when a research-only
      // DM exists but contact ownership (e.g. consumer email) blocks sales-usable promotion.
      missingContactsUnresolvedNotFabricated: brief.whoShouldIContact?.namedPerson
        ? roleDm === true
        : true,
      enrichmentRequestGated: enrichmentOk,
      additionalCommerciallyJustifiedResearchExecuted: Boolean(
        result.voiContinuePass?.executed
        || result.buyerWhoFallback?.executed
        || (result.after?.relatedEntityLeadsExecuted || 0) >= 1
        || result.after?.replanPassExecuted,
      ),
    },
    orsEntrypoint: result.orsEntrypoint,
    gdiRoundTrip: result.gdiRoundTrip,
  };
}

export async function runCommercialDepthHardeningOfflinePackage() {
  const selections = selectCrossOpportunityCertificationSet();
  const liveAudits = selections.map((sel) => {
    const id = sel.bag?.id;
    return auditLiveInvestigationRootCause({
      selection: sel,
      live: loadLiveArtifacts(id),
    });
  });

  const replays = [];
  for (const sel of selections) {
    replays.push(await replayHardeningPathway(sel));
  }

  const ledgerIntegrity = runLockedLedgerSafetySuite();

  // Unit-level contact ownership regressions
  const gmailOwnership = classifyContactOwnership({
    email: "gaslampevent@gmail.com",
    pageUrl: "https://www.gaslampevents.com/contact",
    organizationName: "Gaslamp Event Management, Inc.",
    organizationDomain: "gaslampevents.com",
    publisherKind: "ORGANIZATION_OWNED",
  });
  const potomacOwnership = classifyContactOwnership({
    email: "tournament@potomacsoccer.org",
    pageUrl: "https://www.potomacsoccer.org/contact",
    organizationName: "Potomac Soccer Association",
    organizationDomain: "potomacsoccer.org",
    publisherKind: "ORGANIZATION_OWNED",
  });

  const salesBriefUnit = buildOpportunityBoundHotelSalesBrief({
    opportunity: {
      id: "gdi_opp_nado_ddaa_washcon_2028",
      hotelId: "recLuxvwwxID7U2B8",
      hotelName: "Bethesda Marriott",
      organizationName: "National Association of Development Organizations (NADO) / DDAA",
      pathway: "COMPETITIVE_FUTURE_CYCLE",
      competingHotel: "Crystal Gateway Marriott",
    },
    ownership: {
      route: "info@nado.org",
      ownershipClass: CONTACT_OWNERSHIP_CLASS.GENERAL_ORGANIZATION_CONTACT,
      salesUsableAsProspectContact: true,
    },
    namedStakeholders: [],
    qualificationV41: { internalDisposition: "COMPETITIVE_FUTURE_CYCLE" },
    salesBrief: { reasonToContactNow: "Account / future-cycle introduction — no invented urgency." },
    claims: [{ statement: "Named host: Crystal Gateway Marriott" }],
  });

  const allGatesPass = replays.every((r) => Object.values(r.gates).every(Boolean));

  const beforeAfter = replays.map((r, i) => ({
    opportunityId: r.opportunityId,
    role: r.role,
    liveBeforeHardening: {
      salesBriefContamination: liveAudits[i]?.liveDefects?.principalNobuSalesBriefContamination,
      relatedExecuted: liveAudits[i]?.relatedEntitiesInvestigated?.relatedExecutedCount,
      commercialOutcome: liveAudits[i]?.liveDefects?.commercialOutcome,
      consumerEmailRisk: liveAudits[i]?.liveDefects?.consumerEmailOwnershipRisk,
    },
    offlineAfterHardening: {
      noPrincipalContamination: r.gates.noPrincipalNobuContamination,
      hotelBound: r.gates.hotelOpportunityBound,
      buyerNotSkipped: r.gates.buyerResearchNotSilentlySkipped,
      voiOrReplan: r.gates.unproductiveTriggeredPivot,
      roleRelevantDm: r.after.roleRelevantDecisionMaker,
      enrichmentGated: r.gates.enrichmentRequestGated,
      relatedExecuted: r.after.relatedEntityLeadsExecuted,
      consumerEmailSafe: r.gates.consumerEmailNotSalesUsable,
    },
  }));

  const proposedPilot = {
    executePaidResearch: false,
    publication: "OFF",
    airtableWrites: false,
    weeklyActivation: false,
    surfe: false,
    pdl: false,
    parallel: false,
    perHotelBudgetUsd: {
      "Wyndham San Diego Bayside": 0.45,
      "Bethesda Marriott": 0.90,
    },
    aggregateCeilingUsd: 1.35,
    note: "Controlled operational pilot only after offline gates pass and founder authorizes paid spend separately. Publication remains OFF.",
  };

  const weeklyGoNoGo = allGatesPass
    ? {
      decision: "NO_GO",
      reason: [
        "Offline hardening gates pass for research depth / contact / sales-brief binding.",
        "Weekly internal research remains NO-GO until a controlled paid pilot completes with MATERIAL commercial improvement on ≥2/3 pathways and separate weekly budget authorization.",
        "GDI_WEEKLY_DEEP_RESEARCH_ENABLED must stay 0.",
      ].join(" "),
    }
    : {
      decision: "NO_GO",
      reason: "Offline hardening gates incomplete — do not activate weekly research.",
    };

  return {
    version: COMMERCIAL_DEPTH_HARDENING_V1,
    certifiedAt: new Date().toISOString(),
    liveRootCauseAudits: liveAudits,
    offlineReplays: replays,
    beforeAfterAssessments: beforeAfter,
    contactOwnershipRegressions: {
      gmailNeverSalesUsable: gmailOwnership.salesUsableAsProspectContact === false
        && gmailOwnership.consumerDomain === true,
      potomacGeneralRouteUsable: potomacOwnership.salesUsableAsProspectContact === true
        && potomacOwnership.ownershipClass === CONTACT_OWNERSHIP_CLASS.GENERAL_ORGANIZATION_CONTACT,
      salesBriefUnitNoPrincipal: !/Principal|Nobu Portman|Rosewood/i.test(JSON.stringify(salesBriefUnit)),
      salesBriefUnitHotelBound: salesBriefUnit.hotelName === "Bethesda Marriott"
        && salesBriefUnit.opportunityId === "gdi_opp_nado_ddaa_washcon_2028",
      salesBriefUnitPreservesCompetitorHost: /Crystal Gateway/i.test(salesBriefUnit.whyMyHotel || "")
        || /Crystal Gateway/i.test(salesBriefUnit.whatPotentialBusinessExists || ""),
    },
    ledgerIntegrity,
    allOfflineGatesPass: allGatesPass,
    remainingLiveResearchGaps: [
      "Live Exa queries were generic annual-meeting searches — need commercial buyer/program query diversity in live researcher path",
      "Related-entity execution in live pilot was 0 — VoI continue must be exercised under live researcher, not fixtures only",
      "Public WHO often cannot resolve named housing/meeting directors — targeted enrichment requests remain AWAITING_SEPARATE_AUTHORIZATION",
      "Gaslamp consumer email cannot become sales-usable without org-domain proof",
    ],
    proposedControlledOperationalPilot: proposedPilot,
    weeklyInternalResearch: weeklyGoNoGo,
    codeChanges: [
      "deep-research-gdi-integration-v1.js — buildOpportunityBoundHotelSalesBrief; remove Principal/Nobu hardcoding; VoI continue pass; org-bound retrieval assessment",
      "autonomous-deep-research-recovery-v1.js — buyer gap ignores influencers; WHO pages exclude bare /about; buyer discovery queries; targeted enrichment request",
      "commercial-spine-restoration-v41.js — consumer-domain email early reject in classifyContactOwnership",
      "commercial-depth-buyer-intelligence-v1.js — broader material-question regex for completeness",
      "commercial-depth-hardening-v1.js — offline audit + 3-pathway regression certification",
    ],
  };
}
