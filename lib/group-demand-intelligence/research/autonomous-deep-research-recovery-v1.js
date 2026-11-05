/**
 * Autonomous held-source recovery + buyer WHO fallback for deep research.
 * Removes founder/manual intervention after investigation starts.
 */

export const AUTONOMOUS_DEEP_RESEARCH_RECOVERY_V1 = "gdi-autonomous-deep-research-recovery-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normalizeUrlKey(url) {
  return String(url || "").trim().replace(/\/+$/, "").toLowerCase();
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

/**
 * Derive organization website host from opportunity fields.
 */
export function resolveOrganizationHost(opportunity = {}) {
  const domain = text(opportunity.organizationDomain || opportunity.officialDomain || "");
  if (domain) return domain.replace(/^www\./, "").toLowerCase();
  for (const u of [
    opportunity.officialSource,
    ...(opportunity.seedUrls || []),
    ...(opportunity.evidence || []).map((e) => e.url),
  ]) {
    const h = hostnameOf(u);
    if (h && !/linkedin|facebook|imex|micebook|marriott|hilton|hyatt/i.test(h)) return h;
  }
  return "";
}

/**
 * Build uninvestigated buyer/program source candidates when held URLs block progress.
 */
export function buildAlternativeSourceCandidates(opportunity = {}, { max = 6 } = {}) {
  const host = resolveOrganizationHost(opportunity);
  const investigated = new Set((opportunity.investigatedUrls || []).map(normalizeUrlKey));
  const paths = [
    "/team",
    "/our-team",
    "/people",
    "/leadership",
    "/about",
    "/about-us",
    "/who-we-are",
    "/staff",
    "/contact",
    "/news",
    "/programs",
    "/events",
  ];
  const urls = [];
  if (host) {
    for (const p of paths) {
      const u = `https://www.${host}${p}`;
      if (!investigated.has(normalizeUrlKey(u))) urls.push(u);
    }
  }
  for (const u of opportunity.candidateQueue || []) {
    if (u && !investigated.has(normalizeUrlKey(u))) urls.push(u);
  }
  const unique = [...new Set(urls)].slice(0, max);
  return unique.map((suggestedUrl) => ({
    entityName: /team|people|leadership|who-we-are|staff/i.test(suggestedUrl)
      ? `${opportunity.organizationName || "Organization"} buyer / staff page`
      : `${opportunity.organizationName || "Organization"} programme / news page`,
    suggestedUrl,
    independentPublisher: false,
    entityType: /team|people|leadership|who-we-are|staff/i.test(suggestedUrl) ? "programme" : "hotel_or_accommodation_partner",
    researchQuestion: /team|people|leadership|who-we-are|staff/i.test(suggestedUrl)
      ? `Who influences hotel sourcing, supplier selection, contracting, or program accommodation at ${opportunity.organizationName || "the organization"}?`
      : `What programs, clients, or hotel-placement activity has ${opportunity.organizationName || "the organization"} documented?`,
  }));
}

export function hasRoleRelevantDecisionMaker(people = []) {
  return (people || []).some((p) => {
    const name = text(p?.name || p?.personName);
    if (!name) return false;
    const cls = String(p?.decisionMakerClass || p?.ownershipClass || "");
    const title = text(p?.title || p?.roleTitle || p?.role || "");
    if (/VERIFIED_DECISION_MAKER|POTENTIAL_DECISION_MAKER/i.test(cls)) {
      return /hotel|sourcing|housing|meeting|event|procurement|contract|travel|planner|director|manager/i.test(title)
        || cls === "VERIFIED_DECISION_MAKER";
    }
    return false;
  });
}

export function buyerIntelligenceGap(opportunity = {}, matured = {}) {
  const people = [
    ...(matured.namedStakeholders || []),
    ...(opportunity.namedStakeholders || []),
  ];
  // Influencer-only extractions names do NOT close the buyer gap
  if (hasRoleRelevantDecisionMaker(people)) return false;
  return true;
}

/**
 * True only when a real staff/leadership WHO page was retrieved — not generic /about marketing.
 */
export function buyerWhoPagesRetrieved(matured = {}, researcherTrace = null) {
  const urls = [
    ...(matured.investigatedUrls || []),
    ...((matured.evidence || []).map((e) => e.url)),
    ...((researcherTrace?.executed || []).map((e) => e.url)),
  ];
  return urls.some((u) =>
    /\/(?:team|our-team|people|leadership|who-we-are|staff|board|directors)(?:\/|$|\?)/i.test(String(u || ""))
    || /board-of-directors|leadership-team|our-people/i.test(String(u || "")));
}

/**
 * Structured request for separately authorized paid enrichment (Surfe/PDL) — never auto-invoked.
 */
export function buildTargetedEnrichmentRequest({
  opportunity = {},
  unresolvedBuyerQuestion = null,
  publicAttempts = [],
  contactOwnership = null,
} = {}) {
  return {
    version: AUTONOMOUS_DEEP_RESEARCH_RECOVERY_V1,
    status: "AWAITING_SEPARATE_AUTHORIZATION",
    providerCandidates: ["surfe", "pdl"],
    authorizeSurfe: false,
    authorizePdl: false,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    hotelId: opportunity.hotelId || null,
    organizationName: opportunity.organizationName || null,
    unresolvedBuyerQuestion: unresolvedBuyerQuestion
      || `Who influences hotel sourcing / contracting at ${opportunity.organizationName || "the organization"}?`,
    publicAttemptsSummary: (publicAttempts || []).slice(0, 8),
    existingContactRoute: contactOwnership?.route || opportunity.contactEmail || null,
    existingOwnershipClass: contactOwnership?.ownershipClass || null,
    note: "Public WHO research did not establish a role-relevant named decision-maker. Paid enrichment requires explicit founder GO — do not persist Surfe-derived PII without authorization.",
  };
}

/**
 * Prepare opportunity so held-source memory does not abort research:
 * clear stale attempts, inject uninvestigated alternatives into pending/seeds.
 */
export function prepareOpportunityForAutonomousDeepResearch(opportunity = {}, options = {}) {
  const alts = buildAlternativeSourceCandidates(opportunity, { max: options.maxAlternatives || 6 });
  const investigated = new Set((opportunity.investigatedUrls || []).map(normalizeUrlKey));
  const freshSeeds = [
    ...(opportunity.seedUrls || []).filter((u) => !investigated.has(normalizeUrlKey(u))),
    ...alts.map((a) => a.suggestedUrl),
  ];
  const unresolved = [
    ...(opportunity.unresolvedQuestions || []),
  ];
  if (!unresolved.some((q) => /sourcing|decision|buyer|who\s+/i.test(String(q)))) {
    unresolved.push(
      `Who influences hotel sourcing, supplier selection, contracting, or program accommodation at ${opportunity.organizationName || "the organization"}?`,
    );
  }
  if (!unresolved.some((q) => /program|client|placement|cycle/i.test(String(q)))) {
    unresolved.push(
      `What specific programs, clients, or hotel-placement / next-cycle activity is documented for ${opportunity.organizationName || "the organization"}?`,
    );
  }

  return {
    ...opportunity,
    researchAttempts: options.resetAttempts === false ? (opportunity.researchAttempts || []) : [],
    relatedEntityLeadsExecuted: options.resetAttempts === false
      ? Number(opportunity.relatedEntityLeadsExecuted || 0)
      : 0,
    unresolvedQuestions: [...new Set(unresolved.map(text).filter(Boolean))],
    pendingDiscoverySearches: [
      ...(opportunity.pendingDiscoverySearches || []).filter(
        (p) => p?.suggestedUrl && !investigated.has(normalizeUrlKey(p.suggestedUrl)),
      ),
      ...alts,
    ],
    seedUrls: [...new Set(freshSeeds)].slice(0, 8),
    candidateQueue: [...new Set([
      ...(opportunity.candidateQueue || []),
      ...alts.map((a) => a.suggestedUrl),
    ])].slice(0, 8),
    notes: [
      ...(opportunity.notes || []),
      "autonomous_held_source_recovery_prepared",
      alts.length ? `alternative_sources_queued:${alts.length}` : "no_alternative_host_urls",
    ],
    autonomousRecovery: {
      version: AUTONOMOUS_DEEP_RESEARCH_RECOVERY_V1,
      alternativesQueued: alts.length,
      organizationHost: resolveOrganizationHost(opportunity),
    },
  };
}

/**
 * After ORS pass: if buyer gap remains and WHO pages not tried, force buyer follow-up via researcher.
 */
export function buildBuyerDiscoveryQueries(opportunity = {}, hotelName = "") {
  const org = opportunity.organizationName || "organization";
  const hotel = hotelName || opportunity.hotelName || "";
  return [
    `"${org}" (director OR manager OR coordinator) (meeting OR housing OR hotel OR sourcing OR events OR procurement)`,
    `"${org}" ("hotel sourcing" OR "venue selection" OR "housing bureau" OR "room block" OR "event planner")`,
    hotel
      ? `"${org}" ${hotel} (hotel OR lodging OR meeting OR reception)`
      : `"${org}" (incentive OR conference OR tournament) hotel`,
  ];
}

export async function ensureBuyerWhoResearchExecuted({
  opportunity,
  matured,
  researcher,
  drainRelatedEntityFollowUpsThroughOrs,
  mergeDeepResearchIntoOpportunity,
  runPublicBuyerIntelligencePass,
  remainingBudgetUsd = 0.5,
  targetHotel = "",
} = {}) {
  const gap = buyerIntelligenceGap(opportunity, matured);
  const whoDone = buyerWhoPagesRetrieved(matured, researcher?.trace);
  if (!gap || !researcher?.execute) {
    return {
      executed: false,
      reason: !gap ? "buyer_gap_closed_role_relevant_dm_present" : "no_researcher",
      matured,
      buyerPass: null,
      targetedEnrichmentRequest: null,
    };
  }

  const alts = buildAlternativeSourceCandidates({
    ...matured,
    organizationName: opportunity.organizationName,
    organizationDomain: opportunity.organizationDomain || matured.organizationDomain,
    officialSource: opportunity.officialSource || matured.officialSource,
    investigatedUrls: matured.investigatedUrls || opportunity.investigatedUrls,
    candidateQueue: opportunity.candidateQueue || matured.candidateQueue,
  });
  // Prefer true WHO paths — not generic /about marketing pages
  const buyerUrls = alts
    .map((a) => a.suggestedUrl)
    .filter((u) => /team|people|leadership|who-we-are|staff|board|directors/i.test(u))
    .slice(0, 4);

  let working = { ...matured, pendingDiscoverySearches: alts };
  let drainExtra = null;
  if (drainRelatedEntityFollowUpsThroughOrs && alts.length && !whoDone) {
    drainExtra = await drainRelatedEntityFollowUpsThroughOrs({
      state: working,
      researcher,
      targetHotel: targetHotel || opportunity.hotelName,
      remainingBudgetUsd,
      maxFollowUps: 3,
      paidBlocked: true,
    });
    working = { ...working, ...drainExtra.state };
  }

  const textQueries = buildBuyerDiscoveryQueries(opportunity, targetHotel || opportunity.hotelName);
  const buyerFetch = await researcher.execute({
    opportunity: {
      ...working,
      organizationName: opportunity.organizationName,
      seedUrls: buyerUrls,
      officialSource: buyerUrls[0] || working.officialSource,
      pendingDiscoverySearches: alts,
    },
    action: {
      strategy: "related_entity_followup",
      // Text queries first so Exa buyer/program discovery cannot be crowded out by URL seeds
      queries: [
        ...((!whoDone || gap) ? textQueries.slice(0, 2) : []),
        ...buyerUrls,
      ].filter(Boolean).slice(0, 5),
    },
  });

  const merged = mergeDeepResearchIntoOpportunity(working, {
    evidence: (buyerFetch.evidence || []).map((e) => ({ ...e, relatedEntity: true, topic: "buyer" })),
    investigatedUrls: (buyerFetch.evidence || []).map((e) => e.url).filter(Boolean),
    namedStakeholders: buyerFetch.deepPatch?.namedStakeholdersFound || [],
  });

  const buyerPass = runPublicBuyerIntelligencePass({
    evidence: merged.opportunity.evidence || [],
    organizationName: opportunity.organizationName,
    organizationDomain: opportunity.organizationDomain || resolveOrganizationHost(opportunity),
    existingContactEmail: opportunity.contactEmail || opportunity.contact?.businessEmail || null,
  });

  // Keep influencer names for transparency but do not treat as gap-closed DMs
  const roleRelevant = hasRoleRelevantDecisionMaker([
    ...(merged.opportunity.namedStakeholders || []),
    ...(buyerPass.namedStakeholders || []),
  ]);

  const next = {
    ...merged.opportunity,
    namedStakeholders: [
      ...(merged.opportunity.namedStakeholders || []),
      ...(buyerPass.namedStakeholders || []),
    ],
    decisionMakerGap: roleRelevant
      ? null
      : (buyerPass.decisionMakerGap
        || "Named hotel-sourcing / contracting decision-maker not established from public evidence"),
    notes: [
      ...(merged.opportunity.notes || []),
      "autonomous_buyer_who_fallback_executed",
      whoDone ? "buyer_who_prior_pages_insufficient_continued_discovery" : "buyer_who_pages_and_queries_executed",
    ],
    relatedEntityLeadsExecuted: Number(merged.opportunity.relatedEntityLeadsExecuted || 0)
      + Number(drainExtra?.relatedEntityLeadsFollowed || 0)
      + ((buyerFetch.evidence || []).length ? 1 : 0),
  };

  const enrichmentRequest = roleRelevant
    ? null
    : buildTargetedEnrichmentRequest({
      opportunity,
      publicAttempts: [
        ...(buyerUrls || []),
        ...(textQueries || []),
        ...((researcher?.trace?.executed || []).map((e) => e.url)),
      ],
      contactOwnership: buyerPass.contactOwnership || null,
    });

  return {
    executed: true,
    reason: roleRelevant
      ? "buyer_who_established_role_relevant_dm"
      : "buyer_intelligence_gap_forced_who_and_discovery_queries",
    matured: next,
    buyerPass,
    buyerUrlsAttempted: buyerUrls,
    buyerQueriesAttempted: textQueries,
    drainExtra,
    targetedEnrichmentRequest: enrichmentRequest,
    version: AUTONOMOUS_DEEP_RESEARCH_RECOVERY_V1,
  };
}
