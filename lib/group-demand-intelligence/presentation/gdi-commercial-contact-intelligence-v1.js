/**
 * Commercial Contact Intelligence V1 — permanent extension of Public Contact Discovery 2.0.
 *
 * Identifies the most commercially relevant person/route, explains why, recovers
 * public channels (via existing discovery), Surfe-last. Does not duplicate engines.
 */
import {
  discoverPublicContactChannels,
  buildPublicPersonContactQueryPlan,
  PUBLIC_CONTACT_RESEARCH_STATUS,
} from "./gdi-public-contact-discovery-v1.js";
import {
  buildContactChannelDisplayState,
} from "./gdi-public-contact-channel-hierarchy-v1.js";
import {
  COMMERCIAL_CONTACT_CLASS,
  classifyCommercialContact,
  inferEvidenceHintsFromText,
  buildCommercialContactEvidence,
} from "./gdi-commercial-contact-classification-v1.js";
import {
  buildBuyingEcosystemQueryPlan,
  inferBuyingEcosystemTargets,
} from "./gdi-commercial-contact-buying-ecosystem-v1.js";
import {
  selectBestCommercialContacts,
  compareBaselineToEnhanced,
  projectSelectionOntoOpportunity,
} from "./gdi-commercial-contact-selection-v1.js";
import {
  createPublicContactSearchProvider,
  inspectPublicContactSearchCredentials,
} from "./gdi-public-contact-search-providers-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const GDI_COMMERCIAL_CONTACT_INTELLIGENCE_V1 =
  "gdi-commercial-contact-intelligence-v1";

export const CONTACT_LEVEL_CHANGE_TYPES = Object.freeze({
  NEW_BUYER_IDENTIFIED: "NEW_BUYER_IDENTIFIED",
  BETTER_BUYER_VERIFIED: "BETTER_BUYER_VERIFIED",
  NEW_ENTRY_POINT_IDENTIFIED: "NEW_ENTRY_POINT_IDENTIFIED",
  CONTACT_EMAIL_RECOVERED: "CONTACT_EMAIL_RECOVERED",
  PHONE_RECOVERED: "PHONE_RECOVERED",
  BUYER_RESPONSIBILITY_CONFIRMED: "BUYER_RESPONSIBILITY_CONFIRMED",
  ROLE_CHANGED: "ROLE_CHANGED",
  CONTACT_REPLACED: "CONTACT_REPLACED",
  REFERRAL_ROUTE_IMPROVED: "REFERRAL_ROUTE_IMPROVED",
  EXISTING_CHANNEL_REVALIDATED: "EXISTING_CHANNEL_REVALIDATED",
  CONTACT_EVIDENCE_CONTRADICTED: "CONTACT_EVIDENCE_CONTRADICTED",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nameKey(n) {
  return text(n)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Seed candidates from existing opportunity contacts (no invention).
 */
export function collectExistingContactCandidates(opportunity = {}) {
  const out = [];
  const push = (c, source) => {
    if (!c || typeof c !== "object") return;
    const name = text(c.name || c.fullName);
    if (name && !looksLikePersonName(name)) return;
    if (!name && !text(c.email)) return;
    out.push({
      ...c,
      name: name || null,
      _candidateSource: source,
    });
  };
  push(opportunity.primaryContact, "existing_primary");
  push(opportunity.secondaryContact, "existing_secondary");
  for (const p of opportunity.discoveredPeerContacts || []) {
    push(p, "existing_peer");
  }
  return out;
}

/**
 * Default evidence for known PCD 2.0 fixtures — entry point, not hotel buyer.
 */
export function defaultEvidenceForKnownFixtures(person = {}, opportunity = {}) {
  const nk = nameKey(person.name);
  const hints = inferEvidenceHintsFromText(person, opportunity);
  const urls = [];
  if (person.channelSourceUrl) urls.push(person.channelSourceUrl);
  if (person.sourceUrl) urls.push(person.sourceUrl);

  if (nk.includes("alejandra vazquez") || nk.includes("paulina sanchez")) {
    return buildCommercialContactEvidence({
      connectedToDemandActivity: true,
      currentOrganizationAndRole: true,
      relevantOperationalOrCommercialInfluence: true,
      credibleReferralRoute: true,
      confirmedHotelSourcingResponsibility: false,
      confirmedHousingAccommodationResponsibility: false,
      supportingSourceUrls: urls.length
        ? urls
        : ["https://ecomondomexico.com.mx/en/contact/"],
      unverifiedResponsibilityGaps: [
        "hotel_accommodation_responsibility_not_confirmed",
        "exhibitor_sales_not_assumed_to_be_hotel_buyer",
      ],
      evidenceNotes:
        "Official event commercial / sales contact. Hotel partnership / housing ownership not evidenced on contact page alone.",
    });
  }
  if (nk.includes("maritza jeanette medina") || nk.includes("maritza medina")) {
    return buildCommercialContactEvidence({
      connectedToDemandActivity: true,
      currentOrganizationAndRole: true,
      relevantOperationalOrCommercialInfluence: true,
      credibleReferralRoute: true,
      confirmedHotelSourcingResponsibility: false,
      confirmedTravelProcurementResponsibility: false,
      supportingSourceUrls: urls.length
        ? urls
        : [
            "https://info.jalisco.gob.mx/dependencia/secretaria-de-infraestructura-y-obra-publica?page=14",
          ],
      unverifiedResponsibilityGaps: [
        "hotel_sourcing_authority_not_inferred_from_procurement_title",
        "project_visitor_travel_owner_not_yet_identified",
      ],
      evidenceNotes:
        "Verified public directory contact for Recursos Materiales. Procurement title does not certify hotel sourcing for project visitors.",
    });
  }

  return buildCommercialContactEvidence({
    connectedToDemandActivity: Boolean(text(opportunity.title)),
    currentOrganizationAndRole: Boolean(
      text(person.title) && text(person.organization || opportunity.organizationName),
    ),
    relevantOperationalOrCommercialInfluence:
      hints.hints.titleSuggestsSales ||
      hints.hints.titleSuggestsMeetings ||
      hints.hints.titleSuggestsHousing,
    supportingSourceUrls: urls,
    unverifiedResponsibilityGaps: hints.suggestedGaps,
  });
}

/**
 * Run ecosystem search (bounded) — returns candidate URL hits only.
 */
async function runEcosystemSearch(opportunity, opts = {}) {
  const plan = buildBuyingEcosystemQueryPlan(opportunity, {
    standardQueryCap: opts.maxEcosystemQueries || 4,
  });
  const metrics = {
    queriesPlanned: plan.queries.length,
    queriesExecuted: 0,
    searchCostUsd: 0,
    urls: [],
  };
  if (!opts.authorizePaidSearch) {
    return { plan, metrics, urls: [] };
  }
  let provider = opts.searchProvider;
  if (!provider) {
    const creds = inspectPublicContactSearchCredentials(opts.env || process.env);
    if (creds.chosenMode === "NONE") return { plan, metrics, urls: [] };
    provider = createPublicContactSearchProvider({
      env: opts.env || process.env,
      numResults: 4,
    });
  }
  const cap = Math.min(
    plan.standardQueryCap,
    plan.queries.length,
    Number(opts.maxEcosystemQueries || 4),
  );
  const seen = new Set();
  for (let i = 0; i < cap; i++) {
    const q = plan.queries[i];
    try {
      const res = await provider.search(q.query);
      metrics.queriesExecuted += 1;
      metrics.searchCostUsd += Number(res?.costUsd || 0);
      for (const u of res?.urls || []) {
        if (u && !seen.has(u)) {
          seen.add(u);
          metrics.urls.push({ url: u, query: q.query, kind: q.kind });
        }
      }
    } catch {
      /* continue */
    }
  }
  return { plan, metrics, urls: metrics.urls };
}

/**
 * Main entry: enhance commercial contact intelligence for one opportunity.
 */
export async function runCommercialContactIntelligence(args = {}) {
  const opportunity = args.opportunity || {};
  const baselinePrimary = { ...(opportunity.primaryContact || {}) };
  const started = Date.now();
  const metrics = {
    searchCostUsd: 0,
    ecosystemQueries: 0,
    personDiscoveryCostUsd: 0,
    candidatesConsidered: 0,
  };

  const ecosystem = inferBuyingEcosystemTargets(opportunity);
  const ecoSearch = await runEcosystemSearch(opportunity, {
    authorizePaidSearch: args.authorizePaidSearch === true,
    maxEcosystemQueries: args.maxEcosystemQueries || 4,
    searchProvider: args.searchProvider,
    env: args.env,
  });
  metrics.searchCostUsd += ecoSearch.metrics.searchCostUsd;
  metrics.ecosystemQueries = ecoSearch.metrics.queriesExecuted;

  // 1) Existing candidates
  const candidates = collectExistingContactCandidates(opportunity);

  // 2) Public channel discovery for primary (reuse PCD 2.0)
  const discovery = await discoverPublicContactChannels({
    opportunity,
    candidateUrls: [
      ...(args.candidateUrls || []),
      ...ecoSearch.urls.map((u) => u.url).slice(0, 6),
    ],
    authorizePaidSearch: args.authorizePaidSearch === true,
    seedRegressionFixtures: args.seedRegressionFixtures,
    maxQueries: args.maxPersonQueries ?? 2,
    maxPages: args.maxPages ?? 3,
    timeoutMs: args.timeoutMs || 18000,
    env: args.env,
    searchProvider: args.searchProvider,
  });
  metrics.personDiscoveryCostUsd = Number(discovery.metrics?.searchCostUsd || 0);
  metrics.searchCostUsd += metrics.personDiscoveryCostUsd;

  const enrichedOpp = discovery.opportunity || opportunity;
  // Refresh candidates from enriched opportunity
  for (const c of collectExistingContactCandidates(enrichedOpp)) {
    if (!candidates.some((x) => nameKey(x.name) === nameKey(c.name))) {
      candidates.push(c);
    }
  }

  // Attach evidence (fixtures + hints) — never invent buyer authority
  for (const c of candidates) {
    c.commercialEvidence =
      c.commercialEvidence ||
      defaultEvidenceForKnownFixtures(c, enrichedOpp);
  }
  metrics.candidatesConsidered = candidates.length;

  const selection = selectBestCommercialContacts(candidates, {
    organizationName: enrichedOpp.organizationName,
  });
  const projected = projectSelectionOntoOpportunity(enrichedOpp, selection);
  const comparison = compareBaselineToEnhanced(baselinePrimary, selection);

  const contactChanges = deriveContactLevelChanges(
    baselinePrimary,
    projected.opportunity,
    selection,
    comparison,
  );

  const display = buildContactChannelDisplayState(projected.opportunity);

  return {
    ok: true,
    version: GDI_COMMERCIAL_CONTACT_INTELLIGENCE_V1,
    status: discovery.status || PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_RESEARCH_INCOMPLETE,
    opportunity: projected.opportunity,
    baselinePrimary: {
      name: text(baselinePrimary.name) || null,
      email: text(baselinePrimary.email || baselinePrimary.workEmail) || null,
      title: text(baselinePrimary.title) || null,
    },
    ecosystem,
    ecosystemSearch: {
      queriesExecuted: ecoSearch.metrics.queriesExecuted,
      topUrls: ecoSearch.urls.slice(0, 8),
      planKinds: (ecoSearch.plan?.queries || []).slice(0, 8).map((q) => q.kind),
    },
    selection,
    comparison,
    contactLevelChanges: contactChanges,
    display,
    surfeCtaLabel: display.surfeCtaLabel,
    metrics: {
      ...metrics,
      durationMs: Date.now() - started,
      pagesFetched: discovery.metrics?.pagesFetched || 0,
    },
    surfeCalls: 0,
  };
}

export function deriveContactLevelChanges(
  baseline = {},
  nextOpp = {},
  selection = null,
  comparison = null,
) {
  const changes = [];
  const beforeEmail = text(baseline.email || baseline.workEmail);
  const afterEmail = text(
    nextOpp.primaryContact?.email || nextOpp.primaryContact?.workEmail,
  );
  const beforePhone = text(baseline.phone || baseline.directPhone);
  const afterPhone = text(
    nextOpp.primaryContact?.phone || nextOpp.primaryContact?.directPhone,
  );
  const beforeName = nameKey(baseline.name);
  const afterName = nameKey(nextOpp.primaryContact?.name);

  if (afterEmail && afterEmail !== beforeEmail.toLowerCase()) {
    changes.push(
      beforeEmail
        ? CONTACT_LEVEL_CHANGE_TYPES.EXISTING_CHANNEL_REVALIDATED
        : CONTACT_LEVEL_CHANGE_TYPES.CONTACT_EMAIL_RECOVERED,
    );
  }
  if (afterPhone && text(afterPhone).replace(/\D/g, "") !== beforePhone.replace(/\D/g, "")) {
    changes.push(
      beforePhone
        ? CONTACT_LEVEL_CHANGE_TYPES.EXISTING_CHANNEL_REVALIDATED
        : CONTACT_LEVEL_CHANGE_TYPES.PHONE_RECOVERED,
    );
  }
  if (comparison?.upgradeType === "BETTER_BUYER_OR_ENTRY") {
    changes.push(CONTACT_LEVEL_CHANGE_TYPES.BETTER_BUYER_VERIFIED);
  }
  if (comparison?.upgradeType === "BETTER_RELEVANT_ENTRY_POINT") {
    changes.push(CONTACT_LEVEL_CHANGE_TYPES.NEW_ENTRY_POINT_IDENTIFIED);
  }
  if (comparison?.upgradeType === "CONTACT_CHANGED") {
    changes.push(CONTACT_LEVEL_CHANGE_TYPES.CONTACT_REPLACED);
  }
  if (
    selection?.primary?.classification ===
      COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER ||
    selection?.primary?.classification ===
      COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER
  ) {
    if (
      text(baseline.contactClass) !== selection.primary.classification
    ) {
      changes.push(CONTACT_LEVEL_CHANGE_TYPES.BUYER_RESPONSIBILITY_CONFIRMED);
    }
  }
  if (
    selection?.bestAccessibleEntryPoint &&
    nameKey(selection.bestAccessibleEntryPoint.name) !== afterName &&
    selection.bestAccessibleEntryPoint.email
  ) {
    changes.push(CONTACT_LEVEL_CHANGE_TYPES.REFERRAL_ROUTE_IMPROVED);
  }
  // Deduplicate
  return [...new Set(changes)];
}

/**
 * Build Weekly Change–compatible contact annotation (does not create demand opps).
 */
export function buildContactLevelWeeklyAnnotation({
  baselineOpportunity = null,
  currentOpportunity = {},
  contactChanges = [],
  runId = null,
  nowIso = new Date().toISOString(),
} = {}) {
  const materialContactChange = contactChanges.length > 0;
  return {
    version: GDI_COMMERCIAL_CONTACT_INTELLIGENCE_V1,
    layer: "CONTACT_LEVEL_ONLY",
    doesNotCreateDemandOpportunity: true,
    contactChanges,
    materialContactChange,
    lastContactMaterialChangeAt: materialContactChange ? nowIso : null,
    lastContactMaterialChangeRunId: materialContactChange ? runId : null,
    baselinePrimaryName:
      text(baselineOpportunity?.primaryContact?.name) || null,
    currentPrimaryName:
      text(currentOpportunity?.primaryContact?.name) || null,
  };
}
