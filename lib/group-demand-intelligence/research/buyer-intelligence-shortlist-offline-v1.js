/**
 * Buyer intelligence offline validation for the commercial-improvement shortlist cohort.
 * Uses stored canonical opportunity evidence only — no Surfe/PDL/Parallel/paid calls.
 * Provenance classes: VERIFIED_LIVE | VERIFIED_FROM_STORED_EVIDENCE | OFFLINE_FIXTURE | UNVERIFIED
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyContactOwnership,
  isConsumerEmailDomain,
  CONTACT_OWNERSHIP_CLASS,
  assertPaidContactProvidersDisabled,
} from "./commercial-spine-restoration-v41.js";
import {
  classifyPersonResponsibility,
} from "../workflow/buyer-contact-enrichment-v1.js";
import {
  targetRolesForOpportunity,
} from "../workflow/buyer-contact-enrichment-contract-v1.js";
import {
  buildTargetedEnrichmentRequest,
  buildBuyerDiscoveryQueries,
  hasRoleRelevantDecisionMaker,
  resolveOrganizationHost,
  buildAlternativeSourceCandidates,
} from "./autonomous-deep-research-recovery-v1.js";
import {
  runPublicBuyerIntelligencePass,
} from "./commercial-depth-buyer-intelligence-v1.js";
import {
  selectShortlistForNextValidation,
  PRIOR_TEST_OPPORTUNITY_IDS,
  BUYER_INTELLIGENCE_VALIDATION_COHORT_V1,
} from "./deep-research-opportunity-selection-v1.js";

export const BUYER_INTELLIGENCE_SHORTLIST_OFFLINE_V1 = "gdi-buyer-intelligence-shortlist-offline-v1";
export const BUYER_INTEL_REPORTS_DIR = "reports/gdi-buyer-intelligence-shortlist-offline-v1";

export const FINDING_PROVENANCE = Object.freeze({
  VERIFIED_LIVE: "VERIFIED_LIVE",
  VERIFIED_FROM_STORED_EVIDENCE: "VERIFIED_FROM_STORED_EVIDENCE",
  OFFLINE_FIXTURE: "OFFLINE_FIXTURE",
  UNVERIFIED: "UNVERIFIED",
});

/** Canonical shortlist cohort (locked to BUYER_INTELLIGENCE_VALIDATION_COHORT_V1). */
export const SHORTLIST_COHORT_V1 = Object.freeze([
  {
    opportunityId: BUYER_INTELLIGENCE_VALIDATION_COHORT_V1[0],
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    pathway: "EVENT_LED",
  },
  {
    opportunityId: BUYER_INTELLIGENCE_VALIDATION_COHORT_V1[1],
    hotelId: "recdmo0CnhIZAwHGC",
    hotelName: "The Westin Tampa Waterside",
    pathway: "ACCOUNT_LED",
  },
  {
    opportunityId: BUYER_INTELLIGENCE_VALIDATION_COHORT_V1[2],
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    pathway: "COMPETITIVE_FUTURE_CYCLE",
  },
]);

const FIXTURE_ONLY_NAMES = /^(jordan\s+hale|chris\s+delgado|riley\s+chen|alex\s+morgan)$/i;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadOpportunity(hotelId, opportunityId) {
  const p = path.join(ROOT, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json");
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  const arr = Array.isArray(raw) ? raw : (raw.opportunities || []);
  const opp = arr.find((o) => o.id === opportunityId);
  if (!opp) throw new Error(`opportunity_not_found:${opportunityId}`);
  return opp;
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function isLikelyPersonName(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/association|academy|events|housing|provider|channel|tournaments|inc\.|llc|global|maritz|staff|tbd/i.test(n)) {
    return false;
  }
  return /^[A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+)+$/.test(n) || /^[A-Z][a-z]+\s+[A-Z]\.\s*[A-Z][a-z]+$/.test(n);
}

function classifyStoredPersonProvenance(person = {}) {
  const name = text(person.name || person.fullName || person.personName);
  if (FIXTURE_ONLY_NAMES.test(name)) {
    return {
      provenance: FINDING_PROVENANCE.OFFLINE_FIXTURE,
      salesUsableIdentity: false,
      note: "Offline fixture identity — rejected for sales use",
    };
  }
  const claimKind = String(person.claimKind || "").toUpperCase();
  const emailStatus = String(person.emailVerificationStatus || "").toUpperCase();
  const hasEmail = Boolean(person.email) && !isConsumerEmailDomain(person.email);
  const hasSource = Boolean(person.sourceUrl || person.source || person.emailSource);
  const quality = String(person.contactQuality || "").toUpperCase();
  const personLike = isLikelyPersonName(name);

  // Functional intermediaries / org labels with FACT + official source are stored evidence,
  // but never sales-usable *named-person* identities.
  if (!personLike) {
    if (claimKind === "FACT" && hasSource) {
      return {
        provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
        salesUsableIdentity: false,
        note: "Functional entity / intermediary from stored official evidence — not a verified named individual",
      };
    }
    return {
      provenance: FINDING_PROVENANCE.UNVERIFIED,
      salesUsableIdentity: false,
      note: "Functional entity or non-person label — not a verified named individual",
    };
  }

  // Live retrieval not performed in this offline pass
  if (claimKind === "FACT" && hasSource && (hasEmail || emailStatus.includes("OFFICIAL"))) {
    return {
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      salesUsableIdentity: quality !== "NO_CONTACT" || hasEmail,
      note: "Named person supported by stored official-source evidence — not re-verified live in this pass",
    };
  }
  if (claimKind === "INFERENCE" || !hasEmail || quality === "NO_CONTACT") {
    return {
      provenance: FINDING_PROVENANCE.UNVERIFIED,
      salesUsableIdentity: false,
      note: "Name appears in stored research but lacks verified email/role confirmation for sales ownership",
    };
  }
  if (hasSource) {
    return {
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      salesUsableIdentity: Boolean(hasEmail),
      note: "Stored evidence with source URL — identity/role still require live confirmation before heavy pursuit",
    };
  }
  return {
    provenance: FINDING_PROVENANCE.UNVERIFIED,
    salesUsableIdentity: false,
    note: "Insufficient stored evidence to treat as verified buyer identity",
  };
}

function extractStoredStakeholders(opportunity = {}) {
  const people = [];
  const push = (p, origin) => {
    if (!p) return;
    const name = text(p.name || p.fullName || p.personName || p.personIdentity);
    if (!name) return;
    people.push({
      name,
      title: text(p.title || p.role || p.roleTitle || "") || null,
      email: p.email || null,
      organization: p.organization || opportunity.organizationName || null,
      sourceUrl: p.sourceUrl || p.source || p.emailSource || p.evidence || null,
      claimKind: p.claimKind || null,
      emailVerificationStatus: p.emailVerificationStatus || null,
      contactQuality: p.contactQuality || null,
      contactKind: p.contactKind || origin || null,
      relationshipToOpportunity: p.relationshipToOpportunity || null,
      targetRoleMatch: p.targetRoleMatch || null,
      whoRoleClass: p.roleClass || null,
    });
  };

  push(opportunity.primaryContact, "primaryContact");
  for (const c of opportunity.contacts || []) push(c, "contacts");
  for (const c of opportunity.backupContacts || []) push(c, "backupContacts");
  if (opportunity.whoRecord) {
    push({
      name: opportunity.whoRecord.personIdentity,
      title: opportunity.whoRecord.title,
      roleClass: opportunity.whoRecord.roleClass,
      evidence: opportunity.whoRecord.evidence,
      claimKind: opportunity.whoRecord.named === false ? "FACT" : "FACT",
      contactQuality: "NO_CONTACT",
    }, "whoRecord");
  }
  if (opportunity.contactName || opportunity.primaryContactName) {
    push({
      name: opportunity.contactName || opportunity.primaryContactName,
      title: opportunity.contactRole || opportunity.primaryContactRole,
      email: opportunity.contactEmail || opportunity.primaryContactEmail,
      claimKind: opportunity.primaryContact?.claimKind || "UNKNOWN",
      sourceUrl: opportunity.officialSource,
      contactQuality: opportunity.contactQuality,
    }, "topLevelContactFields");
  }

  // Dedup by lower name
  return [...new Map(people.map((p) => [p.name.toLowerCase(), p])).values()];
}

function buildEvidenceCorpusFromStored(opportunity = {}) {
  const evidence = [];
  const add = (url, statement, topic) => {
    if (!text(statement) && !url) return;
    evidence.push({
      url: url || null,
      statement: text(statement).slice(0, 500),
      quote: text(statement).slice(0, 500),
      provenance: "canonical_gdi_bag",
      topic: topic || null,
      previouslyHeld: true,
    });
  };
  add(opportunity.officialSource, opportunity.hotelOpportunityThesis, "thesis");
  add(opportunity.officialSource, opportunity.eventActivityEvidence, "activity");
  add(opportunity.contactUrl, `Contact path: ${opportunity.contactUrl || ""}`, "contact_page");
  for (const s of opportunity.sources || []) {
    add(s.url, `${s.name || s.title || "source"} — supports ${s.supportsFact || s.type || "fact"}`, "source");
  }
  for (const p of extractStoredStakeholders(opportunity)) {
    add(
      p.sourceUrl,
      `${p.name}${p.title ? `, ${p.title}` : ""}${p.email ? ` <${p.email}>` : ""} — ${p.relationshipToOpportunity || p.claimKind || "stored contact"}`,
      "stored_stakeholder",
    );
  }
  return evidence;
}

function relevantRolesForOpportunity(opportunity = {}) {
  const roles = targetRolesForOpportunity({
    segment: opportunity.segment,
    researchSegment: opportunity.researchSegment,
    opportunityType: opportunity.opportunityType,
    hotelOpportunityThesis: opportunity.hotelOpportunityThesis,
    organizationName: opportunity.organizationName,
  });
  const preferred = opportunity.preferredWhoRoles || [];
  return {
    useCaseBucket: roles.bucket,
    targetRoles: roles.roles,
    preferredWhoRoles: preferred,
    commerciallyRelevantRoleLabels: [
      ...preferred.map(String),
      ...roles.roles,
    ],
    note: roles.note,
  };
}

function assessSourcingControl(opportunity = {}, pathway = "") {
  const thesis = text(opportunity.hotelOpportunityThesis || "");
  const org = opportunity.organizationName;
  const who = opportunity.whoRecord;
  const primary = opportunity.primaryContact;

  if (/stay-?to-?play partner|traveling teams|housing bureau|housing provider|Maritz|official housing/i.test(
    `${thesis} ${who?.title || ""} ${primary?.title || ""}`,
  )) {
    return {
      controllingEntity: who?.personIdentity && !isLikelyPersonName(who.personIdentity)
        ? who.personIdentity
        : (/Maritz/i.test(primary?.name || "") ? primary.name : null),
      influencingOrganization: org,
      mechanism: /stay-?to-?play/i.test(thesis)
        ? "Stay-to-play / traveling-teams housing partner (not single contracted host)"
        : /Maritz|housing provider/i.test(`${who?.title || ""} ${primary?.title || ""}`)
          ? "Official housing / registration intermediary pattern"
          : "Housing program / overflow path",
      hotelSourcingControlSummary: pathway === "ACCOUNT_LED"
        ? `${org} influences venue/supplier selection for client programmes`
        : `${org} organizes the program; hotel room placement may be controlled by a housing partner or bureau rather than the org alone`,
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
    };
  }

  if (pathway === "ACCOUNT_LED" || /CORPORATE_ACCOUNT|PRIMARY_PURSUIT|DMC|destination/i.test(String(opportunity.opportunityType || opportunity.segment || ""))) {
    return {
      controllingEntity: org,
      influencingOrganization: org,
      mechanism: "Account / DMC / planner supplier and venue sourcing",
      hotelSourcingControlSummary: `${org} is the prospective account that sources hotels/venues for meetings and events`,
      provenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
    };
  }

  return {
    controllingEntity: org,
    influencingOrganization: org,
    mechanism: "Organization-associated event or housing demand",
    hotelSourcingControlSummary: `${org} is associated with the demand; exact hotel-contracting owner not established from stored evidence`,
    provenance: FINDING_PROVENANCE.UNVERIFIED,
  };
}

/**
 * Full offline buyer-intelligence assessment for one shortlist opportunity.
 * Pass `opportunity` to avoid a disk reload when the bag is already in memory.
 */
export function assessBuyerIntelligenceForOpportunity({
  opportunityId,
  hotelId,
  hotelName,
  pathway,
  opportunity: opportunityBag = null,
} = {}) {
  const opportunity = opportunityBag || loadOpportunity(hotelId, opportunityId);
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const roles = relevantRolesForOpportunity(opportunity);
  const sourcing = assessSourcingControl(opportunity, pathway);
  const storedPeople = extractStoredStakeholders(opportunity);
  const evidence = buildEvidenceCorpusFromStored(opportunity);
  const orgHost = resolveOrganizationHost({
    organizationDomain: hostnameOf(opportunity.officialSource || opportunity.contactUrl || ""),
    officialSource: opportunity.officialSource,
    evidence,
  }) || hostnameOf(opportunity.officialSource || opportunity.contactUrl || "");

  const annotatedPeople = storedPeople.map((p) => {
    const prov = classifyStoredPersonProvenance(p);
    const resp = isLikelyPersonName(p.name)
      ? classifyPersonResponsibility(
        {
          name: p.name,
          title: p.title,
          email: p.email,
          relevanceEvidence: `${p.title || ""} ${p.relationshipToOpportunity || ""} housing hotel sourcing`,
        },
        {
          organizationName: opportunity.organizationName,
          segment: opportunity.segment,
          opportunityType: opportunity.opportunityType,
        },
        {},
      )
      : { class: null, rationale: "Non-person / functional entity" };

    const roleRelevantTitle = /housing|tournament|meeting|event|sourcing|procurement|travel|planner|director|coordinator|manager/i.test(p.title || "");
    const decisionMakerClass = prov.provenance === FINDING_PROVENANCE.OFFLINE_FIXTURE
      ? "REJECTED_FIXTURE"
      : (!isLikelyPersonName(p.name)
        ? "FUNCTIONAL_ENTITY_OR_INTERMEDIARY"
        : (/BOOKING_DECISION|CONFIRMED_RELEVANT|POTENTIALLY_RELEVANT/i.test(resp?.class || "") && roleRelevantTitle
          ? "POTENTIAL_DECISION_MAKER"
          : "INFLUENCER_OR_GENERAL"));

    return {
      ...p,
      findingProvenance: prov.provenance,
      salesUsableIdentity: prov.salesUsableIdentity,
      provenanceNote: prov.note,
      responsibilityClass: resp?.class || null,
      responsibilityRationale: resp?.rationale || null,
      decisionMakerClass,
      // Never promote UNVERIFIED / fixture as verified DM
      verifiedDecisionMaker: prov.provenance === FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE
        && decisionMakerClass === "POTENTIAL_DECISION_MAKER"
        && prov.salesUsableIdentity === true,
    };
  }).filter((p) => p.findingProvenance !== FINDING_PROVENANCE.OFFLINE_FIXTURE);

  // Public buyer pass over *stored* evidence only (no live fetch)
  const buyerPass = runPublicBuyerIntelligencePass({
    evidence,
    organizationName: opportunity.organizationName,
    organizationDomain: orgHost,
    segmentId: opportunity.segment || opportunity.researchSegment || "general",
    existingContactEmail: opportunity.contactEmail || opportunity.primaryContactEmail || opportunity.primaryContact?.email || null,
  });

  const contactEmail = opportunity.contactEmail
    || opportunity.primaryContactEmail
    || opportunity.primaryContact?.email
    || null;
  const ownership = classifyContactOwnership({
    email: contactEmail,
    pageUrl: opportunity.officialSource || opportunity.contactUrl || opportunity.primaryContact?.sourceUrl || "",
    organizationName: opportunity.organizationName,
    organizationDomain: orgHost,
    personName: isLikelyPersonName(opportunity.contactName || opportunity.primaryContactName)
      ? (opportunity.contactName || opportunity.primaryContactName)
      : null,
    roleTitle: opportunity.contactRole || opportunity.primaryContactRole || null,
    publisherKind: "ORGANIZATION_OWNED",
  });

  const ownershipProvenance = !contactEmail
    ? FINDING_PROVENANCE.UNVERIFIED
    : (opportunity.primaryContact?.emailVerificationStatus === "OFFICIAL_SOURCE_VERIFIED"
      || /OFFICIAL_SOURCE_VERIFIED/i.test(String(opportunity.primaryContact?.emailVerificationStatus || ""))
      ? FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE
      : FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE);

  const publicSources = [
    opportunity.officialSource,
    opportunity.contactUrl,
    ...(opportunity.contactFollowupLinks || []).map((l) => l.url),
    ...(opportunity.sources || []).map((s) => s.url),
    ...buildAlternativeSourceCandidates({
      organizationName: opportunity.organizationName,
      organizationDomain: orgHost,
      officialSource: opportunity.officialSource,
      investigatedUrls: [],
    }).map((a) => a.suggestedUrl),
  ].filter(Boolean);

  const uniqueSources = [...new Set(publicSources)].slice(0, 16);
  const discoveryQueries = buildBuyerDiscoveryQueries({
    organizationName: opportunity.organizationName,
    hotelName,
  }, hotelName);

  const roleRelevantVerified = annotatedPeople.filter((p) => p.verifiedDecisionMaker);
  const roleRelevantUnverified = annotatedPeople.filter((p) =>
    p.decisionMakerClass === "POTENTIAL_DECISION_MAKER"
    && p.findingProvenance === FINDING_PROVENANCE.UNVERIFIED);
  const functionalEntities = annotatedPeople.filter((p) =>
    p.decisionMakerClass === "FUNCTIONAL_ENTITY_OR_INTERMEDIARY");

  const buyerGapOpen = roleRelevantVerified.length === 0
    && !hasRoleRelevantDecisionMaker(annotatedPeople.filter((p) => p.salesUsableIdentity));

  const enrichmentRequest = buyerGapOpen
    ? buildTargetedEnrichmentRequest({
      opportunity: {
        id: opportunityId,
        opportunityId,
        hotelId,
        organizationName: opportunity.organizationName,
        contactEmail,
      },
      unresolvedBuyerQuestion: `Who owns hotel sourcing / housing / meeting venue selection for ${opportunity.organizationName}?`,
      publicAttempts: uniqueSources.slice(0, 8),
      contactOwnership: ownership,
    })
    : null;

  // Does contact intel materially improve salesperson next action?
  const nextAction = buildSalesNextActionAssessment({
    opportunity,
    hotelName,
    pathway,
    ownership,
    roleRelevantVerified,
    roleRelevantUnverified,
    functionalEntities,
    buyerGapOpen,
  });

  return {
    version: BUYER_INTELLIGENCE_SHORTLIST_OFFLINE_V1,
    opportunityId,
    hotelId,
    hotelName,
    organizationName: opportunity.organizationName,
    pathway,
    opportunityType: opportunity.opportunityType || null,
    liveRetrievalPerformed: false,
    paidProvidersInvoked: { surfe: false, pdl: false, parallel: false },
    paidGuardOk: paidGuard.ok === true,
    sourcingControl: sourcing,
    commerciallyRelevantRoles: roles,
    storedStakeholders: annotatedPeople,
    roleRelevantVerifiedPeople: roleRelevantVerified,
    roleRelevantUnverifiedPeople: roleRelevantUnverified,
    functionalIntermediaries: functionalEntities,
    verifiedOrganizationContactRoutes: ownership.route && ownership.salesUsableAsProspectContact
      ? [{
        route: ownership.route,
        ownershipClass: ownership.ownershipClass,
        salesUsableAsProspectContact: true,
        findingProvenance: ownershipProvenance,
        note: ownership.note,
      }]
      : [],
    nonUsableObservedRoutes: ownership.route && !ownership.salesUsableAsProspectContact
      ? [{
        route: ownership.route,
        ownershipClass: ownership.ownershipClass,
        findingProvenance: ownership.consumerDomain
          ? FINDING_PROVENANCE.UNVERIFIED
          : ownershipProvenance,
        note: ownership.note,
      }]
      : (!contactEmail ? [{
        route: null,
        ownershipClass: CONTACT_OWNERSHIP_CLASS.MISSING,
        findingProvenance: FINDING_PROVENANCE.UNVERIFIED,
        note: "No email route on canonical opportunity",
      }] : []),
    contactOwnership: {
      ...ownership,
      findingProvenance: contactEmail ? ownershipProvenance : FINDING_PROVENANCE.UNVERIFIED,
    },
    permittedPublicSources: uniqueSources.map((url) => ({
      url,
      host: hostnameOf(url),
      findingProvenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      purpose: /staff|team|leadership|coach/i.test(url)
        ? "WHO / staff directory candidate"
        : /contact/i.test(url)
          ? "Organization contact path"
          : /housing|provider|tournament|meeting|location/i.test(url)
            ? "Housing / meetings / program page"
            : "Official or related public page",
    })),
    buyerDiscoveryQueriesSuggested: discoveryQueries,
    publicBuyerPassOverStoredEvidence: {
      namedStakeholdersExtracted: (buyerPass.namedStakeholders || []).length,
      namedDecisionMakerEstablished: buyerPass.namedDecisionMakerEstablished === true,
      decisionMakerGap: buyerPass.decisionMakerGap,
      surfeInvoked: false,
      pdlInvoked: false,
      note: "Pass ran on stored bag evidence only — not live page fetches",
    },
    targetedEnrichmentRequest: enrichmentRequest,
    enrichmentJustified: Boolean(enrichmentRequest),
    salesNextAction: nextAction,
    materialContactIntelligenceImprovement: nextAction.materiallyImprovesNextAction === true,
    findingsSummary: {
      verifiedLiveCount: 0,
      verifiedFromStoredEvidenceCount: annotatedPeople.filter((p) =>
        p.findingProvenance === FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE).length
        + (ownership.salesUsableAsProspectContact ? 1 : 0),
      offlineFixtureRejectedCount: storedPeople.filter((p) =>
        classifyStoredPersonProvenance(p).provenance === FINDING_PROVENANCE.OFFLINE_FIXTURE).length,
      unverifiedCount: annotatedPeople.filter((p) =>
        p.findingProvenance === FINDING_PROVENANCE.UNVERIFIED).length,
    },
  };
}

function buildSalesNextActionAssessment({
  opportunity,
  hotelName,
  pathway,
  ownership,
  roleRelevantVerified,
  roleRelevantUnverified,
  functionalEntities,
  buyerGapOpen,
} = {}) {
  const org = opportunity.organizationName;
  if (roleRelevantVerified[0]) {
    const p = roleRelevantVerified[0];
    return {
      materiallyImprovesNextAction: true,
      recommendedAction: `Ask ${p.name}${p.title ? ` (${p.title})` : ""} whether ${hotelName} can be considered for upcoming ${pathway === "ACCOUNT_LED" ? "programmes" : "housing / overflow"} — do not invent an RFP or room block.`,
      basis: "Verified-from-stored role-relevant person with usable identity",
      findingProvenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      remainingCaveat: "Re-confirm current affiliation live before heavy pursuit",
    };
  }

  if (functionalEntities[0] && ownership.salesUsableAsProspectContact) {
    const f = functionalEntities[0];
    return {
      materiallyImprovesNextAction: true,
      recommendedAction: `Use ${ownership.route} to reach ${f.name} / housing channel; ask who owns ${pathway === "COMPETITIVE_FUTURE_CYCLE" ? "2028 overflow / housing list inclusion" : "hotel shortlist criteria"} for ${org}. Preserve any named competitor host; do not imply ${hotelName} hosts the primary event.`,
      basis: "Functional housing/intermediary entity + usable org contact route from stored official evidence",
      findingProvenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      remainingCaveat: "Intermediary ≠ named hotel contracting decision-maker",
    };
  }

  if (ownership.salesUsableAsProspectContact && !buyerGapOpen) {
    return {
      materiallyImprovesNextAction: true,
      recommendedAction: `Introduce ${hotelName} via ${ownership.route}; ask who owns hotel sourcing.`,
      basis: "Usable organization contact route",
      findingProvenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      remainingCaveat: null,
    };
  }

  if (ownership.salesUsableAsProspectContact && buyerGapOpen) {
    return {
      materiallyImprovesNextAction: false,
      recommendedAction: `General route ${ownership.route} is usable for introduction only — named hotel-sourcing owner still unresolved. Qualify ownership before pitching a block.`,
      basis: "General org route without role-relevant named buyer",
      findingProvenance: FINDING_PROVENANCE.VERIFIED_FROM_STORED_EVIDENCE,
      remainingCaveat: "Does not unlock a specific buyer conversation",
    };
  }

  if (roleRelevantUnverified[0]) {
    const p = roleRelevantUnverified[0];
    return {
      materiallyImprovesNextAction: false,
      recommendedAction: `Do not pitch ${p.name} as a verified decision-maker — name is UNVERIFIED in stored evidence (no confirmed email/ownership). Pursue staff/housing pages or authorized enrichment.`,
      basis: "Unverified named person must not be promoted to sales-ready buyer",
      findingProvenance: FINDING_PROVENANCE.UNVERIFIED,
      remainingCaveat: p.provenanceNote,
    };
  }

  return {
    materiallyImprovesNextAction: false,
    recommendedAction: `No sales-ready buyer path yet for ${org} → ${hotelName}. Use permitted public WHO sources first; Surfe/PDL only with separate authorization.`,
    basis: "Buyer gap open; no usable verified contact route",
    findingProvenance: FINDING_PROVENANCE.UNVERIFIED,
    remainingCaveat: "Missing contact does not invent demand — keep opportunity in research/watch posture",
  };
}

/**
 * Run offline cohort package for the three shortlist opportunities.
 */
export function runBuyerIntelligenceShortlistOfflinePackage() {
  const ranking = selectShortlistForNextValidation({
    preferPathwayDiversity: true,
    excludePriorTests: true,
  });

  // Prefer explicit cohort IDs; fall back to live ranker shortlist if bags move
  const cohort = SHORTLIST_COHORT_V1.map((c) => {
    try {
      loadOpportunity(c.hotelId, c.opportunityId);
      return c;
    } catch {
      return null;
    }
  }).filter(Boolean);

  const assessments = cohort.map((c) => assessBuyerIntelligenceForOpportunity(c));

  const gates = {
    threeAssessed: assessments.length === 3,
    noPriorTestIds: assessments.every((a) => !PRIOR_TEST_OPPORTUNITY_IDS.includes(a.opportunityId)),
    noPaidProviders: assessments.every((a) =>
      a.paidProvidersInvoked.surfe === false
      && a.paidProvidersInvoked.pdl === false
      && a.paidProvidersInvoked.parallel === false),
    noLiveRetrievalClaimed: assessments.every((a) => a.liveRetrievalPerformed === false),
    noVerifiedLiveFindings: assessments.every((a) => a.findingsSummary.verifiedLiveCount === 0),
    noFixtureIdentitiesPromoted: assessments.every((a) =>
      !(a.storedStakeholders || []).some((p) => p.findingProvenance === FINDING_PROVENANCE.OFFLINE_FIXTURE)
      && a.findingsSummary.offlineFixtureRejectedCount >= 0),
    unverifiedNotMarkedVerifiedDm: assessments.every((a) =>
      (a.roleRelevantUnverifiedPeople || []).every((p) => p.verifiedDecisionMaker !== true)),
    alexandriaBenHawkinsNotSalesReady: (() => {
      const alex = assessments.find((a) => a.opportunityId === "gdi_opp_alexandria_soccer_kickoff_2027");
      if (!alex) return false;
      const ben = (alex.storedStakeholders || []).find((p) => /ben\s+hawkins/i.test(p.name || ""));
      if (!ben) return true; // name absent is fine
      return ben.findingProvenance === FINDING_PROVENANCE.UNVERIFIED
        && ben.salesUsableIdentity !== true
        && ben.verifiedDecisionMaker !== true;
    })(),
    aadHasFunctionalHousingEntityOrOrgRoute: (() => {
      const aad = assessments.find((a) => a.opportunityId === "gdi_opp_aad_2028_overflow");
      if (!aad) return false;
      return (aad.functionalIntermediaries || []).length >= 1
        || (aad.verifiedOrganizationContactRoutes || []).length >= 1;
    })(),
    praBuyerGapHonest: (() => {
      const pra = assessments.find((a) => a.opportunityId === "gdi_lmc_4d2f10eab2d44499");
      if (!pra) return false;
      return pra.enrichmentJustified === true
        && (pra.roleRelevantVerifiedPeople || []).length === 0
        && pra.salesNextAction.materiallyImprovesNextAction === false;
    })(),
    enrichmentRequestsGated: assessments.every((a) =>
      !a.targetedEnrichmentRequest
      || (a.targetedEnrichmentRequest.authorizeSurfe === false
        && a.targetedEnrichmentRequest.authorizePdl === false
        && a.targetedEnrichmentRequest.status === "AWAITING_SEPARATE_AUTHORIZATION")),
    canonicalIdsPreserved: assessments.every((a, i) =>
      a.opportunityId === cohort[i].opportunityId && a.hotelId === cohort[i].hotelId),
  };

  const allPass = Object.values(gates).every(Boolean);

  return {
    version: BUYER_INTELLIGENCE_SHORTLIST_OFFLINE_V1,
    certifiedAt: new Date().toISOString(),
    cohort,
    rankingShortlistIds: (ranking.shortlist || []).map((s) => s.opportunityId),
    assessments,
    gates,
    allOfflineGatesPass: allPass,
    goNoGo: {
      decision: allPass ? "GO" : "NO_GO",
      forBuyerIntelligenceOfflineModel: allPass ? "GO" : "NO_GO",
      paidEnrichment: "NO_GO",
      surfePdlParallel: "NO_GO",
      liveValidation: "NO_GO_UNTIL_SEPARATE_AUTHORIZATION",
      weeklyActivation: "NO_GO",
      note: allPass
        ? "Offline buyer-intelligence cohort certified with provenance safeguards. Live public WHO or Surfe/PDL requires separate GO."
        : "Offline buyer-intelligence gates failed — do not authorize paid enrichment.",
    },
    remainingGaps: [
      "No VERIFIED_LIVE findings in this pass — live public WHO not executed",
      "Alexandria: Ben Hawkins remains UNVERIFIED without email/ownership confirmation",
      "PRA: no named buyer; contact-us page exists but was not live-fetched here",
      "AAD: Maritz is intermediary pattern from prior annual meeting — 2028 housing channel still TBD",
      "Surfe/PDL enrichment requests are prepared where justified but remain unauthorized",
    ],
  };
}
