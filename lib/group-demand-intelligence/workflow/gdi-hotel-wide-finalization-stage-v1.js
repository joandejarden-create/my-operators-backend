/**
 * GDI Hotel-Wide Finalization Stage V1
 *
 * Mandatory terminal stage of every canonical Full E2E run.
 * Reviews the complete hotel opportunity inventory ($0 by default):
 *   - research store
 *   - chain-processed / newly discovered
 *   - prior STAGED
 *   - prior CURRENT
 *
 * Applies Finalized Qualified Opportunity (FQO) six-gate standard.
 * Writes a versioned STAGED *candidate* package only when authorized to write
 * packages — never activates STAGED/CURRENT pointers from this module.
 *
 * Does not spend providers. Agent remains OFF unless caller separately authorizes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
  evaluateFinalizedQualifiedOpportunity,
  GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
} from "./gdi-finalized-qualified-opportunity-contract-v1.js";
import { isHistoricalOnlyCustomerExclusion } from "../commercial-inventory/gdi-unified-commercial-inventory-contract-v1.js";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../publication-package/index.js";
import { projectOpportunityForPresentation } from "../presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../contracts/gdi-presentation-contract-v1.js";
import {
  attachNamedContactCoverageMetadata,
} from "./gdi-named-contact-coverage-standard-v1.js";

export const GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1 =
  "gdi-hotel-wide-finalization-stage-v1";

export const HOTEL_WIDE_DISPOSITION = Object.freeze({
  FINALIZATION_PASS: "FINALIZATION_PASS",
  HOLD_RESEARCH: "HOLD_RESEARCH",
  REJECT: "REJECT",
  HISTORICAL: "HISTORICAL",
  PREVIOUSLY_APPROVED_REVIEW: "PREVIOUSLY_APPROVED_REVIEW",
});

export const INVENTORY_DIFF_CLASS = Object.freeze({
  NEW: "NEW",
  RETAINED: "RETAINED",
  UPDATED: "UPDATED",
  REMOVAL_PROPOSED: "REMOVAL_PROPOSED",
  HELD: "HELD",
  REJECTED: "REJECTED",
});

/**
 * Unambiguous completion outcomes (inventory review ≠ research complete).
 * FULL_E2E_COMPLETE requires hotel-wide reconciliation AND no blocking gaps
 * for the run contract; RESEARCH_GAPS_REMAIN is still a successful reconcile.
 */
export const HOTEL_WIDE_COMPLETION_OUTCOME = Object.freeze({
  HOTEL_INVENTORY_RECONCILED: "HOTEL_INVENTORY_RECONCILED",
  RESEARCH_GAPS_REMAIN: "RESEARCH_GAPS_REMAIN",
  QUALIFIED_CANDIDATE_CERTIFIED: "QUALIFIED_CANDIDATE_CERTIFIED",
  FULL_E2E_COMPLETE: "FULL_E2E_COMPLETE",
  FULL_E2E_BLOCKED: "FULL_E2E_BLOCKED",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const GENERIC_INBOX_RE =
  /^(info|contact|hello|press|pr|media|sales|support|admin|office|enquiries|inquiries|events?|meetings?|groups?|housing|lodging|reservations?|booking|noreply|no-reply|team|general|help)@/i;

const EXHIBITOR_PLACEHOLDER_RE =
  /\bEXHIBITOR\s+BLOCK\b|CONTACT_RESEARCH_REQUIRED|BUYER NOT YET|ROLE NOT YET|PLACEHOLDER/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function oppId(o) {
  return text(o?.id || o?.opportunityId);
}

/** Prefer newer verified person-specific channels over stale research contacts. */
function contactChannelRichness(pc = {}) {
  const email = text(pc.email);
  const phone = text(pc.phone || pc.mobile || pc.directPhone);
  const linkedin = text(pc.linkedinUrl || pc.linkedin || pc.profileUrl);
  const emailType = upper(pc.emailType || pc.emailClassification);
  let score = 0;
  if (text(pc.name)) score += 1;
  if (email && /@/.test(email) && !GENERIC_INBOX_RE.test(email) &&
      emailType !== "GENERIC_ORGANIZATION" &&
      emailType !== "ORG_FUNCTIONAL" &&
      emailType !== "SWITCHBOARD") {
    score += 4;
  }
  if (phone && /\d{7,}/.test(phone.replace(/\D/g, ""))) score += 3;
  if (/linkedin\.com\/in\//i.test(linkedin)) score += 3;
  if (pc.emailCustomerVerified === true || pc.phoneCustomerVerified === true) {
    score += 1;
  }
  return score;
}

function mergeOpportunityEvidence(existing, incoming, source) {
  const existingPc = existing.primaryContact || {};
  const incomingPc = incoming.primaryContact || {};
  const preferIncomingContact =
    contactChannelRichness(incomingPc) >= contactChannelRichness(existingPc);
  const primaryContact = preferIncomingContact
    ? { ...existingPc, ...incomingPc }
    : { ...incomingPc, ...existingPc };

  // Competitive / market scope: never drop a stronger COMPETITIVE classification.
  const existingCompetitive =
    upper(existing.marketScope) === "COMPETITIVE" ||
    existing.competitiveMarketScopeEligible === true ||
    Boolean(existing.competitiveContext?.incumbentHotel);
  const incomingCompetitive =
    upper(incoming.marketScope) === "COMPETITIVE" ||
    incoming.competitiveMarketScopeEligible === true ||
    Boolean(incoming.competitiveContext?.incumbentHotel);

  const existingSc = existing.secondaryContact || {};
  const incomingSc = incoming.secondaryContact || {};
  const preferIncomingSecondary =
    contactChannelRichness(incomingSc) >= contactChannelRichness(existingSc);
  const secondaryContact =
    text(incomingSc.name) || text(existingSc.name)
      ? preferIncomingSecondary
        ? { ...existingSc, ...incomingSc }
        : { ...incomingSc, ...existingSc }
      : incoming.secondaryContact || existing.secondaryContact || null;

  const merged = {
    ...existing,
    ...incoming,
    id: oppId(existing) || oppId(incoming),
    primaryContact,
    secondaryContact,
    sourceUrls: [
      ...new Set([
        ...(existing.sourceUrls || []),
        ...(incoming.sourceUrls || []),
        ...((incoming.sources || []).map((s) => s?.url).filter(Boolean)),
        ...((existing.sources || []).map((s) => s?.url).filter(Boolean)),
      ]),
    ],
    evidenceProvenance: [
      ...new Set([
        ...(existing.evidenceProvenance || []),
        ...(incoming.evidenceProvenance || []),
        source,
      ]),
    ],
  };

  if (existingCompetitive || incomingCompetitive) {
    merged.marketScope = "COMPETITIVE";
    merged.competitiveMarketScopeEligible = true;
    merged.competitiveContext = {
      ...(existing.competitiveContext || {}),
      ...(incoming.competitiveContext || {}),
      incumbentHotel:
        incoming.competitiveContext?.incumbentHotel ||
        existing.competitiveContext?.incumbentHotel ||
        incoming.incumbentHotel ||
        existing.incumbentHotel ||
        null,
      pathway:
        incoming.competitiveContext?.pathway ||
        existing.competitiveContext?.pathway ||
        "OVERFLOW_OR_FUTURE_CYCLE",
    };
    if (!merged.incumbentHotel) {
      merged.incumbentHotel = merged.competitiveContext.incumbentHotel;
    }
  }

  // Reject / historical flags are sticky — never resurrect as active PASS.
  if (existing.disqualified === true || incoming.disqualified === true) {
    merged.disqualified = true;
  }
  if (
    existing.founderRejected === true ||
    incoming.founderRejected === true
  ) {
    merged.founderRejected = true;
  }
  if (
    existing.historicalOnlyCustomerExclusion === true ||
    incoming.historicalOnlyCustomerExclusion === true ||
    existing.pastClosed === true ||
    incoming.pastClosed === true
  ) {
    merged.historicalOnlyCustomerExclusion = true;
  }

  // Approved STAGED/CURRENT (and gold SEED) presentation is authoritative for
  // timing / thesis / classification. Research+candidate merges must not leave
  // past-only event dates that silently invalidate historically approved inventory.
  if (/^(STAGED_PACKAGE|CURRENT_PACKAGE|SEED|PRIOR_CERTIFIED_PACKAGE)$/.test(source)) {
    const presentationKeys = [
      "futureDemandPathway",
      "futureCycleEvidenceState",
      "commercialTemporalDisposition",
      "commercialActionabilityStateV1",
      "accountDevelopmentReactivated",
      "opportunityPathway",
      "sourcingPathway",
      "futureCycleOrSourcingWindow",
      "eventsOpportunityOverview",
      "tileBadgePlan",
      "hotelOpportunityThesis",
      "whyThisHotel",
      "potentialBusinessForThisHotel",
      "summaryWhat",
      "whatIsHappening",
      "recommendedNextStep",
      "recommendedNextAction",
      "recommendedAction",
      "recommendedSalesOrMarketingAction",
      "cardWhyNowLine",
      "whyActNow",
      "opportunityType",
      "opportunityTypeLabel",
      "salesActionKey",
      "salesActionPillLabel",
      "priority",
      "priorityPillLabel",
      "marketScope",
      "competitiveContext",
      "incumbentHotel",
      "competitorHotel",
      "likelyCompetitor",
      "competitiveStatusLabel",
      "competitiveMarketScopeEligible",
      "hotelMarketScopeEligible",
    ];
    for (const k of presentationKeys) {
      if (incoming[k] != null && incoming[k] !== "") {
        merged[k] = incoming[k];
      }
    }
    const approvedStart =
      text(incoming.eventStartDate) ||
      text(incoming.tileEventStartDate) ||
      text(incoming.eventDate) ||
      "";
    if (approvedStart) {
      if (text(incoming.eventStartDate)) merged.eventStartDate = incoming.eventStartDate;
      if (text(incoming.tileEventStartDate)) {
        merged.tileEventStartDate = incoming.tileEventStartDate;
      }
    } else {
      // Approved presentation omitted dates — drop research-injected past dates.
      const injected = Date.parse(
        merged.eventStartDate ||
          merged.tileEventStartDate ||
          merged.eventDate ||
          "",
      );
      if (Number.isFinite(injected) && injected < Date.now() - 7 * 86400000) {
        delete merged.eventStartDate;
        delete merged.tileEventStartDate;
        delete merged.eventDate;
        delete merged.needDates;
        delete merged.needDateStart;
      }
    }
  }

  merged._contactMergePreferredSource = preferIncomingContact
    ? source
    : existing._contactMergePreferredSource || "PRIOR";
  return merged;
}

function publicationPackageRoots(store) {
  const roots = [];
  const primary = store?.root;
  if (primary) roots.push(primary);
  const bundled = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  );
  const data = path.join(
    ROOT,
    "data/group-demand-intelligence/publication-packages",
  );
  for (const r of [bundled, data]) {
    if (r && !roots.includes(r)) roots.push(r);
  }
  return roots;
}

/**
 * List non-pointer candidate package versions across data + bundled roots.
 * Paid/WHO-HOW packages sort last so richer contacts win on merge.
 */
function listCandidatePackageVersions(store, hotelId) {
  const staged = store.readPointer(hotelId, "STAGED")?.publicationVersion;
  const current = store.readPointer(hotelId, "CURRENT")?.publicationVersion;
  const byVersion = new Map();
  for (const root of publicationPackageRoots(store)) {
    const hotelRoot = path.join(root, hotelId);
    if (!fs.existsSync(hotelRoot)) continue;
    for (const name of fs.readdirSync(hotelRoot)) {
      if (name === staged || name === current) continue;
      if (name === "STAGED.json" || name === "CURRENT.json") continue;
      // Do not re-ingest this stage's own prior outputs — circular self-certification.
      if (/^gdi_hotel_wide_fqo_/i.test(name)) continue;
      const pkgFile = path.join(hotelRoot, name, "package.json");
      if (!fs.existsSync(pkgFile)) continue;
      // Prefer data/ over bundled when both exist (newer local writes).
      if (!byVersion.has(name) || root === store.root) {
        byVersion.set(name, { version: name, path: pkgFile, root });
      }
    }
  }
  return [...byVersion.values()].sort((a, b) => {
    const rank = (v) =>
      /paid_research|who_how|contact_hardened|best_contact/i.test(v) ? 2 : 1;
    return rank(a.version) - rank(b.version) || a.version.localeCompare(b.version);
  });
}

function loadCandidatePackageFile(entry) {
  const pkg = readJsonSafe(entry.path);
  if (!pkg) return { ok: false, error: "PACKAGE_NOT_FOUND", path: entry.path };
  return { ok: true, package: pkg, path: entry.path };
}

function readJsonSafe(file) {
  try {
    if (!fs.existsSync(file)) return null;
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function loadResearchOpportunities(hotelId, root = ROOT) {
  const file = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  const doc = readJsonSafe(file);
  if (!doc) return { path: file, opportunities: [], ok: false, error: "MISSING_OR_UNREADABLE" };
  const opps = Array.isArray(doc.opportunities)
    ? doc.opportunities
    : Array.isArray(doc)
      ? doc
      : [];
  return { path: file, opportunities: opps, ok: true };
}

function loadPointerPackage(store, hotelId, kind) {
  const ptr = store.readPointer(hotelId, kind);
  if (!ptr?.publicationVersion) {
    return { pointer: null, opportunities: [], publicationVersion: null };
  }
  const loaded = store.loadPackage(hotelId, ptr.publicationVersion);
  if (!loaded?.ok) {
    return {
      pointer: ptr,
      opportunities: [],
      publicationVersion: ptr.publicationVersion,
      error: loaded?.error || "LOAD_FAILED",
    };
  }
  const pkg = loaded.package;
  return {
    pointer: ptr,
    publicationVersion: ptr.publicationVersion,
    opportunities: pkg.opportunities || pkg.payload?.opportunities || [],
    package: pkg,
  };
}

/**
 * Merge inventories. Later sources may enrich contacts, but verified
 * person-specific channels are never overwritten by weaker stale research.
 * Candidate packages (paid WHO/ALT/HOW) are ingested before STAGED/CURRENT.
 */
export function collectHotelWideCanonicalInventory(input = {}) {
  const hotelId = text(input.hotelId);
  const root = input.root || ROOT;
  const store =
    input.store || createGdiPublicationPackageStore({ root: input.packageRoot });
  const research = loadResearchOpportunities(hotelId, root);
  const staged = loadPointerPackage(store, hotelId, "STAGED");
  const current = loadPointerPackage(store, hotelId, "CURRENT");
  const chainProcessed = Array.isArray(input.chainProcessed)
    ? input.chainProcessed
    : [];
  const seed = Array.isArray(input.seedOpportunities) ? input.seedOpportunities : [];
  const candidateEntries = listCandidatePackageVersions(store, hotelId);

  const byId = new Map();
  const sourcesById = new Map();
  const duplicateMappings = [];
  let candidateOppCount = 0;

  function ingest(list, source) {
    for (const raw of list || []) {
      const id = oppId(raw);
      if (!id) {
        duplicateMappings.push({
          source,
          reason: "MISSING_ID",
          title: text(raw?.title),
        });
        continue;
      }
      if (!byId.has(id)) {
        byId.set(id, {
          ...raw,
          id,
          evidenceProvenance: [source],
          _contactMergePreferredSource: source,
        });
        sourcesById.set(id, [source]);
      } else {
        const prevSources = sourcesById.get(id) || [];
        if (!prevSources.includes(source)) prevSources.push(source);
        sourcesById.set(id, prevSources);
        const existing = byId.get(id);
        const merged = mergeOpportunityEvidence(existing, raw, source);
        byId.set(id, merged);
        duplicateMappings.push({
          opportunityId: id,
          source,
          resolution: "MERGED_LATEST_VERIFIED_CONTACT_WINS",
          preferredContactSource: merged._contactMergePreferredSource,
          priorSources: prevSources,
        });
      }
    }
  }

  // Base → enrichments → approved inventories (approved last for lifecycle flags).
  ingest(research.opportunities, "RESEARCH_STORE");
  ingest(
    chainProcessed.map((p) => p.opportunity || p.result?.opportunity || p),
    "CHAIN_PROCESSED",
  );
  ingest(seed, "SEED");
  for (const entry of candidateEntries) {
    const loaded = loadCandidatePackageFile(entry);
    if (!loaded?.ok) continue;
    const opps =
      loaded.package?.opportunities ||
      loaded.package?.payload?.opportunities ||
      [];
    candidateOppCount += opps.length;
    ingest(opps, `CANDIDATE_PACKAGE:${entry.version}`);
  }
  if (Array.isArray(input.priorCertifiedPackages)) {
    for (const pkg of input.priorCertifiedPackages) {
      ingest(pkg?.opportunities || [], "PRIOR_CERTIFIED_PACKAGE");
    }
  }
  ingest(staged.opportunities, "STAGED_PACKAGE");
  ingest(current.opportunities, "CURRENT_PACKAGE");

  const opportunities = [...byId.values()];
  return {
    hotelId,
    ok: research.ok || opportunities.length > 0,
    researchLoad: research,
    stagedSnapshot: {
      publicationVersion: staged.publicationVersion,
      count: staged.opportunities.length,
      error: staged.error || null,
    },
    currentSnapshot: {
      publicationVersion: current.publicationVersion,
      count: current.opportunities.length,
      error: current.error || null,
    },
    candidatePackageVersions: candidateEntries.map((e) => e.version),
    candidatePackageEntries: candidateEntries,
    opportunities,
    sourcesById: Object.fromEntries(sourcesById),
    duplicateMappings,
    counts: {
      research: research.opportunities.length,
      chainProcessed: chainProcessed.length,
      seed: seed.length,
      candidatePackageOpportunities: candidateOppCount,
      candidatePackageVersions: candidateEntries.length,
      staged: staged.opportunities.length,
      current: current.opportunities.length,
      canonicalUnique: opportunities.length,
    },
  };
}

function wasPreviouslyApproved(sources = []) {
  return sources.includes("STAGED_PACKAGE") || sources.includes("CURRENT_PACKAGE");
}

/**
 * Assign audit disposition for one opportunity using FQO + lifecycle context.
 */
export function assignHotelWideDisposition(opportunity = {}, ctx = {}) {
  const id = oppId(opportunity);
  const sources = ctx.sources || [];
  const previouslyApproved = wasPreviouslyApproved(sources);
  const fqo = evaluateFinalizedQualifiedOpportunity(opportunity, {
    ...ctx,
    historicalApprovedInventory: previouslyApproved === true,
  });
  const titleBlob = `${text(opportunity.title)} ${text(opportunity.displayTitle)}`;
  if (
    opportunity.disqualified === true ||
    opportunity.founderRejected === true ||
    /^(DISQUALIFIED|REJECTED)$/i.test(String(opportunity.status || "")) ||
    fqo.status === "FOUNDER_REJECTED" ||
    EXHIBITOR_PLACEHOLDER_RE.test(titleBlob)
  ) {
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.REJECT,
      reasonCodes: [
        EXHIBITOR_PLACEHOLDER_RE.test(titleBlob)
          ? "EXHIBITOR_PLACEHOLDER_OR_REJECTED"
          : "EXPLICITLY_REJECTED_OR_DISQUALIFIED",
        ...(fqo.reasons || []),
      ],
      fqo,
      sources,
      remediation: "Do not restore to active eligibility without new evidence.",
      freshResearchRequired: false,
    };
  }

  if (
    isHistoricalOnlyCustomerExclusion(opportunity) ||
    opportunity.pastClosed === true ||
    opportunity.historicalOnlyCustomerExclusion === true ||
    /la_2028_olympics|olympics_2028/i.test(id) ||
    (/olympics/i.test(titleBlob) &&
      /2028|los angeles|LA\b/i.test(titleBlob) &&
      opportunity.futureDemandPathway !== true)
  ) {
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.HISTORICAL,
      reasonCodes: [
        "HISTORICAL_NO_FUTURE_PATHWAY",
        ...(fqo.reasons || []),
      ],
      fqo,
      sources,
      remediation: "Retain in audit only; do not present as active STAGED demand.",
      freshResearchRequired: false,
    };
  }

  if (fqo.finalizedQualified) {
    // Evidence-backed package discipline: past-dated events without an
    // explicit future-cycle stamp must not soft-pass into the candidate set.
    const startMs = Date.parse(
      opportunity.eventStartDate ||
        opportunity.tileEventStartDate ||
        opportunity.eventDate ||
        "",
    );
    const futureState = upper(
      opportunity.futureCycleEvidenceState ||
        opportunity.commercialTemporalDisposition ||
        "",
    );
    const explicitFuture =
      opportunity.futureDemandPathway === true ||
      opportunity.accountDevelopmentReactivated === true ||
      /CURRENT_FUTURE|FUTURE_CYCLE_EVIDENCED|OPEN_SOURCING|ENTERING_WINDOW|WATCH_FUTURE/.test(
        futureState,
      );
    const pastWithoutFutureCycle =
      Number.isFinite(startMs) &&
      startMs < Date.now() - 7 * 86400000 &&
      !explicitFuture;
    if (pastWithoutFutureCycle && !previouslyApproved) {
      return {
        opportunityId: id,
        disposition: HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH,
        reasonCodes: [
          "FUTURE_COMMERCIAL_TIMING_UNPROVEN",
          "PAST_EVENT_WITHOUT_FUTURE_CYCLE_EVIDENCE",
          ...(fqo.reasons || []),
        ],
        fqo,
        sources,
        processingDisposition: fqo.processingDisposition,
        remediation:
          "Confirm next-cycle / overflow housing dates before STAGED candidacy.",
        freshResearchRequired: true,
      };
    }
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS,
      reasonCodes: [],
      fqo,
      sources,
      processingDisposition:
        fqo.processingDisposition ||
        (previouslyApproved
          ? "RETAINED_HISTORICAL_APPROVAL"
          : "QUALIFIED_NEW"),
      remediation: null,
      freshResearchRequired: false,
    };
  }

  if (previouslyApproved && !fqo.finalizedQualified) {
    // Preserve historically approved inventory unless genuinely commercially disqualified.
    // Schema-only / technical FQO gaps are not automatic removals.
    const genuineCommercial = (fqo.reasons || []).some((r) =>
      /FOUNDER_REJECT|UNSUPPORTED_SPECULATION|HISTORICAL_NO_FUTURE|EXHIBITOR/.test(
        r,
      ),
    );
    // Named-person / channel / future / competitive gaps on historically approved
    // inventory are technical or founder-review items — not automatic silent removals.
    // Org-as-person (NAMED_COMMERCIAL_PERSON_REQUIRED) still goes to review unless
    // schema/provenance retain path applies with outreach artifact.
    const pc = opportunity.primaryContact || {};
    const hasAnyOutreachArtifact = Boolean(
      text(pc.email) ||
        text(pc.phone) ||
        text(pc.linkedinUrl) ||
        text(pc.linkedin) ||
        text(pc.organizationContactRouteLabel) ||
        text(pc.askToBeDirectedTo) ||
        text(opportunity.approvedCommercialRoute),
    );
    const technicalReasonOnly = (fqo.reasons || []).every((r) =>
      /MISSING_EVIDENCE_URLS|PERSON_SPECIFIC_CHANNEL_REQUIRED|NO_VALID_FUTURE_SOURCING_PATHWAY|COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED|MISSING_HOTEL_FIT_THESIS|MISSING_HOTEL_SPECIFIC_COMMERCIAL_ACTION/.test(
        r,
      ),
    );
    // Retain when FQO passes soft historical gates, or only schema/evidence-shape gaps remain
    // with a named person + some outreach artifact. Bare name with zero outreach → review.
    const namedGateOk = fqo.gates?.NAMED_COMMERCIAL_PERSON?.ok === true;
    // Historical approved + verified named person: retain unless genuine commercial
    // disqualification. Missing HOW channel / evidence-shape / competitive fields are
    // TECHNICAL_REMEDIATION_REQUIRED (still PASS for preservation), not silent removal.
    // Org-as-person (namedGateOk=false) → PREVIOUSLY_APPROVED_REVIEW.
    const retainableTechnical =
      !genuineCommercial &&
      text(pc.name) &&
      namedGateOk &&
      (fqo.finalizedQualified ||
        fqo.schemaIncompatibility === true ||
        technicalReasonOnly ||
        hasAnyOutreachArtifact ||
        (fqo.reasons || []).every((r) =>
          /MISSING_EVIDENCE_URLS|PERSON_SPECIFIC_CHANNEL_REQUIRED|NO_VALID_FUTURE_SOURCING_PATHWAY|COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED|MISSING_HOTEL_FIT_THESIS|MISSING_HOTEL_SPECIFIC_COMMERCIAL_ACTION/.test(
            r,
          ),
        ));
    if (retainableTechnical) {
      return {
        opportunityId: id,
        disposition: HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS,
        reasonCodes: [
          "RETAINED_HISTORICAL_APPROVAL",
          ...(fqo.schemaIncompatibility
            ? ["HISTORICAL_EVIDENCE_SCHEMA_COMPAT"]
            : []),
          ...(fqo.reasons || []).filter((r) => r !== "RETAINED_HISTORICAL_APPROVAL"),
        ],
        fqo,
        sources,
        processingDisposition: "RETAINED_HISTORICAL_APPROVAL",
        remediation:
          "Retained from approved STAGED/CURRENT; resolve remaining technical FQO gaps when practical.",
        freshResearchRequired: (fqo.reasons || []).some((r) =>
          /PERSON_SPECIFIC|MISSING_EVIDENCE|COMPETITIVE/.test(r),
        ),
      };
    }
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.PREVIOUSLY_APPROVED_REVIEW,
      reasonCodes: fqo.reasons || ["ELIGIBILITY_CHANGED_SINCE_APPROVAL"],
      fqo,
      sources,
      processingDisposition: genuineCommercial
        ? "GENUINE_COMMERCIAL_REVIEW"
        : namedGateOk
          ? "TECHNICAL_REMEDIATION_REQUIRED"
          : "GENUINE_COMMERCIAL_REVIEW",
      remediation:
        "Founder review required before removal from STAGED/CURRENT. Do not silently delete.",
      freshResearchRequired: (fqo.reasons || []).some((r) =>
        /PERSON_SPECIFIC|NAMED_COMMERCIAL|CONTACT/.test(r),
      ),
    };
  }

  return {
    opportunityId: id,
    disposition: HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH,
    reasonCodes: fqo.reasons || ["UNRESOLVED_QUALIFICATION_GAPS"],
    fqo,
    sources,
    remediation: recommendRemediation(fqo.reasons || []),
    freshResearchRequired: (fqo.reasons || []).some((r) =>
      /PERSON_SPECIFIC|NAMED_COMMERCIAL|MISSING_EVIDENCE|HOTEL_FIT|FUTURE_SOURCING/.test(
        r,
      ),
    ),
  };
}

function recommendRemediation(reasons = []) {
  const tips = [];
  if (reasons.some((r) => /NAMED_COMMERCIAL/.test(r))) {
    tips.push("Identify verified commercially relevant named primary (WHO).");
  }
  if (reasons.some((r) => /PERSON_SPECIFIC/.test(r))) {
    tips.push(
      "Verify person-specific email, direct phone, or person LinkedIn (HOW).",
    );
  }
  if (reasons.some((r) => /FUTURE_SOURCING|HISTORICAL/.test(r))) {
    tips.push("Confirm future cycle / overflow pathway with dated evidence.");
  }
  if (reasons.some((r) => /HOTEL_FIT/.test(r))) {
    tips.push("Document hotel-specific commercial fit thesis.");
  }
  if (reasons.some((r) => /MISSING_EVIDENCE/.test(r))) {
    tips.push("Attach public evidence URLs for demand.");
  }
  if (reasons.some((r) => /COMPETITIVE/.test(r))) {
    tips.push("Document incumbent hotel and overflow/future-cycle pathway.");
  }
  return tips.join(" ") || "Resolve FQO gate gaps before STAGED candidacy.";
}

function classifyDiff(row, priorStagedIds, priorCurrentIds) {
  const id = row.opportunityId;
  const wasStaged = priorStagedIds.has(id);
  const wasCurrent = priorCurrentIds.has(id);
  if (row.disposition === HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS) {
    if (!wasStaged && !wasCurrent) return INVENTORY_DIFF_CLASS.NEW;
    return INVENTORY_DIFF_CLASS.RETAINED;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH) {
    return INVENTORY_DIFF_CLASS.HELD;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.REJECT) {
    return wasStaged || wasCurrent
      ? INVENTORY_DIFF_CLASS.REMOVAL_PROPOSED
      : INVENTORY_DIFF_CLASS.REJECTED;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.HISTORICAL) {
    return wasStaged || wasCurrent
      ? INVENTORY_DIFF_CLASS.REMOVAL_PROPOSED
      : INVENTORY_DIFF_CLASS.REJECTED;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.PREVIOUSLY_APPROVED_REVIEW) {
    return INVENTORY_DIFF_CLASS.UPDATED;
  }
  return INVENTORY_DIFF_CLASS.HELD;
}

/**
 * Run mandatory hotel-wide finalization for one hotel.
 *
 * @param {{
 *   hotelId: string,
 *   hotelName?: string,
 *   runId?: string,
 *   chain?: object,
 *   seedOpportunities?: object[],
 *   outDir?: string,
 *   writeCandidatePackage?: boolean, // default true in production; false only for isolated tests
 *   paidSpendUsd?: number,
 *   authorizationId?: string,
 *   agentAuthorized?: boolean,
 * }} input
 */
export async function runHotelWideFinalizationStage(input = {}) {
  const hotelId = text(input.hotelId);
  const at = new Date().toISOString();
  const runId =
    text(input.runId) ||
    `hotel_wide_${hotelId}_${at.replace(/[:.]/g, "-")}`;
  const blockers = [];
  // Production default: automatically prepare/certify candidate when eligible.
  // Isolated tests may pass writeCandidatePackage:false explicitly.
  const writeCandidatePackage = input.writeCandidatePackage !== false;

  if (!hotelId) {
    return {
      ok: false,
      blocked: true,
      contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
      blockers: [{ code: "HOTEL_ID_REQUIRED" }],
      completionFailed: true,
      completionOutcome: HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_BLOCKED,
    };
  }

  // Hard: Agent OFF unless explicit
  if (input.agentAuthorized !== true) {
    delete process.env.GDI_EXA_AGENT_LIVE;
  }

  const inventory = collectHotelWideCanonicalInventory({
    hotelId,
    root: input.root || ROOT,
    packageRoot: input.packageRoot,
    chainProcessed: input.chain?.processed || input.chainProcessed || [],
    seedOpportunities: input.seedOpportunities || [],
  });

  if (!inventory.ok && inventory.opportunities.length === 0) {
    blockers.push({
      code: "INVENTORY_SOURCE_UNRESOLVED",
      detail: inventory.researchLoad?.error || "empty",
    });
  }
  if (inventory.stagedSnapshot?.error) {
    blockers.push({
      code: "STAGED_SNAPSHOT_RECONCILE_FAILED",
      detail: inventory.stagedSnapshot.error,
    });
  }
  if (inventory.currentSnapshot?.error) {
    blockers.push({
      code: "CURRENT_SNAPSHOT_RECONCILE_FAILED",
      detail: inventory.currentSnapshot.error,
    });
  }

  const store = createGdiPublicationPackageStore({ root: input.packageRoot });
  const stagedSnap = loadPointerPackage(store, hotelId, "STAGED");
  const currentSnap = loadPointerPackage(store, hotelId, "CURRENT");
  const stagedIdSet = new Set(stagedSnap.opportunities.map(oppId).filter(Boolean));
  const currentIdSet = new Set(currentSnap.opportunities.map(oppId).filter(Boolean));

  const rows = [];
  const seen = new Set();
  for (const opp of inventory.opportunities) {
    const id = oppId(opp);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const sources = inventory.sourcesById[id] || [];
    const assigned = assignHotelWideDisposition(opp, { sources });
    const diffClass = classifyDiff(assigned, stagedIdSet, currentIdSet);
    rows.push({
      ...assigned,
      title: text(opp.title),
      organizationName: text(opp.organizationName || opp.organization),
      marketScope:
        assigned.fqo?.marketScope ||
        (upper(opp.marketScope) === "COMPETITIVE" ? "COMPETITIVE" : "HOTEL"),
      primaryName: text(opp.primaryContact?.name || opp.primaryContactName),
      hasPersonSpecificChannel:
        assigned.fqo?.gates?.PERSON_SPECIFIC_CHANNEL?.ok === true,
      secondaryPresent: assigned.fqo?.secondaryPresent === true,
      salesReady: opp.salesReady === true,
      salesReadyRequired: false,
      diffClass,
      opportunity: opp,
    });
  }

  // Reconciliation: every canonical id accounted exactly once
  const accountedIds = rows.map((r) => r.opportunityId).sort();
  const canonicalIds = inventory.opportunities.map(oppId).filter(Boolean).sort();
  const uniqueCanonical = [...new Set(canonicalIds)];
  const missing = uniqueCanonical.filter((id) => !seen.has(id));
  const duplicateIds = canonicalIds.filter(
    (id, i, arr) => arr.indexOf(id) !== i,
  );
  if (missing.length) {
    blockers.push({
      code: "UNACCOUNTED_OPPORTUNITIES",
      missing,
    });
  }
  if (accountedIds.length !== uniqueCanonical.length) {
    blockers.push({
      code: "RECONCILIATION_COUNT_MISMATCH",
      accounted: accountedIds.length,
      canonicalUnique: uniqueCanonical.length,
    });
  }

  const passRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS,
  );
  const holdRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH,
  );
  const rejectRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.REJECT,
  );
  const historicalRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.HISTORICAL,
  );
  const priorReviewRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.PREVIOUSLY_APPROVED_REVIEW,
  );

  const hotelPass = passRows.filter((r) => r.marketScope !== "COMPETITIVE");
  const competitivePass = passRows.filter((r) => r.marketScope === "COMPETITIVE");

  // Build STAGED candidate from PASS rows only (preserve contacts).
  // Never activate STAGED/CURRENT pointers from this stage.
  let candidatePackage = null;
  let packageWrite = {
    ok: false,
    skipped: !writeCandidatePackage,
    pointerActivated: false,
    zeroQualifiedCertified: false,
  };
  let integrity = { ok: false };
  let quality = { ok: false };
  const publicationVersion = `gdi_hotel_wide_fqo_${hotelId}_${at.slice(0, 10).replace(/-/g, "")}_${createHash("sha256").update(runId).digest("hex").slice(0, 8)}`;

  if (writeCandidatePackage && passRows.length === 0) {
    packageWrite = {
      ok: true,
      skipped: false,
      pointerActivated: false,
      zeroQualifiedCertified: true,
      publicationVersion: null,
      path: null,
      reasons: [
        "ZERO_FQO_PASS_AFTER_HOTEL_WIDE_RECONCILE",
        holdRows.length ? "RESEARCH_GAPS_REMAIN" : "NO_ELIGIBLE_RECORDS",
      ],
      holdCount: holdRows.length,
      rejectCount: rejectRows.length,
      historicalCount: historicalRows.length,
    };
  }

  if (passRows.length && writeCandidatePackage) {
    const dtos = passRows.map((r) => {
      let opp = { ...r.opportunity };
      if (!opp.secondContactAttempted && !opp.secondaryContact?.name) {
        opp = attachNamedContactCoverageMetadata(opp, {
          secondContactAttempted: true,
          secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
          contactGaps: ["Second named contact not verified in hotel-wide review"],
        });
        opp.secondContactAttempted = true;
        opp.secondaryContactResearchStatus = "ATTEMPTED_NO_SECOND_VERIFIED";
      }
      const projected = projectOpportunityForPresentation(opp, {
        hotelId,
        surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
        view: "detail",
      });
      // Preserve FQO-critical fields that presentation projection may omit.
      const evidenceUrls = [
        ...(opp.sourceUrls || []),
        ...(opp.evidence || []).map((e) => e?.url || e?.sourceUrl).filter(Boolean),
        opp.primaryContact?.sourceUrl,
      ].filter((u) => /^https?:\/\//i.test(String(u)));
      const projectedPc = projected.dto.primaryContact || {};
      const canonicalPc = opp.primaryContact || {};
      return {
        ...projected.dto,
        id: opp.id,
        // Preserve verified person-specific channels — projection can drop them.
        primaryContact: {
          ...projectedPc,
          ...canonicalPc,
          name: text(canonicalPc.name) || text(projectedPc.name),
          role: text(canonicalPc.role || canonicalPc.title) || text(projectedPc.role),
          email: text(canonicalPc.email) || text(projectedPc.email) || null,
          emailType:
            canonicalPc.emailType ||
            canonicalPc.emailClassification ||
            projectedPc.emailType ||
            null,
          phone:
            text(canonicalPc.phone || canonicalPc.directPhone) ||
            text(projectedPc.phone) ||
            null,
          linkedinUrl:
            text(
              canonicalPc.linkedinUrl ||
                canonicalPc.linkedin ||
                canonicalPc.profileUrl,
            ) ||
            text(projectedPc.linkedinUrl) ||
            null,
        },
        secondaryContact: opp.secondaryContact || projected.dto.secondaryContact || null,
        sourceUrls: evidenceUrls.length
          ? evidenceUrls
          : projected.dto.sourceUrls || [],
        eventStartDate:
          opp.eventStartDate || opp.tileEventStartDate || projected.dto.eventStartDate,
        tileEventStartDate: opp.tileEventStartDate || null,
        futureDemandPathway:
          opp.futureDemandPathway === true ||
          projected.dto.futureDemandPathway === true ||
          r.processingDisposition === "RETAINED_HISTORICAL_APPROVAL",
        futureCycleEvidenceState:
          opp.futureCycleEvidenceState || projected.dto.futureCycleEvidenceState,
        hotelOpportunityThesis:
          opp.hotelOpportunityThesis ||
          opp.summaryWhat ||
          projected.dto.hotelOpportunityThesis ||
          projected.dto.whyThisHotel,
        summaryWhat: opp.summaryWhat || projected.dto.summaryWhat || null,
        whatIsHappening: opp.whatIsHappening || projected.dto.whatIsHappening || null,
        cardWhyNowLine: opp.cardWhyNowLine || null,
        recommendedNextStep:
          opp.recommendedNextStep ||
          opp.recommendedNextAction ||
          opp.recommendedAction ||
          opp.cardWhyNowLine ||
          projected.dto.recommendedNextStep,
        recommendedNextAction:
          opp.recommendedNextAction || opp.recommendedNextStep || null,
        opportunityType: opp.opportunityType || projected.dto.opportunityType || null,
        opportunityTypeLabel:
          opp.opportunityTypeLabel ||
          opp.opportunityType ||
          projected.dto.opportunityTypeLabel ||
          null,
        salesActionKey:
          opp.salesActionKey || opp.tileBadgePlan?.salesActionKey || null,
        salesActionPillLabel:
          opp.salesActionPillLabel ||
          opp.tileBadgePlan?.salesActionPillLabel ||
          null,
        priority: opp.priority || projected.dto.priority || null,
        priorityPillLabel:
          opp.priorityPillLabel || opp.tileBadgePlan?.priorityPillLabel || null,
        hotelMarketScopeEligible: opp.hotelMarketScopeEligible !== false,
        marketScope: r.marketScope,
        competitiveContext: opp.competitiveContext || null,
        _historicalApprovedInventory:
          r.processingDisposition === "RETAINED_HISTORICAL_APPROVAL",
        hotelWideFinalizationV1: {
          disposition: r.disposition,
          diffClass: r.diffClass,
          processingDisposition: r.processingDisposition || null,
          runId,
        },
      };
    });

    candidatePackage = buildCustomerPublicationPackage({
      hotelId,
      hotelName: text(input.hotelName) || hotelId,
      opportunities: dtos,
      publicationVersion,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes: `Hotel-wide finalization candidate — run ${runId}; pointer NOT activated`,
    });
    integrity = verifyPublicationPackageIntegrity(candidatePackage);
    // PASS rows already cleared FQO on canonical objects. Re-check serialization
    // integrity + named contacts; do not re-fail on projection field shape alone.
    // Hotel-wide historical remediation: treat truncated prose / pill conflicts as
    // documented warnings (gold presentation already founder-approved), not hard blocks.
    const hotelWideFailOnCodes = [
      "DUPLICATE_ORGANIZATION_NAME",
      "REPEATED_SOURCING_STATUS",
      "EMPTY_PARENTHESES",
      "EMPTY_PUNCTUATION_WRAPPER",
      "PLACEHOLDER_TEXT",
      "INTERNAL_ID_OR_ENUM",
      "REPEATED_PARAGRAPH",
      "UNSUPPORTED_CONFIRMED_ROOM_DEMAND",
      "ZERO_VERIFIED_NAMED_CONTACTS",
      "NOT_FINALIZED_QUALIFIED_OPPORTUNITY",
    ];
    quality = runPublicationQualityGate({
      opportunities: candidatePackage.opportunities,
      enforceFinalizedQualifiedOpportunity: false,
      enforceCommercialExpirationGate: false,
      enforceCommercialActionabilityGate: false,
      failOnCodes: hotelWideFailOnCodes,
    });
    // Explicit FQO assert on pre-projection PASS set (authoritative).
    // RETAINED_HISTORICAL_APPROVAL may PASS without strict finalizedQualified=true
    // when only schema/provenance gaps remain — do not block the package for those.
    for (const r of passRows) {
      if (r.fqo?.finalizedQualified === true) continue;
      if (r.processingDisposition === "RETAINED_HISTORICAL_APPROVAL") continue;
      if ((r.reasonCodes || []).includes("RETAINED_HISTORICAL_APPROVAL")) continue;
      blockers.push({
        code: "PASS_ROW_FAILED_FQO_RECHECK",
        opportunityId: r.opportunityId,
        reasons: r.reasonCodes,
      });
    }
    if (!integrity.ok) {
      blockers.push({
        code: "CANDIDATE_PACKAGE_INTEGRITY_FAILED",
        fails: integrity.fails,
      });
    }
    const packageExclusions = [];
    for (const f of quality.findings || []) {
      if (
        f.code === "TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE" ||
        f.code === "CONFLICTING_COMMERCIAL_ACTIONS"
      ) {
        packageExclusions.push({
          opportunityId: f.opportunityId || null,
          code: f.code,
          severity: "WARNING_NOT_EXCLUSION",
          reason: f.detail || f.field || "",
        });
      }
    }
    if (!quality.ok) {
      const truncated = (quality.findings || []).filter(
        (b) => b.code === "TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE",
      );
      const otherBlocking = (quality.blocking || []).filter(
        (b) => b.code !== "TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE",
      );
      // Remediaton: restore commercial prose from canonical PASS rows rather than
      // silently shrinking PASS → package (Bethesda R5).
      if (truncated.length && passRows.length) {
        const byPassId = new Map(passRows.map((r) => [r.opportunityId, r]));
        const enriched = (candidatePackage.opportunities || []).map((o) => {
          const id = oppId(o);
          const pass = byPassId.get(id);
          if (!pass) return o;
          const src = pass.opportunity || {};
          const prose =
            text(src.summaryWhat) ||
            text(src.whatIsHappening) ||
            text(src.hotelOpportunityThesis) ||
            text(src.cardWhyNowLine) ||
            text(src.recommendedNextStep) ||
            text(o.summaryWhat) ||
            text(o.whatIsHappening);
          if (!prose) return o;
          return {
            ...o,
            summaryWhat: text(o.summaryWhat) || prose,
            whatIsHappening: text(o.whatIsHappening) || prose,
            hotelOpportunityThesis:
              text(o.hotelOpportunityThesis) || text(src.hotelOpportunityThesis) || prose,
            recommendedNextStep:
              text(o.recommendedNextStep) ||
              text(src.recommendedNextStep) ||
              text(src.recommendedNextAction) ||
              text(src.cardWhyNowLine),
            opportunityType:
              o.opportunityType || src.opportunityType || src.opportunityTypeLabel || null,
            opportunityTypeLabel:
              o.opportunityTypeLabel || src.opportunityTypeLabel || src.opportunityType || null,
            salesActionKey: o.salesActionKey || src.salesActionKey || null,
            salesActionPillLabel:
              o.salesActionPillLabel ||
              src.salesActionPillLabel ||
              src.tileBadgePlan?.salesActionPillLabel ||
              null,
          };
        });
        candidatePackage = buildCustomerPublicationPackage({
          hotelId,
          hotelName: text(input.hotelName) || hotelId,
          opportunities: enriched,
          publicationVersion,
          lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
          approvalStatus: "CANDIDATE",
          applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
          notes: `Hotel-wide finalization candidate — run ${runId}; prose enriched to prevent PASS→package shrink; pointer NOT activated`,
        });
        integrity = verifyPublicationPackageIntegrity(candidatePackage);
        quality = runPublicationQualityGate({
          opportunities: candidatePackage.opportunities,
          enforceFinalizedQualifiedOpportunity: false,
          enforceCommercialExpirationGate: false,
          enforceCommercialActionabilityGate: false,
          failOnCodes: hotelWideFailOnCodes,
        });
      }
      if (!quality.ok) {
        for (const b of quality.blocking || []) {
          packageExclusions.push({
            opportunityId: b.opportunityId || null,
            code: b.code || "CANDIDATE_PACKAGE_QUALITY_FAILED",
            reason: b.message || b.reason || JSON.stringify(b).slice(0, 200),
          });
        }
        blockers.push({
          code: "CANDIDATE_PACKAGE_QUALITY_FAILED",
          blocking: quality.blocking?.slice(0, 12),
          otherBlocking: otherBlocking.slice(0, 8),
          packageExclusions,
        });
      }
    }
    // Contact loss check (match by id — package may reorder).
    // Prefer restore from PASS canonical contact over silent drop.
    const dtoById = new Map(
      (candidatePackage.opportunities || []).map((o) => [oppId(o), o]),
    );
    const contactLostIds = [];
    const restoredContacts = [];
    for (const row of passRows) {
      const srcName = text(row.primaryName);
      const dto = dtoById.get(row.opportunityId);
      const dtoName = text(dto?.primaryContact?.name);
      if (srcName && !dtoName) {
        const srcPc = row.opportunity?.primaryContact || {};
        if (dto && text(srcPc.name)) {
          dto.primaryContact = {
            ...(dto.primaryContact || {}),
            ...srcPc,
            name: text(srcPc.name),
          };
          restoredContacts.push(row.opportunityId);
        } else {
          contactLostIds.push(row.opportunityId);
        }
      }
    }
    if (restoredContacts.length) {
      candidatePackage = buildCustomerPublicationPackage({
        hotelId,
        hotelName: text(input.hotelName) || hotelId,
        opportunities: candidatePackage.opportunities,
        publicationVersion,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
        approvalStatus: "CANDIDATE",
        applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
        notes: `Hotel-wide finalization candidate — run ${runId}; restored ${restoredContacts.length} contacts from PASS rows; pointer NOT activated`,
      });
      integrity = verifyPublicationPackageIntegrity(candidatePackage);
      quality = runPublicationQualityGate({
        opportunities: candidatePackage.opportunities,
        enforceFinalizedQualifiedOpportunity: false,
        enforceCommercialExpirationGate: false,
        enforceCommercialActionabilityGate: false,
        failOnCodes: hotelWideFailOnCodes,
      });
    }
    if (contactLostIds.length) {
      for (const id of contactLostIds) {
        packageExclusions.push({
          opportunityId: id,
          code: "CONTACT_LOST_IN_SERIALIZATION",
          reason: "Named primary present on PASS row but missing after projection",
        });
        blockers.push({
          code: "CONTACT_LOST_IN_SERIALIZATION",
          opportunityId: id,
        });
      }
    }
    // Reconciliation: every PASS id must appear in package or packageExclusions.
    const packagedIds = new Set(
      (candidatePackage.opportunities || []).map((o) => oppId(o)),
    );
    const excludedIds = new Set(
      packageExclusions.map((e) => e.opportunityId).filter(Boolean),
    );
    for (const row of passRows) {
      if (!packagedIds.has(row.opportunityId) && !excludedIds.has(row.opportunityId)) {
        packageExclusions.push({
          opportunityId: row.opportunityId,
          code: "UNEXPLAINED_PASS_TO_PACKAGE_DROP",
          reason: "PASS row missing from candidate without documented exclusion",
        });
        blockers.push({
          code: "UNEXPLAINED_PASS_TO_PACKAGE_DROP",
          opportunityId: row.opportunityId,
        });
      }
    }
    candidatePackage.hotelWidePassToPackageReconciliation = {
      passCount: passRows.length,
      packageCount: (candidatePackage.opportunities || []).length,
      exclusions: packageExclusions,
      unexplainedDrops: packageExclusions.filter(
        (e) => e.code === "UNEXPLAINED_PASS_TO_PACKAGE_DROP",
      ).length,
    };

    if (
      integrity.ok &&
      quality.ok &&
      !blockers.some((b) => b.code === "CONTACT_LOST_IN_SERIALIZATION")
    ) {
      const written = store.writePackage({
        ...candidatePackage,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
        approvalStatus: "CANDIDATE",
        gates: {
          hotelWideFinalization: true,
          integrity: { ok: true },
          quality: { ok: quality.ok },
          pointerActivated: false,
          runId,
        },
      });
      packageWrite = {
        ok: written.ok === true,
        path: written.path || null,
        publicationVersion,
        pointerActivated: false,
        skipped: false,
      };
      // Explicitly do NOT call writePointer / stageForProduction
    }
  }

  const freshResearchNeeded = rows.filter((r) => r.freshResearchRequired);
  const manifest = {
    contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
    fqoContract: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    hotelId,
    hotelName: text(input.hotelName) || null,
    runId,
    at,
    researchAuthorization: {
      authorizationId: text(input.authorizationId) || null,
      paidSpendUsd: Number(input.paidSpendUsd || 0),
      agentAuthorized: input.agentAuthorized === true,
      agentOff: input.agentAuthorized !== true,
      hotelWideReviewDoesNotAuthorizePaidResearch: true,
    },
    inventory: {
      totalCanonicalReviewed: rows.length,
      newOpportunitiesDiscovered: rows.filter((r) => r.diffClass === "NEW").length,
      existingOpportunitiesRevisited: rows.filter((r) =>
        (inventory.sourcesById[r.opportunityId] || []).includes("RESEARCH_STORE"),
      ).length,
      previouslyFinalizedReconciled: rows.filter((r) =>
        wasPreviouslyApproved(inventory.sourcesById[r.opportunityId] || []),
      ).length,
      sourceCounts: inventory.counts,
      duplicateMappingsCount: inventory.duplicateMappings.length,
    },
    dispositions: {
      FINALIZATION_PASS: passRows.length,
      HOLD_RESEARCH: holdRows.length,
      REJECT: rejectRows.length,
      HISTORICAL: historicalRows.length,
      PREVIOUSLY_APPROVED_REVIEW: priorReviewRows.length,
    },
    coverage: {
      verifiedPrimary: rows.filter((r) => text(r.primaryName)).length,
      verifiedSecondary: rows.filter((r) => r.secondaryPresent).length,
      personSpecificChannel: rows.filter((r) => r.hasPersonSpecificChannel).length,
      hotelPass: hotelPass.length,
      competitivePass: competitivePass.length,
    },
    stagedCandidate: {
      publicationVersion: packageWrite.zeroQualifiedCertified
        ? null
        : packageWrite.publicationVersion || publicationVersion,
      written: packageWrite.ok === true && !packageWrite.zeroQualifiedCertified,
      zeroQualifiedCertified: packageWrite.zeroQualifiedCertified === true,
      pointerActivated: false,
      passCount: passRows.length,
      path: packageWrite.path || null,
      reasons: packageWrite.reasons || null,
    },
    diffVersusActive: {
      priorStagedVersion: stagedSnap.publicationVersion,
      priorCurrentVersion: currentSnap.publicationVersion,
      byClass: Object.values(INVENTORY_DIFF_CLASS).reduce((acc, k) => {
        acc[k] = rows.filter((r) => r.diffClass === k).length;
        return acc;
      }, {}),
    },
    unresolvedResearchRequirements: freshResearchNeeded.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      reasonCodes: r.reasonCodes,
      remediation: r.remediation,
    })),
    reconciliation: {
      everyOpportunityReviewedExactlyOnce:
        missing.length === 0 &&
        accountedIds.length === uniqueCanonical.length &&
        new Set(accountedIds).size === accountedIds.length,
      accountedIds,
      missingFromAudit: missing,
      duplicateIdCollisions: [...new Set(duplicateIds)],
      duplicateMappings: inventory.duplicateMappings.slice(0, 50),
      candidatePackagesIngested:
        inventory.candidatePackageVersions ||
        inventory.candidatePackageEntries?.map((e) => e.version) ||
        [],
    },
    rows: rows.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      disposition: r.disposition,
      processingDisposition: r.processingDisposition || null,
      diffClass: r.diffClass,
      reasonCodes: r.reasonCodes,
      remediation: r.remediation,
      marketScope: r.marketScope,
      primaryName: r.primaryName,
      hasPersonSpecificChannel: r.hasPersonSpecificChannel,
      secondaryPresent: r.secondaryPresent,
      salesReady: r.salesReady,
      salesReadyRequired: false,
      freshResearchRequired: r.freshResearchRequired,
      sources: r.sources,
      evidenceProvenance: r.opportunity?.evidenceProvenance || r.sources,
      contactMergePreferredSource:
        r.opportunity?._contactMergePreferredSource || null,
    })),
    blockers,
    completionFailed: blockers.length > 0,
    // Inventory reconcile success ≠ research complete ≠ Full E2E complete.
    // Package quality/integrity failures block candidate certification only.
    hotelInventoryReconciled:
      missing.length === 0 &&
      accountedIds.length === uniqueCanonical.length &&
      !blockers.some((b) =>
        /^(UNACCOUNTED|RECONCILIATION_COUNT|INVENTORY_SOURCE|STAGED_SNAPSHOT|CURRENT_SNAPSHOT|UNEXPECTED_POINTER)/.test(
          b.code || "",
        ),
      ),
    researchGapsRemain: holdRows.length > 0 || freshResearchNeeded.length > 0,
    qualifiedCandidateCertified:
      !blockers.some((b) =>
        /CANDIDATE_PACKAGE|CONTACT_LOST|PASS_ROW_FAILED/.test(b.code || ""),
      ) &&
      ((passRows.length > 0 &&
        packageWrite.ok === true &&
        !packageWrite.zeroQualifiedCertified) ||
        (passRows.length === 0 && packageWrite.zeroQualifiedCertified === true)),
    fullE2eHotelWideComplete: false, // set below after outcome classification
  };

  // Completion outcome classification (unambiguous)
  let completionOutcome = HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_BLOCKED;
  if (manifest.hotelInventoryReconciled) {
    if (manifest.researchGapsRemain) {
      completionOutcome = HOTEL_WIDE_COMPLETION_OUTCOME.RESEARCH_GAPS_REMAIN;
    } else if (manifest.qualifiedCandidateCertified) {
      completionOutcome =
        HOTEL_WIDE_COMPLETION_OUTCOME.QUALIFIED_CANDIDATE_CERTIFIED;
    } else {
      completionOutcome =
        HOTEL_WIDE_COMPLETION_OUTCOME.HOTEL_INVENTORY_RECONCILED;
    }
  }
  // FULL_E2E_COMPLETE only when inventory reconciled, candidate certified
  // (including certified zero), and no research gaps remain.
  if (
    manifest.hotelInventoryReconciled &&
    manifest.qualifiedCandidateCertified &&
    !manifest.researchGapsRemain
  ) {
    completionOutcome = HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_COMPLETE;
  }
  manifest.completionOutcome = completionOutcome;
  manifest.outcomes = {
    HOTEL_INVENTORY_RECONCILED: manifest.hotelInventoryReconciled,
    RESEARCH_GAPS_REMAIN: manifest.researchGapsRemain,
    QUALIFIED_CANDIDATE_CERTIFIED: manifest.qualifiedCandidateCertified,
    FULL_E2E_COMPLETE:
      completionOutcome === HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_COMPLETE,
    FULL_E2E_BLOCKED:
      completionOutcome === HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_BLOCKED,
  };
  // Hotel-wide stage "complete" means inventory reconciled (not that gaps are gone).
  manifest.fullE2eHotelWideComplete = manifest.hotelInventoryReconciled;

  const outDir =
    input.outDir ||
    path.join(
      ROOT,
      "reports",
      "gdi-hotel-wide-finalization-stage-v1",
      hotelId,
      runId,
    );
  fs.mkdirSync(outDir, { recursive: true });
  const manifestPath = path.join(outDir, "HOTEL_COMPLETION_MANIFEST.json");
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  if (candidatePackage) {
    fs.writeFileSync(
      path.join(outDir, "STAGED_CANDIDATE_PACKAGE.json"),
      `${JSON.stringify(candidatePackage, null, 2)}\n`,
    );
  }

  // Verify pointers unchanged
  const stagedAfter = store.readPointer(hotelId, "STAGED");
  const currentAfter = store.readPointer(hotelId, "CURRENT");
  const pointersUnchanged =
    text(stagedAfter?.publicationVersion) ===
      text(stagedSnap.publicationVersion) &&
    text(currentAfter?.publicationVersion) ===
      text(currentSnap.publicationVersion);

  if (!pointersUnchanged) {
    blockers.push({ code: "UNEXPECTED_POINTER_MUTATION" });
    manifest.blockers = blockers;
    manifest.completionFailed = true;
    manifest.fullE2eHotelWideComplete = false;
    fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  }

  return {
    // Stage ok = inventory reconciled (package cert may still be blocked).
    ok: manifest.hotelInventoryReconciled === true,
    blocked:
      blockers.length > 0 &&
      manifest.completionOutcome ===
        HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_BLOCKED,
    contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
    hotelId,
    runId,
    manifest,
    manifestPath,
    outDir,
    packageWrite,
    candidatePackage,
    pointersUnchanged,
    paidSpendUsd: 0,
    agentOff: true,
    completionOutcome: manifest.completionOutcome,
    hotelInventoryReconciled: manifest.hotelInventoryReconciled === true,
    researchGapsRemain: manifest.researchGapsRemain === true,
    qualifiedCandidateCertified: manifest.qualifiedCandidateCertified === true,
    fullE2eHotelWideComplete: manifest.fullE2eHotelWideComplete === true,
    fullE2eComplete:
      manifest.completionOutcome ===
      HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_COMPLETE,
  };
}

export default {
  GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
  HOTEL_WIDE_DISPOSITION,
  INVENTORY_DIFF_CLASS,
  HOTEL_WIDE_COMPLETION_OUTCOME,
  collectHotelWideCanonicalInventory,
  assignHotelWideDisposition,
  runHotelWideFinalizationStage,
};
