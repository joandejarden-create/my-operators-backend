/**
 * Phase 2 — reconcile live Exa pilot findings → unique demand identities →
 * evidence-backed commercial dispositions (zero-cost).
 *
 * Uses existing commercial eligibility / contact standards conceptually.
 * Does NOT invent buyers. Does NOT publish. Does NOT spend.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { CONTACT_ELIGIBILITY_CLASS } from "../contact-eligibility-standard-v1.js";

export const LIVE_PILOT_COMMERCIAL_QUALIFICATION_PHASE2_V1 =
  "gdi-live-pilot-commercial-qualification-phase2-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const RAW_DIR = path.join(
  ROOT,
  "reports/gdi-live-discovery-pilot-v1-executed/raw-exa"
);

export const PHASE2_DISPOSITION = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  NEEDS_MORE_RESEARCH: "NEEDS_MORE_RESEARCH",
  WATCH: "WATCH",
  REJECTED: "REJECTED",
});

const RUN_MAP = Object.freeze([
  {
    runId: "agent_run_44defea7079746569baa8bc8f25930e0",
    hotelId: "recCAGqkERSXgiku4",
    hotelName: "Sheraton Guadalajara Expo",
    segmentId: "associations_conferences_events",
    costUsd: 0.1,
  },
  {
    runId: "agent_run_bbe46836323144e48c6952d2fb1d1012",
    hotelId: "recCAGqkERSXgiku4",
    hotelName: "Sheraton Guadalajara Expo",
    segmentId: "corporate_accounts_scaling",
    costUsd: 0.1,
  },
  {
    runId: "agent_run_7aaf010b0b3347a0a256f0bce394c426",
    hotelId: "recTk5I8VOzsPhxDz",
    hotelName: "voco Cancún Zona Hotelera",
    segmentId: "associations_conferences_events",
    costUsd: 0.1,
  },
  {
    runId: "agent_run_e4d96ee5156f4a989ae806f9985ecfea",
    hotelId: "recTk5I8VOzsPhxDz",
    hotelName: "voco Cancún Zona Hotelera",
    segmentId: "corporate_accounts_scaling",
    costUsd: 0.1,
  },
]);

const INVENTORY_REL = Object.freeze({
  recCAGqkERSXgiku4:
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/sheraton-guadalajara-expo-inventory.json",
  recTk5I8VOzsPhxDz:
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/voco-cancun-zona-hotelera-inventory.json",
  recXh4NgF8sDbi9cQ:
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/wyndham-alltra-punta-cana-inventory.json",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha12(s) {
  return createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
}

export function normalizeDemandKey(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

const DEMAND_STOPWORDS = new Set([
  "americas",
  "america",
  "mexico",
  "mexicana",
  "mexicano",
  "guadalajara",
  "cancun",
  "cancún",
  "internacional",
  "national",
  "nacional",
  "congreso",
  "convencion",
  "convention",
  "hotel",
  "hoteles",
  "expo",
  "feria",
  "evento",
  "event",
  "group",
  "meeting",
  "meetings",
  "association",
  "sociedad",
  "instituto",
  "area",
  "facility",
  "opening",
  "announced",
]);

function distinctiveTokens(s) {
  return normalizeDemandKey(s)
    .split(" ")
    .filter((t) => t.length > 3 && !DEMAND_STOPWORDS.has(t));
}

export function fuzzyDemandMatch(a, b) {
  const na = normalizeDemandKey(a);
  const nb = normalizeDemandKey(b);
  if (!na || !nb) return false;
  if (na === nb || na.includes(nb) || nb.includes(na)) return true;
  const at = distinctiveTokens(a);
  const bt = distinctiveTokens(b);
  if (!at.length || !bt.length) return false;
  const aSet = new Set(at);
  const overlap = bt.filter((t) => aSet.has(t)).length;
  // Require majority of distinctive tokens AND at least 2 shared tokens
  // to avoid geography/stopword false enrichments (e.g. SASinno↔ITB Americas).
  return overlap >= 2 && overlap / Math.min(at.length, bt.length) >= 0.6;
}

function collectUrls(row) {
  const company =
    row.companyIdentity && typeof row.companyIdentity === "object"
      ? row.companyIdentity
      : null;
  return [
    ...((row.supportingSources || []).map((s) => s?.url || s?.sourceUrl)),
    ...((row.supportingEvidence || []).map((s) => s?.url || s?.sourceUrl)),
    ...((row.evidenceClaims || []).map((s) => s?.url || s?.sourceUrl)),
    company?.website,
    company?.officialWebsite,
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));
}

function identityFromRow(row, segmentId) {
  const company =
    row.companyIdentity && typeof row.companyIdentity === "object"
      ? row.companyIdentity
      : null;
  const organizationName =
    text(
      row.organizingEntity ||
        row.corporateOrAssociationOwner ||
        company?.canonicalName ||
        company?.legalOrTradingName ||
        ""
    ) || null;
  const activityTitle =
    text(
      row.verifiedEventName ||
        row.eventIdentity ||
        row.verifiedBusinessActivity ||
        row.businessTrigger ||
        ""
    ) || null;
  return {
    organizationName,
    activityTitle,
    demandIdentityKey: normalizeDemandKey(
      `${organizationName || ""}|${activityTitle || ""}|${segmentId}`
    ),
  };
}

function loadInventory(hotelId) {
  const rel = INVENTORY_REL[hotelId];
  if (!rel) return [];
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return [];
  return JSON.parse(fs.readFileSync(p, "utf8")).rows || [];
}

function loadHistoricalLedger(hotelId) {
  const roots = [
    "C:/Dev/dealality/_railway-lean-stage-gdi-spice/data/group-demand-intelligence/discovery-ledgers",
  ];
  for (const root of roots) {
    const p = path.join(root, hotelId, "four-hotel-14-workflow-full-e2e-v1.json");
    if (!fs.existsSync(p)) continue;
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    return { path: p, candidates: doc.candidates || [] };
  }
  return { path: null, candidates: [] };
}

function loadSeedResearchStates(hotelId) {
  const dir = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunity-research-state"
  );
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.startsWith("aimbridge_seed_") && f.endsWith(".json"))
    .map((f) => {
      try {
        return JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

/**
 * Zero-cost contact enrichment from prior seed contact passes.
 * Preserves source attribution; does not invent contacts.
 */
export function enrichContactsFromSeedState(liveRow, seed) {
  if (!seed) return liveRow.contacts;
  const named = (seed.contacts || [])
    .filter((c) => text(c.name))
    .map((c) => ({
      name: text(c.name),
      role: text(c.title || c.role || ""),
      organization: text(c.organization || ""),
      email: c.email || null,
      phone: c.phone || null,
      sourceUrl: c.sourceUrl || null,
      retrievedAt: c.retrievedAt || null,
      class:
        seed.contactQualification === "DIRECT_BUYER"
          ? CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER || "DIRECT_BUYER"
          : seed.contactQualification === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
              seed.namedContactStatus === "NAMED_COMMERCIAL_ENTRY_POINT"
            ? "RELEVANT_COMMERCIAL_ENTRY_POINT"
            : seed.contactQualification || "RESEARCH_CANDIDATE",
      note: `Preserved from seed ${seed.opportunityId} zero-cost contact pass`,
      verified: Boolean(c.sourceUrl),
    }));

  const functional = [
    ...(liveRow.contacts?.functionalRoutes || []),
    ...(seed.functionalRoutes || []).map((r) =>
      typeof r === "string"
        ? { route: r, class: "FUNCTIONAL_CONTACT_ROUTE", note: "from seed" }
        : r
    ),
  ];

  const verifiedEntry = named.filter(
    (c) =>
      c.verified &&
      (c.class === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
        c.class === "DIRECT_BUYER" ||
        c.class === "INTERMEDIARY_OR_REFERRAL")
  ).length;
  const verifiedDirect = named.filter(
    (c) => c.verified && c.class === "DIRECT_BUYER"
  ).length;

  return {
    namedContacts: named,
    functionalRoutes: functional,
    verifiedDirectBuyers: verifiedDirect,
    verifiedEntryPoints: verifiedEntry,
    contactStatus: verifiedDirect
      ? "DIRECT_BUYER"
      : verifiedEntry
        ? "RELEVANT_COMMERCIAL_ENTRY_POINT"
        : named.length
          ? "NOT_VERIFIED_NAMED_CANDIDATES_PRESENT"
          : functional.length
            ? "FUNCTIONAL_CONTACT_ROUTE_ONLY"
            : "NOT_VERIFIED",
    seedOpportunityId: seed.opportunityId || null,
    seedContactQualification: seed.contactQualification || null,
    seedSourcingNote: seed.sourcingNote || null,
  };
}

/**
 * Detect duplicate commercial opportunities across live findings + inventory + historical.
 * Exported for regression tests.
 */
export function detectDuplicateCommercialOpportunities({
  liveFindings = [],
  inventoryRows = [],
  historicalRows = [],
} = {}) {
  const clusters = new Map();
  function add(item, source) {
    const key =
      item.demandIdentityKey ||
      normalizeDemandKey(
        `${item.organizationName || ""}|${item.activityTitle || item.title || ""}`
      );
    if (!key) return;
    if (!clusters.has(key)) clusters.set(key, []);
    clusters.get(key).push({ ...item, source });
  }
  for (const f of liveFindings) add(f, "live");
  for (const r of inventoryRows) {
    add(
      {
        organizationName: r.organizationName,
        activityTitle: r.title,
        candidateId: r.candidateId,
      },
      "inventory"
    );
  }
  for (const r of historicalRows) {
    add(
      {
        organizationName: r.organizationName || r.normalizedOrganization,
        activityTitle: r.programmeName || r.normalizedProgramme || r.title,
        discoveryId: r.discoveryId,
      },
      "historical"
    );
  }
  const duplicates = [];
  for (const [key, members] of clusters) {
    const sources = new Set(members.map((m) => m.source));
    if (members.length > 1 && sources.size >= 1) {
      const liveCount = members.filter((m) => m.source === "live").length;
      if (liveCount > 1 || (liveCount >= 1 && sources.size > 1)) {
        duplicates.push({
          demandIdentityKey: key,
          memberCount: members.length,
          sources: [...sources],
          members: members.map((m) => ({
            source: m.source,
            organizationName: m.organizationName,
            activityTitle: m.activityTitle,
            id: m.candidateId || m.discoveryId || null,
          })),
        });
      }
    }
  }
  return { duplicateClusterCount: duplicates.length, duplicates };
}

function assessContacts(row, urls) {
  const rawContacts = [
    ...(row.organizerContactCandidates || []),
    ...(row.contactCandidates || []),
    ...(row.suggestedContacts || []),
  ].filter(Boolean);

  const named = [];
  for (const c of rawContacts) {
    const name = text(c.name || c.fullName || c.personName || "");
    const role = text(c.role || c.title || c.jobTitle || "");
    if (!name) continue;
    named.push({
      name,
      role,
      class: CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE,
      note: "Present in Exa structured output only — not independently verified in Phase 2 zero-cost pass",
    });
  }

  const housingUrl = urls.find((u) =>
    /hospedaje|housing|hotel-sede|sede-y-hospedaje|alojamiento|lodging/i.test(u)
  );
  const functional = housingUrl
    ? [
        {
          route: housingUrl,
          class: "FUNCTIONAL_CONTACT_ROUTE",
          note: "Official housing/sede page — organizational route, not a named buyer",
        },
      ]
    : [];

  return {
    namedContacts: named,
    functionalRoutes: functional,
    verifiedDirectBuyers: 0,
    verifiedEntryPoints: 0,
    contactStatus: named.length
      ? "NOT_VERIFIED_NAMED_CANDIDATES_PRESENT"
      : functional.length
        ? "FUNCTIONAL_CONTACT_ROUTE_ONLY"
        : "NOT_VERIFIED",
  };
}

function assessDemand(row, segmentId) {
  const urls = collectUrls(row);
  const id = identityFromRow(row, segmentId);
  const dates =
    text(row.verifiedDates || row.verifiedStartDate || row.activityDate || "") ||
    null;
  const venue = text(row.verifiedVenue || row.incumbentVenue || "") || null;
  const city = text(row.eventLocationCity || row.activityGeography || "") || null;
  const status = text(row.eventStatus || row.discoveryDisposition || "") || null;
  const sourcing = text(
    row.hotelSourcingStatus || row.sourcingStatus || row.sourcingAccessibility || ""
  );
  const relevance = row.hotelMarketRelevance || row.preliminaryHotelFit || null;
  const rooms =
    row.potentialGuestroomDemand ||
    row.peakRoomsPublished ||
    row.gate2HospitalityDemand ||
    null;
  const meetings =
    row.potentialMeetingDemand || row.potentialCateringDemand || null;

  const facts = [];
  if (id.organizationName) facts.push(`org:${id.organizationName}`);
  if (id.activityTitle) facts.push(`activity:${id.activityTitle}`);
  if (dates) facts.push(`dates:${dates}`);
  if (venue) facts.push(`venue:${venue}`);
  if (city) facts.push(`city:${city}`);
  if (urls.length) facts.push(`sources:${urls.length}`);

  const inferences = [];
  if (relevance) inferences.push(`hotel_relevance_field:${JSON.stringify(relevance).slice(0, 160)}`);
  if (rooms) inferences.push("guestroom_demand_field_present");
  if (meetings) inferences.push("meetings_or_catering_field_present");

  const unknowns = [];
  if (!dates) unknowns.push("dates_unconfirmed");
  if (!sourcing || /unknown|unannounced|open_or_unannounced/i.test(sourcing)) {
    unknowns.push("sourcing_accessibility_unconfirmed");
  }
  unknowns.push("named_buyer_not_verified");
  if (!rooms && !meetings) unknowns.push("revenue_mechanism_not_quantified");

  return {
    ...id,
    urls: [...new Set(urls)],
    dates,
    venue,
    city,
    eventStatus: status,
    sourcingStatus: sourcing || null,
    demandFamily: row.demandFamily || row.growthSignalType || segmentId,
    facts,
    inferences,
    unknowns,
    hasHttpEvidence: urls.length > 0,
    overflowSpeculative:
      /overflow|proximity|adjacent|expo/i.test(
        JSON.stringify(relevance || "") + JSON.stringify(row.preliminaryHotelFit || "")
      ) && !/confirmed|contracted|housing.?bureau/i.test(sourcing || ""),
  };
}

/**
 * Disposition without inventing Sales Ready (no verified named buyer).
 */
export function classifyPhase2Disposition(assessment, contacts, match) {
  if (!assessment.organizationName && !assessment.activityTitle) {
    return {
      disposition: PHASE2_DISPOSITION.REJECTED,
      reason: "missing_identity",
      publicationReady: false,
    };
  }
  if (!assessment.hasHttpEvidence) {
    return {
      disposition: PHASE2_DISPOSITION.REJECTED,
      reason: "no_http_source_evidence",
      publicationReady: false,
    };
  }
  if (match?.kind === "duplicate_live") {
    return {
      disposition: PHASE2_DISPOSITION.REJECTED,
      reason: "duplicate_of_other_live_finding",
      publicationReady: false,
      matchedId: match.id,
    };
  }

  // Sales Ready requires verified named buyer/entry — Phase 2 has none verified.
  if (contacts.verifiedDirectBuyers > 0 || contacts.verifiedEntryPoints > 0) {
    return {
      disposition: PHASE2_DISPOSITION.SALES_READY,
      reason: "verified_named_commercial_entry_with_evidence",
      publicationReady: false, // still needs founder publication gate
    };
  }

  const future =
    /upcoming|confirmed|2026|2027|announced|opening/i.test(
      `${assessment.eventStatus || ""} ${assessment.dates || ""} ${assessment.activityTitle || ""}`
    );
  const geoOk = Boolean(assessment.city) || Boolean(assessment.venue);

  if (match?.kind === "inventory_enrichment") {
    return {
      disposition: PHASE2_DISPOSITION.NEEDS_MORE_RESEARCH,
      reason: "enriches_existing_inventory_contact_and_sourcing_still_open",
      publicationReady: false,
      matchedInventoryId: match.id,
      nextAction: "deep_contact_and_housing_bureau_verification_on_existing_opportunity",
    };
  }

  if (assessment.overflowSpeculative && !/housing|sede|hospedaje/i.test(assessment.urls.join(" "))) {
    return {
      disposition: PHASE2_DISPOSITION.WATCH,
      reason: "hotel_fit_largely_speculative_overflow_without_housing_evidence",
      publicationReady: false,
      nextAction: "confirm_housing_bureau_or_sourcing_channel",
    };
  }

  if (future && geoOk && assessment.hasHttpEvidence) {
    // Defensible future demand with sources but no verified named buyer:
    // research-stage Account Development hypothesis — NOT customer-publication-ready.
    return {
      disposition: PHASE2_DISPOSITION.ACCOUNT_DEVELOPMENT,
      reason:
        "evidence_backed_future_demand_without_verified_named_buyer_internal_research_stage",
      publicationReady: false,
      customerPublicationReady: false,
      nextAction: "identify_direct_buyer_or_relevant_commercial_entry_point",
    };
  }

  return {
    disposition: PHASE2_DISPOSITION.NEEDS_MORE_RESEARCH,
    reason: "plausible_demand_essential_gaps_remain",
    publicationReady: false,
    nextAction: "resolve_dates_sourcing_and_named_contact",
  };
}

export function loadLivePilotFindings() {
  const findings = [];
  for (const meta of RUN_MAP) {
    const rawPath = path.join(RAW_DIR, `${meta.runId}.json`);
    if (!fs.existsSync(rawPath)) {
      throw new Error(`missing_raw_exa:${rawPath}`);
    }
    const raw = JSON.parse(fs.readFileSync(rawPath, "utf8"));
    const rows = raw.structured?.candidates || [];
    rows.forEach((row, idx) => {
      const assessment = assessDemand(row, meta.segmentId);
      const contacts = assessContacts(row, assessment.urls);
      findings.push({
        findingIndex: idx,
        providerRunId: meta.runId,
        hotelId: meta.hotelId,
        hotelName: meta.hotelName,
        segmentId: meta.segmentId,
        retrievalCostUsd: meta.costUsd,
        rawPath,
        candidateId: `live_${sha12(
          `${meta.hotelId}|${meta.segmentId}|${assessment.demandIdentityKey}`
        )}`,
        ...assessment,
        contacts,
        rawSchemaSnippet: {
          discoveryDisposition: row.discoveryDisposition || null,
          hotelSourcingStatus: row.hotelSourcingStatus || row.sourcingStatus || null,
          eventStatus: row.eventStatus || null,
        },
      });
    });
  }
  return findings;
}

export function reconcileLivePilotFindings(findings = loadLivePilotFindings()) {
  const byHotel = new Map();
  for (const f of findings) {
    if (!byHotel.has(f.hotelId)) byHotel.set(f.hotelId, []);
    byHotel.get(f.hotelId).push(f);
  }

  const waterfall = {
    providerFindings: findings.length,
    evidenceSupported: 0,
    hotelRelevant: 0,
    uniqueDemandActivities: 0,
    newOpportunities: 0,
    existingEnrichments: 0,
    rejectedSignals: 0,
    duplicateLiveCollapsed: 0,
  };

  const unique = [];
  const rejected = [];
  const seenKeys = new Map();

  for (const f of findings) {
    if (f.hasHttpEvidence) waterfall.evidenceSupported += 1;
    else {
      waterfall.rejectedSignals += 1;
      rejected.push({ ...f, rejectReason: "no_http_source_evidence" });
      continue;
    }

    const geoRelevant =
      /guadalajara|cancun|cancún|zapopan|jalisco|quintana|mexico|expo/i.test(
        `${f.city || ""} ${f.venue || ""} ${f.activityTitle || ""} ${f.organizationName || ""}`
      ) || Boolean(f.city);
    if (geoRelevant) waterfall.hotelRelevant += 1;

    const prior = seenKeys.get(`${f.hotelId}|${f.demandIdentityKey}`);
    if (prior) {
      waterfall.duplicateLiveCollapsed += 1;
      prior.duplicateFindings = prior.duplicateFindings || [];
      prior.duplicateFindings.push(f.candidateId);
      prior.urls = [...new Set([...(prior.urls || []), ...f.urls])];
      continue;
    }

    const inventory = loadInventory(f.hotelId);
    const seeds = loadSeedResearchStates(f.hotelId);
    const invHit = inventory.find((row) =>
      fuzzyDemandMatch(
        `${f.organizationName || ""} ${f.activityTitle || ""}`,
        `${row.organizationName || ""} ${row.title || ""}`
      )
    );
    const seedHit =
      seeds.find((s) =>
        fuzzyDemandMatch(
          `${f.organizationName || ""} ${f.activityTitle || ""}`,
          `${s.organizationName || ""} ${s.title || ""}`
        )
      ) ||
      (invHit
        ? seeds.find((s) => s.opportunityId === invHit.candidateId)
        : null);
    const hist = loadHistoricalLedger(f.hotelId);
    const histHit = (hist.candidates || []).find((row) =>
      fuzzyDemandMatch(
        `${f.organizationName || ""} ${f.activityTitle || ""}`,
        `${row.organizationName || row.normalizedOrganization || ""} ${row.programmeName || row.normalizedProgramme || ""}`
      )
    );

    const match = invHit
      ? { kind: "inventory_enrichment", id: invHit.candidateId }
      : seedHit
        ? { kind: "seed_state_enrichment", id: seedHit.opportunityId }
        : null;

    const contacts = seedHit
      ? enrichContactsFromSeedState(f, seedHit)
      : f.contacts;

    let disposition = classifyPhase2Disposition(f, contacts, match);
    // Respect prior seed commercial posture: named lodging coordinators on a
    // preferred-list partnership pathway stay Account Development, not inflated Sales Ready.
    if (
      seedHit &&
      String(seedHit.disposition || seedHit.commercialClassification || "")
        .toUpperCase()
        .includes("ACCOUNT_DEVELOPMENT") &&
      disposition.disposition === PHASE2_DISPOSITION.SALES_READY
    ) {
      disposition = {
        disposition: PHASE2_DISPOSITION.ACCOUNT_DEVELOPMENT,
        reason:
          "seed_verified_entry_point_preserved_as_account_development_pathway_not_open_block",
        publicationReady: false,
        customerPublicationReady: false,
        nextAction:
          "Pitch hotel inclusion via verified lodging coordinator; confirm preferred-list status",
      };
    }
    // Inventory enrichments with verified seed contacts escalate NMR → AD.
    if (
      match?.kind === "inventory_enrichment" &&
      disposition.disposition === PHASE2_DISPOSITION.NEEDS_MORE_RESEARCH &&
      (contacts.verifiedEntryPoints > 0 || contacts.verifiedDirectBuyers > 0)
    ) {
      disposition = {
        disposition: PHASE2_DISPOSITION.ACCOUNT_DEVELOPMENT,
        reason:
          "inventory_enrichment_with_seed_verified_commercial_entry_point",
        publicationReady: false,
        customerPublicationReady: false,
        matchedInventoryId: match.id,
        nextAction:
          contacts.seedSourcingNote ||
          "Continue via verified entry point; do not claim contracted block",
      };
    }
    const record = {
      ...f,
      contacts,
      match,
      historicalMatchId: histHit?.discoveryId || histHit?.opportunityId || null,
      seedOpportunityId: seedHit?.opportunityId || null,
      disposition: disposition.disposition,
      dispositionReason: disposition.reason,
      customerPublicationReady: disposition.customerPublicationReady === true,
      nextAction: disposition.nextAction || null,
      salesAction:
        disposition.nextAction ||
        (disposition.disposition === PHASE2_DISPOSITION.SALES_READY
          ? "Pursue via verified named commercial entry; confirm block status before claiming inventory"
          : disposition.disposition === PHASE2_DISPOSITION.ACCOUNT_DEVELOPMENT
            ? contacts.verifiedEntryPoints > 0 || contacts.verifiedDirectBuyers > 0
              ? "Pitch hotel inclusion via verified lodging/housing contact; confirm preferred-list status before claiming inventory"
              : "Internal research continuation: identify named buyer/entry point before sales outreach"
            : disposition.disposition === PHASE2_DISPOSITION.NEEDS_MORE_RESEARCH
              ? "Resolve evidence gaps before commercial pursuit"
              : disposition.disposition === PHASE2_DISPOSITION.WATCH
                ? "Monitor housing/sourcing signals; do not pitch as confirmed overflow"
                : "Do not pursue"),
    };
    seenKeys.set(`${f.hotelId}|${f.demandIdentityKey}`, record);
    unique.push(record);
    if (match?.kind === "inventory_enrichment") waterfall.existingEnrichments += 1;
    else if (disposition.disposition === PHASE2_DISPOSITION.REJECTED) {
      waterfall.rejectedSignals += 1;
      rejected.push(record);
    } else waterfall.newOpportunities += 1;
  }

  waterfall.uniqueDemandActivities = unique.length;

  const dupScan = detectDuplicateCommercialOpportunities({
    liveFindings: unique,
    inventoryRows: [
      ...loadInventory("recCAGqkERSXgiku4"),
      ...loadInventory("recTk5I8VOzsPhxDz"),
    ],
    historicalRows: [
      ...loadHistoricalLedger("recCAGqkERSXgiku4").candidates.slice(0, 300),
      ...loadHistoricalLedger("recTk5I8VOzsPhxDz").candidates.slice(0, 300),
    ],
  });

  return {
    version: LIVE_PILOT_COMMERCIAL_QUALIFICATION_PHASE2_V1,
    at: new Date().toISOString(),
    waterfall,
    unique,
    rejected,
    duplicateScan: dupScan,
    dispositionCounts: Object.fromEntries(
      Object.values(PHASE2_DISPOSITION).map((d) => [
        d,
        unique.filter((u) => u.disposition === d).length,
      ])
    ),
    contactSummary: {
      verifiedDirectBuyers: unique.reduce(
        (n, u) => n + (u.contacts?.verifiedDirectBuyers || 0),
        0
      ),
      verifiedEntryPoints: unique.reduce(
        (n, u) => n + (u.contacts?.verifiedEntryPoints || 0),
        0
      ),
      functionalRoutes: unique.filter((u) => u.contacts?.functionalRoutes?.length)
        .length,
      notVerified: unique.filter((u) => u.contacts?.contactStatus === "NOT_VERIFIED")
        .length,
    },
    paidSpendUsd: 0,
    note:
      "Dispositions are research-stage. ACCOUNT_DEVELOPMENT here is internal research posture — not customer-publication-ready Account Development.",
  };
}

export function persistPhase2Dispositions(reconcileDoc, opts = {}) {
  const outDir =
    opts.outDir ||
    path.join(ROOT, "reports/gdi-full-e2e-commercial-qualification-phase2-v1");
  fs.mkdirSync(outDir, { recursive: true });
  const file = path.join(outDir, "reconciliation-and-dispositions.json");
  fs.writeFileSync(file, `${JSON.stringify(reconcileDoc, null, 2)}\n`);

  // Durable per-candidate research state (alongside existing hotel research dirs)
  for (const u of reconcileDoc.unique || []) {
    const dir = path.join(
      ROOT,
      "data/group-demand-intelligence/hotels",
      u.hotelId,
      "opportunity-research-state"
    );
    fs.mkdirSync(dir, { recursive: true });
    const statePath = path.join(dir, `${u.candidateId}.json`);
    const prior = fs.existsSync(statePath)
      ? JSON.parse(fs.readFileSync(statePath, "utf8"))
      : {};
    const next = {
      ...prior,
      version: LIVE_PILOT_COMMERCIAL_QUALIFICATION_PHASE2_V1,
      hotelId: u.hotelId,
      segmentId: u.segmentId,
      candidateId: u.candidateId,
      organizationName: u.organizationName,
      activityTitle: u.activityTitle,
      providerRunId: u.providerRunId,
      evidenceUrls: u.urls,
      disposition: u.disposition,
      dispositionReason: u.dispositionReason,
      customerPublicationReady: false,
      contacts: u.contacts,
      facts: u.facts,
      inferences: u.inferences,
      unknowns: u.unknowns,
      nextAction: u.nextAction,
      salesAction: u.salesAction,
      match: u.match,
      historicalMatchId: u.historicalMatchId || null,
      researchCostUsd: 0,
      stopReason: "PHASE2_ZERO_COST_DISPOSITION_COMPLETE",
      lastSuccessfulCheckpoint: {
        kind: "PHASE2_COMMERCIAL_DISPOSITION",
        at: new Date().toISOString(),
      },
      publicationOccurred: false,
      airtableWrites: 0,
      updatedAt: new Date().toISOString(),
    };
    fs.writeFileSync(statePath, `${JSON.stringify(next, null, 2)}\n`);
  }
  return { outDir, file, persistedCandidates: (reconcileDoc.unique || []).length };
}

export default {
  LIVE_PILOT_COMMERCIAL_QUALIFICATION_PHASE2_V1,
  loadLivePilotFindings,
  reconcileLivePilotFindings,
  persistPhase2Dispositions,
  detectDuplicateCommercialOpportunities,
  classifyPhase2Disposition,
};
