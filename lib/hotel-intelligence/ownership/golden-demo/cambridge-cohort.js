/**
 * Golden Demo #2 — Cambridge Beaches / Dovetail cohort.
 * Parallel to GSF Mexico cohort; no Cambridge-only UI.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyRoomsResolutionToPortfolioHotel,
  buildResearchRoomsObservationsFromExistingCorpus,
  mergeResearchRoomsIntoAssets,
} from "../../property-fundamentals/index.js";

export const GOLDEN_DEMO_DOVETAIL_VERSION = "golden-demo-dovetail-cohort-v1";
export const CAMBRIDGE_DEEP_RESEARCH_VERSION = "cambridge-beaches-deep-research-v1";
export const CAMBRIDGE_AIRTABLE_ID = "recIwaP1etgx2g9nA";
export const CAMBRIDGE_HOTEL_ID = "dhl_06G6TRD5N8Q1YVXKSFD1A5E2NT";
export const DOVETAIL_SLUG = "dovetail-and-co";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DOVETAIL_FIXTURE = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/dovetail-hospitality-cohort-v1.json"
);
const CAMBRIDGE_DEEP = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/cambridge-beaches-deep-research-v1.json"
);

let cachedCohort = null;
let cachedDeep = null;
let cachedResearchRoomsObs = null;

/**
 * Existing Cambridge Webhound / research corpus → rooms observations (no paid calls).
 */
export function loadCambridgeResearchRoomsObservations(hotels) {
  if (cachedResearchRoomsObs && !hotels) return cachedResearchRoomsObs;
  const list =
    hotels ||
    (loadCambridgeDeepResearch()?.portfolio_notes?.assets || []).map((a) => ({
      name: a.name,
    }));
  cachedResearchRoomsObs = buildResearchRoomsObservationsFromExistingCorpus(list, {
    source_title: "Cambridge Full HI Webhound corpus (existing artifact)",
    defaultConfidence: "HIGH",
  });
  return cachedResearchRoomsObs;
}

function findCohortHotel(cohort, id) {
  const key = String(id || "").trim();
  if (!key) return null;
  return (
    (cohort.hotels || []).find(
      (h) =>
        h.airtable_record_id === key ||
        h.property_census_record_id === key ||
        h.hotel_id === key
    ) || null
  );
}

export function loadDovetailGoldenDemoCohort() {
  if (cachedCohort) return cachedCohort;
  cachedCohort = JSON.parse(fs.readFileSync(DOVETAIL_FIXTURE, "utf8"));
  return cachedCohort;
}

export function loadCambridgeDeepResearch() {
  if (cachedDeep) return cachedDeep;
  if (!fs.existsSync(CAMBRIDGE_DEEP)) return null;
  cachedDeep = JSON.parse(fs.readFileSync(CAMBRIDGE_DEEP, "utf8"));
  return cachedDeep;
}

export function clearCambridgeCohortCache() {
  cachedCohort = null;
  cachedDeep = null;
  cachedResearchRoomsObs = null;
}

/**
 * Portfolio assets with research rooms fallback applied (Census still wins via resolver).
 */
export function resolveDovetailPortfolioAssets(deep = loadCambridgeDeepResearch()) {
  const baseAssets = deep?.portfolio_notes?.assets || [];
  const storedObs = deep?.property_fundamentals?.rooms_observations || [];
  const corpusObs = loadCambridgeResearchRoomsObservations(baseAssets);
  const observations = [...storedObs, ...corpusObs];
  const mergedAssets = mergeResearchRoomsIntoAssets(baseAssets, corpusObs);
  return { assets: mergedAssets, observations };
}

export function isCambridgeHotelId(id) {
  const key = String(id || "").trim();
  return key === CAMBRIDGE_AIRTABLE_ID || key === CAMBRIDGE_HOTEL_ID;
}

function orgOf(cohort) {
  return cohort.organization || cohort.owner;
}

function verificationBucket(status) {
  const s = String(status || "").toLowerCase();
  if (s === "verified") return "VERIFIED";
  if (s === "high") return "HIGH";
  if (s === "probable" || s === "needs_review") return "PROBABLE";
  return "UNKNOWN";
}

/**
 * Ownership report for Cambridge Beaches — same response shape as GSF builder.
 */
export function buildCambridgeHotelOwnershipReport(airtableRecordId) {
  const cohort = loadDovetailGoldenDemoCohort();
  const org = orgOf(cohort);
  const hotel = findCohortHotel(cohort, airtableRecordId);
  if (!hotel) {
    return { ok: false, error: "cambridge_hotel_not_in_cohort" };
  }

  const deep = loadCambridgeDeepResearch();
  const brand =
    deep?.brand_resolution?.current_trading_brand ||
    deep?.brand_resolution?.current_brand ||
    hotel.affiliation_display ||
    hotel.brand ||
    "Independent";
  const brandStatus = deep?.brand_resolution?.current_status || deep?.brand_resolution?.status || "CURRENT";
  const propco = (deep?.ownership_chain || []).find((n) => n.role === "propco");
  const econ = (deep?.ownership_chain || []).find((n) => n.role === "economic_owner");
  const opRes = deep?.operator_resolution || {};
  const operatorName =
    opRes.current_operator?.name ||
    hotel.operator ||
    hotel.management_company ||
    "Operator status contested — Dovetail stewardship vs Benchmark/Pyramid";
  const operatorStatus = opRes.current_operator?.status || "CONTESTED";
  const sources = deep?.sources || [];
  const people = deep?.people || [];
  const market = deep?.market_intelligence || {};
  const profile = deep?.property_profile || {};

  return {
    ok: true,
    in_cohort: true,
    intelligence_case: "A",
    version: GOLDEN_DEMO_DOVETAIL_VERSION,
    deep_research_version: deep?.version || CAMBRIDGE_DEEP_RESEARCH_VERSION,
    data_status: deep?.research_status || cohort.data_status,
    costar_firewall: "enforced",
    golden_demo: "GOLDEN_DEMO_2",
    hotel: {
      ...hotel,
      brand_display: brand,
      brand_display_status: brandStatus,
      brand_display_note: deep?.brand_resolution?.resolution || hotel.identity_notes || null,
      rooms: hotel.rooms || profile.rooms_suites || 86,
      positioning_statement:
        profile.positioning ||
        "Independent luxury cottage resort on a private Sandys peninsula — stewarded by Dovetail + Co.",
      market_intelligence: market,
      property_profile: profile,
    },
    organization: {
      slug: org.slug,
      entity_id: org.entity_id,
      display_name: org.display_name,
      legal_name: org.legal_name,
      website: org.website,
      known_hotel_count: (cohort.hotels || []).length,
      roles: org.organization_roles || ["owner", "developer", "operator", "sponsor"],
      not_asserted_as: org.not_asserted_as || ["natural_person_ubo"],
      headquarters: "New York, USA (public materials)",
    },
    ownership_group: {
      slug: org.slug,
      entity_id: org.entity_id,
      display_name: org.display_name,
      legal_name: org.legal_name,
      website: org.website,
      framing: "organization_sponsor_owner_developer_operator_contested",
      roles: org.organization_roles,
      not_asserted_as: org.not_asserted_as,
    },
    deep_research: deep || null,
    report: {
      case: "A",
      case_label: "Strong ownership intelligence — private Bermuda sponsor / PropCo (operator contested)",
      executive_summary: {
        text:
          `Economic sponsor: ${econ?.name || org.display_name} (HIGH) via 2021 acquisition. ` +
          `Bermuda property / hotel-development entity: ${propco?.name || "Cambridge Beaches Holdings Limited"} (HIGH — Tourism Investment Order 2022 + ministerial ownership statement; deed scan not completed). ` +
          `Operator: ${operatorStatus} — Dovetail first-party stewardship vs Benchmark/Pyramid continuity evidence; do not treat 2021 Benchmark announcement as proven CURRENT. ` +
          `Brand: ${brand} (${brandStatus}) — not Beaches Resorts. ` +
          `Rooms: ${profile.rooms_suites || 86}. Acreage: best-supported 23 acres (PROBABLE) with documented 20-acre conflict on tourism listings. ` +
          `Major event: 2021 Dovetail acquisition from Frascati Hotel Company; Butterfield Bank associated as lender.`,
        verification_bucket: "HIGH",
        source_count: sources.length,
        data_status: deep?.research_status || "NATIVE_PARTIAL",
        what_we_know: [
          "Cambridge Beaches Holdings Limited is the Order-defined hotel developer and ministerial property owner",
          "Dovetail + Co acquired the resort in 2021 from Frascati Hotel Company",
          "Phil Hospod is Founder & CEO of Dovetail; Karla Bruning is journalism-associated principal (not deed-asserted)",
          "Independent brand / cottage resort product (~86 suites)",
          "Tourism Investment Order 2022 renovation scope exists",
        ],
        what_remains_uncertain: [
          "Current exclusive operator (Dovetail vs Benchmark/Pyramid)",
          "Deed-level freehold instrument",
          "CBHL beneficial ownership / directors from registry",
          "Butterfield loan amounts / charges",
          "Full redevelopment completion vs remaining phases",
        ],
        why_this_hotel_matters:
          "Second Golden Demo archetype: private Bermuda ownership / Tourism Investment / sponsor principals / operator temporal ambiguity — proves Hotel Intelligence beyond listed-company PropCo filings.",
      },
      ownership_and_control: {
        economic_owner_or_group: {
          name: econ?.name || org.display_name,
          known: true,
          status: "HIGH",
          note: econ?.note || null,
        },
        legal_property_owner_propco: {
          name: propco?.name || "Cambridge Beaches Holdings Limited",
          known: true,
          status: propco?.confidence || "HIGH",
          note: propco?.note || null,
        },
        registered_operating_company: {
          name: null,
          known: false,
          status: "UNKNOWN",
          note: "Operating company distinct from CBHL not independently resolved; operator contested.",
        },
        parent_sponsor: {
          name: org.display_name,
          known: true,
          status: "HIGH",
          note: "Dovetail + Co economic sponsor / steward since 2021.",
        },
        operator: {
          name: operatorName,
          known: operatorStatus !== "CONTESTED",
          verification_bucket: verificationBucket(opRes.current_operator?.confidence || "probable"),
          relationship_type: "OPERATED_BY",
          status: operatorStatus,
          note: opRes.current_operator?.note || opRes.benchmark_temporal?.classification || null,
        },
        brand: {
          name: brand,
          known: true,
          status: brandStatus,
          note: deep?.brand_resolution?.resolution || null,
        },
        developer: {
          name: "Cambridge Beaches Holdings Limited (Order hotel developer); Dovetail stewardship of reinvestment",
          known: true,
          status: "HIGH",
        },
        ownership_structure: {
          structure: "Private sponsor (Dovetail) via Bermuda company Cambridge Beaches Holdings Limited",
          known: true,
          status: "HIGH",
          note: "Natural-person UBO beyond sponsor principals not deed-verified.",
        },
        lender: {
          name: "Butterfield Bank",
          known: true,
          status: "HIGH",
          note: "Acquisition / project lender per Hospod public statement; amounts not public.",
        },
      },
      ownership_chain: deep?.ownership_chain || null,
      decision_authority: {
        status: "PARTIAL",
        message:
          "Named people verified for title/organization where noted; decision and legal signing authority remain independent claims.",
        people: people.map((p) => ({
          name: p.name,
          title: p.title,
          organization: p.organization,
          role_category: p.decision_authority_category || p.role_category || p.category,
          category: p.decision_authority_category || p.role_category || p.category,
          decision_authority_category: p.decision_authority_category || p.role_category || p.category,
          strategic_relevance: p.strategic_relevance || p.relationship_to_hotel,
          relationship_to_hotel: p.relationship_to_hotel,
          person_verified: p.person_verified !== false,
          title_verified: p.title_verified === true,
          organization_verified: p.organization_verified !== false,
          decision_authority: p.decision_authority || "Authority Not Verified",
          legal_signing_authority: p.legal_signing_authority || "Authority Not Verified",
          confidence: p.confidence || "HIGH",
          authority_note: p.authority_note || null,
          contact: p.contact || null,
          professional_profile_url: p.professional_profile_url || p.professional_profile?.url || null,
          professional_profile_verified:
            p.professional_profile_verified === true || p.professional_profile?.verified === true,
          professional_profile_type: p.professional_profile_type || p.professional_profile?.type || null,
        })),
      },
      organization_and_portfolio: {
        organization_name: org.display_name,
        framing: cohort.portfolio_framing?.title || "Known Dovetail Hotel Relationships",
        do_not_say: cohort.portfolio_framing?.do_not_say || null,
        hotels: (() => {
          const { assets, observations } = resolveDovetailPortfolioAssets(deep);
          const rows = assets.length ? assets : cohort.hotels || [];
          return rows.map((h) => {
            const resolved = applyRoomsResolutionToPortfolioHotel(
              {
                name: h.name,
                rooms: h.rooms,
                rooms_confidence: h.rooms_confidence || h.confidence,
                rooms_source_type: h.rooms_source_type,
                census_rooms: h.census_rooms ?? null,
              },
              { researchObservations: observations }
            );
            return {
              hotel: h.name,
              market: h.market || h.city || null,
              rooms: resolved.rooms ?? null,
              rooms_source: resolved.rooms_source || null,
              rooms_provenance: resolved.rooms_provenance || null,
              brand: h.brand || h.affiliation_display || null,
              relationship: Array.isArray(h.relationships)
                ? h.relationships.join(" · ")
                : h.relationship_type || null,
              status: h.status || (h.is_current === false ? "Historical" : "Open"),
              confidence: verificationBucket(h.confidence || h.verification_status || "probable"),
              note: h.note || h.identity_notes || null,
            };
          });
        })(),
      },
      relationships: {
        edges: deep?.relationships || [],
        note: "Current + historical edges only where evidence supports; Benchmark OPERATED_BY is temporally qualified.",
      },
      property_history: deep?.property_history || [],
      redevelopment: deep?.redevelopment || null,
      market_intelligence: market,
      // Surface on report root (same contract as GSF) — workspace reads report.research_gaps, not research_status.gaps.
      research_gaps: (deep?.research_gaps || []).map((g) =>
        typeof g === "string"
          ? { what: g, why: "Affects ownership, operator, or diligence confidence", state: "Unresolved" }
          : g
      ),
      brand_chronology:
        deep?.brand_chronology ||
        [
          deep?.brand_resolution?.current_brand || brand
            ? {
                date: "CURRENT",
                label: deep?.brand_resolution?.current_brand || brand,
                brand: deep?.brand_resolution?.current_brand || brand,
                status: deep?.brand_resolution?.current_status || brandStatus || "CURRENT",
              }
            : null,
          {
            date: "2021-05-20",
            label: "Benchmark announces Cambridge will join portfolio / Benchmark to operate",
            brand: "Benchmark / Pyramid (announced operator relationship)",
            status: "ANNOUNCED",
          },
        ].filter(Boolean),
      brand_resolution: deep?.brand_resolution || null,
      corporate_contacts: {
        headquarters: org.headquarters || "New York, USA (public materials)",
        website: deep?.corporate_contacts?.sponsor_website || org.website || null,
        corporate_phone: null,
        business_email: null,
        property_phone: deep?.corporate_contacts?.hotel_phone || null,
        property_email: null,
        property_website: deep?.corporate_contacts?.hotel_website || null,
      },
      commercial_pursuit: (() => {
        const raw = deep?.commercial_pursuit || {};
        const gaps = deep?.research_gaps || [];
        return {
          why_matters:
            raw.why_matters ||
            "Private Bermuda sponsor / PropCo / Tourism Investment archetype with contested operator evidence — second Golden Demo ownership pattern.",
          who_controls_relationship: raw.who_controls_relationship || null,
          portfolio_leverage:
            raw.portfolio_leverage ||
            (Array.isArray(deep?.portfolio_notes?.assets)
              ? `Known Dovetail-linked hotel relationships: ${deep.portfolio_notes.assets.length} assets in research notes.`
              : null),
          timing_signals:
            raw.timing_signals ||
            "2021 Dovetail acquisition; 2021 Benchmark announcement; Tourism Investment Order 2022; ongoing renovation / award cycle.",
          approach_organization:
            raw.approach_organization ||
            (Array.isArray(raw.likely_approach_paths) ? raw.likely_approach_paths.join(" · ") : null),
          missing_before_outreach:
            raw.missing_before_outreach ||
            (Array.isArray(gaps) ? gaps.slice(0, 4).join("; ") : null),
          brand_relationships_summary:
            raw.brand_relationships_summary ||
            `${brand} (${brandStatus}) — not Beaches Resorts; Benchmark/Pyramid is operator-temporal, not current brand flag.`,
          operator_relationships_summary:
            raw.operator_relationships_summary ||
            `Operator ${operatorStatus}: Dovetail first-party stewardship vs Benchmark/Pyramid continuity evidence.`,
          do_not_assume: raw.do_not_assume || [],
          likely_approach_paths: raw.likely_approach_paths || [],
        };
      })(),
      research_status: {
        message:
          deep?.research_status === "NATIVE_PARTIAL_PENDING_WEBHOUND"
            ? "Native research populated; founder-authorized Webhound Full Investigation in progress or pending merge."
            : deep?.research_status || "PARTIAL",
        gaps: deep?.research_gaps || [],
        webhound_session_id: deep?.webhound_session_id || null,
        webhound_budget_usd: deep?.webhound_budget_usd || 5,
      },
      evidence: {
        source_count: sources.length,
        sources: sources.map((s) => ({
          provider: s.provider,
          title: s.title,
          url: s.url,
          published_date: s.published_date || null,
          observed_date: s.observed_date || deep?.observed_date,
          evidence_excerpt: s.note || s.title,
          authority: s.authority,
          confidence: "high",
        })),
        what_it_proves:
          "Private Bermuda PropCo/developer entity + Dovetail 2021 acquisition stewardship + Tourism Investment redevelopment frame.",
        what_it_does_not_prove:
          "Deed natural-person UBO; exclusive CURRENT operator identity; full renovation completion; Butterfield loan quantum.",
      },
    },
  };
}

export function buildDovetailOwnershipGroupProfile(slug = DOVETAIL_SLUG) {
  if (String(slug || "").trim() !== DOVETAIL_SLUG) {
    return { ok: false, error: "group_not_found" };
  }
  const cohort = loadDovetailGoldenDemoCohort();
  const org = orgOf(cohort);
  const deep = loadCambridgeDeepResearch();
  const framing = cohort.portfolio_framing || {};

  const { assets: researchAssets, observations: researchRoomsObs } =
    resolveDovetailPortfolioAssets(deep);

  const byName = new Map();
  for (const h of cohort.hotels || []) {
    byName.set(String(h.name || "").toLowerCase(), { ...h });
  }
  for (const asset of researchAssets) {
    const key = String(asset.name || "").toLowerCase();
    if (!key) continue;
    const existing = byName.get(key) || {};
    const relationships = Array.isArray(asset.relationships) ? asset.relationships : [];
    byName.set(key, {
      ...existing,
      name: asset.name || existing.name,
      market: asset.market || existing.market || existing.city,
      city: existing.city || null,
      rooms: asset.rooms != null ? asset.rooms : existing.rooms,
      rooms_confidence: asset.rooms_confidence || asset.confidence || existing.rooms_confidence,
      rooms_source_type: asset.rooms_source_type || existing.rooms_source_type,
      rooms_source_url: asset.rooms_source_url || null,
      rooms_provenance: asset.rooms_provenance || null,
      accommodation_units: asset.accommodation_units ?? existing.accommodation_units ?? null,
      brand: asset.brand || existing.brand,
      status: asset.status || existing.status || "Open",
      relationship_type:
        existing.relationship_type ||
        (relationships.includes("OWNED_BY") || /OWNED/i.test(relationships.join(" "))
          ? "OWNED_BY"
          : relationships.includes("OPERATED_BY") || /OPERATED_BY/i.test(relationships.join(" "))
            ? "OPERATED_BY"
            : relationships[0] || "SPONSORED_BY"),
      secondary_relationship_type: existing.secondary_relationship_type || null,
      economic_owner_verified:
        existing.economic_owner_verified === true ||
        relationships.some((r) => /OWNED/i.test(String(r))),
      verification_status: asset.confidence || existing.verification_status || "probable",
      airtable_record_id: existing.airtable_record_id || null,
      property_census_record_id: existing.property_census_record_id || null,
      hotel_id: existing.hotel_id || null,
      census_rooms: existing.census_rooms ?? existing.rooms_census ?? null,
    });
  }

  const hotels = [...byName.values()].map((h) => {
    const b = h.affiliation_display || h.brand || "Independent";
    const resolved = applyRoomsResolutionToPortfolioHotel(
      {
        ...h,
        name: h.name,
        rooms: h.rooms,
        census_rooms: h.census_rooms ?? null,
        rooms_confidence: h.rooms_confidence || h.verification_status || h.confidence,
        rooms_source_type: h.rooms_source_type || "validated_research",
      },
      { researchObservations: researchRoomsObs }
    );
    return {
      ...h,
      ...resolved,
      brand_display: b,
      brand_display_status: "CURRENT",
      market_display: h.market || (h.city ? `${h.city}` : "Unknown market"),
      verification_bucket: verificationBucket(h.verification_status || h.confidence || "probable"),
      rooms_display: resolved.rooms_display ?? resolved.rooms ?? null,
      rooms_status: resolved.rooms_status || (resolved.rooms != null ? "LIVE" : "UNKNOWN"),
      economic_owner_status: h.economic_owner_verified ? "VERIFIED" : "NOT_YET_VERIFIED",
      relationship_type: h.relationship_type || "SPONSORED_BY",
    };
  });

  const ownedOrControlled = hotels.filter(
    (h) => h.relationship_type === "OWNED_BY" || h.economic_owner_verified
  ).length;
  const operatedOrManaged = hotels.length - ownedOrControlled;
  const summary = {
    total_relationships: hotels.length,
    owned_or_controlled_verified: ownedOrControlled,
    operated_or_managed: operatedOrManaged,
    ownership_unknown: Math.max(0, hotels.length - ownedOrControlled),
  };
  const byBucket = { VERIFIED: 0, HIGH: 0, PROBABLE: 0, UNKNOWN: 0 };
  for (const h of hotels) byBucket[h.verification_bucket] = (byBucket[h.verification_bucket] || 0) + 1;
  const markets = [...new Set(hotels.map((h) => h.market_display).filter(Boolean))].sort();
  const knownRooms = hotels.reduce((s, h) => s + (h.rooms != null ? Number(h.rooms) || 0 : 0), 0);

  return {
    ok: true,
    version: GOLDEN_DEMO_DOVETAIL_VERSION,
    data_status: cohort.data_status || "CONTROLLED_DEMO",
    costar_firewall: "enforced",
    page_framing: {
      title: framing.title || "Known Dovetail Hotel Relationships",
      kicker: "Organization profile",
      do_not_say: framing.do_not_say || "All hotels owned and operated by Dovetail + Co",
      summary,
    },
    // Canonical key shared with GSF — Organization tab consumes `group`, not a hotel-specific shape.
    group: {
      ...org,
      display_name: org.display_name,
      legal_name: org.legal_name,
      website: org.website,
      headquarters: "New York, USA (public materials)",
      headquarters_status: "PROBABLE",
      known_current_hotel_count: hotels.length,
      known_hotel_relationships: hotels.length,
      known_rooms: knownRooms || null,
      known_rooms_status: knownRooms ? "LIVE" : "UNKNOWN",
      markets,
      evidence_status: "HIGH",
      evidence_summary: org.evidence_basis || null,
      verification_breakdown: byBucket,
      relationship_summary: summary,
      leadership: (deep?.people || [])
        .filter((p) => /dovetail/i.test(String(p.organization || "")))
        .map((p) => ({ name: p.name, title: p.title, confidence: p.confidence })),
    },
    portfolio: hotels,
    why_this_matters: {
      text:
        org.evidence_basis ||
        "Dovetail portfolio relationships vary by asset — distinguish sponsor, ownership via PropCo, and contested operator roles before outreach.",
    },
  };
}
