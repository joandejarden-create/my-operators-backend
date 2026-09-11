/**
 * Mexico Explorer demo cohort — Sheraton Guadalajara Expo + Real Inn / voco Cancún.
 * Deep-research enrichment from $5 Full HI Webhound (auto_promote=false).
 * Operator ≠ owner. No invented VERIFIED UBO / deed claims.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyRoomsResolutionToPortfolioHotel } from "../../property-fundamentals/index.js";

export const MEXICO_EXPLORER_DEMO_VERSION = "mexico-explorer-demo-cohort-v1";
export const SHERATON_GDL_AIRTABLE_ID = "recsYJb2R1jarPpK3";
export const REAL_INN_CANCUN_AIRTABLE_ID = "recTYaiA4S6fR6ixx";
export const SHERATON_GDL_DEEP_RESEARCH_VERSION = "sheraton-gdl-expo-deep-research-v1";
export const REAL_INN_CANCUN_DEEP_RESEARCH_VERSION = "real-inn-cancun-deep-research-v1";

/** Census Property IDs for Alliance six-pack siblings (rooms fallback when research missing). */
const ALLIANCE_SIBLING_CENSUS_IDS = Object.freeze({
  cancun: REAL_INN_CANCUN_AIRTABLE_ID,
  guadalajara_expo: "recGZZCek9vDQGG1L",
  ciudad_juarez: "rec79Xs4mZkuiWnuN",
  san_luis_potosi: "recFspIiglYxJp1N1",
  nuevo_laredo: "recogJrXdZHRV06Bl",
  torreon: "recZxCHVNG0bDQhfG",
});

function resolveMexicoAssetCensusId(asset = {}) {
  if (asset.census_record_id || asset.airtable_record_id) {
    return asset.census_record_id || asset.airtable_record_id;
  }
  const name = String(asset.name || "");
  if (/Sheraton Guadalajara Expo/i.test(name)) return SHERATON_GDL_AIRTABLE_ID;
  if (/Cancún|Cancun|Real Inn Cancun/i.test(name)) return ALLIANCE_SIBLING_CENSUS_IDS.cancun;
  if (/Guadalajara Expo/i.test(name) && /voco|Real Inn/i.test(name)) {
    return ALLIANCE_SIBLING_CENSUS_IDS.guadalajara_expo;
  }
  if (/Ciudad Juarez|Ciudad Juárez/i.test(name)) return ALLIANCE_SIBLING_CENSUS_IDS.ciudad_juarez;
  if (/San Luis Potosi|San Luis Potosí/i.test(name)) {
    return ALLIANCE_SIBLING_CENSUS_IDS.san_luis_potosi;
  }
  if (/Nuevo Laredo/i.test(name)) return ALLIANCE_SIBLING_CENSUS_IDS.nuevo_laredo;
  if (/Torreon|Torreón/i.test(name)) return ALLIANCE_SIBLING_CENSUS_IDS.torreon;
  return null;
}

function mapMexicoPortfolioAsset(a, orgSlug) {
  const relationships = Array.isArray(a.relationships) ? a.relationships : [];
  const ownedByAlliance = relationships.some((r) =>
    /OWNED_BY_ALLIANCE|OWNED_BY\b/i.test(String(r))
  );
  const siblingPackage = relationships.some((r) =>
    /SIBLING_CONVERSION_PORTFOLIO|SIBLING_HNF_CONVERSION_PACKAGE/i.test(String(r))
  );
  const operatedByAimbridge = relationships.some((r) =>
    /OPERATED_BY_AIMBRIDGE|OPERATED_BY/i.test(String(r))
  );
  const ownedByHnf = relationships.some((r) => /OWNED_BY_HNF|OWNED_BY\b/i.test(String(r)));

  let owned = false;
  let relationshipType = "RELATED";
  let secondary = null;

  if (orgSlug === "aimbridge-latam") {
    // Operator org: never assert economic ownership from management edges.
    owned = false;
    relationshipType = "OPERATED_BY";
    secondary = null;
  } else if (orgSlug === "alliance-hotel-management") {
    owned = ownedByAlliance || siblingPackage;
    relationshipType = owned ? "OWNED_BY" : operatedByAimbridge ? "OPERATED_BY" : "RELATED";
    secondary = operatedByAimbridge ? "OPERATED_BY" : null;
  } else if (orgSlug === "inmobiliaria-hnf") {
    owned = ownedByHnf || siblingPackage || ownedByAlliance;
    relationshipType = owned
      ? "OWNED_BY"
      : operatedByAimbridge
        ? "OPERATED_BY"
        : siblingPackage
          ? "OWNED_BY"
          : "RELATED";
    secondary = operatedByAimbridge ? "OPERATED_BY" : null;
  } else {
    owned = ownedByAlliance || ownedByHnf || relationships.some((r) => /OWNED/i.test(String(r)));
    relationshipType = owned
      ? "OWNED_BY"
      : operatedByAimbridge
        ? "OPERATED_BY"
        : siblingPackage
          ? "OWNED_BY"
          : "RELATED";
    secondary = operatedByAimbridge ? "OPERATED_BY" : null;
  }

  const censusId = resolveMexicoAssetCensusId(a);
  const cohortHotel = censusId
    ? findHotel(loadMexicoExplorerDemoCohort(), censusId)
    : null;
  const researchRooms = a.rooms ?? null;
  const censusRooms =
    a.census_rooms ??
    (cohortHotel?.rooms != null && Number(cohortHotel.rooms) > 0 ? cohortHotel.rooms : null);

  const resolved = applyRoomsResolutionToPortfolioHotel({
    name: a.name,
    rooms: researchRooms,
    rooms_confidence: a.rooms_confidence || a.confidence || "HIGH",
    rooms_source_type: a.rooms_source_type || a.rooms_basis || "validated_research",
    census_rooms: censusRooms,
  });

  return {
    name: a.name,
    city: a.market || null,
    market: a.market || null,
    rooms: resolved.rooms ?? null,
    rooms_display: resolved.rooms_display ?? resolved.rooms ?? null,
    rooms_status: resolved.rooms_status || (resolved.rooms != null ? "LIVE" : "UNKNOWN"),
    rooms_source: resolved.rooms_source || null,
    rooms_confidence: resolved.rooms_confidence || null,
    rooms_provenance: resolved.rooms_provenance || null,
    rooms_basis: a.rooms_basis || null,
    brand_display: a.brand || null,
    relationship_type: relationshipType,
    secondary_relationship_type: secondary,
    relationship_note:
      a.note ||
      (orgSlug === "alliance-hotel-management"
        ? "Public package ownership (Alliance); day-to-day management = Aimbridge LATAM — not Alliance."
        : orgSlug === "aimbridge-latam"
          ? "Aimbridge LATAM operates / manages — ownership is a separate economic claim."
          : null),
    economic_owner_verified: owned,
    verification_bucket: verificationBucket(a.confidence || "high"),
    status: a.status || "Open",
    airtable_record_id: censusId,
    census_record_id: censusId,
  };
}

function collectAimbridgeOperatedAssets() {
  const sheraton = loadMexicoExplorerDeepResearch(SHERATON_GDL_AIRTABLE_ID);
  const realInn = loadMexicoExplorerDeepResearch(REAL_INN_CANCUN_AIRTABLE_ID);
  const byName = new Map();
  for (const a of realInn?.portfolio_notes?.assets || []) {
    byName.set(String(a.name || "").toLowerCase(), a);
  }
  for (const a of sheraton?.portfolio_notes?.assets || []) {
    const key = String(a.name || "").toLowerCase();
    if (!byName.has(key)) byName.set(key, a);
  }
  return [...byName.values()];
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const COHORT_FIXTURE = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/mexico-explorer-demo-cohort-v1.json"
);
const SHERATON_DEEP = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/sheraton-gdl-expo-deep-research-v1.json"
);
const REAL_INN_DEEP = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/real-inn-cancun-deep-research-v1.json"
);

let cachedCohort = null;
let cachedDeepById = null;

export function loadMexicoExplorerDemoCohort() {
  if (cachedCohort) return cachedCohort;
  cachedCohort = JSON.parse(fs.readFileSync(COHORT_FIXTURE, "utf8"));
  return cachedCohort;
}

function loadDeepMap() {
  if (cachedDeepById) return cachedDeepById;
  cachedDeepById = new Map();
  if (fs.existsSync(SHERATON_DEEP)) {
    const d = JSON.parse(fs.readFileSync(SHERATON_DEEP, "utf8"));
    cachedDeepById.set(SHERATON_GDL_AIRTABLE_ID, d);
  }
  if (fs.existsSync(REAL_INN_DEEP)) {
    const d = JSON.parse(fs.readFileSync(REAL_INN_DEEP, "utf8"));
    cachedDeepById.set(REAL_INN_CANCUN_AIRTABLE_ID, d);
  }
  return cachedDeepById;
}

export function loadMexicoExplorerDeepResearch(airtableRecordId) {
  const key = String(airtableRecordId || "").trim();
  return loadDeepMap().get(key) || null;
}

export function clearMexicoExplorerDemoCohortCache() {
  cachedCohort = null;
  cachedDeepById = null;
}

export function isMexicoExplorerDemoHotelId(id) {
  const key = String(id || "").trim();
  return key === SHERATON_GDL_AIRTABLE_ID || key === REAL_INN_CANCUN_AIRTABLE_ID;
}

function findHotel(cohort, id) {
  let key = String(id || "").trim();
  // Common Airtable id typo: "rect…" instead of "rec…"
  if (/^rect[A-Za-z0-9]{14}$/.test(key)) {
    key = "rec" + key.slice(4);
  }
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

function findOrg(cohort, slug) {
  return (cohort.organizations || []).find((o) => o.slug === slug) || null;
}

function verificationBucket(status) {
  const s = String(status || "").toLowerCase();
  if (s === "verified" || s === "high" || s === "confirmed") return "HIGH";
  if (s === "probable" || s === "needs_review") return "PROBABLE";
  if (s === "former") return "HIGH";
  return "UNKNOWN";
}

function mapPeople(people) {
  return (people || []).map((p) => ({
    name: p.name,
    title: p.title,
    organization: p.organization,
    role_category: p.decision_authority_category || p.role_category || p.category,
    category: p.decision_authority_category || p.role_category || p.category,
    decision_authority_category: p.decision_authority_category || p.role_category || p.category,
    strategic_relevance: p.strategic_relevance || p.relationship_to_hotel,
    relationship_to_hotel: p.relationship_to_hotel,
    bio: p.bio || null,
    person_verified: p.person_verified !== false,
    title_verified: p.title_verified === true,
    organization_verified: p.organization_verified !== false,
    decision_authority: p.decision_authority || "Authority Not Verified",
    legal_signing_authority: p.legal_signing_authority || "Authority Not Verified",
    confidence: p.confidence || "HIGH",
    authority_note: p.authority_note || null,
    contact: p.contact || null,
    professional_profile: p.professional_profile || null,
    professional_profile_url:
      p.professional_profile_url || (p.professional_profile && p.professional_profile.url) || null,
    professional_profile_verified:
      p.professional_profile_verified === true ||
      (p.professional_profile && p.professional_profile.verified === true),
    professional_profile_type:
      p.professional_profile_type || (p.professional_profile && p.professional_profile.type) || null,
    linkedin_url: p.linkedin_url || p.professional_profile_url || null,
  }));
}

/**
 * Normalize deep-research contacts to the Ownership tab kv shape
 * (Cambridge/KGPV: website, headquarters, corporate_phone, business_email, property_*).
 * Never put operator hotel-listing URLs in the primary Website field.
 */
function normalizeCorporateContacts(deep, hotel, profile = {}) {
  const raw = deep?.corporate_contacts || {};
  const propertyWebsite =
    raw.property_website ||
    raw.hotel_website ||
    hotel.website ||
    profile.website ||
    null;
  const website =
    raw.website ||
    raw.sponsor_website ||
    propertyWebsite ||
    null;
  const headquarters =
    raw.headquarters ||
    profile.address ||
    null;
  return {
    headquarters,
    website,
    sponsor_website: raw.sponsor_website || null,
    operator_website: raw.operator_website || null,
    property_website: propertyWebsite,
    corporate_phone: raw.corporate_phone || raw.phone || null,
    business_email: raw.business_email || raw.email || null,
    property_phone: raw.property_phone || raw.hotel_phone || profile.phone || null,
    property_email: raw.property_email || raw.hotel_email || null,
    notes: raw.notes || null,
  };
}

function orgPayloadFromDeep(deep, cohort, hotel, contacts = null) {
  const econ = (deep?.ownership_chain || []).find((n) => n.role === "economic_owner");
  const opName = deep?.operator_resolution?.current_operator?.name;
  const slugGuess =
    hotel.airtable_record_id === SHERATON_GDL_AIRTABLE_ID
      ? "inmobiliaria-hnf"
      : "alliance-hotel-management";
  const fromCohort = findOrg(cohort, slugGuess);
  const display =
    econ?.name ||
    fromCohort?.display_name ||
    (hotel.airtable_record_id === SHERATON_GDL_AIRTABLE_ID
      ? "HNF / Newton family sphere"
      : "Alliance Hotel Management");
  const normalized = contacts || normalizeCorporateContacts(deep, hotel);
  return {
    slug: fromCohort?.slug || slugGuess,
    entity_id: fromCohort?.entity_id || `dle_mexico_demo_${slugGuess}`,
    display_name: display,
    legal_name:
      (deep?.ownership_chain || []).find((n) => n.role === "propco")?.name ||
      fromCohort?.legal_name ||
      display,
    website: normalized.website || fromCohort?.website || null,
    known_hotel_count: (deep?.portfolio_notes?.assets || []).length || 1,
    roles: ["owner", "sponsor"],
    not_asserted_as: ["natural_person_ubo", "deed_verified_without_registry"],
    headquarters: normalized.headquarters || null,
    phone: normalized.corporate_phone || null,
    email: normalized.business_email || null,
    evidence_basis: `Webhound Full HI ${deep?.webhound_session_id || ""}`.trim(),
    operator_platform: opName || null,
  };
}

/**
 * Ownership report — Case A when deep research present; Case B Census fallback otherwise.
 */
export function buildMexicoExplorerDemoOwnershipReport(airtableRecordId) {
  const cohort = loadMexicoExplorerDemoCohort();
  const hotel = findHotel(cohort, airtableRecordId);
  if (!hotel) {
    return { ok: false, error: "mexico_demo_hotel_not_in_cohort" };
  }

  const deep = loadMexicoExplorerDeepResearch(airtableRecordId);
  if (!deep) {
    return buildCaseBFallback(cohort, hotel);
  }

  const propco = (deep.ownership_chain || []).find((n) => n.role === "propco");
  const econ = (deep.ownership_chain || []).find((n) => n.role === "economic_owner");
  const sponsors = (deep.ownership_chain || []).find((n) => n.role === "sponsor_principals");
  const opRes = deep.operator_resolution || {};
  const operatorName =
    opRes.current_operator?.name || hotel.operator || hotel.management_company || null;
  const brand =
    deep.brand_resolution?.current_brand ||
    deep.brand_resolution?.current_trading_brand ||
    hotel.brand ||
    hotel.affiliation_display;
  const brandStatus =
    deep.brand_resolution?.current_status || deep.brand_resolution?.status || "CURRENT";
  // Prefer current marketed property name over former Census trading name.
  const displayHotelName =
    deep.brand_resolution?.current_property_name ||
    deep.property_profile?.current_marketed_name ||
    deep.property_profile?.canonical_name ||
    hotel.canonical_trading_name ||
    hotel.name;
  const sources = deep.sources || [];
  const people = deep.people || [];
  const profile = deep.property_profile || {};
  const contacts = normalizeCorporateContacts(deep, hotel, profile);
  const orgPayload = orgPayloadFromDeep(deep, cohort, hotel, contacts);
  const propcoKnown = Boolean(propco?.name);
  const econKnown = Boolean(econ?.name);

  const execParts = [
    econKnown
      ? `Economic owner / group: ${econ.name} (${econ.confidence || "HIGH"}).`
      : "Economic owner: Not yet verified.",
    propcoKnown
      ? `PropCo / title vehicle: ${propco.name} (${propco.confidence || "HIGH"}).`
      : "PropCo / legal title: UNKNOWN (registry extract not obtained).",
    `Operator: ${operatorName || "unresolved"} (${opRes.current_operator?.confidence || "HIGH"} — ${opRes.current_operator?.status || "CURRENT"}).`,
    `Brand: ${brand} (${brandStatus}).`,
    sponsors?.name
      ? `Sponsor principals (PROBABLE): ${sponsors.name}.`
      : null,
    "Natural-person UBO / deed folio not asserted as VERIFIED without registry.",
  ].filter(Boolean);

  return {
    ok: true,
    in_cohort: true,
    intelligence_case: "A",
    version: MEXICO_EXPLORER_DEMO_VERSION,
    deep_research_version: deep.version,
    data_status: deep.research_status || "COMPLETED_WITH_OPEN_QUESTIONS",
    costar_firewall: "enforced",
    golden_demo: "MEXICO_EXPLORER_DEMO",
    hotel: {
      ...hotel,
      name: displayHotelName,
      canonical_trading_name: displayHotelName,
      former_trading_names: hotel.former_trading_names || ["Real Inn Cancun", "Real Inn Cancún"],
      brand: brand,
      brand_display: brand,
      brand_display_status: brandStatus,
      brand_display_note:
        deep.brand_resolution?.resolution || hotel.identity_notes || null,
      affiliation_display: brand,
      rooms: hotel.rooms || profile.rooms_suites || null,
      positioning_statement: profile.positioning || null,
      property_profile: profile,
      economic_owner_verified: econKnown && verificationBucket(econ.confidence) === "HIGH",
    },
    organization: orgPayload,
    ownership_group: {
      ...orgPayload,
      framing: "organization_owner_sponsor_operator_separated",
    },
    deep_research: deep,
    report: {
      case: "A",
      case_label: propcoKnown
        ? "Strong ownership intelligence — public owner/PropCo path with operator/brand separation (open registry gaps)"
        : "Strong ownership intelligence — public package/economic owner with operator/brand separation; Mexican PropCo / title vehicle still unknown",
      executive_summary: {
        text: execParts.join(" "),
        verification_bucket: propcoKnown || econKnown ? "HIGH" : "PROBABLE",
        source_count: sources.length,
        data_status: deep.research_status || "COMPLETED_WITH_OPEN_QUESTIONS",
        what_we_know: [
          econKnown ? `${econ.name} as economic owner/group (${econ.confidence})` : null,
          propcoKnown ? `${propco.name} as PropCo candidate (${propco.confidence})` : null,
          `${operatorName} as current operator`,
          `${brand} as ${brandStatus} brand`,
          `${(people || []).length} named people with role evidence`,
        ].filter(Boolean),
        what_remains_uncertain: (deep.research_gaps || []).slice(0, 6),
        why_this_hotel_matters:
          deep.commercial_pursuit?.why_matters ||
          "Mexico Explorer demo ownership / operator / brand separation archetype from Full HI Webhound.",
      },
      ownership_and_control: {
        economic_owner_or_group: econKnown
          ? {
              name: econ.name,
              known: true,
              status: econ.confidence || "HIGH",
              note: econ.note || null,
            }
          : {
              name: null,
              known: false,
              status: "NOT_YET_VERIFIED",
              note: "Owner: Not yet verified.",
            },
        legal_property_owner_propco: propcoKnown
          ? {
              name: propco.name,
              known: true,
              status: propco.confidence || "HIGH",
              note: propco.note || null,
            }
          : {
              name: null,
              known: false,
              status: "UNKNOWN",
              note: propco?.note || "PropCo / legal title unknown.",
            },
        parent_sponsor: {
          name: orgPayload.display_name,
          known: true,
          status: econ?.confidence || "PROBABLE",
          note: sponsors?.note || econ?.note || null,
        },
        operator: {
          name: operatorName,
          known: Boolean(operatorName),
          verification_bucket: verificationBucket(opRes.current_operator?.confidence || "high"),
          relationship_type: "OPERATED_BY",
          status: opRes.current_operator?.status || "CURRENT",
          note:
            opRes.current_operator?.note ||
            (opRes.former_operator
              ? `Former: ${opRes.former_operator.name} (${opRes.former_operator.confidence}).`
              : null),
        },
        brand: {
          name: brand,
          known: Boolean(brand),
          status: brandStatus,
          brand_family: deep.brand_resolution?.brand_family || hotel.brand_family || null,
          note: deep.brand_resolution?.resolution || null,
        },
        ownership_structure: {
          structure: propcoKnown
            ? `Owner/sponsor via ${propco.name}; operated by ${operatorName}; branded ${brand}`
            : `Public owner group ${econ?.name || "unresolved"}; Mexican PropCo unknown; operated by ${operatorName}`,
          known: true,
          status: propcoKnown ? "HIGH" : "PROBABLE",
          note: "Operator ≠ owner enforced.",
        },
      },
      ownership_chain: deep.ownership_chain || null,
      decision_authority: {
        status: "PARTIAL",
        message:
          "Named people verified for title/organization where noted; decision and legal signing authority remain independent claims.",
        people: mapPeople(people),
        roles: (people || []).map((p) => ({
          role: p.role_category || p.category,
          name: p.name,
          title: p.title,
          organization: p.organization,
          status: p.confidence,
          decision_authority: p.decision_authority || "Authority Not Verified",
          legal_signing_authority: p.legal_signing_authority || "Authority Not Verified",
          note: p.authority_note,
        })),
      },
      organization_and_portfolio: {
        organization_name: orgPayload.display_name,
        framing: deep.portfolio_notes?.framing || "Known related hotel relationships",
        hotels: (deep.portfolio_notes?.assets || []).map((h) => {
          const row = mapMexicoPortfolioAsset(
            h,
            hotel.airtable_record_id === REAL_INN_CANCUN_AIRTABLE_ID
              ? "alliance-hotel-management"
              : "inmobiliaria-hnf"
          );
          return {
            hotel: row.name,
            name: row.name,
            market: row.market || null,
            rooms: row.rooms ?? null,
            rooms_source: row.rooms_source || null,
            rooms_provenance: row.rooms_provenance || null,
            brand: row.brand_display || null,
            relationship: Array.isArray(h.relationships)
              ? h.relationships.join(" · ")
              : null,
            status: h.status || "Open",
            confidence: verificationBucket(h.confidence || "high"),
            note: h.note || null,
          };
        }),
      },
      relationships: {
        edges: deep.relationships || [],
        note: "Current + historical edges only where evidence supports.",
      },
      // UI Relationship Detail historically read `from`/`to`; keep both shapes.
      relationship_edges: (deep.relationships || []).map((e) => ({
        ...e,
        from: e.from || e.subject || null,
        to: e.to || e.object || null,
        type: e.type || e.relationship_type || null,
        temporal_status: e.temporal_status || e.temporal || null,
      })),
      evidence: {
        claim: `${hotel.name} → ownership/operator/brand structure from Full HI Webhound (auto_promote=false)`,
        verification_bucket: propcoKnown || econKnown ? "HIGH" : "PROBABLE",
        source_count: sources.length,
        sources: sources.map((s) => ({
          provider: s.provider,
          title: s.title,
          url: s.url,
          published_date: null,
          observed_date: s.observed_date || deep.observed_date,
          evidence_excerpt: s.note || s.title,
          authority: s.authority,
          confidence: "high",
        })),
        what_it_proves:
          "Supports public ownership/PropCo path, current operator, brand chronology, portfolio siblings, and named people/LinkedIn where verified.",
        what_it_does_not_prove:
          "Does not prove deed-level freehold without registry extract; does not prove natural-person UBO; does not auto-promote Census fields.",
      },
      brand_chronology: deep.brand_chronology || null,
      brand_resolution: deep.brand_resolution
        ? {
            ...deep.brand_resolution,
            census_claim_preserved: deep.brand_resolution.census_claim_preserved || null,
          }
        : null,
      organizations: deep.organizations || null,
      corporate_contacts: contacts,
      property_history: deep.property_history || [],
      research_gaps: (deep.research_gaps || []).map((g) =>
        typeof g === "string"
          ? { what: g, why: "Affects ownership, operator, or diligence confidence", state: "Unresolved" }
          : g
      ),
      commercial_pursuit: deep.commercial_pursuit || null,
      research_status: {
        message: `Full HI Webhound compiled (${deep.webhound_session_id}); auto_promote=false.`,
        webhound_url: deep.webhound_url || null,
        prompt_standard: deep.prompt_standard || "FULL_HOTEL_INTELLIGENCE_PROMPT_V1",
      },
    },
  };
}

const MEXICO_ORG_SLUGS = new Set([
  "inmobiliaria-hnf",
  "alliance-hotel-management",
  "aimbridge-latam",
  "mexico-explorer-demo",
]);

/**
 * Organization tab payload — same `group` + `portfolio` shape as GSF/Dovetail.
 */
export function buildMexicoExplorerOwnershipGroupProfile(slug) {
  const key = String(slug || "").trim();
  if (!MEXICO_ORG_SLUGS.has(key) && key !== "hoteles-camino-real" && key !== "marriott-international" && key !== "ihg-voco") {
    return { ok: false, error: "group_not_found" };
  }

  const cohort = loadMexicoExplorerDemoCohort();
  const org =
    findOrg(cohort, key) ||
    (key === "mexico-explorer-demo" ? findOrg(cohort, "aimbridge-latam") : null);
  if (!org && key !== "mexico-explorer-demo") {
    return { ok: false, error: "group_not_found" };
  }

  const focusDeep =
    key === "alliance-hotel-management"
      ? loadMexicoExplorerDeepResearch(REAL_INN_CANCUN_AIRTABLE_ID)
      : key === "inmobiliaria-hnf"
        ? loadMexicoExplorerDeepResearch(SHERATON_GDL_AIRTABLE_ID)
        : key === "aimbridge-latam"
          ? loadMexicoExplorerDeepResearch(SHERATON_GDL_AIRTABLE_ID) ||
            loadMexicoExplorerDeepResearch(REAL_INN_CANCUN_AIRTABLE_ID)
          : loadMexicoExplorerDeepResearch(SHERATON_GDL_AIRTABLE_ID) ||
            loadMexicoExplorerDeepResearch(REAL_INN_CANCUN_AIRTABLE_ID);

  const assets =
    key === "aimbridge-latam"
      ? collectAimbridgeOperatedAssets()
      : focusDeep?.portfolio_notes?.assets || [];

  const hotels = assets.length
    ? assets.map((a) => mapMexicoPortfolioAsset(a, key))
    : (cohort.hotels || []).map((h) => {
        const resolved = applyRoomsResolutionToPortfolioHotel({
          ...h,
          name: h.name,
          rooms: h.rooms,
          census_rooms: h.census_rooms ?? (h.rooms != null && Number(h.rooms) > 0 ? h.rooms : null),
        });
        return {
          ...h,
          ...resolved,
          brand_display: h.affiliation_display || h.brand,
          rooms_display: resolved.rooms_display ?? resolved.rooms ?? h.rooms,
          verification_bucket: verificationBucket(h.verification_status),
          market_display: h.market || h.city,
        };
      });

  const displayOrg = org || {
    slug: key,
    display_name: "Mexico Explorer Demo",
    legal_name: null,
    website: null,
  };
  const contacts = normalizeCorporateContacts(
    focusDeep,
    findHotel(cohort, hotels.find((h) => h.airtable_record_id)?.airtable_record_id) ||
      cohort.hotels?.[0] ||
      {},
    focusDeep?.property_profile || {}
  );

  const ownedOrControlled = hotels.filter(
    (h) => h.relationship_type === "OWNED_BY" || h.economic_owner_verified
  ).length;
  const operatedPrimary = hotels.filter((h) => h.relationship_type === "OPERATED_BY").length;
  const managedSeparately = hotels.filter((h) => h.secondary_relationship_type === "OPERATED_BY").length;
  const summary =
    key === "aimbridge-latam"
      ? {
          total_relationships: hotels.length,
          owned_or_controlled_verified: 0,
          operated_or_managed: hotels.length,
          ownership_unknown: 0,
          third_party_operated_count: 0,
          operator_name: "Aimbridge LATAM",
        }
      : {
          total_relationships: hotels.length,
          owned_or_controlled_verified: ownedOrControlled,
          operated_or_managed: operatedPrimary,
          ownership_unknown: Math.max(0, hotels.length - ownedOrControlled - operatedPrimary),
          third_party_operated_count: managedSeparately,
          operator_name:
            focusDeep?.portfolio_notes?.operator_of_portfolio ||
            focusDeep?.operator_resolution?.current_operator?.name ||
            null,
        };
  const knownRooms = hotels.reduce((s, h) => s + (h.rooms != null ? Number(h.rooms) || 0 : 0), 0);
  const markets = [
    ...new Set(hotels.map((h) => h.market || h.city || h.market_display).filter(Boolean)),
  ].sort();

  return {
    ok: true,
    version: MEXICO_EXPLORER_DEMO_VERSION,
    data_status: focusDeep?.research_status || cohort.data_status,
    costar_firewall: "enforced",
    page_framing: {
      title: `Known ${displayOrg.display_name} Hotel Relationships`,
      kicker: "Organization profile",
      do_not_say: "Operator listings imply economic ownership",
      summary,
    },
    group: {
      ...displayOrg,
      display_name: displayOrg.display_name,
      legal_name: displayOrg.legal_name || displayOrg.display_name,
      website: contacts.website || displayOrg.website || null,
      headquarters: contacts.headquarters || null,
      headquarters_status: contacts.headquarters ? "HIGH" : "UNKNOWN",
      known_current_hotel_count: hotels.length,
      known_hotel_relationships: hotels.length,
      known_rooms: knownRooms || null,
      known_rooms_status: knownRooms ? "LIVE" : "UNKNOWN",
      markets,
      evidence_status: "HIGH",
      evidence_summary:
        focusDeep?.portfolio_notes?.framing ||
        displayOrg.evidence_basis ||
        focusDeep?.commercial_pursuit?.why_matters ||
        null,
      relationship_summary: summary,
      leadership: (focusDeep?.people || []).slice(0, 8).map((p) => ({
        name: p.name,
        title: p.title,
        confidence: p.confidence,
      })),
    },
    portfolio: hotels,
    why_this_matters: {
      text:
        focusDeep?.portfolio_notes?.framing ||
        focusDeep?.commercial_pursuit?.why_matters ||
        "Mexico Explorer demo — ownership / operator / brand separation from Full HI research.",
    },
  };
}

function mexicoEdgeCategory(rel) {
  const t = String(rel || "").toUpperCase();
  if (/OWN|CONTROL|SPONSOR/.test(t)) return "ownership";
  if (/OPERAT|MANAGE|ASSET/.test(t)) return "operator";
  if (/BRAND|FRANCHIS/.test(t)) return "brand";
  return "other";
}

function mexicoProductEdgeLabel(rel) {
  const t = String(rel || "").toUpperCase();
  if (/OWN|CONTROL/.test(t)) return "Owns";
  if (/SPONSOR/.test(t)) return "Sponsors";
  if (/OPERAT|MANAGE|ASSET/.test(t)) return "Operates";
  if (/BRAND|FRANCHIS/.test(t)) return "Brands";
  return String(rel || "Related").replace(/_/g, " ");
}

/**
 * Focus Graph neighbors for Mexico Explorer demo orgs / hotels.
 * GSF `buildNeighborsPayload` delegates here when node_id matches Mexico entity_ids.
 */
export function buildMexicoExplorerNeighborsPayload(opts = {}) {
  const cohort = loadMexicoExplorerDemoCohort();
  const nodeId = String(opts.nodeId || opts.entity_id || "").trim();
  let nodeType = String(opts.nodeType || opts.node_type || "organization").trim();
  if (nodeType === "ownership_group") nodeType = "organization";

  const org =
    (cohort.organizations || []).find(
      (o) =>
        o.entity_id === nodeId ||
        o.slug === nodeId ||
        (nodeId && String(nodeId).startsWith("dle_mexico_demo_") && o.entity_id === nodeId)
    ) || null;

  const hotelMatch =
    !org && nodeId
      ? findHotel(cohort, nodeId) ||
        (cohort.hotels || []).find((h) => h.hotel_id === nodeId)
      : null;

  if (!org && !hotelMatch && nodeType !== "hotel") {
    return { ok: false, error: "node_not_found", version: MEXICO_EXPLORER_DEMO_VERSION };
  }

  const focusOrg =
    org ||
    (hotelMatch
      ? findOrg(
          cohort,
          hotelMatch.airtable_record_id === REAL_INN_CANCUN_AIRTABLE_ID
            ? "alliance-hotel-management"
            : "inmobiliaria-hnf"
        )
      : null);

  if (!focusOrg) {
    return { ok: false, error: "node_not_found", version: MEXICO_EXPLORER_DEMO_VERSION };
  }

  const groupProfile = buildMexicoExplorerOwnershipGroupProfile(focusOrg.slug);
  const portfolio = groupProfile?.portfolio || [];
  const limit = Math.max(1, Number(opts.limit != null ? opts.limit : 12) || 12);
  const categoryFilter = new Set(
    []
      .concat(opts.relationshipCategories || opts.relationship_categories || [])
      .map((c) => String(c).toLowerCase())
      .filter(Boolean)
  );

  const nodesById = new Map();
  const edges = [];

  nodesById.set(focusOrg.entity_id, {
    id: focusOrg.entity_id,
    type: "organization",
    roles: focusOrg.organization_roles || ["owner"],
    not_asserted_as: focusOrg.not_asserted_as || [],
    label: focusOrg.display_name,
    legal_name: focusOrg.legal_name,
    slug: focusOrg.slug,
    website: focusOrg.website || null,
    known_hotel_relationship_count: portfolio.length,
    entity_type: focusOrg.entity_type || "company",
  });

  let truncated = false;
  let shown = 0;
  for (const h of portfolio) {
    if (shown >= limit) {
      truncated = true;
      break;
    }
    const hotelId =
      h.airtable_record_id ||
      h.census_record_id ||
      `mexico_hotel_${String(h.name || shown).toLowerCase().replace(/\s+/g, "_")}`;
    const rel = h.relationship_type || "RELATED";
    const cat = mexicoEdgeCategory(rel);
    if (categoryFilter.size && !categoryFilter.has(cat)) continue;

    nodesById.set(hotelId, {
      id: hotelId,
      type: "hotel",
      label: h.name,
      airtable_record_id: h.airtable_record_id || null,
      brand: h.brand_display || null,
      rooms: h.rooms ?? null,
      market: h.market || h.city || null,
      primary_relationship_to_org: rel,
    });

    edges.push({
      relationship_id: `rel_${hotelId}_${focusOrg.entity_id}_${rel}`,
      from: hotelId,
      to: focusOrg.entity_id,
      relationship_type: rel,
      relationship_label: mexicoProductEdgeLabel(rel),
      category: cat,
      verification_status: h.verification_bucket || "HIGH",
      verification_bucket: h.verification_bucket || "HIGH",
      is_current: true,
      temporal_status: "current",
      does_not_imply_ownership: rel === "OPERATED_BY",
    });
    shown += 1;
  }

  const centerHotel = hotelMatch;
  return {
    ok: true,
    empty: edges.length === 0,
    version: MEXICO_EXPLORER_DEMO_VERSION,
    data_status: cohort.data_status,
    costar_firewall: "enforced",
    semantics_note:
      "Edge labels are authoritative. OPERATED_BY must not be read as OWNED_BY.",
    center: {
      id: nodeType === "hotel" ? centerHotel?.airtable_record_id || nodeId : focusOrg.entity_id,
      type: nodeType === "hotel" ? "hotel" : "organization",
      label:
        nodeType === "hotel"
          ? centerHotel?.name || nodeId
          : focusOrg.display_name,
    },
    hops: 1,
    nodes: [...nodesById.values()],
    edges,
    pagination: {
      truncated,
      limit,
      expand_all: false,
      total_hotel_relationships: portfolio.length,
      shown_hotel_relationships: shown,
    },
  };
}

function isMexicoExplorerOrgNodeId(nodeId) {
  const key = String(nodeId || "").trim();
  if (!key) return false;
  if (key.startsWith("dle_mexico_demo_")) return true;
  const cohort = loadMexicoExplorerDemoCohort();
  return (cohort.organizations || []).some((o) => o.entity_id === key || o.slug === key);
}

export function tryBuildMexicoExplorerNeighborsPayload(opts = {}) {
  const nodeId = String(opts.nodeId || opts.entity_id || "").trim();
  if (isMexicoExplorerDemoHotelId(nodeId) || isMexicoExplorerOrgNodeId(nodeId)) {
    return buildMexicoExplorerNeighborsPayload(opts);
  }
  return null;
}

function buildCaseBFallback(cohort, hotel) {
  const operatorOrg =
    hotel.airtable_record_id === SHERATON_GDL_AIRTABLE_ID
      ? findOrg(cohort, "aimbridge-latam")
      : findOrg(cohort, "hoteles-camino-real");
  const brandParent =
    hotel.airtable_record_id === SHERATON_GDL_AIRTABLE_ID
      ? findOrg(cohort, "marriott-international")
      : null;
  const operatorName =
    hotel.operator || hotel.management_company || operatorOrg?.display_name || null;
  const brandDisplay = hotel.affiliation_display || hotel.brand || "—";
  const orgPayload = operatorOrg
    ? {
        slug: operatorOrg.slug,
        entity_id: operatorOrg.entity_id,
        display_name: operatorOrg.display_name,
        legal_name: operatorOrg.legal_name,
        website: operatorOrg.website || null,
        known_hotel_count: 1,
        roles: operatorOrg.organization_roles || ["operator", "manager"],
        not_asserted_as:
          operatorOrg.not_asserted_as ||
          ["verified_economic_owner", "propco", "natural_person_ubo"],
        evidence_basis: operatorOrg.evidence_basis || null,
      }
    : null;
  const sources = [];
  if (hotel.website) {
    sources.push({
      provider: "first_party_brand",
      title: `${hotel.name} — brand / property page`,
      url: hotel.website,
      published_date: null,
      observed_date: "2026-09-08",
      authority: "first_party",
      confidence: "high_for_brand_identity_only",
      note: "Supports brand / trading identity only — not economic ownership.",
    });
  }
  sources.push({
    provider: "dealality_hotel_census",
    title: "Dealality Hotel Census (Radar)",
    url: null,
    published_date: null,
    observed_date: "2026-09-08",
    authority: "internal_census",
    confidence: "high_for_operator_brand_rooms",
    note: "Management Company, brand, rooms, and geo from Hotel Census — not a deed or PropCo filing.",
  });

  return {
    ok: true,
    in_cohort: true,
    intelligence_case: "B",
    version: MEXICO_EXPLORER_DEMO_VERSION,
    deep_research_version: null,
    data_status: cohort.data_status,
    costar_firewall: "enforced",
    golden_demo: "MEXICO_EXPLORER_DEMO",
    hotel: {
      ...hotel,
      brand_display: brandDisplay,
      brand_display_status: "CURRENT_CENSUS",
      brand_display_note: hotel.identity_notes || null,
      economic_owner_verified: false,
    },
    organization: orgPayload,
    ownership_group: orgPayload
      ? { ...orgPayload, framing: "organization_operator_not_verified_owner" }
      : null,
    deep_research: null,
    report: {
      case: "B",
      case_label: "Partial intelligence — operator / brand known; ownership unresolved",
      executive_summary: {
        text:
          `${hotel.name}: operator/management company is ${operatorName || "unresolved"} ` +
          `(Census). Brand affiliation is ${hotel.brand || "unresolved"}. ` +
          `Economic owner and PropCo are not yet verified — no fabricated ownership is shown.`,
        verification_bucket: "PROBABLE",
        source_count: sources.length,
        data_status: "PARTIAL",
      },
      ownership_and_control: {
        economic_owner_or_group: {
          name: null,
          known: false,
          status: "NOT_YET_VERIFIED",
          note: "Owner: Not yet verified — Census Management Company is not treated as owner.",
        },
        legal_property_owner_propco: {
          name: null,
          known: false,
          status: "UNKNOWN",
          note: "PropCo / legal title unknown.",
        },
        operator: {
          name: operatorName,
          known: Boolean(operatorName),
          status: operatorName ? "CENSUS_MANAGEMENT_COMPANY" : "UNKNOWN",
          note: "Operator / management from Hotel Census only.",
        },
        brand: {
          name: hotel.brand,
          known: Boolean(hotel.brand),
          status: "CURRENT_CENSUS",
          brand_family: hotel.brand_family || hotel.parent_company || null,
          note: hotel.identity_notes || null,
        },
      },
      ownership_chain: [],
      organizations: [orgPayload, brandParent]
        .filter(Boolean)
        .map((o) => ({
          entity_id: o.entity_id || o.slug,
          display_name: o.display_name,
          roles: o.organization_roles || o.roles,
          not_asserted_as: o.not_asserted_as,
        })),
      relationships: [
        {
          subject: hotel.name,
          subject_airtable_record_id: hotel.airtable_record_id,
          relationship_type: "OPERATED_BY",
          object: operatorName,
          object_entity_id: operatorOrg?.entity_id || null,
          verification_bucket: "PROBABLE",
          note: "Census Management Company — does not imply OWNED_BY.",
        },
        hotel.brand
          ? {
              subject: hotel.name,
              subject_airtable_record_id: hotel.airtable_record_id,
              relationship_type: "BRANDED_BY",
              object: hotel.brand,
              object_entity_id: brandParent?.entity_id || null,
              verification_bucket: "PROBABLE",
              note: "Brand affiliation from Census / first-party brand materials.",
            }
          : null,
      ].filter(Boolean),
      evidence: {
        claim: `${hotel.name} → OPERATED_BY → ${operatorName || "unknown"} (Census); ownership unresolved`,
        verification_bucket: "PROBABLE",
        source_count: sources.length,
        sources,
        what_it_proves:
          "Supports operator/management company and brand affiliation for demo presentation.",
        what_it_does_not_prove:
          "Does not prove economic owner, PropCo, natural-person UBO, or that Management Company is the owner.",
      },
      research_gaps: [
        "Verified economic owner / ownership group",
        "PropCo / legal title entity",
        "Natural-person beneficial ownership",
        "Deed- or registry-backed ownership evidence",
      ],
      research_status: {
        message:
          "Partial demo intelligence — deep-research fixture missing; Census / first-party only.",
      },
      commercial_pursuit: {
        approach_organization:
          "Start with verified operator / brand channels only until ownership is resolved.",
        missing_before_outreach:
          "Resolve economic owner / PropCo / signatory authority before ownership-targeted outreach.",
      },
    },
  };
}
