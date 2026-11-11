/**
 * ADP → Hotel Property Census field population V1.
 * Recover existing Dealality evidence + apply taxonomy maps.
 * NULL_FILL only — never overwrite populated HPC values.
 * Never invent rooms/lat/lng/phone without governed profile evidence.
 */

import {
  countryToSubContinent,
  resolveCensusSubContinent,
  isBlankGeoValue,
  GEOGRAPHY_SOURCE,
  SUB_CONTINENT_LABELS,
} from "./geography-enrichment-contract.js";
import { countryToDealalityRegion } from "./region.js";
import {
  isFieldEmpty,
  normalizeCountryForHpc,
  extractProfileValue,
  proposeNullFillsFromAdpProfile,
} from "./hpc-full-onboarding-contract-v1.js";

export const HPC_ADP_FIELD_POPULATION_V1 = "HPC_ADP_FIELD_POPULATION_V1";

/** Priority writable fields for this population pass */
export const HPC_POPULATION_PRIORITY_FIELDS = Object.freeze([
  "Sub-Continent",
  "Continent",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
  "Address",
  "Phone",
  "Rooms / Keys",
  "Current Brand",
  "Brand Family",
  "Property Type",
  "Official Property URL",
  "Operator / Management Company",
  "Meeting Space Flag",
  "F&B Flag",
  "Resort / Leisure Flag",
  "Spa Flag",
  "Pool Flag",
  "Asset Context",
  "Amenities - Structured Tags",
  "Family / Source Family",
  "Affiliation Status",
  "Canonical Property Name",
  "State / Region",
]);

/** Live HPC Property Type select — prefer canonical labels for new fills */
export const HPC_PROPERTY_TYPE_CANONICAL = Object.freeze([
  "Hotel",
  "Resort",
  "Boutique Hotel",
  "Extended Stay",
  "All-Inclusive",
  "Serviced Apartment",
  "Mixed-Use",
  "Other",
  "Unknown",
]);

export const COUNTRY_TO_CONTINENT = Object.freeze({
  "United States": "North America",
  USA: "North America",
  Mexico: "North America",
  Canada: "North America",
  Bermuda: "North America",
  "Dominican Republic": "North America",
  Jamaica: "North America",
  "Puerto Rico": "North America",
  Grenada: "North America",
  Colombia: "South America",
  Brazil: "South America",
  Spain: "Europe",
  Italy: "Europe",
  Germany: "Europe",
  Switzerland: "Europe",
  "United Kingdom": "Europe",
  UK: "Europe",
  France: "Europe",
});

/**
 * Map profile / attribute signals → canonical Property Type.
 */
export function resolveCanonicalPropertyType(profile, hpcFields = {}) {
  const existing = hpcFields["Property Type"];
  if (!isFieldEmpty(existing) && HPC_PROPERTY_TYPE_CANONICAL.includes(existing)) {
    return { value: existing, source: "existing_canonical", skip: true };
  }
  // Legacy service-model / specialty slugs are NOT valid Property Type semantics.
  // Remap proposals are owned by hpc-field-semantic-validation-v1 (governed overwrite).
  // NULL_FILL path must never write these slugs into empty cells.
  const slugMap = {
    full_service: "Hotel",
    select_service: "Hotel",
    historic_full_service: "Hotel",
    luxury_resort_castle: "Resort",
    golf_resort: "Resort",
    "Full Service": "Hotel",
    "Select Service": "Hotel",
    Boutique: "Boutique Hotel",
    Lifestyle: "Hotel",
  };
  if (!isFieldEmpty(existing) && slugMap[existing]) {
    return {
      value: slugMap[existing],
      source: "legacy_property_type_remap",
      skip: true, // do not NULL_FILL-overwrite here; semantic validator applies correction
      semanticRemapCandidate: true,
    };
  }

  const attrs = new Set(
    (profile?.attributes || []).map((a) => String(a).toLowerCase())
  );
  const name = String(profile?.name || hpcFields["Property Name"] || "").toLowerCase();
  const chain = String(profile?.chainScale || "").toLowerCase();

  if (
    attrs.has("all_inclusive") ||
    attrs.has("all-inclusive") ||
    /all[- ]inclusive/i.test(name)
  ) {
    return { value: "All-Inclusive", source: "profile_attributes", skip: false };
  }
  if (
    attrs.has("extended_stay") ||
    attrs.has("serviced_apartment") ||
    chain.includes("extended")
  ) {
    return { value: "Extended Stay", source: "profile_attributes", skip: false };
  }
  if (attrs.has("boutique_feel") || attrs.has("boutique") || /boutique/i.test(name)) {
    return { value: "Boutique Hotel", source: "profile_attributes", skip: false };
  }
  if (
    attrs.has("resort") ||
    attrs.has("beachfront") ||
    attrs.has("private_beaches") ||
    attrs.has("golf_access") ||
    attrs.has("heritage_castle") ||
    /resort|beach|spa resort/i.test(name)
  ) {
    return { value: "Resort", source: "profile_attributes_or_name", skip: false };
  }
  if (profile?.propertyType && HPC_PROPERTY_TYPE_CANONICAL.includes(profile.propertyType)) {
    return { value: profile.propertyType, source: "adp_profile.propertyType", skip: false };
  }
  // Default branded full-service / select → Hotel
  return { value: "Hotel", source: "default_hotel_class", skip: false };
}

export function resolveAssetContext(profile) {
  const attrs = new Set(
    (profile?.attributes || []).map((a) => String(a).toLowerCase())
  );
  const sub = String(profile?.submarket || "").toLowerCase();
  if (
    attrs.has("airport") ||
    attrs.has("airport_shuttle") ||
    attrs.has("park_and_fly") ||
    /airport/i.test(sub)
  ) {
    return "Airport";
  }
  if (
    attrs.has("beachfront") ||
    attrs.has("waterfront") ||
    attrs.has("marina") ||
    attrs.has("private_beaches") ||
    attrs.has("oceanfront")
  ) {
    return "Beach / Waterfront";
  }
  if (attrs.has("resort") || attrs.has("golf_access") || attrs.has("heritage_castle")) {
    return "Resort Destination";
  }
  if (
    attrs.has("urban_hotel") ||
    attrs.has("urban_lifestyle") ||
    attrs.has("urban_luxury") ||
    attrs.has("times_square_location") ||
    attrs.has("business_district")
  ) {
    return "Urban";
  }
  if (attrs.has("suburban") || /suburban|fairview|pooks hill/i.test(sub)) {
    return "Suburban";
  }
  return null;
}

export function deriveMeetingSpaceFlag(profile, hpcFields = {}) {
  if (!isFieldEmpty(hpcFields["Meeting Space Flag"])) {
    return { skip: true };
  }
  const ms = profile?.meetingSpace;
  if (ms && typeof ms === "object") {
    const rooms = Number(ms.meetingRooms);
    const sqft = Number(ms.totalSqFt || ms.totalSqM);
    if ((Number.isFinite(rooms) && rooms > 0) || (Number.isFinite(sqft) && sqft > 0)) {
      return { value: true, source: "adp_profile.meetingSpace", skip: false };
    }
    if (Array.isArray(ms.rooms) && ms.rooms.length > 0) {
      return { value: true, source: "adp_profile.meetingSpace.rooms", skip: false };
    }
  }
  const attrs = (profile?.attributes || []).map((a) => String(a).toLowerCase());
  if (attrs.some((a) => /meeting_space|ballroom|event_space|conference/.test(a))) {
    return { value: true, source: "adp_profile.attributes", skip: false };
  }
  return { skip: true, reason: "NO_MEETING_EVIDENCE" };
}

export function deriveFnBFlag(profile, hpcFields = {}) {
  if (!isFieldEmpty(hpcFields["F&B Flag"])) {
    return { skip: true };
  }
  if (Array.isArray(profile?.restaurants) && profile.restaurants.length > 0) {
    return { value: true, source: "adp_profile.restaurants", skip: false };
  }
  const attrs = (profile?.attributes || []).map((a) => String(a).toLowerCase());
  if (
    attrs.some((a) =>
      /restaurant|dining|fine_dining|on_site_restaurant|f_?and_?b|food_beverage|coopers_mill/.test(
        a
      )
    )
  ) {
    return { value: true, source: "adp_profile.attributes", skip: false };
  }
  // Full-service branded hotels almost always have F&B — but only assert when
  // attributes or name strongly imply on-site dining; else leave research.
  if (attrs.includes("full_service") && attrs.some((a) => /spa|m_club|lobby/.test(a))) {
    return { value: true, source: "adp_profile.full_service_with_amenity_signal", skip: false };
  }
  return { skip: true, reason: "NO_FB_EVIDENCE" };
}

export function deriveAmenityFlag(profile, hpcFields, field, patterns) {
  if (!isFieldEmpty(hpcFields[field])) return { skip: true };
  const attrs = (profile?.attributes || []).map((a) => String(a).toLowerCase());
  if (attrs.some((a) => patterns.some((re) => re.test(a)))) {
    return { value: true, source: "adp_profile.attributes", skip: false };
  }
  return { skip: true, reason: "NO_EVIDENCE" };
}

/**
 * Build NULL_FILL proposals for one ADP hotel from profile + HPC row + taxonomy.
 */
export function planAdpHpcFieldPopulation({ profile, hpcFields = {} } = {}) {
  const proposals = [];
  const ambiguous = [];
  const notApplicable = [];
  const seen = new Set();

  function propose(field, value, source, confidence = "GOVERNED") {
    if (seen.has(field)) return;
    if (isFieldEmpty(value) && value !== false && value !== 0) return;
    if (!isFieldEmpty(hpcFields[field])) return;
    seen.add(field);
    proposals.push({
      field,
      value,
      source,
      confidence,
      mode: "NULL_FILL",
    });
  }

  // 1) Reuse existing ADP profile → HPC map
  for (const p of proposeNullFillsFromAdpProfile(profile, hpcFields)) {
    propose(p.field, p.value, p.source, p.confidence);
  }

  const country =
    normalizeCountryForHpc(hpcFields.Country) ||
    normalizeCountryForHpc(profile?.country) ||
    String(hpcFields.Country || profile?.country || "").trim();

  // 2) Continent from country when empty
  if (isBlankGeoValue(hpcFields.Continent) && country) {
    const cont =
      COUNTRY_TO_CONTINENT[country] ||
      COUNTRY_TO_CONTINENT[normalizeCountryForHpc(country)] ||
      null;
    if (cont) propose("Continent", cont, "dealality_country_continent_map", "TAXONOMY");
  }

  // 3) Sub-Continent from shared taxonomy
  if (isBlankGeoValue(hpcFields["Sub-Continent"])) {
    const sub = resolveCensusSubContinent(null, country);
    if (sub && SUB_CONTINENT_LABELS.includes(sub)) {
      propose(
        "Sub-Continent",
        sub,
        GEOGRAPHY_SOURCE.subContinentCountryDerived,
        "TAXONOMY"
      );
    } else if (country) {
      ambiguous.push({
        field: "Sub-Continent",
        country,
        reason: "COUNTRY_NOT_IN_SUBCONTINENT_SELECT",
        dealalityRegion: countryToDealalityRegion(country),
        note: "Live HPC Sub-Continent select has no option for this country — steward review",
      });
    }
  }

  // Region column does not exist on Hotel Property Census (live) — document only
  notApplicable.push({
    field: "Region",
    reason: "NOT_IN_HPC_SCHEMA",
    note: "Dealality Region taxonomy applies to legacy Hotel Census / Brand Explorer — not a column on this table",
    wouldDerive: country ? countryToDealalityRegion(country) : null,
  });

  // 4) Market / Submarket from ADP profile (Dealality product geography)
  if (isBlankGeoValue(hpcFields.Market) && profile?.market) {
    propose("Market", String(profile.market).trim(), "adp_profile.market", "PROFILE_GOVERNED");
  }
  if (isBlankGeoValue(hpcFields.Submarket) && profile?.submarket) {
    propose(
      "Submarket",
      String(profile.submarket).trim(),
      "adp_profile.submarket",
      "PROFILE_GOVERNED"
    );
  }

  // 5) Property Type → canonical
  if (isFieldEmpty(hpcFields["Property Type"])) {
    const pt = resolveCanonicalPropertyType(profile, hpcFields);
    if (!pt.skip && pt.value) {
      propose("Property Type", pt.value, pt.source, "TAXONOMY");
    }
  }

  // 6) Asset Context
  if (isFieldEmpty(hpcFields["Asset Context"])) {
    const ac = resolveAssetContext(profile);
    if (ac) propose("Asset Context", ac, "adp_profile.attributes", "PROFILE_INFERRED");
  }

  // 7) Flags (true only with evidence — never invent false absences except Independent/Future Opening)
  const meet = deriveMeetingSpaceFlag(profile, hpcFields);
  if (!meet.skip) propose("Meeting Space Flag", meet.value, meet.source, "PROFILE_GOVERNED");
  const fb = deriveFnBFlag(profile, hpcFields);
  if (!fb.skip) propose("F&B Flag", fb.value, fb.source, "PROFILE_GOVERNED");
  const resort = deriveAmenityFlag(profile, hpcFields, "Resort / Leisure Flag", [
    /resort/,
    /beach/,
    /leisure/,
    /marina/,
    /golf/,
  ]);
  if (!resort.skip) propose("Resort / Leisure Flag", resort.value, resort.source);
  const spa = deriveAmenityFlag(profile, hpcFields, "Spa Flag", [/spa/]);
  if (!spa.skip) propose("Spa Flag", spa.value, spa.source);
  const pool = deriveAmenityFlag(profile, hpcFields, "Pool Flag", [/pool/, /infinity_pool/]);
  if (!pool.skip) propose("Pool Flag", pool.value, pool.source);
  const fitness = deriveAmenityFlag(profile, hpcFields, "Fitness Flag", [
    /fitness/,
    /gym/,
    /wellness_fitness/,
  ]);
  if (!fitness.skip) propose("Fitness Flag", fitness.value, fitness.source);
  const parking = deriveAmenityFlag(profile, hpcFields, "Parking Flag", [
    /parking/,
    /valet/,
    /park_and_fly/,
  ]);
  if (!parking.skip) propose("Parking Flag", parking.value, parking.source);
  const shuttle = deriveAmenityFlag(profile, hpcFields, "Airport Shuttle Flag", [
    /airport_shuttle/,
    /shuttle/,
  ]);
  if (!shuttle.skip) propose("Airport Shuttle Flag", shuttle.value, shuttle.source);
  const beach = deriveAmenityFlag(profile, hpcFields, "Beach / Waterfront Flag", [
    /beach/,
    /waterfront/,
    /oceanfront/,
    /marina/,
    /private_beaches/,
  ]);
  if (!beach.skip) propose("Beach / Waterfront Flag", beach.value, beach.source);
  const extStay = deriveAmenityFlag(profile, hpcFields, "Extended Stay Flag", [
    /extended_stay/,
    /serviced_apartment/,
  ]);
  if (!extStay.skip) propose("Extended Stay Flag", extStay.value, extStay.source);
  const mixed = deriveAmenityFlag(profile, hpcFields, "Mixed-Use Flag", [/mixed_use/, /mixed-use/]);
  if (!mixed.skip) propose("Mixed-Use Flag", mixed.value, mixed.source);
  const brandedRes = deriveAmenityFlag(profile, hpcFields, "Branded Residences Flag", [
    /branded_residence/,
    /residences/,
  ]);
  if (!brandedRes.skip) {
    propose("Branded Residences Flag", brandedRes.value, brandedRes.source);
  }

  // 8) Amenities structured tags
  if (
    isFieldEmpty(hpcFields["Amenities - Structured Tags"]) &&
    Array.isArray(profile?.attributes) &&
    profile.attributes.length
  ) {
    propose(
      "Amenities - Structured Tags",
      profile.attributes.join(", "),
      "adp_profile.attributes",
      "PROFILE_GOVERNED"
    );
  }

  // 9) Lat/lng / rooms / phone already covered by proposeNullFillsFromAdpProfile
  const lat = profile?.latitude ?? profile?.lat ?? null;
  const lng = profile?.longitude ?? profile?.lng ?? null;
  // Never treat 0/0 as populated — Null Island requires governed correction, not skip
  const latNeeds =
    hpcFields.Latitude == null ||
    hpcFields.Latitude === "" ||
    (Number(hpcFields.Latitude) === 0 && Number(hpcFields.Longitude) === 0) ||
    !Number.isFinite(Number(hpcFields.Latitude));
  const lngNeeds =
    hpcFields.Longitude == null ||
    hpcFields.Longitude === "" ||
    (Number(hpcFields.Latitude) === 0 && Number(hpcFields.Longitude) === 0) ||
    !Number.isFinite(Number(hpcFields.Longitude));
  if (
    latNeeds &&
    Number.isFinite(Number(lat)) &&
    Number.isFinite(Number(lng)) &&
    !(Number(lat) === 0 && Number(lng) === 0)
  ) {
    propose("Latitude", Number(lat), "adp_profile.latitude", "PROFILE_GOVERNED");
  }
  if (
    lngNeeds &&
    Number.isFinite(Number(lat)) &&
    Number.isFinite(Number(lng)) &&
    !(Number(lat) === 0 && Number(lng) === 0)
  ) {
    propose("Longitude", Number(lng), "adp_profile.longitude", "PROFILE_GOVERNED");
  }

  // 10) Market / Submarket composite
  const market = !isBlankGeoValue(hpcFields.Market)
    ? String(hpcFields.Market).trim()
    : profile?.market
      ? String(profile.market).trim()
      : "";
  const submarket = !isBlankGeoValue(hpcFields.Submarket)
    ? String(hpcFields.Submarket).trim()
    : profile?.submarket
      ? String(profile.submarket).trim()
      : "";
  if (isFieldEmpty(hpcFields["Market / Submarket"]) && market) {
    const composite = submarket ? `${market} / ${submarket}` : market;
    propose(
      "Market / Submarket",
      composite,
      "dealality_market_submarket_composite",
      "TAXONOMY"
    );
  }

  // 11) Postal Code from address
  if (isFieldEmpty(hpcFields["Postal Code"])) {
    const addr = String(hpcFields.Address || profile?.address || "");
    const postal = extractPostalFromAddress(addr, country);
    if (postal) propose("Postal Code", postal, "derived_from_address", "DERIVED");
  }

  // 12) Hotel Class / Segment from chainScale
  if (isFieldEmpty(hpcFields["Hotel Class / Segment"])) {
    const cls = resolveHotelClassSegment(profile, hpcFields);
    if (cls) propose("Hotel Class / Segment", cls, "adp_profile.chainScale", "PROFILE_GOVERNED");
  }

  // 13) Affiliation / Independent flags from Current Brand + Affiliation Status
  const brandName = String(hpcFields["Current Brand"] || profile?.brand || "").trim();
  const family = String(
    hpcFields["Brand Family"] || profile?.parentCompany || ""
  ).trim();
  if (isFieldEmpty(hpcFields["Affiliation Status"]) && brandName) {
    const familySource = String(
      hpcFields["Family / Source Family"] || family || ""
    ).trim();
    if (
      /independent/i.test(brandName) ||
      /independent/i.test(family) ||
      /^independent$/i.test(familySource)
    ) {
      propose("Affiliation Status", "Independent", "brand_or_family_independent", "TAXONOMY");
    } else if (
      /curio|autograph|vignette|tribute|tapestry|collection|individuals|luxury collection/i.test(
        brandName
      ) ||
      (profile?.attributes || []).some((a) => /soft_brand/i.test(String(a)))
    ) {
      propose(
        "Affiliation Status",
        "Soft-Branded / Collection",
        "brand_soft_collection",
        "TAXONOMY"
      );
    } else if (brandName) {
      propose("Affiliation Status", "Branded", "current_brand_present", "TAXONOMY");
    }
  }
  const affResolved =
    hpcFields["Affiliation Status"] ||
    proposals.find((p) => p.field === "Affiliation Status")?.value;
  // Airtable omits unchecked checkboxes — only write Independent Hotel Flag=true
  // when independently evidenced; leave empty for branded (Not Independent class).
  if (isFieldEmpty(hpcFields["Independent Hotel Flag"]) && affResolved === "Independent") {
    propose(
      "Independent Hotel Flag",
      true,
      "affiliation_status_independent",
      "TAXONOMY"
    );
  }
  if (isFieldEmpty(hpcFields["Independent Classification"]) && affResolved) {
    if (affResolved === "Independent") {
      propose(
        "Independent Classification",
        "True Independent",
        "affiliation_status_derived",
        "TAXONOMY"
      );
    } else if (
      affResolved === "Branded" ||
      affResolved === "Soft-Branded / Collection"
    ) {
      propose(
        "Independent Classification",
        "Not Independent",
        "affiliation_status_derived",
        "TAXONOMY"
      );
    }
  }

  // Future Opening Flag: leave empty for operating hotels (checkbox false does not
  // round-trip reliably on Airtable). Documented as conditional/process.

  // 15) Operator / Owner from profile
  const op = profile?.operator || profile?.managementCompany;
  if (isFieldEmpty(hpcFields["Operator / Management Company"]) && op) {
    propose(
      "Operator / Management Company",
      String(op).trim(),
      "adp_profile.operator",
      "PROFILE_GOVERNED"
    );
  }
  if (isFieldEmpty(hpcFields["Owner Name"]) && profile?.owner) {
    propose("Owner Name", String(profile.owner).trim(), "adp_profile.owner", "PROFILE_GOVERNED");
  }

  // 16) Opening Date from yearBuilt (Jan 1 of year — steward may refine)
  if (isFieldEmpty(hpcFields["Opening Date"]) && profile?.yearBuilt) {
    const y = Number(profile.yearBuilt);
    if (Number.isFinite(y) && y >= 1800 && y <= 2100) {
      propose(
        "Opening Date",
        `${y}-01-01`,
        "adp_profile.yearBuilt",
        "PROFILE_YEAR_ONLY"
      );
    }
  }

  // 17) Source URL from official property URL / meeting source
  if (isFieldEmpty(hpcFields["Source URL"])) {
    const url =
      hpcFields["Official Property URL"] ||
      profile?.officialPropertyPageUrl ||
      profile?.officialWebsite ||
      profile?.website ||
      profile?.meetingSpace?.source ||
      null;
    if (url && /^https?:\/\//i.test(String(url))) {
      propose("Source URL", String(url).trim(), "official_property_or_meeting_url", "PROFILE_GOVERNED");
    }
  }
  if (
    isFieldEmpty(hpcFields["Source Type"]) &&
    (!isFieldEmpty(hpcFields["Source URL"]) ||
      proposals.some((p) => p.field === "Source URL"))
  ) {
    propose("Source Type", "official_property_page", "source_url_present", "TAXONOMY");
  }
  if (
    isFieldEmpty(hpcFields["Source Confidence"]) &&
    (!isFieldEmpty(hpcFields["Source URL"]) ||
      proposals.some((p) => p.field === "Source URL"))
  ) {
    propose("Source Confidence", "High", "official_url_present", "TAXONOMY");
  }

  // 18) Rooms confidence when rooms present or proposed
  const roomsVal =
    hpcFields["Rooms / Keys"] ??
    proposals.find((p) => p.field === "Rooms / Keys")?.value ??
    profile?.rooms;
  if (
    isFieldEmpty(hpcFields["Rooms Confidence"]) &&
    Number.isFinite(Number(roomsVal)) &&
    Number(roomsVal) > 0
  ) {
    const conf = profile?.roomCountConfidence
      ? String(profile.roomCountConfidence)
      : "High";
    const allowed = ["Exact", "High", "Medium", "Low", "Insufficient", "Unknown", "Hold"];
    propose(
      "Rooms Confidence",
      allowed.includes(conf) ? conf : "High",
      "rooms_value_present",
      "TAXONOMY"
    );
  }
  if (
    isFieldEmpty(hpcFields["Rooms Evidence Tier"]) &&
    Number.isFinite(Number(roomsVal)) &&
    Number(roomsVal) > 0
  ) {
    propose(
      "Rooms Evidence Tier",
      "Tier 2 Official Hotel Website",
      "rooms_from_governed_profile_or_official",
      "TAXONOMY"
    );
  }

  // 19) Narrative fields from verified facts (AI summary clearly labeled)
  const narratives = buildNarrativeFills(profile, hpcFields);
  for (const n of narratives) {
    propose(n.field, n.value, n.source, n.confidence);
  }

  // 20) Data Confidence Tier when identity + geo + rooms mostly present
  if (isFieldEmpty(hpcFields["Data Confidence Tier"])) {
    const hasRooms = Number.isFinite(Number(roomsVal)) && Number(roomsVal) > 0;
    const hasGeo =
      !isFieldEmpty(hpcFields.Latitude) && !isFieldEmpty(hpcFields.Longitude);
    const hasBrand = !isFieldEmpty(hpcFields["Current Brand"] || brandName);
    if (hasRooms && hasGeo && hasBrand) {
      propose("Data Confidence Tier", "High", "identity_geo_rooms_present", "TAXONOMY");
    } else if (hasGeo && hasBrand) {
      propose("Data Confidence Tier", "Medium", "identity_geo_present", "TAXONOMY");
    }
  }

  // 21) Operator Type / Management Model — only when operator known for branded
  if (
    isFieldEmpty(hpcFields["Operator Type"]) &&
    (!isFieldEmpty(hpcFields["Operator / Management Company"]) ||
      proposals.some((p) => p.field === "Operator / Management Company"))
  ) {
    const opName = String(
      hpcFields["Operator / Management Company"] ||
        proposals.find((p) => p.field === "Operator / Management Company")?.value ||
        ""
    );
    if (/marriott|hilton|ihg|hyatt|wyndham|choice|radisson/i.test(opName) &&
        /managed|management/i.test(opName) === false) {
      // third-party named operators
      propose("Operator Type", "Third-Party Operator", "named_third_party_operator", "TAXONOMY");
    } else if (/aimbridge|faranda|hotel equities|arbor|newbond/i.test(opName)) {
      propose("Operator Type", "Third-Party Operator", "named_third_party_operator", "TAXONOMY");
    }
  }

  // Conditional / process fields — document, do not invent
  for (const f of CONDITIONAL_OR_PROCESS_FIELDS) {
    if (isFieldEmpty(hpcFields[f])) {
      notApplicable.push({
        field: f,
        reason: "CONDITIONAL_OR_PROCESS",
        note: "Steward/workflow/opportunity field — not auto-filled without explicit evidence",
      });
    }
  }

  return {
    contractVersion: HPC_ADP_FIELD_POPULATION_V1,
    proposals,
    ambiguous,
    notApplicable,
    dryRunPatch: Object.fromEntries(proposals.map((p) => [p.field, p.value])),
  };
}

/** Process / opportunity / steward fields — never auto-invent */
export const CONDITIONAL_OR_PROCESS_FIELDS = Object.freeze([
  "Possible Operator Target",
  "Possible Soft-Brand Candidate",
  "Possible Brand Conversion Candidate",
  "Possible Owner Outreach Target",
  "Possible Financing Target",
  "Possible Dealality Opportunity",
  "Hotel Property Steward Review",
  "Human Review Required",
  "Next Review Needed",
  "Ownership Review Status",
  "Operator Review Status",
  "Steward Review Status",
  "Notes for Steward",
  "Prior Brand",
  "Affiliation Start Date",
  "Affiliation As-Of Date",
  "Brand-Unassigned Reason",
  "Renovation / Conversion Status",
  "Renovation / Conversion Date",
  "Renovation / Conversion Source URL",
  "Developer Name",
  "Developer Source URL",
  "Developer Confidence",
  "Owner Source URL",
  "Operator Source URL",
  // Shell / discovery / HBX / radar workflow (not ADP profile facts)
  "Shell Insert Batch ID",
  "Shell Insert Country Batch",
  "Shell Insert Date",
  "Shell Insert Source Mix",
  "Shell Dedupe Confidence",
  "VIC Freeze Hash",
  "Discovery Source",
  "Discovery Date",
  "Source Candidate Type",
  "Candidate Source Count",
  "Review Status",
  "Phone Review Status",
  "Phone Reviewed Date",
  "Phone Notes",
  "Rooms Notes",
  "Rooms Reviewed Date",
  "Geocode Reviewed Date",
  "Last Reviewed Date",
  "Public Census Eligibility",
  "Public Display Confidence",
  "Public Display Review Status",
  "Radar Display Reason",
  "Radar Display Status",
  "Radar Geography Status",
  "Enrichment Priority",
  "Enrichment Status",
  "HBX Chain Code",
  "HBX Hotel Code",
  "HBX Category Code",
  "HBX Source Status",
  "HBX Content Review Status",
  "HBX Linkage Confidence",
  "Hotel Property Brand Affiliations",
  "Hotel Property Source Evidence",
  "Opening Date Source URL",
  "Relationship Confidence",
  "Dealality Hotel ID",
  "Data Eligible",
  "Geocode Provider",
  "Geocode Method",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Address Source URL",
  "Phone Source URL",
  "Phone Source Type",
  "Phone Confidence",
  "Brand Confidence",
  "Owner Confidence",
  "Operator Confidence",
  "Owner Type",
  "Operator Type",
  "Management Model",
  "Rooms Source Type",
]);

export function extractPostalFromAddress(address, country) {
  const a = String(address || "");
  if (!a) return null;
  // US ZIP
  const us = a.match(/\b(\d{5})(?:-\d{4})?\b/);
  if (us && /united states|usa|us\b/i.test(String(country || a))) return us[1];
  // Spain / Italy / Germany / Switzerland / Colombia / DR / Mexico 5-digit
  const eu5 = a.match(/\b(\d{5})\b/);
  if (eu5) return eu5[1];
  // UK outward+inward
  const uk = a.match(/\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/i);
  if (uk) return uk[1].toUpperCase().replace(/\s+/, " ");
  // generic trailing postal before country
  const gen = a.match(new RegExp(",\\s*([A-Z0-9][A-Z0-9\\s-]{2,10})\\s*,\\s*[A-Za-z\\s]+$"));
  if (gen) return gen[1].trim();
  return null;
}

export function resolveHotelClassSegment(profile, hpcFields = {}) {
  const scale = String(profile?.chainScale || hpcFields["Hotel Class / Segment"] || "")
    .trim();
  const allowed = [
    "Luxury",
    "Upper Upscale",
    "Upscale",
    "Upper Midscale",
    "Midscale",
    "Economy",
    "Independent / Unclassified",
    "Unknown",
  ];
  if (allowed.includes(scale)) return scale;
  const lower = scale.toLowerCase();
  if (lower.includes("luxury")) return "Luxury";
  if (lower.includes("upper upscale")) return "Upper Upscale";
  if (lower.includes("upscale")) return "Upscale";
  if (lower.includes("upper mid")) return "Upper Midscale";
  if (lower.includes("midscale")) return "Midscale";
  if (lower.includes("economy")) return "Economy";
  // Infer from brand family when chainScale blank
  const brand = String(hpcFields["Current Brand"] || profile?.brand || "").toLowerCase();
  if (/st\.?\s*regis|jw marriott|w hotel|nobu|luxury collection|ritz/i.test(brand)) {
    return "Luxury";
  }
  if (/marriott|westin|sheraton|renaissance|hilton(?!\s*garden)|autograph|curio/i.test(brand)) {
    return "Upper Upscale";
  }
  if (/ac hotel|courtyard|yotel|wyndham(?!\s*grand)|radisson(?!\s*blu)/i.test(brand)) {
    return "Upscale";
  }
  if (/independent|cambridge beaches|spice island|waterstone|casas del/i.test(brand)) {
    return "Independent / Unclassified";
  }
  return null;
}

export function buildNarrativeFills(profile, hpcFields = {}) {
  const out = [];
  const name = String(hpcFields["Property Name"] || profile?.name || "").trim();
  const brand = String(hpcFields["Current Brand"] || profile?.brand || "").trim();
  const city = String(hpcFields.City || profile?.city || "").trim();
  const market = String(hpcFields.Market || profile?.market || "").trim();
  const rooms = hpcFields["Rooms / Keys"] ?? profile?.rooms;
  const attrs = (profile?.attributes || []).map((a) => String(a).replace(/_/g, " "));
  const pos =
    typeof profile?.positioning === "string"
      ? profile.positioning.trim()
      : profile?.positioning?.summary
        ? String(profile.positioning.summary).trim()
        : "";

  if (isFieldEmpty(hpcFields["Short Property Summary"]) && name) {
    const bits = [name];
    if (brand && !name.toLowerCase().includes(brand.toLowerCase())) bits.push(`(${brand})`);
    if (city) bits.push(`in ${city}`);
    if (Number.isFinite(Number(rooms)) && Number(rooms) > 0) {
      bits.push(`— ${Number(rooms)} keys`);
    }
    out.push({
      field: "Short Property Summary",
      value: bits.join(" "),
      source: "dealality_derived_short_summary",
      confidence: "DERIVED",
    });
  }

  if (isFieldEmpty(hpcFields["Property Positioning"])) {
    const text =
      pos ||
      [
        brand ? `${brand} property` : "Hotel",
        market || city ? `serving ${market || city}` : null,
        attrs.slice(0, 4).length ? `signals: ${attrs.slice(0, 4).join("; ")}` : null,
      ]
        .filter(Boolean)
        .join(" — ");
    if (text) {
      out.push({
        field: "Property Positioning",
        value: text,
        source: pos ? "adp_profile.positioning" : "dealality_derived_positioning",
        confidence: pos ? "PROFILE_GOVERNED" : "DERIVED",
      });
    }
  }

  if (isFieldEmpty(hpcFields["Hotel Description - AI Summary"]) && name) {
    const meetRooms = profile?.meetingSpace?.meetingRooms;
    const parts = [
      `${name} is a ${brand || "hospitality"} property${city ? ` in ${city}` : ""}.`,
    ];
    if (Number.isFinite(Number(rooms)) && Number(rooms) > 0) {
      parts.push(`Inventory: ${Number(rooms)} rooms/keys.`);
    }
    if (Number.isFinite(Number(meetRooms)) && Number(meetRooms) > 0) {
      parts.push(`Meeting inventory includes ${meetRooms} meeting rooms.`);
    }
    if (attrs.length) {
      parts.push(`Governed attributes: ${attrs.slice(0, 8).join(", ")}.`);
    }
    parts.push(
      "(AI-assisted summary from Dealality-governed ADP profile facts — not official source text.)"
    );
    out.push({
      field: "Hotel Description - AI Summary",
      value: parts.join(" "),
      source: "dealality_ai_summary_from_profile",
      confidence: "DERIVED_AI",
    });
  }

  return out;
}

export { countryToSubContinent, countryToDealalityRegion };
