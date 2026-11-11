/**
 * HPC field-semantic validation V1
 *
 * Ensures classification fields share one meaning per hotel.
 * Detects incorrect populated values (not only empties).
 * Does not invent facts or create new Airtable select options.
 */

import { isFieldEmpty } from "./hpc-full-onboarding-contract-v1.js";
import {
  HPC_PROPERTY_TYPE_CANONICAL,
  COUNTRY_TO_CONTINENT,
} from "./hpc-adp-field-population-v1.js";
import {
  hasValidHpcCoordinates,
  auditHotelCoordinates,
} from "./hpc-geographic-integrity-v1.js";
import { countryToSubContinent } from "./geography-enrichment-contract.js";
import {
  classifyHpcFieldImportance,
  HPC_FIELD_IMPORTANCE,
} from "./hpc-field-importance-v1.js";

export const HPC_FIELD_SEMANTIC_VALIDATION_V1 = "HPC_FIELD_SEMANTIC_VALIDATION_V1";
export const HPC_FIELD_SEMANTICS_CERT = "hpc_field_semantics_cert";

/** Fundamental accommodation category — NOT service model / not positioning detail. */
export const PROPERTY_TYPE_SEMANTICS = Object.freeze({
  field: "Property Type",
  meaning:
    "Fundamental accommodation / product category (Hotel, Resort, Boutique Hotel, Extended Stay, All-Inclusive, Serviced Apartment, Mixed-Use, Other, Unknown).",
  not:
    "Service intensity (full/select), historic character, golf, castle, or luxury positioning — those belong in Hotel Class / Segment, Asset Context, Property Positioning, or structured tags.",
  canonical: HPC_PROPERTY_TYPE_CANONICAL,
});

/**
 * Legacy / mixed values observed in live HPC Property Type select.
 * Map onto canonical Property Type; optionally enrich Property Positioning.
 */
export const PROPERTY_TYPE_LEGACY_REMAP = Object.freeze({
  full_service: {
    propertyType: "Hotel",
    positioningHint: null,
    reason: "full_service is a service-model label, not a Property Type category",
  },
  select_service: {
    propertyType: "Hotel",
    positioningHint: null,
    reason: "select_service is a service-model label, not a Property Type category",
  },
  historic_full_service: {
    propertyType: "Hotel",
    positioningHint: "Historic full-service hotel",
    reason: "Historic character is Property Positioning; category remains Hotel",
  },
  luxury_resort_castle: {
    propertyType: "Resort",
    positioningHint: "Castle / heritage luxury resort",
    reason: "Castle/luxury detail is positioning; category is Resort",
  },
  golf_resort: {
    propertyType: "Resort",
    positioningHint: "Golf resort",
    reason: "Golf specialization is positioning; category is Resort",
  },
  // HE-era labels if still present
  "Full Service": {
    propertyType: "Hotel",
    positioningHint: null,
    reason: "Full Service is service-model terminology",
  },
  "Select Service": {
    propertyType: "Hotel",
    positioningHint: null,
    reason: "Select Service is service-model terminology",
  },
  Boutique: {
    propertyType: "Boutique Hotel",
    positioningHint: null,
    reason: "Normalize Boutique → Boutique Hotel",
  },
  Lifestyle: {
    propertyType: "Hotel",
    positioningHint: "Lifestyle hotel",
    reason: "Lifestyle is positioning, not a distinct Property Type option in canonical set",
  },
});

export const CANONICAL_FIELD_SEMANTICS = Object.freeze({
  "Property Type": PROPERTY_TYPE_SEMANTICS,
  "Hotel Class / Segment": {
    field: "Hotel Class / Segment",
    meaning:
      "Chain-scale / quality band (Luxury → Economy, Independent / Unclassified, Unknown).",
    not: "Property Type or Asset Context",
  },
  "Asset Context": {
    field: "Asset Context",
    meaning:
      "Physical / geographic operating context (Urban, Airport, Suburban, Beach / Waterfront, Resort Destination, …).",
    not: "Brand class or service model",
  },
  "Property Positioning": {
    field: "Property Positioning",
    meaning:
      "Free-text hotel-specific commercial positioning (historic, golf, castle, lifestyle, etc.).",
    not: "A substitute for governed Property Type",
  },
  "Current Brand": {
    field: "Current Brand",
    meaning: "Operating brand / flag identity on the property.",
  },
  "Brand Family": {
    field: "Brand Family",
    meaning: "Parent company / loyalty ecosystem (e.g. Marriott International).",
  },
  "Operator / Management Company": {
    field: "Operator / Management Company",
    meaning: "Entity that manages day-to-day operations — may differ from brand.",
  },
  "Operator Type": {
    field: "Operator Type",
    meaning: "How the brand/operator relationship is structured.",
  },
  "Management Model": {
    field: "Management Model",
    meaning: "Contract form (Management Agreement, Franchise, Lease, …).",
  },
  "Affiliation Status": {
    field: "Affiliation Status",
    meaning: "Branded vs soft-brand vs independent classification at census grain.",
  },
  "Independent Hotel Flag": {
    field: "Independent Hotel Flag",
    meaning: "True only when the property is not currently brand-affiliated.",
  },
  "Independent Classification": {
    field: "Independent Classification",
    meaning: "Nuanced independent / soft-brand / formerly-branded status.",
  },
  Market: {
    field: "Market",
    meaning: "Dealality commercial market (not STR Market).",
  },
  Submarket: {
    field: "Submarket",
    meaning: "Dealality corridor / submarket label within Market.",
  },
  Country: { field: "Country", meaning: "ISO-style country name used by Dealality geography." },
  Continent: { field: "Continent", meaning: "Continent consistent with Country." },
  "Sub-Continent": {
    field: "Sub-Continent",
    meaning: "Dealality sub-continent consistent with Country (restricted select).",
  },
});

export const SEMANTIC_ISSUE_CODE = Object.freeze({
  PROPERTY_TYPE_LEGACY_SLUG: "PROPERTY_TYPE_LEGACY_SLUG",
  PROPERTY_TYPE_UNKNOWN_VALUE: "PROPERTY_TYPE_UNKNOWN_VALUE",
  INVALID_COORDINATES: "INVALID_COORDINATES",
  GEO_COUNTRY_MISMATCH: "GEO_COUNTRY_MISMATCH",
  CONTINENT_COUNTRY_MISMATCH: "CONTINENT_COUNTRY_MISMATCH",
  SUBCONTINENT_COUNTRY_MISMATCH: "SUBCONTINENT_COUNTRY_MISMATCH",
  INDEPENDENT_BRANDED_CONFLICT: "INDEPENDENT_BRANDED_CONFLICT",
  BRANDED_WITHOUT_BRAND: "BRANDED_WITHOUT_BRAND",
  OPERATOR_EQUALS_BRAND_WITHOUT_MODEL: "OPERATOR_EQUALS_BRAND_WITHOUT_MODEL",
  AMENITY_FALSE_WITHOUT_EVIDENCE: "AMENITY_FALSE_WITHOUT_EVIDENCE",
  SELECT_NOT_IN_SCHEMA: "SELECT_NOT_IN_SCHEMA",
});

function schemaChoices(schema, fieldName) {
  const f = (schema?.fields || []).find((x) => x.name === fieldName);
  return Array.isArray(f?.choices) ? f.choices : null;
}

function choiceAllowed(schema, fieldName, value) {
  const choices = schemaChoices(schema, fieldName);
  if (!choices || !choices.length) return true;
  return choices.includes(value);
}

/**
 * Propose Property Type canonicalization (and optional positioning).
 */
export function proposePropertyTypeCorrection(currentValue, { schema } = {}) {
  if (isFieldEmpty(currentValue)) return null;
  const raw = String(currentValue).trim();
  if (HPC_PROPERTY_TYPE_CANONICAL.includes(raw)) {
    return null; // already canonical
  }
  const remap = PROPERTY_TYPE_LEGACY_REMAP[raw];
  if (remap) {
    if (!choiceAllowed(schema, "Property Type", remap.propertyType)) {
      return {
        blocked: true,
        code: SEMANTIC_ISSUE_CODE.PROPERTY_TYPE_LEGACY_SLUG,
        field: "Property Type",
        existingValue: raw,
        proposedValue: remap.propertyType,
        reason: `${remap.reason} — but '${remap.propertyType}' not in live select; schema authorization required`,
        confidence: "HIGH",
        requiresSchemaAuth: true,
      };
    }
    return {
      blocked: false,
      code: SEMANTIC_ISSUE_CODE.PROPERTY_TYPE_LEGACY_SLUG,
      field: "Property Type",
      existingValue: raw,
      proposedValue: remap.propertyType,
      positioningHint: remap.positioningHint,
      reason: remap.reason,
      confidence: "HIGH",
      evidenceRule: "PROPERTY_TYPE_TAXONOMY_V1",
      authorization: "GOVERNED_SEMANTIC_CORRECTION",
      importance: HPC_FIELD_IMPORTANCE.IMPORTANT,
    };
  }
  return {
    blocked: true,
    code: SEMANTIC_ISSUE_CODE.PROPERTY_TYPE_UNKNOWN_VALUE,
    field: "Property Type",
    existingValue: raw,
    proposedValue: null,
    reason: "Value is not in canonical Property Type set and has no governed remap",
    confidence: "MEDIUM",
    requiresManualReview: true,
    importance: HPC_FIELD_IMPORTANCE.IMPORTANT,
  };
}

function mergePositioning(existing, hint) {
  if (!hint) return null;
  const cur = String(existing || "").trim();
  if (!cur) return hint;
  if (cur.toLowerCase().includes(hint.toLowerCase())) return null;
  return `${cur}\n${hint}`;
}

/**
 * Validate one hotel's HPC fields. Returns discrepancies + auto-applicable corrections.
 */
export function validateHotelHpcSemantics({
  adpPropertyId,
  hpcRecordId,
  hotelName = null,
  fields = {},
  schema = null,
  profile = null,
} = {}) {
  const discrepancies = [];
  const corrections = [];
  const f = fields || {};

  // --- Property Type ---
  const pt = proposePropertyTypeCorrection(f["Property Type"], { schema });
  if (pt) {
    discrepancies.push({
      adpPropertyId,
      hpcRecordId,
      hotelName,
      field: pt.field,
      existingValue: pt.existingValue,
      proposedValue: pt.proposedValue,
      code: pt.code,
      reason: pt.reason,
      confidence: pt.confidence,
      importance: pt.importance || classifyHpcFieldImportance(pt.field),
      autoApplicable: !pt.blocked && !pt.requiresSchemaAuth && !pt.requiresManualReview,
      authorization: pt.authorization || "PENDING_REVIEW",
      evidenceRule: pt.evidenceRule || null,
      downstreamImpact: "property_attribute_intelligence, report Property Snapshot labels",
    });
    if (!pt.blocked && pt.proposedValue) {
      corrections.push({
        field: "Property Type",
        from: pt.existingValue,
        to: pt.proposedValue,
        reason: pt.reason,
        confidence: pt.confidence,
        code: pt.code,
      });
      const pos = mergePositioning(f["Property Positioning"], pt.positioningHint);
      if (pos) {
        corrections.push({
          field: "Property Positioning",
          from: f["Property Positioning"] || null,
          to: pos,
          reason: "Preserve specialized classification removed from Property Type",
          confidence: "HIGH",
          code: pt.code,
        });
        discrepancies.push({
          adpPropertyId,
          hpcRecordId,
          hotelName,
          field: "Property Positioning",
          existingValue: f["Property Positioning"] || null,
          proposedValue: pos,
          code: pt.code,
          reason: "Carry forward specialization into Property Positioning",
          confidence: "HIGH",
          importance: HPC_FIELD_IMPORTANCE.ENRICHMENT,
          autoApplicable: true,
          authorization: "GOVERNED_SEMANTIC_CORRECTION",
          evidenceRule: "PROPERTY_TYPE_TAXONOMY_V1",
          downstreamImpact: "presentation only",
        });
      }
    }
  }

  // --- Select membership ---
  for (const fieldName of [
    "Property Type",
    "Hotel Class / Segment",
    "Asset Context",
    "Continent",
    "Sub-Continent",
    "Operator Type",
    "Management Model",
    "Affiliation Status",
    "Independent Classification",
  ]) {
    const val = f[fieldName];
    if (isFieldEmpty(val)) continue;
    if (!choiceAllowed(schema, fieldName, val)) {
      // Legacy remaps already covered for Property Type
      if (fieldName === "Property Type" && PROPERTY_TYPE_LEGACY_REMAP[val]) continue;
      discrepancies.push({
        adpPropertyId,
        hpcRecordId,
        hotelName,
        field: fieldName,
        existingValue: val,
        proposedValue: null,
        code: SEMANTIC_ISSUE_CODE.SELECT_NOT_IN_SCHEMA,
        reason: "Value not present in live Airtable select choices",
        confidence: "HIGH",
        importance: classifyHpcFieldImportance(fieldName),
        autoApplicable: false,
        authorization: "SCHEMA_OR_MANUAL_REVIEW",
        evidenceRule: "LIVE_SCHEMA",
        downstreamImpact: "select integrity",
      });
    }
  }

  // --- Coordinates ---
  const lat = f.Latitude;
  const lng = f.Longitude;
  const coordAudit = auditHotelCoordinates({
    propertyId: adpPropertyId,
    hotelName,
    latitude: lat,
    longitude: lng,
    country: f.Country,
    city: f.City,
  });
  if (!coordAudit.valid) {
    const isHardInvalid =
      !hasValidHpcCoordinates(lat, lng) ||
      ["ZERO_ZERO", "MISSING", "OUT_OF_RANGE", "WRONG_COUNTRY_BBOX", "SWAP_SUSPECTED"].includes(
        coordAudit.primaryIssue
      );
    discrepancies.push({
      adpPropertyId,
      hpcRecordId,
      hotelName,
      field: "Latitude",
      existingValue: `${lat},${lng}`,
      proposedValue: null,
      code: isHardInvalid
        ? SEMANTIC_ISSUE_CODE.INVALID_COORDINATES
        : SEMANTIC_ISSUE_CODE.GEO_COUNTRY_MISMATCH,
      reason: `Coordinate integrity: ${coordAudit.primaryIssue}`,
      confidence: "HIGH",
      importance: HPC_FIELD_IMPORTANCE.CRITICAL,
      autoApplicable: false,
      authorization: "GEOGRAPHIC_INTEGRITY_CORRECTION",
      evidenceRule: "HPC_GEOGRAPHIC_INTEGRITY_V1",
      downstreamImpact: "maps, market validation — fail-closed when critical",
    });
  }

  // --- Continent / Sub-Continent vs Country ---
  const country = String(f.Country || "").trim();
  if (country && !isFieldEmpty(f.Continent)) {
    const expectedCont = COUNTRY_TO_CONTINENT[country];
    if (expectedCont && f.Continent !== expectedCont) {
      const corr = {
        field: "Continent",
        from: f.Continent,
        to: expectedCont,
        reason: `Continent must follow Country (${country} → ${expectedCont})`,
        confidence: "HIGH",
        code: SEMANTIC_ISSUE_CODE.CONTINENT_COUNTRY_MISMATCH,
      };
      if (choiceAllowed(schema, "Continent", expectedCont)) {
        corrections.push(corr);
      }
      discrepancies.push({
        adpPropertyId,
        hpcRecordId,
        hotelName,
        field: "Continent",
        existingValue: f.Continent,
        proposedValue: expectedCont,
        code: SEMANTIC_ISSUE_CODE.CONTINENT_COUNTRY_MISMATCH,
        reason: corr.reason,
        confidence: "HIGH",
        importance: HPC_FIELD_IMPORTANCE.IMPORTANT,
        autoApplicable: choiceAllowed(schema, "Continent", expectedCont),
        authorization: "GOVERNED_SEMANTIC_CORRECTION",
        evidenceRule: "COUNTRY_TO_CONTINENT",
        downstreamImpact: "geography filters",
      });
    }
  }
  if (country && !isFieldEmpty(f["Sub-Continent"])) {
    const expectedSc = countryToSubContinent(country);
    if (expectedSc && f["Sub-Continent"] !== expectedSc) {
      // Only auto-correct when expected is in schema
      const applicable = choiceAllowed(schema, "Sub-Continent", expectedSc);
      discrepancies.push({
        adpPropertyId,
        hpcRecordId,
        hotelName,
        field: "Sub-Continent",
        existingValue: f["Sub-Continent"],
        proposedValue: expectedSc,
        code: SEMANTIC_ISSUE_CODE.SUBCONTINENT_COUNTRY_MISMATCH,
        reason: `Sub-Continent should follow Country mapping (${country} → ${expectedSc})`,
        confidence: "HIGH",
        importance: HPC_FIELD_IMPORTANCE.IMPORTANT,
        autoApplicable: applicable,
        authorization: applicable
          ? "GOVERNED_SEMANTIC_CORRECTION"
          : "SCHEMA_OR_MANUAL_REVIEW",
        evidenceRule: "countryToSubContinent",
        downstreamImpact: "geography filters",
      });
      if (applicable) {
        corrections.push({
          field: "Sub-Continent",
          from: f["Sub-Continent"],
          to: expectedSc,
          reason: `Sub-Continent must follow Country (${country})`,
          confidence: "HIGH",
          code: SEMANTIC_ISSUE_CODE.SUBCONTINENT_COUNTRY_MISMATCH,
        });
      }
    }
  }

  // --- Brand / independent consistency ---
  const aff = f["Affiliation Status"];
  const indepFlag = f["Independent Hotel Flag"] === true;
  const brand = f["Current Brand"];
  if (indepFlag && aff === "Branded") {
    discrepancies.push({
      adpPropertyId,
      hpcRecordId,
      hotelName,
      field: "Independent Hotel Flag",
      existingValue: true,
      proposedValue: false,
      code: SEMANTIC_ISSUE_CODE.INDEPENDENT_BRANDED_CONFLICT,
      reason: "Independent Hotel Flag true conflicts with Affiliation Status=Branded",
      confidence: "HIGH",
      importance: HPC_FIELD_IMPORTANCE.IMPORTANT,
      autoApplicable: true,
      authorization: "GOVERNED_SEMANTIC_CORRECTION",
      evidenceRule: "AFFILIATION_CONSISTENCY",
      downstreamImpact: "independent filters, BPP lens",
    });
    corrections.push({
      field: "Independent Hotel Flag",
      from: true,
      to: false,
      reason: "Clear independent flag when Affiliation Status is Branded",
      confidence: "HIGH",
      code: SEMANTIC_ISSUE_CODE.INDEPENDENT_BRANDED_CONFLICT,
    });
  }
  if (aff === "Branded" && isFieldEmpty(brand)) {
    discrepancies.push({
      adpPropertyId,
      hpcRecordId,
      hotelName,
      field: "Current Brand",
      existingValue: null,
      proposedValue: profile?.brand || null,
      code: SEMANTIC_ISSUE_CODE.BRANDED_WITHOUT_BRAND,
      reason: "Affiliation Status=Branded but Current Brand empty",
      confidence: profile?.brand ? "MEDIUM" : "LOW",
      importance: HPC_FIELD_IMPORTANCE.IMPORTANT,
      autoApplicable: !!profile?.brand,
      authorization: profile?.brand
        ? "GOVERNED_SEMANTIC_CORRECTION"
        : "RESEARCH_REQUIRED",
      evidenceRule: "AFFILIATION_CONSISTENCY",
      downstreamImpact: "BPP, brand membership",
    });
    if (profile?.brand) {
      corrections.push({
        field: "Current Brand",
        from: null,
        to: profile.brand,
        reason: "Fill brand from ADP profile when Affiliation Status is Branded",
        confidence: "MEDIUM",
        code: SEMANTIC_ISSUE_CODE.BRANDED_WITHOUT_BRAND,
      });
    }
  }

  // --- Amenity false without evidence: flag for research, never auto-flip to true ---
  for (const flag of [
    "Meeting Space Flag",
    "F&B Flag",
    "Spa Flag",
    "Pool Flag",
    "Fitness Flag",
    "Parking Flag",
    "Beach / Waterfront Flag",
    "Resort / Leisure Flag",
    "Extended Stay Flag",
  ]) {
    if (f[flag] !== false) continue;
    // Only raise when profile strongly suggests true and flag is false
    const attrs = new Set(
      (profile?.attributes || []).map((a) => String(a).toLowerCase())
    );
    const suggestTrue =
      (flag === "Spa Flag" && attrs.has("spa")) ||
      (flag === "Pool Flag" && (attrs.has("pool") || attrs.has("rooftop_pool"))) ||
      (flag === "Beach / Waterfront Flag" &&
        (attrs.has("beachfront") || attrs.has("waterfront") || attrs.has("oceanfront"))) ||
      (flag === "Meeting Space Flag" &&
        ((profile?.meetingSpace?.meetingRooms || 0) > 0 ||
          attrs.has("meeting_space") ||
          attrs.has("ballroom"))) ||
      (flag === "F&B Flag" &&
        (attrs.has("restaurant") || attrs.has("multiple_restaurants") || attrs.has("fnb"))) ||
      (flag === "Resort / Leisure Flag" && attrs.has("resort")) ||
      (flag === "Extended Stay Flag" && attrs.has("extended_stay"));
    if (!suggestTrue) continue;
    discrepancies.push({
      adpPropertyId,
      hpcRecordId,
      hotelName,
      field: flag,
      existingValue: false,
      proposedValue: true,
      code: SEMANTIC_ISSUE_CODE.AMENITY_FALSE_WITHOUT_EVIDENCE,
      reason:
        "Flag is false while ADP profile attributes indicate presence — do not treat missing mention as false; research before flip",
      confidence: "MEDIUM",
      importance: HPC_FIELD_IMPORTANCE.ENRICHMENT,
      autoApplicable: false,
      authorization: "RESEARCH_REQUIRED",
      evidenceRule: "AMENITY_EVIDENCE_GATE",
      downstreamImpact: "Reality Gap / attribute intelligence",
    });
  }

  const critical = discrepancies.filter(
    (d) => d.importance === HPC_FIELD_IMPORTANCE.CRITICAL
  );
  const important = discrepancies.filter(
    (d) => d.importance === HPC_FIELD_IMPORTANCE.IMPORTANT
  );
  const auto = corrections.filter((c) =>
    discrepancies.some(
      (d) =>
        d.field === c.field &&
        d.autoApplicable &&
        String(d.proposedValue) === String(c.to)
    )
  );

  const certPass = critical.length === 0;
  return {
    contractVersion: HPC_FIELD_SEMANTIC_VALIDATION_V1,
    certGate: HPC_FIELD_SEMANTICS_CERT,
    adpPropertyId,
    hpcRecordId,
    hotelName,
    discrepancyCount: discrepancies.length,
    criticalCount: critical.length,
    importantCount: important.length,
    autoCorrectionCount: auto.length,
    discrepancies,
    corrections: auto,
    allCorrections: corrections,
    cert: {
      gate: HPC_FIELD_SEMANTICS_CERT,
      passed: certPass,
      blockers: critical.map((d) => d.code),
      warnings: important.map((d) => d.code),
      notes: certPass
        ? important.length
          ? "CRITICAL clear; IMPORTANT semantic backlog retained"
          : "No critical or important semantic defects"
        : "Critical semantic defects must fail-closed",
    },
  };
}

/**
 * Assert helpers for tests / Full E2E.
 */
export function assertHpcFieldSemanticsCert(result) {
  if (!result?.cert) {
    return { pass: false, failures: ["NO_CERT_RESULT"] };
  }
  return {
    pass: result.cert.passed === true,
    failures: result.cert.blockers || [],
    warnings: result.cert.warnings || [],
  };
}
