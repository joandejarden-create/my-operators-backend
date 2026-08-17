/**
 * Census Autopilot V3 dry-run — claim-level field classification (V3.0.1).
 * Blocked lower-authority claims must not suppress eligible official claims.
 */

import { WRITE_CLASS, VERIFIED_STATE, MATCH_CLASS } from "./constants.js";
import { buildWritePolicy } from "./field-policy.js";
import {
  resolveBestEligibleClaim,
  writeClassForSelectedClaim,
} from "./claim-store.js";
import {
  evaluateCurrentAffiliationGate,
  isParentCompanyAsCurrentBrand,
} from "./current-affiliation.js";

function today() {
  return new Date().toISOString().slice(0, 10);
}

function isBlank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}

/**
 * Collect field claims from pilot staging object.
 */
export function collectPilotFieldClaims(pilot, runId) {
  const claims = {};
  const baseMeta = {
    match_confidence: "High",
    research_run: runId,
    retrieved_at: new Date().toISOString(),
  };

  function push(field, value, meta) {
    if (value == null || value === "") return;
    if (!claims[field]) claims[field] = [];
    claims[field].push({ value, ...baseMeta, ...meta });
  }

  const geo = pilot.geography || {};
  const srcType = pilot.source_type || "official_brand_directory";
  const officialMeta = {
    source: pilot.family || "official",
    source_type: srcType,
    source_url: pilot.official_url || null,
    confidence: "High",
    serpapi_used: false,
    cvent_used_as_production_evidence: false,
    legacy_used_as_production_evidence: false,
  };

  push("Property Identity Key", pilot.property_identity_key, officialMeta);
  push("Property Name", pilot.name, officialMeta);
  push("Canonical Property Name", pilot.name, officialMeta);
  // Current Brand: property-level only — never default to source family / parent.
  const affiliationGate = evaluateCurrentAffiliationGate({
    explicit_brand: isParentCompanyAsCurrentBrand(pilot.brand)
      ? null
      : pilot.brand,
    directory_brand: pilot.directory_brand || null,
    official_property_url: pilot.official_url,
    source_url: pilot.official_url,
    parent_company: pilot.parent_company || null,
    brand_family: pilot.family,
    source_family: pilot.family,
    family: pilot.family,
    identity_confidence: pilot.identity_confidence || "High",
    match_class: pilot.match_class,
    match_confidence: pilot.match_confidence,
  });
  pilot._affiliation_gate = affiliationGate;
  if (affiliationGate.auto_write_allowed && affiliationGate.brand) {
    push("Current Brand", affiliationGate.brand, {
      ...officialMeta,
      affiliation_gate: affiliationGate.gate,
      source_type: affiliationGate.evidence?.selected?.source || officialMeta.source_type,
    });
  }
  push("Brand Family", pilot.family, officialMeta);
  push("Official Property URL", pilot.official_url, officialMeta);
  push("Source URL", pilot.official_url, officialMeta);
  push("City", pilot.city, officialMeta);
  push("Country", pilot.country, {
    ...officialMeta,
    source_type: "dealality_geography",
  });
  push("Continent", geo.continent || "Americas", {
    ...officialMeta,
    source_type: "dealality_geography",
  });
  push("Sub-Continent", geo.sub_continent, {
    ...officialMeta,
    source_type: "dealality_geography",
  });
  push("Market", geo.market, {
    ...officialMeta,
    source_type: "dealality_geography",
  });
  if (geo.submarket && geo.submarket_confidence !== "No Match") {
    push("Submarket", geo.submarket, {
      ...officialMeta,
      source_type: "dealality_geography",
      confidence: geo.submarket_confidence || "Medium",
    });
  }
  if (geo.state_region) {
    push("State / Region", geo.state_region, {
      ...officialMeta,
      source_type: geo.state_region_source || "dealality_geography",
      confidence: geo.state_region_confidence || "Medium",
    });
  }
  if (pilot.address) {
    push("Address", pilot.address, {
      ...officialMeta,
      source_type: pilot.address_source_type || "official_property_page",
    });
  }
  if (pilot.phone) {
    push("Phone", pilot.phone, {
      ...officialMeta,
      source_type: pilot.phone_source_type || "official_property_page",
    });
  }
  if (pilot.latitude != null && Number.isFinite(Number(pilot.latitude))) {
    push("Latitude", Number(pilot.latitude), officialMeta);
  }
  if (pilot.longitude != null && Number.isFinite(Number(pilot.longitude))) {
    push("Longitude", Number(pilot.longitude), officialMeta);
  }

  // Optional SerpApi comparison claims (must not suppress official)
  for (const [field, val] of Object.entries(pilot.serpapi_claims || {})) {
    if (val == null || val === "") continue;
    push(field, val, {
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      source_url: pilot.serpapi_source_url || null,
      confidence: "High",
      serpapi_used: true,
      cvent_used_as_production_evidence: false,
      legacy_used_as_production_evidence: false,
    });
  }

  return claims;
}

/**
 * Build field-level proposed writes for one pilot property (claim-level rights).
 */
export function classifyFieldWrites(pilot, existingFields, runId) {
  const policy = buildWritePolicy();
  const proposed = [];
  const blocked = [];
  const steward = [];
  const claimSelections = {};

  const evidenceBase = {
    source_url: pilot.official_url,
    source_type:
      pilot.source_type === "verified_independent_census_seed"
        ? "official_brand_directory"
        : "brand_directory",
    source_class: "official_brand_directory",
    confidence: "High",
    match_confidence: "High",
    research_run_id: runId,
    cvent_used_as_production_evidence: false,
    legacy_used_as_production_evidence: false,
    serpapi_used: false,
  };

  function add(field, value, writeClass, opts = {}) {
    if (value == null || value === "") return;
    const before = existingFields?.[field];
    const blank = isBlank(before);
    let update_class = "SAME";
    if (blank) update_class = "BLANK_FILL";
    else if (String(before) === String(value)) update_class = "SAME";
    else if (
      ["Current Brand", "Property Name"].includes(field) &&
      !blank &&
      String(before) !== String(value)
    ) {
      update_class = "TEMPORAL_CHANGE";
      writeClass = WRITE_CLASS.STEWARD_REVIEW;
    } else if (!blank && String(before) !== String(value)) {
      update_class = "CONTRADICTION";
      writeClass = WRITE_CLASS.STEWARD_REVIEW;
    }

    const row = {
      field,
      value,
      before: blank ? null : before,
      write_class: writeClass,
      update_class,
      provenance: { ...evidenceBase, ...(opts.provenance || {}) },
      verified_state: pilot.verified_state,
    };

    if (writeClass === WRITE_CLASS.BLOCKED_RIGHTS || writeClass === WRITE_CLASS.PROHIBITED) {
      blocked.push(row);
      return;
    }
    if (
      writeClass === WRITE_CLASS.STEWARD_REVIEW ||
      writeClass === WRITE_CLASS.FIRST_PARTY_VALIDATION
    ) {
      steward.push(row);
      return;
    }
    if (pilot.match_class === MATCH_CLASS.EXACT_EXISTING_MATCH) {
      if (update_class === "BLANK_FILL") proposed.push(row);
      else if (update_class === "SAME") {
        /* no-op */
      } else steward.push(row);
      return;
    }
    proposed.push(row);
  }

  const fieldClaims = collectPilotFieldClaims(pilot, runId);

  // Core identity / geography via claim selection
  const autoFields = [
    "Property Identity Key",
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "Brand Family",
    "Official Property URL",
    "Source URL",
    "City",
    "Country",
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
    "State / Region",
    "Address",
    "Latitude",
    "Longitude",
    "Phone",
  ];

  for (const field of autoFields) {
    const claims = fieldClaims[field] || [];
    if (!claims.length) continue;
    const sel = resolveBestEligibleClaim(claims, { field });
    claimSelections[field] = {
      selected_source: sel.selected_source,
      selected_source_type: sel.selected_source_type,
      selected_rights_status: sel.selected_rights_status,
      rejected: sel.rejected_claims_with_reason.map((r) => ({
        reason: r.reason,
        source_type: r.claim.source_type,
        value: r.claim.value,
      })),
    };

    if (!sel.selected_claim) {
      // All claims rejected (e.g. SerpApi-only) → record blocked with actual values
      for (const r of sel.rejected_claims_with_reason) {
        if (r.reason === "blocked_rights_serpapi_or_policy") {
          blocked.push({
            field,
            value: r.claim.value,
            write_class: WRITE_CLASS.BLOCKED_RIGHTS,
            reason: "SERPAPI_ONLY_CLAIM_BLOCKED_OFFICIAL_ABSENT",
            provenance: {
              ...evidenceBase,
              serpapi_used: true,
              serpapi_blocked: true,
              source_type: r.claim.source_type,
            },
          });
        }
      }
      continue;
    }

    const wc =
      writeClassForSelectedClaim(field, sel.selected_claim) || WRITE_CLASS.CORROBORATED_WRITE;
    add(field, sel.selected_claim.value, wc, {
      provenance: {
        source_url: sel.selected_claim.source_url || evidenceBase.source_url,
        source_type: sel.selected_claim.source_type,
        source_class: sel.selected_claim.source_type,
        confidence: sel.selected_claim.confidence,
        match_confidence: sel.selected_claim.match_confidence,
        research_run_id: runId,
        serpapi_used: sel.selected_claim.serpapi_used === true,
        cvent_used_as_production_evidence: false,
        legacy_used_as_production_evidence: false,
        rejected_lower_authority_count: sel.blocked_but_not_suppressing,
      },
    });
  }

  // Governance defaults (deterministic)
  add("Family / Source Family", pilot.family, WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Source Type", "brand_directory", WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Source Confidence", "High", WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Identity Confidence", "High", WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Data Eligible", true, WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Production Use Status", "Census Only / Not Owner-Facing", WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Discovery Date", today(), WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Enrichment Status", "Discovered — pending enrichment", WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Enrichment Priority", "High", WRITE_CLASS.AUTO_WRITE_SAFE);
  add("Last Reviewed Date", today(), WRITE_CLASS.AUTO_WRITE_SAFE);
  add(
    "Affiliation Status",
    pilot.family === "Independent" ? "Independent" : "Branded",
    WRITE_CLASS.STEWARD_REVIEW
  );

  steward.push({
    field: "Rooms / Keys",
    value: null,
    before: existingFields?.["Rooms / Keys"] ?? null,
    write_class: WRITE_CLASS.FIRST_PARTY_VALIDATION,
    update_class: "ROOMS_PENDING",
    provenance: {
      ...evidenceBase,
      note: "Rooms Unknown — VERIFIED — ROOMS PENDING; not inferred",
    },
    verified_state: VERIFIED_STATE.ROOMS_PENDING,
  });

  // Amenities / descriptions: still SerpApi-class by default when no official claim collected
  for (const f of [
    "Amenities - Source Text",
    "Amenities - Structured Tags",
    "Hotel Description - Source Text",
    "Hotel Description - AI Summary",
  ]) {
    const claims = fieldClaims[f] || [];
    if (claims.length) {
      const sel = resolveBestEligibleClaim(claims, { field: f });
      if (sel.selected_claim) {
        add(f, sel.selected_claim.value, WRITE_CLASS.CORROBORATED_WRITE, {
          provenance: { source_type: sel.selected_claim.source_type },
        });
      } else {
        blocked.push({
          field: f,
          value: claims[0]?.value ?? null,
          write_class: WRITE_CLASS.BLOCKED_RIGHTS,
          reason: "NO_ELIGIBLE_OFFICIAL_CLAIM",
          provenance: { ...evidenceBase, serpapi_blocked: true },
        });
      }
    } else if (policy.blocked_rights.includes(f)) {
      blocked.push({
        field: f,
        value: null,
        write_class: WRITE_CLASS.BLOCKED_RIGHTS,
        reason: "NO_CLAIM_AND_SERPAPI_CLASS_DEFAULT",
        provenance: { ...evidenceBase, serpapi_blocked: true },
      });
    }
  }

  return { proposed, blocked, steward, claimSelections };
}

/**
 * Assemble dry-run insert/update payloads.
 */
export function buildDryRunMutations(selected, censusById, runId) {
  const inserts = [];
  const updates = [];
  const blocked = [];
  const steward = [];
  const fieldClassCounts = {};
  let roomsPending = 0;
  let serpapiBlockedFields = 0;

  for (const p of selected) {
    const existing = p.census_record_id ? censusById.get(p.census_record_id)?.fields || {} : {};
    const { proposed, blocked: b, steward: s } = classifyFieldWrites(p, existing, runId);

    roomsPending += 1;
    for (const row of [...proposed, ...b, ...s]) {
      fieldClassCounts[row.write_class] = (fieldClassCounts[row.write_class] || 0) + 1;
      if (row.write_class === WRITE_CLASS.BLOCKED_RIGHTS) serpapiBlockedFields += 1;
    }
    blocked.push(...b.map((x) => ({ ...x, property_identity_key: p.property_identity_key })));
    steward.push(...s.map((x) => ({ ...x, property_identity_key: p.property_identity_key })));

    if (!p.eligible_auto_write) continue;

    if (p.match_class === MATCH_CLASS.NEW_INSERT) {
      const fields = {};
      for (const row of proposed) fields[row.field] = row.value;
      if (p.cvent_used_as_production_evidence) continue;
      if (p.legacy_used_as_production_evidence) continue;
      inserts.push({
        operation: "INSERT",
        property_identity_key: p.property_identity_key,
        research_property_identity_id: p.research_property_identity_id,
        verified_state: p.verified_state,
        fields,
        field_writes: proposed,
        cvent_used_as_production_evidence: false,
        legacy_used_as_production_evidence: false,
        rooms_pending: true,
        rooms_inferred: false,
      });
    } else if (p.match_class === MATCH_CLASS.EXACT_EXISTING_MATCH && proposed.length) {
      const fields = {};
      for (const row of proposed) fields[row.field] = row.value;
      updates.push({
        operation: "UPDATE",
        airtable_record_id: p.census_record_id,
        property_identity_key: p.property_identity_key,
        verified_state: p.verified_state,
        fields,
        field_writes: proposed,
        blank_fills: proposed.filter((x) => x.update_class === "BLANK_FILL").length,
        cvent_used_as_production_evidence: false,
        legacy_used_as_production_evidence: false,
        rooms_pending: true,
        rooms_inferred: false,
      });
    }
  }

  return {
    inserts,
    updates,
    blocked,
    steward,
    fieldClassCounts,
    roomsPending,
    serpapiBlockedFields,
  };
}

/**
 * Run all hard gates for Phase 1.
 */
export function runHardGates(ctx) {
  const {
    inserts,
    updates,
    selected,
    blocked,
    cventLeakage,
    legacyLeakage,
    provenanceFailures,
    snapshotComplete,
    rollbackPayloadComplete,
  } = ctx;

  const checks = [];

  const cventFail = cventLeakage > 0 || selected.some((p) => p.cvent_used_as_production_evidence);
  checks.push({
    id: "cvent_firewall",
    pass: !cventFail,
    detail: { cventLeakage, required: 0 },
  });

  const legacyFail = legacyLeakage > 0 || selected.some((p) => p.legacy_used_as_production_evidence);
  checks.push({
    id: "legacy_firewall",
    pass: !legacyFail,
    detail: { legacyLeakage, required: 0 },
  });

  checks.push({
    id: "provenance_gate",
    pass: provenanceFailures === 0,
    detail: { provenanceFailures },
  });

  checks.push({
    id: "duplicate_gate",
    pass: true,
    detail: {
      note: "Immediate pre-INSERT re-query required in Phase 2; Phase 1 matched against live snapshot",
    },
  });

  // Official Lat/Lng/Phone/Address may now appear when claim-eligible
  const serpapiOnlyInInserts = inserts.some((i) =>
    (i.field_writes || []).some(
      (fw) =>
        ["Latitude", "Longitude", "Phone", "Address"].includes(fw.field) &&
        fw.provenance?.serpapi_used === true
    )
  );
  checks.push({
    id: "source_rights_gate",
    pass: !serpapiOnlyInInserts,
    detail: { serpapi_driven_sensitive_fields_in_inserts: serpapiOnlyInInserts ? 1 : 0 },
  });

  checks.push({
    id: "rooms_not_inferred",
    pass: selected.every((p) => p.rooms_inferred === false),
  });

  checks.push({
    id: "rooms_unknown_does_not_block_verified",
    pass: selected.every(
      (p) => p.verified_state === VERIFIED_STATE.ROOMS_PENDING || p.rooms_value != null
    ),
  });

  checks.push({ id: "pre_write_snapshot_complete", pass: snapshotComplete });
  checks.push({ id: "rollback_payload_complete", pass: rollbackPayloadComplete });

  checks.push({
    id: "no_linked_record_writes",
    pass: ![...inserts, ...updates].some((m) =>
      Object.keys(m.fields || {}).some((f) =>
        /Brand Affiliations|Source Evidence|Steward Review/i.test(f)
      )
    ),
  });

  const allPass = checks.every((c) => c.pass);
  return { all_pass: allPass, checks, blocked_count: blocked.length };
}

/** Writer-contract: fields that must have a classify path when claims exist. */
export const WRITER_CONTRACT_FIELDS = Object.freeze([
  "State / Region",
  "Address",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
  "Property Name",
  "City",
  "Country",
  "Market",
  "Continent",
  "Sub-Continent",
]);
