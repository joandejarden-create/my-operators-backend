/**
 * Classify HE 5.0 pilot mutations under standing conditional authorization.
 * Does not write Airtable. READY label alone is insufficient.
 */

import {
  APPROVAL_CLASS,
  HE50_STANDING_APPLY_GATE,
  isLegacyHotelCensusDestination,
  resolveDestinationMeta,
  HE50_PILOT_HOTEL_ALLOWLIST,
} from "./standing-authorization-v1.js";
import { HE50_HPC_TABLE_ID, HE50_BETHESDA_HPC_ID } from "./contract.js";

export const HE50_CONDITIONAL_APPROVAL_V1 = "HE50_CONDITIONAL_APPROVAL_V1";

const VERIFIED_STATUSES = new Set([
  "VERIFIED_FREE",
  "VERIFIED_FREE_AUTONOMOUS",
  "MIN_IDENTITY_FREE_VERIFIED",
  "MATCHED_PLUS_FREE_CORROBORATION",
  "SOURCE_CONFLICT_DOCUMENTED", // dual reno — evidence CREATE ok, not single overwrite
  "EVIDENCE_ONLY",
]);

function pilotKeyFromMutation(m) {
  const id = String(m.id || "");
  if (id.includes("ac_hotel")) return "ac_hotel_newtown_square";
  if (id.includes("carton")) return "carton_house_fairmont";
  if (id.includes("hgi_")) return "hgi_newtown_square_radnor";
  if (id.includes("papagayo")) return "secrets_papagayo";
  if (id.includes("st_lucia")) return "secrets_st_lucia";
  if (id.includes("cap_cana")) return "secrets_cap_cana";
  if (id.includes("jw_marriott") || id.includes("costa_elena")) return "jw_marriott_costa_elena";
  if (id.includes("BETH") || m.canonicalEntityId === HE50_BETHESDA_HPC_ID) {
    return "bethesda_marriott";
  }
  return null;
}

function inPilotScope(m) {
  const key = pilotKeyFromMutation(m);
  if (key && HE50_PILOT_HOTEL_ALLOWLIST.includes(key)) return true;
  const cid = String(m.canonicalEntityId || "");
  if (cid && HE50_PILOT_HOTEL_ALLOWLIST.includes(cid)) return true;
  return false;
}

/**
 * Map dry-run mutation into an executable plan (or quarantine reason).
 */
export function classifyPilotMutation(mutation, opts = {}) {
  const m = mutation || {};
  const reasons = [];
  const plan = {
    mutationId: m.id,
    action: m.action,
    destinationTable: m.destinationTable,
    field: m.field,
    canonicalEntityId: m.canonicalEntityId || null,
    pilotKey: pilotKeyFromMutation(m),
    approvalClass: null,
    executable: false,
    operations: [],
    quarantineReason: null,
  };

  if (!HE50_STANDING_APPLY_GATE.standingAuthorization && !opts.forceStandingAuth) {
    plan.approvalClass = APPROVAL_CLASS.FOUNDER_APPROVAL_REQUIRED;
    plan.quarantineReason = "standing_authorization_disabled";
    return plan;
  }

  if (isLegacyHotelCensusDestination(m.destinationTable)) {
    plan.approvalClass = APPROVAL_CLASS.BLOCKED_OUT_OF_SCOPE;
    plan.quarantineReason = "legacy_hotel_census_destination";
    return plan;
  }

  if (!inPilotScope(m)) {
    plan.approvalClass = APPROVAL_CLASS.BLOCKED_OUT_OF_SCOPE;
    plan.quarantineReason = "outside_eight_hotel_pilot";
    return plan;
  }

  if (m.action === "NO_OP" || m.action === "REFERENCE_PRIOR_DRY_RUN") {
    plan.approvalClass = APPROVAL_CLASS.SKIPPED_NO_CHANGE;
    plan.executable = false;
    reasons.push(m.action);
    plan.quarantineReason = reasons.join(";");
    return plan;
  }

  if (
    m.proposedValue &&
    typeof m.proposedValue === "object" &&
    m.proposedValue.mullenAssociation
  ) {
    const assoc = String(m.proposedValue.mullenAssociation);
    // Block only verified/owned promotions — proposed/not-verified holdings are allowed on CREATE payload (not written as edges).
    if (
      /OWNED_BY/i.test(assoc) ||
      (/VERIFIED_HOLDING/i.test(assoc) && !/NOT_VERIFIED/i.test(assoc))
    ) {
      plan.approvalClass = APPROVAL_CLASS.FOUNDER_APPROVAL_REQUIRED;
      plan.quarantineReason = "would_promote_proposed_mullen_holding";
      return plan;
    }
  }

  const dest = resolveDestinationMeta(m.destinationTable);
  if (!dest) {
    // Composite labels like "HI Organizations + …"
    if (/HI Organizations|Relationships/i.test(String(m.destinationTable || ""))) {
      plan.approvalClass = APPROVAL_CLASS.SKIPPED_NO_CHANGE;
      plan.quarantineReason = "prior_dry_run_reference_not_auto_applied";
      return plan;
    }
    plan.approvalClass = APPROVAL_CLASS.BLOCKED_OUT_OF_SCOPE;
    plan.quarantineReason = "destination_not_in_allowlist";
    return plan;
  }

  const vs = String(m.verificationStatus || "");
  if (!VERIFIED_STATUSES.has(vs) && m.founderRecommendation !== "READY") {
    plan.approvalClass = APPROVAL_CLASS.QUARANTINED_INSUFFICIENT_EVIDENCE;
    plan.quarantineReason = `verification_status:${vs || "missing"}`;
    return plan;
  }

  // --- HPC UPDATE single field ---
  if (m.action === "UPDATE" && dest.tableId === HE50_HPC_TABLE_ID) {
    if (m.field === "Year Built") {
      // Schema has Opening Date, not Year Built — map construction year → Opening Date null-fill
      plan.operations.push({
        kind: "HPC_NULL_FILL",
        recordId: m.canonicalEntityId,
        patch: {
          "Opening Date": `${Number(m.proposedValue)}-01-01`,
          "Opening Date Source URL":
            "https://cmbs.news/property/bethesda-marriott-bethesda-md/",
        },
        mappedFrom: "Year Built",
        evidenceNote: `Construction year ${m.proposedValue} mapped to Opening Date (Year Built field absent on HPC)`,
      });
      plan.approvalClass = APPROVAL_CLASS.AUTO_APPROVED_CANONICAL_UPDATE;
      plan.executable = true;
      return plan;
    }

    if (m.field === "Rooms / Keys" && Number.isFinite(Number(m.proposedValue))) {
      plan.operations.push({
        kind: "HPC_NULL_FILL",
        recordId: m.canonicalEntityId,
        patch: {
          "Rooms / Keys": Number(m.proposedValue),
          ...(m.roomsConfidence
            ? { "Rooms Confidence": titleCaseConfidence(m.roomsConfidence) }
            : {}),
          ...(String(m.supportingSource || "").match(/https?:\/\/\S+/)
            ? {
                "Rooms Source URL": String(m.supportingSource).match(/https?:\/\/\S+/)[0],
              }
            : {}),
        },
      });
      plan.approvalClass = APPROVAL_CLASS.AUTO_APPROVED_CANONICAL_UPDATE;
      plan.executable = true;
      return plan;
    }

    if (m.field === "alias_rooms_identity" && m.proposedValue?.rooms != null) {
      const patch = {
        "Rooms / Keys": Number(m.proposedValue.rooms),
      };
      if (m.proposedValue.website) {
        patch["Official Property URL"] = m.proposedValue.website;
      }
      if (m.proposedValue.officialName) {
        patch["Canonical Property Name"] = m.proposedValue.officialName;
      }
      plan.operations.push({
        kind: "HPC_NULL_FILL",
        recordId: m.canonicalEntityId,
        patch,
        note: "Aliases omitted — no HPC former-name multi-field auto-write in this wave",
      });
      plan.approvalClass = APPROVAL_CLASS.AUTO_APPROVED_CANONICAL_UPDATE;
      plan.executable = true;
      return plan;
    }

    plan.approvalClass = APPROVAL_CLASS.QUARANTINED_INSUFFICIENT_EVIDENCE;
    plan.quarantineReason = `unmapped_hpc_update_field:${m.field}`;
    return plan;
  }

  // --- HPC CREATE ---
  if (m.action === "CREATE" && dest.tableId === HE50_HPC_TABLE_ID) {
    const pv = m.proposedValue || {};
    if (!pv.name || !pv.country) {
      plan.approvalClass = APPROVAL_CLASS.QUARANTINED_INSUFFICIENT_EVIDENCE;
      plan.quarantineReason = "create_missing_name_or_country";
      return plan;
    }
    const family = inferFamilySourceFamily(pv.brand);
    const fields = {
      "Property Name": pv.name,
      "Canonical Property Name": pv.name,
      "Current Brand": pv.brand || null,
      Country: pv.country,
      City: pv.city || null,
      Address: pv.address || null,
      "Official Property URL": pv.website || null,
      Phone: pv.phone || null,
      "Rooms / Keys": pv.rooms ?? null,
      ...(pv.roomsConfidence
        ? { "Rooms Confidence": titleCaseConfidence(pv.roomsConfidence) }
        : {}),
      "Radar Status": pv.status === "Pipeline" ? "Pipeline" : "Open",
      "Affiliation Status": "Branded",
      "Property Type": inferPropertyType(pv),
      "Identity Confidence": "High",
      "Production Use Status": "Census Only / Not Owner-Facing",
      "Enrichment Status": "Discovered — pending enrichment",
      "Source URL": extractFirstUrl(m.supportingSource),
      "Source Type": "official_property_page",
      "Source Confidence": "High",
    };
    if (family) fields["Family / Source Family"] = family;
    // Never persist mullenAssociation as ownership
    plan.operations.push({
      kind: "HPC_CREATE",
      fields: stripNulls(fields),
      collisionName: pv.name,
      aliases: [],
      city: pv.city,
      country: pv.country,
      brand: pv.brand,
      legacyProvenanceId: pv.legacyHotelCensusProvenanceId || null,
      idempotencyKey: `he50-create:${plan.pilotKey}:${pv.name}`,
    });
    plan.approvalClass = APPROVAL_CLASS.AUTO_APPROVED_CANONICAL_CREATE;
    plan.executable = true;
    return plan;
  }

  // --- HI Graph Evidence CREATE ---
  if (
    m.action === "CREATE" &&
    (dest.tableId === "tblc9JhKT1edr4grE" || /Graph Evidence/i.test(dest.tableName))
  ) {
    const hpcId = m.canonicalEntityId;
    if (!hpcId) {
      plan.approvalClass = APPROVAL_CLASS.QUARANTINED_INSUFFICIENT_EVIDENCE;
      plan.quarantineReason = "evidence_missing_hpc_id";
      return plan;
    }
    const url = extractFirstUrl(m.supportingSource) || "https://dealality.local/evidence";
    plan.operations.push({
      kind: "HI_GRAPH_EVIDENCE_CREATE",
      fields: {
        "Evidence ID": `he50:${m.id}:${hpcId}`,
        "HPC Hotel ID": hpcId,
        "Entity Type": "Other",
        "Entity Record ID": hpcId,
        "Field Name": m.field,
        "Value Observed":
          typeof m.proposedValue === "string"
            ? m.proposedValue
            : JSON.stringify(m.proposedValue, null, 2),
        "Source Name": "HE 5.0 free research",
        "Source Type": "public_web",
        "Source URL": url.startsWith("http") ? url : undefined,
        "Retrieved At": new Date().toISOString(),
        Confidence: "HIGH",
        "Current?": true,
        Notes: m.reason || "",
        "Schema Version": HE50_CONDITIONAL_APPROVAL_V1,
        "Hotel Property Census": [hpcId],
      },
      dedupeByEvidenceId: true,
    });
    plan.approvalClass = APPROVAL_CLASS.AUTO_APPROVED_CANONICAL_LINK;
    plan.executable = true;
    return plan;
  }

  plan.approvalClass = APPROVAL_CLASS.FOUNDER_APPROVAL_REQUIRED;
  plan.quarantineReason = "unclassified_mutation_shape";
  return plan;
}

function titleCaseConfidence(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  if (lower === "high") return "High";
  if (lower === "medium") return "Medium";
  if (lower === "low") return "Low";
  if (lower === "exact") return "Exact";
  return s;
}

function inferFamilySourceFamily(brand) {
  const b = String(brand || "").toLowerCase();
  if (b.includes("marriott") || b.includes("ac hotel") || b.includes("jw ")) return "Marriott";
  if (b.includes("hilton")) return "Hilton";
  if (b.includes("fairmont") || b.includes("accor")) return "Accor";
  if (b.includes("ihg") || b.includes("holiday")) return "IHG";
  if (b.includes("choice") || b.includes("ascend")) return "Choice";
  if (b.includes("wyndham")) return "Wyndham";
  // Hyatt / Secrets — no Family select option; omit rather than invent
  return null;
}

function inferPropertyType(pv) {
  const n = `${pv.name || ""} ${pv.brand || ""}`.toLowerCase();
  if (n.includes("all-inclusive") || n.includes("secrets") || n.includes("resort")) {
    return n.includes("secrets") || n.includes("all-inclusive") ? "All-Inclusive" : "Resort";
  }
  return "Hotel";
}

function extractFirstUrl(src) {
  const m = String(src || "").match(/https?:\/\/[^\s|]+/);
  return m ? m[0] : null;
}

function stripNulls(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (v == null || v === "") continue;
    out[k] = v;
  }
  return out;
}

export function classifyPilotMutationPackage(mutations = []) {
  const classified = (mutations || []).map((m) => classifyPilotMutation(m));
  const summary = {
    total: classified.length,
    executable: classified.filter((c) => c.executable).length,
    byClass: {},
  };
  for (const c of classified) {
    summary.byClass[c.approvalClass] = (summary.byClass[c.approvalClass] || 0) + 1;
  }
  return { contract: HE50_CONDITIONAL_APPROVAL_V1, summary, classified };
}
