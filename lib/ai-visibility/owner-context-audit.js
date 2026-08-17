/**
 * Owner AI Recommendation Intelligence — deal/asset context field audit (read-only).
 * Does not modify schema. Documents readiness for future owner recommendation patterns.
 */

import {
  LOCATION_FORM_TO_AIRTABLE,
  DEALS_ONLY_FORM_FIELDS,
} from "../../api/schemas/deal-setup-fields.js";
import { MIXED_USE_INTAKE_FIELD_NAMES } from "../mixed-use-intake-field-options.js";

export const OWNER_CONTEXT_AUDIT_VERSION = "ai_visibility_owner_context_audit_v1";

/**
 * Desired analytical context for "what does AI recommend for an asset like this?"
 * Mapped against existing Deal Setup / Location & Property fields where known.
 */
export const OWNER_AI_RECOMMENDATION_CONTEXT_FIELDS = Object.freeze([
  {
    key: "country",
    status: "available",
    source: "Location & Property",
    airtableField: LOCATION_FORM_TO_AIRTABLE["Country"] || "Country",
  },
  {
    key: "market",
    status: "partial",
    source: "Location & Property",
    airtableField: "Primary Market Region / Hotel Submarket & Location",
    note: "Dealality market/submarket labels exist; confirm product Market vs corridor Submarket usage for owner AI.",
  },
  {
    key: "chain_scale_positioning",
    status: "available",
    source: "Location & Property",
    airtableField: "Hotel Chain Scale",
  },
  {
    key: "key_count",
    status: "available",
    source: "Location & Property",
    airtableField: "Total Number of Rooms/Keys",
  },
  {
    key: "development_type",
    status: "available",
    source: "Deals",
    airtableField: "Project Type / Stage of Development",
    note: "Project Type on Deals; confirm conversion vs new-build encoding.",
  },
  {
    key: "conversion_or_new_build",
    status: "partial",
    source: "Deals / Location",
    airtableField: "Project Type + conversion-related fields",
    note: "Not a single canonical boolean; inferable from Project Type / PIP / Existing MEP fields.",
  },
  {
    key: "resort_or_urban",
    status: "partial",
    source: "Location & Property",
    airtableField: "Micro-Location Type / Hotel Type",
    note: "Micro-Location Type and Hotel Type exist; no dedicated resort/urban enum guaranteed.",
  },
  {
    key: "branded_residences",
    status: "partial",
    source: "Deals",
    airtableField: "Condo Residences?",
    note: "Condo Residences? on Deals; branded-residences-specific flag may need confirmation.",
  },
  {
    key: "mixed_use",
    status: "available",
    source: "Deals / Market-Performance",
    airtableField: MIXED_USE_INTAKE_FIELD_NAMES.numberOfCondoUnits
      ? "Mixed-use intake fields"
      : "Mixed-use intake",
    note: "Mixed-use intake module present.",
  },
  {
    key: "target_positioning",
    status: "partial",
    source: "Strategic Intent / Market-Performance",
    airtableField: "Preferred Chain Scales / strategic intent fields",
    note: "Preferred Chain Scales and strategic intent fields exist; target positioning may be multi-field.",
  },
]);

/**
 * @returns {{ OWNER_AI_RECOMMENDATION_CONTEXT_READY: string, AVAILABLE_FIELDS: string[], MISSING_FIELDS: string[], PARTIAL_FIELDS: string[], details: object[] }}
 */
export function auditOwnerAiRecommendationContext() {
  const available = [];
  const missing = [];
  const partial = [];
  for (const row of OWNER_AI_RECOMMENDATION_CONTEXT_FIELDS) {
    if (row.status === "available") available.push(row.key);
    else if (row.status === "missing") missing.push(row.key);
    else if (row.status === "partial") partial.push(row.key);
  }

  // Ready enough to prototype owner pattern reads if partial fields are handled explicitly.
  const ready = missing.length === 0 ? "PARTIAL" : "NO";

  return {
    OWNER_AI_RECOMMENDATION_CONTEXT_READY: ready,
    AVAILABLE_FIELDS: available,
    PARTIAL_FIELDS: partial,
    MISSING_FIELDS: missing,
    details: OWNER_AI_RECOMMENDATION_CONTEXT_FIELDS,
    ownerProductName: "AI Recommendation Intelligence",
    layersPreserved: [
      "AI Recommendation Pattern",
      "Dealality Analysis",
      "Owner Process",
    ],
    dealsOnlyFormFieldCount: DEALS_ONLY_FORM_FIELDS?.size || null,
    auditVersion: OWNER_CONTEXT_AUDIT_VERSION,
    schemaChangesProposed: 0,
  };
}
