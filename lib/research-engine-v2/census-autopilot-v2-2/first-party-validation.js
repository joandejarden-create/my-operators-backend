/**
 * First-party validation model + ingestion design (data model only).
 */

export const FIRST_PARTY_SOURCE_TYPE = "FIRST_PARTY_VALIDATION";

export const VALIDATION_FIELDS = Object.freeze([
  "Hotel Name",
  "Brand",
  "Property ID",
  "Rooms / Keys",
  "Operating Status",
  "Opening Date",
  "Operator / Management Company",
  "Address",
]);

/**
 * Build validation targets from wave + rooms outcomes.
 */
export function buildFirstPartyValidationTargets(cohort, waveResults, roomsResults) {
  const roomsById = new Map((roomsResults || []).map((r) => [r.candidate_id, r]));
  const byOrg = new Map();

  const packages = [];
  for (const h of cohort) {
    const wr = (waveResults || []).find((r) => r.candidate_id === h.candidate_id);
    const rr = roomsById.get(h.candidate_id);
    const gaps = [];
    if (!rr?.ok) gaps.push("Rooms / Keys");
    if (!wr?.best_snapshot?.address && !h.city) gaps.push("Address");
    if (!h.property_ids?.length && !wr?.official_property_id) gaps.push("Property ID");
    if (!wr?.operator && !wr?.management_company) gaps.push("Operator / Management Company");
    // Opening date / status almost always unknown in this wave
    gaps.push("Operating Status");
    if (!h.strata?.existing_vic) gaps.push("Opening Date");

    const needsValidation =
      rr?.classification === "FIRST-PARTY VALIDATION" ||
      gaps.includes("Rooms / Keys") ||
      (wr && String(wr.confirmation || "").includes("INDEPENDENTLY") && !rr?.ok);

    if (!needsValidation && gaps.length < 3) continue;

    const org = h.family && h.family !== "Independent" ? h.family : h.brand || "Independent portfolio";
    if (!byOrg.has(org)) {
      byOrg.set(org, {
        organization: org,
        properties_requiring_validation: 0,
        rooms_gaps: 0,
        status_gaps: 0,
        operator_gaps: 0,
        opening_date_gaps: 0,
        property_id_gaps: 0,
      });
    }
    const agg = byOrg.get(org);
    agg.properties_requiring_validation += 1;
    if (gaps.includes("Rooms / Keys")) agg.rooms_gaps += 1;
    if (gaps.includes("Operating Status")) agg.status_gaps += 1;
    if (gaps.includes("Operator / Management Company")) agg.operator_gaps += 1;
    if (gaps.includes("Opening Date")) agg.opening_date_gaps += 1;
    if (gaps.includes("Property ID")) agg.property_id_gaps += 1;

    packages.push({
      property_identity_id: h.property_identity_id,
      candidate_id: h.candidate_id,
      organization: org,
      fields_to_confirm: {
        "Hotel Name": h.name,
        Brand: h.brand || h.family || null,
        "Property ID": h.property_ids?.[0] || wr?.official_property_id || null,
        "Rooms / Keys": rr?.rooms_value ?? null,
        "Operating Status": null,
        "Opening Date": null,
        "Operator / Management Company": null,
        Address: wr?.best_snapshot?.address || null,
      },
      gaps,
      exclude_provenance: ["cvent", "legacy"],
      message_concept:
        "Dealality independently maintains the following information for your portfolio. Please confirm or correct these fields.",
    });
  }

  return {
    version: "first-party-validation-targets-v2.2",
    field_list: VALIDATION_FIELDS,
    organizations: [...byOrg.values()].sort(
      (a, b) => b.properties_requiring_validation - a.properties_requiring_validation
    ),
    packages: packages.slice(0, 500),
    package_count: packages.length,
    pct_of_wave: Math.round((100 * packages.length) / Math.max(1, cohort.length)),
  };
}

export function firstPartyValidationModelMd() {
  return `# First-Party Validation Model (V2.2)

## Purpose
Unresolved Rooms (and selected hard fields) are **not** Autopilot failures. They become an operational validation layer during brand/operator onboarding.

## Package concept
> Dealality independently maintains the following information for your portfolio. Please confirm or correct these fields.

Fields: ${VALIDATION_FIELDS.join(", ")}

**Never include** Cvent or legacy provenance in validation packages.

## Ingestion contract
Every confirmed value carries:

| Field | Rule |
|-------|------|
| \`source_type\` | \`${FIRST_PARTY_SOURCE_TYPE}\` |
| \`organization\` | Brand or management company |
| \`respondent\` | Optional contact |
| \`confirmation_date\` | ISO date |
| \`field\` | Canonical Census field |
| \`confirmed_value\` | Value as confirmed |
| \`prior_value\` | Prior Dealality value or null |
| \`change_type\` | confirm \\| correct \\| supply_new |
| \`evidence_reference\` | Ticket/email/upload id |
| \`confidence\` | **HIGH** |

First-party confirmation is among the strongest Census source classes — stronger than SerpApi staging and secondary web.

## Write class
Maps to \`FIRST_PARTY_VALIDATION\` / Class D preferred fields (Rooms, Operator, Opening Date).

## UI
External onboarding UI is **out of scope** for V2.2 unless trivial. This artifact is the data model + target generation only.
`;
}

/**
 * @param {object} confirmation inbound payload shape
 */
export function validateFirstPartyIngestion(confirmation) {
  const required = [
    "source_type",
    "organization",
    "confirmation_date",
    "field",
    "confirmed_value",
  ];
  const missing = required.filter((k) => confirmation?.[k] == null || confirmation[k] === "");
  const ok =
    missing.length === 0 && confirmation.source_type === FIRST_PARTY_SOURCE_TYPE;
  return {
    ok,
    missing,
    sanitized: ok
      ? {
          ...confirmation,
          confidence: "HIGH",
          change_type: confirmation.change_type || "confirm",
        }
      : null,
    error_path: ok ? null : "validation_error",
  };
}
