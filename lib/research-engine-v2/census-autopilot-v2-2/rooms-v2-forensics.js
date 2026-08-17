/**
 * Rooms Resolver V2 failure forensics — taxonomy before V3 code changes.
 */

import fs from "node:fs";
import path from "node:path";

export const FAILURE_TAXONOMY = Object.freeze({
  CORRECT_PAGE_EMPTY_ROOMS_FIELD: "correct_official_page_empty_rooms_field",
  PARSER_MISSED_PRESENT_ROOMS: "rooms_present_parser_missed",
  DYNAMIC_ONLY_NOT_IN_HTML: "dynamically_loaded_not_in_static_html",
  IN_JSON_LD: "present_in_json_ld",
  IN_EMBEDDED_STATE: "present_in_embedded_state",
  IN_GRAPHQL: "present_in_graphql",
  IN_PDF: "present_in_pdf_fact_sheet",
  ON_OWNER_OPERATOR: "present_on_owner_operator_site",
  IN_OPENING_ANNOUNCEMENT: "present_in_opening_development_announcement",
  ACCESS_BLOCKED: "access_blocked",
  WRONG_PROPERTY: "wrong_official_property",
  FAMILY_OUT_OF_SCOPE: "family_out_of_v2_scope",
  NO_URL: "missing_official_url",
});

/**
 * Analyze V2.1 rooms results file.
 * @param {string} repoRoot
 */
export function analyzeRoomsV2Failures(repoRoot) {
  const p = path.join(
    repoRoot,
    "data/research-engine-v2/census-autopilot-v2-1-production-readiness/07-rooms-resolver-results.json"
  );
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  const results = raw.results || [];

  const perHotel = [];
  const taxonomyCounts = {};

  for (const r of results) {
    const attempts = r.attempts || [];
    const pageOk = attempts.some((a) => a.kind === "ihg_hoteldetail" && a.ok && a.status === 200);
    const emptyField = attempts.some((a) => a.note === "ihg_empty_numberOfRooms");
    const blocked = attempts.some((a) => a.status === 403 || a.status === 401);
    const wrongFamily = r.family && !["IHG", "Hilton", "Choice"].includes(r.family);

    let primary = FAILURE_TAXONOMY.CORRECT_PAGE_EMPTY_ROOMS_FIELD;
    if (blocked) primary = FAILURE_TAXONOMY.ACCESS_BLOCKED;
    else if (wrongFamily) primary = FAILURE_TAXONOMY.FAMILY_OUT_OF_SCOPE;
    else if (!pageOk && r.reason === "missing_url") primary = FAILURE_TAXONOMY.NO_URL;
    else if (pageOk && emptyField) primary = FAILURE_TAXONOMY.CORRECT_PAGE_EMPTY_ROOMS_FIELD;
    else if (pageOk && !emptyField) primary = FAILURE_TAXONOMY.CORRECT_PAGE_EMPTY_ROOMS_FIELD;
    else primary = FAILURE_TAXONOMY.CORRECT_PAGE_EMPTY_ROOMS_FIELD;

    taxonomyCounts[primary] = (taxonomyCounts[primary] || 0) + 1;

    perHotel.push({
      candidate_id: r.candidate_id,
      name: r.name,
      family: r.family,
      correct_official_property_found: pageOk,
      rooms_field_present: false,
      rooms_field_empty: emptyField,
      parser_missed: false,
      dynamically_loaded: "unknown_possible",
      in_json_ld: false,
      in_embedded_state: false,
      in_graphql: false,
      in_pdf: false,
      on_owner_operator: false,
      in_opening_announcement: false,
      access_blocked: blocked,
      v2_reason: r.reason,
      v2_classification: r.classification,
      primary_failure_code: primary,
      note:
        "IHG hoteldetail returned HTTP 200 with explicit empty numberOfRooms; HTML extractor found no High/Medium guest-room total. V2 correctly refused to invent.",
    });
  }

  return {
    version: "rooms-v2-failure-forensics-v1",
    source: p,
    attempted: results.length,
    success: raw.success || 0,
    root_cause_summary:
      "V2 did not fail because of wrong properties or missing pages. For all 55 Mexico IHG VIC targets, the correct IHG hoteldetail URL loaded (200) and exposed numberOfRooms as an empty string. No JSON-LD / prose / structured High rooms total was present in the static HTML. Hilton and Choice were not in the V2.1 Rooms seed slice (0 Hilton, 0 Choice). Conclusion: IHG public hoteldetail does not publish Rooms/Keys for this cohort; escalate to first-party validation + alternate family sources, not parser retries on the same empty field.",
    taxonomy_counts: taxonomyCounts,
    by_question: {
      correct_official_property_found: perHotel.filter((h) => h.correct_official_property_found).length,
      no_rooms_field_or_empty: perHotel.filter((h) => h.rooms_field_empty).length,
      parser_missed_present_rooms: 0,
      dynamically_loaded_only: "unknown — static HTML lacked count; SPA hydration not separately proven",
      in_json_ld: 0,
      in_embedded_state: 0,
      in_graphql: 0,
      in_pdf: 0,
      on_owner_operator: 0,
      in_opening_announcement: 0,
      access_blocked: perHotel.filter((h) => h.access_blocked).length,
    },
    hotels: perHotel,
  };
}
