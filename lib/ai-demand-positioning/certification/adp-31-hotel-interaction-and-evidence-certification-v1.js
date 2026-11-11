/**
 * ADP_31_HOTEL_INTERACTION_AND_EVIDENCE_CERTIFICATION_V1
 *
 * Click-level evidence / modal / loading / close certification.
 * Structural page-load PASS does not satisfy this gate.
 */

export const ADP_31_HOTEL_INTERACTION_AND_EVIDENCE_CERTIFICATION_V1 =
  "ADP_31_HOTEL_INTERACTION_AND_EVIDENCE_CERTIFICATION_V1";

export const INTERACTION_GATE_KEYS = Object.freeze([
  "LINK_DISCOVERY_PASS",
  "ACTION_EXECUTION_PASS",
  "EVIDENCE_REQUEST_PASS",
  "EVIDENCE_PROVENANCE_PASS",
  "EVIDENCE_CONTENT_PASS",
  "MODAL_STRUCTURE_PASS",
  "LOADING_COMPONENT_PASS",
  "CLOSE_CONTROL_PASS",
  "KEYBOARD_INTERACTION_PASS",
  "HOTEL_SWITCH_SAFETY_PASS",
  "BROWSER_VISUAL_PASS",
]);

export function emptyInteractionGates(status = "NOT_VERIFIED") {
  return Object.fromEntries(INTERACTION_GATE_KEYS.map((k) => [k, status]));
}

export function summarizeHotelInteractionRow(row) {
  const gates = row.gates || emptyInteractionGates();
  const hard = [
    "LINK_DISCOVERY_PASS",
    "ACTION_EXECUTION_PASS",
    "EVIDENCE_REQUEST_PASS",
    "EVIDENCE_CONTENT_PASS",
    "MODAL_STRUCTURE_PASS",
    "CLOSE_CONTROL_PASS",
  ];
  const fail = hard.some((k) => gates[k] === "FAIL");
  return {
    ...row,
    hotelPass: fail ? "FAIL" : gates.LINK_DISCOVERY_PASS === "PASS" ? "PASS" : "PARTIAL",
  };
}
