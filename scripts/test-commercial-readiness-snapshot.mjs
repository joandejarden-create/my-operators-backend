/**
 * Acceptance test — Commercial Readiness Snapshot (InterContinental Cartagena sample).
 * Run: node scripts/test-commercial-readiness-snapshot.mjs
 */
import { buildCommercialReadinessSnapshot } from "../lib/commercial-readiness-snapshot-build.js";

const INTERCONTINENTAL_CARTAGENA_INPUTS = {
  hotelWebsiteUrl: "https://www.intercartagena.com/en",
  bookingComUrl: "https://www.booking.com/hotel/co/intercontinental-cartagena.en-gb.html",
  expediaUrl: "https://www.expedia.com/es/Cartagena-Hoteles-Intercontinental-Cartagena-De-Indias.h7985087.Informacion-Hotel",
  googleBusinessProfileUrl: "https://www.google.com/maps/search/InterContinental+Cartagena+de+Indias",
  currentBrandStatus: "Branded",
  currentOperatorStatus: "Brand-managed",
  estimatedOtaShare: "Moderate",
  estimatedDirectBookingShare: "Moderate",
  bookingEngineProvider: "IHG booking engine",
  crmGuestEmailCapture: "Yes",
  mainCommercialConcern: "Poor conversion",
  ownerGoal: "Improve direct bookings",
  estimatedOtaCommission: "18%",
  topSourceMarkets: "Colombia, United States, Mexico, Spain",
  primaryGuestSegments: "Leisure, Business, Couples, Families",
  additionalOwnerNotes:
    "Strong third-party visibility and review volume. Need to evaluate whether direct conversion friction, channel mix strategy, or positioning is the main commercial constraint before operator/brand strategy changes.",
};

function assert(condition, message) {
  if (!condition) {
    console.error("FAIL:", message);
    process.exitCode = 1;
    return false;
  }
  console.log("PASS:", message);
  return true;
}

function includesText(haystack, needle) {
  return String(haystack || "").toLowerCase().includes(String(needle).toLowerCase());
}

const result = buildCommercialReadinessSnapshot(INTERCONTINENTAL_CARTAGENA_INPUTS);
const s = result.snapshot;
const labels = result.labels;

console.log("\n=== Commercial Readiness Snapshot — InterContinental Cartagena ===\n");
console.log("Readiness Level:", labels.readinessLevel);
console.log("Evidence Confidence:", labels.confidence);
console.log("Performance Data Confidence:", labels.performanceDataConfidence);
console.log("OTA Risk:", labels.otaRisk);
console.log("Direct Capability:", labels.directCapability);
console.log("Strategic Diagnosis:", labels.strategicDiagnosis);
console.log("\n--- Executive Summary (first 400 chars) ---\n");
console.log((s.executiveCommercialInterpretation?.summary || "").slice(0, 400) + "...");
console.log("\n--- Recommended Path ---\n");
console.log(s.recommendedPath?.headline);
console.log("\n--- Word count (narrative) ---\n");
console.log("Narrative words:", result.narrative.split(/\s+/).length);

// Acceptance criteria
assert(labels.confidence !== "High", "Evidence confidence is not High without actual performance shares");
assert(labels.performanceDataConfidence === "Low" || labels.performanceDataConfidence === "Moderate", "Performance data confidence is Low or Moderate");
assert(labels.readinessLevel !== "Limited" || includesText(s.commercialReadinessLevel?.rationale, "infrastructure"), "If Limited, rationale explains why — expected Developing for IHG case");
assert(includesText(s.executiveCommercialInterpretation?.summary, "IHG") || includesText(s.executiveCommercialInterpretation?.summary, "brand"), "Recognizes IHG/brand infrastructure");
assert(includesText(s.directBookingCapability?.rationale, "conversion") || includesText(s.directBookingCapability?.label, "Developing"), "Recognizes conversion/infrastructure framing");
assert(includesText(labels.otaRisk, "moderate") || includesText(labels.otaRisk, "Potential"), "OTA risk is not only 'Not enough data'");
assert(includesText(s.strategicDiagnosis?.primaryDiagnosis, "conversion") || includesText(s.strategicDiagnosis?.primaryDiagnosis, "execution"), "Strategic diagnosis distinguishes conversion/execution from brand distribution");
assert(includesText(s.recommendedPath?.headline, "Direct") || includesText(s.recommendedPath?.headline, "Brand-System"), "Recommended path is specific to branded/conversion case");
assert(includesText(s.economicSensitivity?.rationale, "18%"), "Economic sensitivity mentions 18% commission without fake calculations");
assert((s.questionsToResolve || []).length >= 8, "At least 8 case-specific questions");
assert(!includesText(s.recommendedPath?.headline, "generic"), "Recommended path is not generic placeholder text");
assert(result.narrative.split(/\s+/).length >= 150, "Narrative has substantive depth (150+ words)");

const ihgQuestion = (s.questionsToResolve || []).some((q) => includesText(q, "IHG") || includesText(q, "direct channel"));
assert(ihgQuestion || (s.questionsToResolve || []).some((q) => includesText(q, "conversion")), "Questions include IHG/conversion-specific items");

console.log("\n=== Done ===\n");
if (process.exitCode) process.exit(process.exitCode);
