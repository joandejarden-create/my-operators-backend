import { buildCommercialReadinessSnapshot } from "../lib/commercial-readiness-snapshot-build.js";
import { enrichCommercialReadinessSnapshot } from "../lib/commercial-readiness-snapshot-enrich.js";

const INPUTS = {
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
    "Strong third-party visibility and review volume. Need to evaluate conversion friction vs channel strategy.",
};

function assert(condition, message) {
  if (!condition) {
    console.error("FAIL:", message);
    process.exitCode = 1;
    return;
  }
  console.log("PASS:", message);
}

function eq(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

console.log("\n=== Commercial Readiness Enrichment Tests ===\n");

const deterministic = buildCommercialReadinessSnapshot(INPUTS);

const noEnrich = await enrichCommercialReadinessSnapshot({
  deterministicResult: deterministic,
  inputs: INPUTS,
  mode: "standalone",
  enrichNarrative: false,
});
assert(noEnrich.snapshot?.enrichment?.fallbackUsed === true, "Fallback used when enrichment is not requested");
assert(eq(noEnrich.labels, deterministic.labels), "Labels unchanged when enrichment not requested");

// Simulate requested enrichment with provider unavailable (no OPENAI key).
const prevEnabled = process.env.COMMERCIAL_READINESS_LLM_ENRICHMENT_ENABLED;
const prevKey = process.env.OPENAI_API_KEY;
process.env.COMMERCIAL_READINESS_LLM_ENRICHMENT_ENABLED = "1";
delete process.env.OPENAI_API_KEY;

const enrichRequestedNoProvider = await enrichCommercialReadinessSnapshot({
  deterministicResult: deterministic,
  inputs: INPUTS,
  mode: "standalone",
  enrichNarrative: true,
});
assert(enrichRequestedNoProvider.snapshot?.enrichment?.fallbackUsed === true, "Fallback used when provider is unavailable");
assert(eq(enrichRequestedNoProvider.labels, deterministic.labels), "Deterministic labels remain unchanged after fallback");
assert(
  String(enrichRequestedNoProvider.snapshot?.snapshotBasis?.note || "").toLowerCase().includes("url-level content was not programmatically analyzed"),
  "Snapshot still includes URL extraction limitation"
);

assert(
  deterministic.labels.readinessLevel === "Developing" &&
    deterministic.labels.otaRisk === "Potential moderate risk indicated" &&
    deterministic.labels.directCapability === "Developing — infrastructure present, conversion unconfirmed" &&
    deterministic.labels.strategicDiagnosis === "Direct conversion / commercial execution gap" &&
    deterministic.snapshot?.recommendedPath?.headline ===
      "Direct Conversion and Brand-System Utilization Review Before Any Brand/Operator Strategy Change",
  "InterContinental deterministic labels and path remain expected"
);

process.env.COMMERCIAL_READINESS_LLM_ENRICHMENT_ENABLED = prevEnabled;
process.env.OPENAI_API_KEY = prevKey;

console.log("\n=== Done ===\n");
if (process.exitCode) process.exit(process.exitCode);

