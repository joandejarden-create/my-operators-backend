import { initializeCanonical31CertificationFramework } from "../lib/ai-demand-positioning/governance/adp-canonical-31-certification-posture-v1.js";

function assert(cond, message) {
  if (!cond) throw new Error(message);
}

const live = initializeCanonical31CertificationFramework();
assert(live.ok === true, "framework initializes");
assert(live.identityCount === 31, "identity count");
assert(live.certifiedCount === 0, "membership is not certification");
assert(live.hotels.some((row) => row.propertyId === "adp_bethesda_marriott"), "bethesda identity");
assert(live.hotels.every((row) => row.certificationDecision === "NOT_VERIFIED"), "no inferred certification");
assert(live.hotels.every((row) => row.founderApproval === "UNKNOWN"), "founder approval stays unknown");
assert(live.hotels.every((row) => row.publishedStatus === "UNKNOWN"), "publication stays unknown");

const fresh = initializeCanonical31CertificationFramework({ readMatrix: () => null });
assert(fresh.privateMatrixPresent === false, "fresh clone has no private matrix");
assert(fresh.identityCount === 31, "fresh identity count");
assert(fresh.hotels.every((row) => row.measurementAvailability === "UNKNOWN"), "unknown measurements");
assert(fresh.certifiedCount === 0, "fresh clone certifies nothing");

console.log(JSON.stringify({
  ok: true,
  identityCount: live.identityCount,
  certifiedCount: live.certifiedCount,
  privateMatrixPresent: live.privateMatrixPresent,
  freshMeasurementsUnknown: true,
}));
