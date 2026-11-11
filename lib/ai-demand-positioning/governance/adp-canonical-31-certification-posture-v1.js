/**
 * Canonical 31 certification posture.
 * Identity membership is not certification, measurement, founder approval, or publication.
 * Missing local evidence stays UNKNOWN / NOT_VERIFIED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listCanonicalAdp31PropertyIds } from "./adp-operational-registry-hygiene-v1.js";

export const ADP_CANONICAL_31_CERTIFICATION_POSTURE_V1 =
  "ADP_CANONICAL_31_CERTIFICATION_POSTURE_V1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const CANONICAL_31_MATRIX_RELATIVE_PATH =
  "reports/dealality-adp-core-loyalty-execution-v1/FINAL_31_HOTEL_CERTIFICATION_MATRIX.json";

function readDefaultMatrix() {
  const filePath = path.join(ROOT, CANONICAL_31_MATRIX_RELATIVE_PATH);
  if (!fs.existsSync(filePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function recorded(row, key) {
  if (!row || row[key] == null || String(row[key]).trim() === "") return "UNKNOWN";
  return String(row[key]);
}

/**
 * Initialize the 31-hotel framework without granting certification.
 * @param {{ readMatrix?: () => object|null }} [opts]
 */
export function initializeCanonical31CertificationFramework(opts = {}) {
  const ids = listCanonicalAdp31PropertyIds();
  const matrix = typeof opts.readMatrix === "function" ? opts.readMatrix() : readDefaultMatrix();
  const byId = new Map();
  for (const row of matrix?.matrix || []) {
    const propertyId = String(row?.propertyId || "").trim();
    if (propertyId) byId.set(propertyId, row);
  }
  const hotels = ids.map((propertyId) => {
    const row = byId.get(propertyId) || null;
    return {
      propertyId,
      canonicalIdentity: true,
      measurementAvailability: row ? "RECORDED_LOCALLY" : "UNKNOWN",
      recordedCoreStatus: recorded(row, "coreStatus"),
      recordedLoyaltyStatus: recorded(row, "loyaltyStatus"),
      founderApproval: "UNKNOWN",
      publishedStatus: "UNKNOWN",
      administrativeInventory: "SEPARATE_FROM_CERTIFICATION",
      certificationDecision: "NOT_VERIFIED",
    };
  });
  return {
    ok: true,
    framework: ADP_CANONICAL_31_CERTIFICATION_POSTURE_V1,
    privateMatrixPresent: Boolean(matrix?.matrix?.length),
    identityCount: hotels.length,
    certifiedCount: hotels.filter((row) => row.certificationDecision === "CERTIFIED").length,
    hotels,
  };
}
