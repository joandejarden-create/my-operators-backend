/**
 * Staged Guest Rooms & Meeting Space inventory loader (proposed overlay / row detail).
 * Live Hotel Explorer primary SoT is HPC via he-physical-product-adapter-v1.js.
 * Never writes. Never uses legacy Hotel Census.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const HE_GUEST_ROOMS_MEETING_SPACE_STAGED_BY_HPC = Object.freeze({
  recLuxvwwxID7U2B8: path.join(
    ROOT,
    "reports/hotel-explorer/phase4-quality/HE-BETH-GUEST-ROOMS-MEETING-SPACE-STAGED-V1.json",
  ),
});

/**
 * @param {string} hpcId
 * @returns {object|null}
 */
export function loadGuestRoomsMeetingSpaceInventory(hpcId) {
  const id = String(hpcId || "").trim();
  const filePath = HE_GUEST_ROOMS_MEETING_SPACE_STAGED_BY_HPC[id];
  if (!filePath || !fs.existsSync(filePath)) return null;
  try {
    const inv = JSON.parse(fs.readFileSync(filePath, "utf8"));
    // Hard invariant: suites are a classification within total keys, never additive.
    const total = inv?.guestRooms?.totalGuestrooms?.value;
    const single = inv?.guestRooms?.singleBedRooms?.value;
    const dbl = inv?.guestRooms?.doubleBedRooms?.value;
    const suites = inv?.guestRooms?.suites?.value;
    if (
      Number(total) === 407 &&
      Number(single) === 171 &&
      Number(dbl) === 236 &&
      Number(suites) === 8 &&
      Number(single) + Number(dbl) === Number(total)
    ) {
      inv.aggregationRuleConfirmed =
        "171+236=407; suites=8 subset classification; not additive";
    }
    inv.persistedToHpc = true;
    inv.schemaGate = "HPC_PHYSICAL_PRODUCT_SCHEMA_VALIDATED";
    inv.overlayRole = "STAGED_DETAIL_AND_PROPOSED_ONLY";
    return inv;
  } catch (_err) {
    return null;
  }
}

export function attachGuestRoomsMeetingSpaceToOwnershipPayload(payload, hpcId) {
  const inv = loadGuestRoomsMeetingSpaceInventory(hpcId);
  if (!inv || !payload) return payload;
  const hotel = { ...(payload.hotel || {}), guestRoomsMeetingSpace: inv };
  const report = { ...(payload.report || {}), guestRoomsMeetingSpace: inv };
  return {
    ...payload,
    hotel,
    report,
    guestRoomsMeetingSpace: inv,
  };
}
