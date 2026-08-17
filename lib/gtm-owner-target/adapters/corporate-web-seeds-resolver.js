import { resolveMxCorporateSeed } from "./mx-corporate-web-seeds.js";
import { resolveCalaCorporateSeed } from "./cala-corporate-web-seeds.js";
import { resolveDrCorporateSeed } from "./dr-corporate-web-seeds.js";
import { resolveCrCorporateSeed } from "./cr-corporate-web-seeds.js";

/**
 * @param {string} ownerName
 * @returns {import("./mx-corporate-web-seeds.js").MxCorporateWebSeed | null}
 */
export function resolveCorporateWebSeed(ownerName) {
  return (
    resolveMxCorporateSeed(ownerName) ||
    resolveDrCorporateSeed(ownerName) ||
    resolveCrCorporateSeed(ownerName) ||
    resolveCalaCorporateSeed(ownerName)
  );
}
