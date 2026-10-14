/**
 * In-repo San Diego Parallel fixtures (copied from lab freeze).
 * Production runtime must not require the lab folder — fixtures live under
 * fixtures/gdi-parallel-san-diego/. Lab root remains optional for developers.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const SAN_DIEGO_FIXTURE_PACK_VERSION = "gdi-parallel-sd-fixture-pack-v1";
export const SAN_DIEGO_HOTEL_ID = "recnPB3Q02NO8Ma1o";

export function resolveSanDiegoFixtureRoot(override) {
  if (override) return path.resolve(override);
  if (process.env.GDI_PARALLEL_SD_FIXTURE_ROOT) {
    return path.resolve(process.env.GDI_PARALLEL_SD_FIXTURE_ROOT);
  }
  return path.join(ROOT, "fixtures/gdi-parallel-san-diego");
}

function loadJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function loadSanDiegoFixtureCards(opts = {}) {
  const root = resolveSanDiegoFixtureRoot(opts.fixtureRoot);
  const cardsDir = path.join(root, "cards");
  if (!fs.existsSync(cardsDir)) {
    return { root, available: false, cards: [], reviewPackage: null };
  }
  const cards = fs
    .readdirSync(cardsDir)
    .filter((f) => /^sd_card_\d+\.json$/.test(f))
    .sort()
    .map((f) => ({ ...loadJson(path.join(cardsDir, f)), _file: f }));
  const reviewPackage =
    loadJson(path.join(root, "reports/latest-review-package.json")) || null;
  return {
    version: SAN_DIEGO_FIXTURE_PACK_VERSION,
    root,
    available: true,
    cards,
    reviewPackage,
  };
}
