/**
 * Offline CNPJ registry snapshots for regression / API outage resilience.
 * Source: public Receita-style fields verified against Webhound A′ research paths — not production truth imports.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeCnpjDigits } from "./brazil-cnpj-client.js";
import { normalizeMatchText } from "./adapters/match-utils.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_DIR = path.join(__dirname, "../../../../fixtures/ownership-brazil-cnpj-registry");

/** @type {Map<string, object>} */
const loaded = new Map();

function fixturePath(digits) {
  return path.join(FIXTURE_DIR, `${digits}.json`);
}

/**
 * @param {string} cnpj
 * @returns {object|null}
 */
export function loadCnpjRegistryFixture(cnpj) {
  const digits = normalizeCnpjDigits(cnpj);
  if (!digits) return null;
  if (loaded.has(digits)) return loaded.get(digits);

  const fp = fixturePath(digits);
  if (!fs.existsSync(fp)) return null;
  try {
    const raw = JSON.parse(fs.readFileSync(fp, "utf8"));
    loaded.set(digits, raw);
    return raw;
  } catch {
    return null;
  }
}

export function listCnpjRegistryFixtures() {
  if (!fs.existsSync(FIXTURE_DIR)) return [];
  return fs
    .readdirSync(FIXTURE_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(/\.json$/, ""));
}

/**
 * Eval-only: match hotel name + city against fixture nome_fantasia/municipio.
 * Does not import Webhound answers — uses public-registry-shaped snapshots for offline eval.
 * @param {object} hotel
 */
export function findCnpjFixtureByHotelIdentity(hotel) {
  if (String(process.env.OWNERSHIP_EVAL_FIXTURE_DISCOVERY || "0") !== "1") return null;
  const hotelName = normalizeMatchText(hotel?.hotel_name || hotel?.name || "");
  const hotelCity = normalizeMatchText(hotel?.city || "");
  if (!hotelName) return null;

  for (const digits of listCnpjRegistryFixtures()) {
    const rec = loadCnpjRegistryFixture(digits);
    if (!rec) continue;
    const fantasia = normalizeMatchText(rec.nome_fantasia || "");
    const razao = normalizeMatchText(rec.razao_social || "");
    const city = normalizeMatchText(rec.municipio || "");
    const nameHit =
      (fantasia && (fantasia.includes(hotelName) || hotelName.includes(fantasia))) ||
      (razao && hotelName.split(" ").every((w) => w.length < 3 || razao.includes(w)));
    const cityHit = !hotelCity || !city || hotelCity === city || city.includes(hotelCity);
    if (nameHit && cityHit) return { cnpj: digits, record: rec, source: "fixture_hotel_identity_match" };
  }
  return null;
}
