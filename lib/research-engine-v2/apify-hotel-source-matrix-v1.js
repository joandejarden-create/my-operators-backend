/**
 * Persist Apify hotel source matrix (no secrets).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  APIFY_FIRST_PARTY_VERSION,
  APIFY_HOTEL_ACTOR_CATALOG,
  COMPANIES_WITHOUT_FIRST_PARTY_ACTOR,
  LIVE_SOURCE_PRIORITY,
  SOURCE_CLASS,
  emptyActorMatrixRow,
} from "./apify-first-party-extractor-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const APIFY_HOTEL_SOURCE_MATRIX_FP = path.join(
  ROOT,
  "reports/research-engine-v2/apify-hotel-source-matrix.json"
);

export function emptyMatrix(actors = []) {
  return {
    version: APIFY_FIRST_PARTY_VERSION,
    generated_at: new Date().toISOString(),
    SOURCE_CLASS,
    LIVE_SOURCE_PRIORITY,
    COMPANIES_WITHOUT_FIRST_PARTY_ACTOR: [...COMPANIES_WITHOUT_FIRST_PARTY_ACTOR],
    actors: actors.map((a) => emptyActorMatrixRow(a)),
  };
}

export function loadApifyHotelSourceMatrix() {
  try {
    if (!fs.existsSync(APIFY_HOTEL_SOURCE_MATRIX_FP)) {
      return emptyMatrix(APIFY_HOTEL_ACTOR_CATALOG);
    }
    const raw = fs.readFileSync(APIFY_HOTEL_SOURCE_MATRIX_FP, "utf8").replace(/^\uFEFF/, "");
    return JSON.parse(raw);
  } catch {
    return emptyMatrix(APIFY_HOTEL_ACTOR_CATALOG);
  }
}

export function upsertActorMatrixRow(matrix, row) {
  const actors = Array.isArray(matrix.actors) ? matrix.actors : [];
  const idx = actors.findIndex((a) => a.ACTOR_ID === row.ACTOR_ID);
  if (idx >= 0) actors[idx] = { ...actors[idx], ...row };
  else actors.push(row);
  matrix.actors = actors;
  matrix.generated_at = new Date().toISOString();
  return matrix;
}

export function saveApifyHotelSourceMatrix(matrix) {
  fs.mkdirSync(path.dirname(APIFY_HOTEL_SOURCE_MATRIX_FP), { recursive: true });
  const out = {
    ...matrix,
    version: APIFY_FIRST_PARTY_VERSION,
    generated_at: new Date().toISOString(),
    SOURCE_CLASS,
    LIVE_SOURCE_PRIORITY,
  };
  fs.writeFileSync(APIFY_HOTEL_SOURCE_MATRIX_FP, `${JSON.stringify(out, null, 2)}\n`, "utf8");
  return APIFY_HOTEL_SOURCE_MATRIX_FP;
}

export function approvedActorsFromMatrix(matrix) {
  return (matrix?.actors || []).filter(
    (a) => a.USAGE_STATUS === "APIFY_APPROVED_FIRST_PARTY_EXTRACTOR"
  );
}
