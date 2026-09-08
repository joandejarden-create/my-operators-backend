/**
 * Packet 2.8B-2 — Organization-level OwnerPortfolio store.
 * Profiles keyed by owner_entity_id — not hotel cohort arrays.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { compileGsfOwnerPortfolioFromEvidence, GSF_OWNER_ENTITY_ID, GSF_SLUG } from "./compilers/gsf-from-evidence.js";
import {
  compileCambridgeOwnerPortfolioFromEvidence,
  DOVETAIL_ENTITY_ID,
} from "./compilers/cambridge-from-evidence.js";
import {
  compileSheratonHnfOwnerPortfolioFromEvidence,
  compileVocoAllianceOwnerPortfolioFromEvidence,
  HNF_ENTITY_ID,
  ALLIANCE_ENTITY_ID,
} from "./compilers/mexico-from-evidence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../..");
const FIXTURE_DIR = path.join(ROOT, "fixtures/hotel-intelligence/owner-portfolio");

const cache = new Map();

function ensureFixtureDir() {
  if (!fs.existsSync(FIXTURE_DIR)) fs.mkdirSync(FIXTURE_DIR, { recursive: true });
}

function fixturePath(ownerEntityId) {
  const safe = String(ownerEntityId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(FIXTURE_DIR, `${safe}.json`);
}

export function writeOwnerPortfolioProfile(profile, graph = null) {
  if (!profile?.owner_entity_id) throw new Error("owner_entity_id_required");
  ensureFixtureDir();
  const payload = {
    profile,
    graph: graph || null,
    written_at: new Date().toISOString(),
  };
  fs.writeFileSync(fixturePath(profile.owner_entity_id), JSON.stringify(payload, null, 2));
  cache.set(profile.owner_entity_id, payload);
  return payload;
}

export function getOwnerPortfolioRecord(ownerEntityId) {
  const id = String(ownerEntityId || "").trim();
  if (!id) return null;
  if (cache.has(id)) return cache.get(id);
  const file = fixturePath(id);
  if (fs.existsSync(file)) {
    const data = JSON.parse(fs.readFileSync(file, "utf8"));
    cache.set(id, data);
    return data;
  }
  return null;
}

export function getOwnerPortfolioProfile(ownerEntityId) {
  return getOwnerPortfolioRecord(ownerEntityId)?.profile || null;
}

export function getOwnerControlGraph(ownerEntityId) {
  return getOwnerPortfolioRecord(ownerEntityId)?.graph || null;
}

/** Compile + persist golden owners from existing evidence (idempotent). */
export function ensureGoldenOwnerPortfoliosMaterialized() {
  const out = [];
  const gsf = compileGsfOwnerPortfolioFromEvidence();
  writeOwnerPortfolioProfile(gsf.profile, gsf.graph);
  out.push(gsf.profile.owner_entity_id);

  const cam = compileCambridgeOwnerPortfolioFromEvidence();
  writeOwnerPortfolioProfile(cam.profile, cam.graph);
  out.push(cam.profile.owner_entity_id);

  const hnf = compileSheratonHnfOwnerPortfolioFromEvidence();
  writeOwnerPortfolioProfile(hnf.profile, hnf.graph);
  out.push(hnf.profile.owner_entity_id);

  const alliance = compileVocoAllianceOwnerPortfolioFromEvidence();
  writeOwnerPortfolioProfile(alliance.profile, alliance.graph);
  out.push(alliance.profile.owner_entity_id);

  return out;
}

const HOTEL_TO_OWNER = Object.freeze({
  recUNycnMwOVFX0hc: GSF_OWNER_ENTITY_ID,
  recIwaP1etgx2g9nA: DOVETAIL_ENTITY_ID,
  recsYJb2R1jarPpK3: HNF_ENTITY_ID,
  recTYaiA4S6fR6ixx: ALLIANCE_ENTITY_ID,
});

const SLUG_TO_OWNER = Object.freeze({
  [GSF_SLUG]: GSF_OWNER_ENTITY_ID,
  "grupo-hotelero-santa-fe": GSF_OWNER_ENTITY_ID,
  "dovetail-hospitality": DOVETAIL_ENTITY_ID,
  dovetail: DOVETAIL_ENTITY_ID,
  "inmobiliaria-hnf": HNF_ENTITY_ID,
  hnf: HNF_ENTITY_ID,
  "alliance-hotel-management": ALLIANCE_ENTITY_ID,
  alliance: ALLIANCE_ENTITY_ID,
});

export function resolveOwnerEntityId(ref) {
  const raw = String(ref || "").trim();
  if (!raw) return null;
  if (HOTEL_TO_OWNER[raw]) return HOTEL_TO_OWNER[raw];
  if (SLUG_TO_OWNER[raw]) return SLUG_TO_OWNER[raw];
  if (raw.startsWith("dle_") || raw.startsWith("ent_")) return raw;
  return SLUG_TO_OWNER[raw.toLowerCase()] || raw;
}

export function resolveOwnerForHotel(hotelId) {
  const ownerId = HOTEL_TO_OWNER[String(hotelId || "").trim()];
  if (!ownerId) return null;
  ensureGoldenOwnerPortfoliosMaterialized();
  return getOwnerPortfolioProfile(ownerId);
}

export function listMaterializedOwnerIds() {
  ensureFixtureDir();
  ensureGoldenOwnerPortfoliosMaterialized();
  return fs
    .readdirSync(FIXTURE_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(/\.json$/, ""));
}

export { HOTEL_TO_OWNER, SLUG_TO_OWNER, FIXTURE_DIR };
