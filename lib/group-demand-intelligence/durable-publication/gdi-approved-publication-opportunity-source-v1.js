/**
 * Resolve complete approved opportunity records for one-click publish.
 *
 * Production must not depend on local staging/research docs that may omit
 * certified IDs. Preferred durable sources (in order):
 *  1. Caller-supplied opportunities (local preview / request body)
 *  2. Deploy-bundled certified enriched V2 pack (ships with the app revision)
 *  3. Existing published registry snapshots for the hotel
 *
 * Does not invent content. Missing IDs after all sources ⇒ fail closed.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublishedInventoryRegistry,
} from "./gdi-published-inventory-registry-v1.js";
import { resolveGdiPublishedInventoryRoot } from "./resolve-gdi-published-registry-root-v1.js";

export const GDI_APPROVED_PUBLICATION_OPPORTUNITY_SOURCE_V1 =
  "gdi-approved-publication-opportunity-source-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

/** Spice Island certified seven — deploy-bundled enriched V2 pack. */
export const SPICE_ISLAND_HOTEL_ID = "recKRJjcPnb4tVDDS";
export const SPICE_ISLAND_CERTIFIED_SEVEN_IDS = Object.freeze([
  "gdi_s4912_ba7b1cb8a3ab",
  "gdi_s4912_b956aa67a4a2",
  "gdi_s4912_b6d9b1929c05",
  "gdi_s4912_cb2d71ab1b15",
  "gdi_s4912_cc2a8da6ca6e",
  "gdi_s4912_03deda917778",
  "gdi_s4912_3cafec77a21f",
]);

export const SPICE_ISLAND_ENRICHED_V2_PACK_RELATIVE =
  "reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots";

const APPROVED_EVENT_TITLES = Object.freeze({
  gdi_s4912_ba7b1cb8a3ab:
    "71st CARPHA Annual Health Research Conference — One Health",
  gdi_s4912_03deda917778:
    "Savor 10 Grenada — Full Circle 10th Anniversary Gala Dinner & Dance",
  gdi_s4912_3cafec77a21f:
    "Kenwood Travel — Spice Island Beach Resort Weddings and Vow Renewals",
});

export function resolveCertifiedEnrichedPackDir(env = process.env) {
  const explicit = String(env.GDI_APPROVED_PUBLICATION_SNAPSHOT_DIR || "").trim();
  if (explicit) return path.resolve(explicit);
  return path.join(REPO_ROOT, SPICE_ISLAND_ENRICHED_V2_PACK_RELATIVE);
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function loadFromEnrichedPack(opportunityId, packDir) {
  const file = path.join(packDir, `${opportunityId}.json`);
  if (!fs.existsSync(file)) return null;
  const snap = loadJson(file);
  return {
    ...snap,
    id: opportunityId,
    sourceApprovedPublicationPack: GDI_APPROVED_PUBLICATION_OPPORTUNITY_SOURCE_V1,
    sourceApprovedPublicationFile: file,
  };
}

function loadFromRegistry(hotelId, opportunityId, registry) {
  const row = registry
    .listPublished(hotelId)
    .find((r) => r.opportunityId === opportunityId);
  if (!row) return null;
  const doc = registry.loadSnapshot(hotelId, opportunityId, row.publishedVersion);
  const snap = doc?.snapshot || doc;
  if (!snap || typeof snap !== "object") return null;
  return {
    ...snap,
    id: opportunityId,
    hotelId,
    commercialInventoryClass:
      snap.commercialInventoryClass || row.commercialClassification || null,
    commercialClassification:
      snap.commercialClassification || row.commercialClassification || null,
    sourceApprovedPublicationPack: "gdi-published-inventory-registry-v1",
  };
}

/**
 * Resolve the full opportunity set for one-click publish.
 */
export function resolveApprovedPublicationOpportunities(input = {}) {
  const hotelId = text(input.hotelId);
  const requestedIds = Array.isArray(input.opportunityIds)
    ? input.opportunityIds.map(String)
    : null;
  const supplied = Array.isArray(input.opportunities) ? input.opportunities : [];
  const byId = new Map();
  for (const o of supplied) {
    if (o?.id) byId.set(String(o.id), { ...o, hotelId: o.hotelId || hotelId });
  }

  const ids =
    requestedIds && requestedIds.length
      ? requestedIds
      : supplied.map((o) => String(o.id)).filter(Boolean);

  const packDir = resolveCertifiedEnrichedPackDir(input.env || process.env);
  const registry =
    input.registry ||
    createGdiPublishedInventoryRegistry({
      env: input.env || process.env,
      root:
        input.root ||
        resolveGdiPublishedInventoryRoot(input.env || process.env),
    });

  const resolved = [];
  const missing = [];
  const sources = {};

  for (const id of ids) {
    let row = byId.get(id) || null;
    let source = row ? "supplied_local_or_request" : null;
    if (!row) {
      row = loadFromEnrichedPack(id, packDir);
      source = row ? "deploy_bundled_enriched_v2_pack" : null;
    }
    if (!row) {
      row = loadFromRegistry(hotelId, id, registry);
      source = row ? "published_registry_snapshot" : null;
    }
    if (!row) {
      missing.push(id);
      sources[id] = "MISSING";
      continue;
    }
    // Stamp hotel + preserve approved event titles from certified pack when present.
    const approvedTitle = APPROVED_EVENT_TITLES[id];
    resolved.push({
      ...row,
      hotelId,
      id,
      title: approvedTitle || row.title || row.displayTitle || null,
    });
    sources[id] = source;
  }

  // Classification stamp from local registry metadata when pack omits class.
  try {
    const regDoc = registry.loadRegistry?.() || null;
    const hotelOps = regDoc?.hotels?.[hotelId]?.opportunities || {};
    for (const o of resolved) {
      const meta = hotelOps[o.id];
      if (!o.commercialInventoryClass && meta?.commercialClassification) {
        o.commercialInventoryClass = meta.commercialClassification;
        o.commercialClassification = meta.commercialClassification;
      }
    }
  } catch {
    // registry optional for dry resolution
  }

  return {
    ok: missing.length === 0 && resolved.length > 0,
    contract: GDI_APPROVED_PUBLICATION_OPPORTUNITY_SOURCE_V1,
    hotelId,
    opportunityIds: ids,
    opportunities: resolved,
    missing,
    sources,
    packDir,
    approvedEventTitles: APPROVED_EVENT_TITLES,
  };
}

export function assertApprovedEventTitlesPreserved(opportunities = []) {
  const fails = [];
  for (const [id, title] of Object.entries(APPROVED_EVENT_TITLES)) {
    const row = opportunities.find((o) => String(o.id) === id);
    if (!row) continue;
    if (String(row.title || "").trim() !== title) {
      fails.push({ id, expected: title, actual: row.title || null });
    }
  }
  return { ok: fails.length === 0, fails };
}
