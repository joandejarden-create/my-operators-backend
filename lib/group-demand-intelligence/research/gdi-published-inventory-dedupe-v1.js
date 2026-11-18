/**
 * Load CUSTOMER_PUBLISHED (and durable package) inventory for pre-deep dedupe.
 * Prefer published package fingerprint over local opportunities.json when present.
 * Does not mutate packages. Does not spend.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  orgDeepKey,
  orgInventoryMatchKeys,
  editionIdentityKey,
} from "./gdi-paid-deep-two-stage-dedupe-v1.js";

export const GDI_PUBLISHED_INVENTORY_DEDUPE_V1 =
  "gdi-published-inventory-dedupe-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function packageRoots(hotelId) {
  return [
    path.join(ROOT, "data/group-demand-intelligence/publication-packages", String(hotelId)),
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      String(hotelId),
    ),
  ];
}

function readPackageJson(dir) {
  const p = path.join(dir, "package.json");
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Select the best durable published package for a hotel (read-only).
 * Prefer CUSTOMER_PUBLISHED with highest opportunityCount.
 */
export function resolvePublishedPackageMeta(hotelId) {
  const candidates = [];
  for (const root of packageRoots(hotelId)) {
    if (!fs.existsSync(root)) continue;
    for (const name of fs.readdirSync(root)) {
      const dir = path.join(root, name);
      if (!fs.statSync(dir).isDirectory()) continue;
      const pkg = readPackageJson(dir);
      if (!pkg) continue;
      const lifecycle = String(pkg.lifecycleState || "");
      const count = Number(pkg.opportunityCount || (pkg.opportunities || []).length || 0);
      candidates.push({
        dir,
        publicationVersion: pkg.publicationVersion || name,
        lifecycleState: lifecycle,
        opportunityCount: count,
        contentFingerprintSha256: pkg.contentFingerprintSha256 || null,
        package: pkg,
      });
    }
  }
  if (!candidates.length) return null;
  candidates.sort((a, b) => {
    const aPub = a.lifecycleState === "CUSTOMER_PUBLISHED" ? 1 : 0;
    const bPub = b.lifecycleState === "CUSTOMER_PUBLISHED" ? 1 : 0;
    if (bPub !== aPub) return bPub - aPub;
    return b.opportunityCount - a.opportunityCount;
  });
  return candidates[0];
}

export function loadPublishedPackageInventory(hotelId) {
  const orgKeys = new Set();
  const opportunityIds = new Set();
  const editionKeys = new Set();
  const orgToOpportunityIds = new Map();
  const meta = resolvePublishedPackageMeta(hotelId);
  if (!meta) {
    return {
      version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
      hotelId: String(hotelId || ""),
      found: false,
      orgKeys,
      opportunityIds,
      editionKeys,
      orgToOpportunityIds,
      opportunityCount: 0,
      packageMeta: null,
    };
  }
  const rows = Array.isArray(meta.package.opportunities)
    ? meta.package.opportunities
    : [];
  for (const row of rows) {
    const id = String(row.id || row.opportunityId || "");
    if (id) opportunityIds.add(id);
    const keys = orgInventoryMatchKeys(row);
    for (const k of keys) {
      orgKeys.add(k);
      if (!orgToOpportunityIds.has(k)) orgToOpportunityIds.set(k, []);
      if (id) orgToOpportunityIds.get(k).push(id);
    }
    const ed = editionIdentityKey(row);
    if (ed) editionKeys.add(ed);
  }
  // Also ingest inventoryIds.all if present
  for (const id of meta.package.inventoryIds?.all || []) {
    opportunityIds.add(String(id));
  }
  return {
    version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
    hotelId: String(hotelId || ""),
    found: true,
    orgKeys,
    opportunityIds,
    editionKeys,
    orgToOpportunityIds,
    opportunityCount: rows.length || opportunityIds.size,
    packageMeta: {
      publicationVersion: meta.publicationVersion,
      lifecycleState: meta.lifecycleState,
      contentFingerprintSha256: meta.contentFingerprintSha256,
      opportunityCount: meta.opportunityCount,
      dir: meta.dir,
    },
  };
}

/**
 * Compare a candidate against published inventory.
 * Known org + same edition → duplicate.
 * Known org + distinct edition/thesis → enrichment / new occurrence eligible.
 */
export function evaluateAgainstPublishedInventory(opportunity = {}, inventory = null) {
  if (!inventory?.found) {
    return {
      version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
      matched: false,
      decision: "NO_PUBLISHED_INVENTORY",
      reasonCode: null,
      matchedOpportunityIds: [],
    };
  }
  const orgKey = orgDeepKey(opportunity);
  const edition = editionIdentityKey(opportunity);
  const oppId = String(opportunity.id || opportunity.opportunityId || "");
  const matchedIds = [];

  if (oppId && inventory.opportunityIds.has(oppId)) {
    return {
      version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
      matched: true,
      decision: "DUPLICATE_NO_NEW_EVIDENCE",
      reasonCode: "PUBLISHED_OPPORTUNITY_ID_MATCH",
      matchedOpportunityIds: [oppId],
      orgKey,
      editionKey: edition,
    };
  }

  if (edition && inventory.editionKeys.has(edition)) {
    return {
      version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
      matched: true,
      decision: "DUPLICATE_NO_NEW_EVIDENCE",
      reasonCode: "PUBLISHED_EDITION_IDENTITY_MATCH",
      matchedOpportunityIds: [...(inventory.orgToOpportunityIds.get(orgKey) || [])].slice(0, 5),
      orgKey,
      editionKey: edition,
    };
  }

  const orgHit = orgInventoryMatchKeys(opportunity).some((k) =>
    [...inventory.orgKeys].some(
      (inv) => inv === k || inv.startsWith(`${k} `) || k.startsWith(`${inv} `),
    ),
  );

  if (orgHit) {
    const distinctEdition =
      opportunity.futureCycleSupported === true ||
      opportunity.futureCycleOpportunity === true ||
      /OVERFLOW|FUTURE_CYCLE|COMPETITIVE_FUTURE|NEW_OCCURRENCE|TRAINING|PROJECT/i.test(
        `${opportunity.opportunityType || ""} ${opportunity.internalDisposition || ""} ${opportunity.hotelOpportunityThesis || ""} ${opportunity.title || ""}`,
      );
    if (distinctEdition) {
      return {
        version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
        matched: true,
        decision: "EXISTING_OPPORTUNITY_ENRICHMENT",
        reasonCode: "KNOWN_ORG_DISTINCT_OCCURRENCE_OR_THESIS",
        matchedOpportunityIds: [
          ...(inventory.orgToOpportunityIds.get(orgKey) || []),
        ].slice(0, 5),
        orgKey,
        editionKey: edition,
        allowDeepResearch: true,
      };
    }
    return {
      version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
      matched: true,
      decision: "DUPLICATE_NO_NEW_EVIDENCE",
      reasonCode: "PUBLISHED_ORG_WITHOUT_DISTINCT_OCCURRENCE",
      matchedOpportunityIds: [
        ...(inventory.orgToOpportunityIds.get(orgKey) || []),
      ].slice(0, 5),
      orgKey,
      editionKey: edition,
      allowDeepResearch: false,
    };
  }

  return {
    version: GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
    matched: false,
    decision: "NET_NEW_VS_PUBLISHED",
    reasonCode: null,
    matchedOpportunityIds: [],
    orgKey,
    editionKey: edition,
  };
}

export default {
  GDI_PUBLISHED_INVENTORY_DEDUPE_V1,
  resolvePublishedPackageMeta,
  loadPublishedPackageInventory,
  evaluateAgainstPublishedInventory,
};
