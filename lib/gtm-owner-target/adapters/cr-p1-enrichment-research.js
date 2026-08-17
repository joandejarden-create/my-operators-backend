/**
 * P1 Costa Rica brand-decision contact research (2026-07-05).
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildEnrichmentFromSeedContact } from "./mx-corporate-web-first.js";
import { CR_CORPORATE_WEB_SEEDS } from "./cr-corporate-web-seeds.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..", "..", "..");
const CR_QUEUE_JSON = join(
  ROOT,
  "reports",
  "gtm-brand-decision-enrichment-queue-mx-costa-rica-cr-review.json"
);
const BRANDING_JSON = join(
  ROOT,
  "reports",
  "gtm-branding-decision-targets-mx-costa-rica.json"
);

/** @typedef {{ ownerTargetId?: string, slug?: string, contactKey?: string, ownerName?: string, fileKey?: string, enrichment?: object, enrichedBy?: string }} CrP1ResearchSpec */

/** @type {CrP1ResearchSpec[]} */
export const CR_P1_BATCH1_SPECS = [
  {
    slug: "caribe-hospitality-cr",
    contactKey: "primary",
    ownerTargetId: "recat6sUtfIexZ5G3",
  },
  {
    slug: "grupo-leumi-cr",
    contactKey: "primary",
    ownerTargetId: "reccBBmN7D5tzEnuV",
  },
];

/** @type {CrP1ResearchSpec[]} */
export const CR_P1_BATCH2_SPECS = [
  {
    slug: "boena-lodges-cr",
    contactKey: "primary",
    ownerTargetId: "recF9lXdtix68dL5T",
  },
  {
    slug: "alojica-cr",
    contactKey: "primary",
    ownerTargetId: "recqVaRXgZ7Vxkm0r",
  },
];

function loadOwnerTargetIdByName(ownerName) {
  const sources = [CR_QUEUE_JSON, BRANDING_JSON];
  for (const file of sources) {
    try {
      const data = JSON.parse(readFileSync(file, "utf8"));
      const item = (data.items || []).find((i) => {
        const a = String(i.ownerName).toLowerCase();
        const b = String(ownerName).toLowerCase();
        return a === b || a.includes(b) || b.includes(a);
      });
      if (item?.ownerTargetId) return item.ownerTargetId;
    } catch {
      /* try next */
    }
  }
  return null;
}

/**
 * @param {CrP1ResearchSpec} spec
 * @returns {object}
 */
export function buildCrP1Enrichment(spec) {
  const seed = CR_CORPORATE_WEB_SEEDS.find((s) => s.slug === spec.slug);
  if (!seed) throw new Error(`Unknown CR seed slug: ${spec.slug}`);

  if (spec.enrichment) {
    const ownerTargetId =
      spec.ownerTargetId ||
      spec.enrichment.ownerTargetId ||
      loadOwnerTargetIdByName(spec.enrichment.ownerName);
    return {
      ...spec.enrichment,
      ownerTargetId: ownerTargetId || spec.enrichment.ownerTargetId || null,
      enrichedAt: spec.enrichment.enrichedAt || new Date().toISOString().slice(0, 10),
      enrichedBy: spec.enrichment.enrichedBy || "p1_cr_research_2026-07",
      status: "ready",
    };
  }

  const ownerTargetId = spec.ownerTargetId || loadOwnerTargetIdByName(seed.ownerNameMatch[0]);
  const enrichment = buildEnrichmentFromSeedContact(seed, {
    ownerTargetId,
    contactKey: spec.contactKey || "primary",
    enrichedBy: spec.enrichedBy || "p1_cr_research_2026-07",
  });
  if (spec.ownerName) enrichment.ownerName = spec.ownerName;
  return enrichment;
}
