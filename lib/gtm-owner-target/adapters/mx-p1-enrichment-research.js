/**
 * P1 Mexico brand-decision contact research (2026-07-04).
 * V1R = named email on entity domain + proof; V2 = named exec + LinkedIn + proof URL.
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildEnrichmentFromSeedContact } from "./mx-corporate-web-first.js";
import { MX_CORPORATE_WEB_SEEDS } from "./mx-corporate-web-seeds.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..", "..", "..");
const P1_SPRINT_JSON = join(
  ROOT,
  "reports",
  "gtm-brand-decision-enrichment-queue-mx-mexico-p1-sprint.json"
);
const MX_QUEUE_JSON = join(
  ROOT,
  "reports",
  "gtm-brand-decision-enrichment-queue-mx-mexico.json"
);

/** @typedef {{ ownerTargetId?: string, slug?: string, contactKey?: string, enrichment?: object }} P1ResearchSpec */

/** @type {P1ResearchSpec[]} */
export const MX_P1_ENRICHMENT_SPECS = [
  { slug: "fibra-inn", contactKey: "primary" },
  { slug: "fibra-inn", contactKey: "ir_downgrade" },
  { slug: "grupo-brisas", contactKey: "primary" },
  { slug: "grupo-brisas", contactKey: "info_downgrade" },
  { slug: "grupo-diestra", contactKey: "primary" },
  { slug: "pueblo-bonito", contactKey: "primary" },
  { slug: "pulso-inmobiliario", contactKey: "primary" },
  { slug: "irawadi-corp", contactKey: "primary" },
  { slug: "grupo-hotelero-santa-fe", contactKey: "primary" },
  { slug: "velas-resorts", contactKey: "primary" },
  { slug: "hoteles-mx", contactKey: "primary" },
  { slug: "fibra-hotel-mexico", contactKey: "primary" },
  { slug: "karisma-hotels", contactKey: "primary" },
  { slug: "riu-hotels", contactKey: "primary" },
];

/** @type {P1ResearchSpec[]} */
export const MX_P1_BATCH2_SPECS = [
  { slug: "grupo-questro", contactKey: "chairman" },
  { slug: "oasis-hotels", contactKey: "primary" },
  { slug: "american-hotels-group", contactKey: "primary" },
  { slug: "park-mizgal", contactKey: "primary" },
  { slug: "club-viva-international", contactKey: "primary" },
  { slug: "flynn-varde-esperanza", contactKey: "primary" },
  { slug: "irawadi-corp", contactKey: "primary" },
];

function loadOwnerTargetIdByName(ownerName) {
  const sources = [MX_QUEUE_JSON, P1_SPRINT_JSON];
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
 * @param {P1ResearchSpec} spec
 * @returns {object}
 */
export function buildP1Enrichment(spec) {
  const seed = MX_CORPORATE_WEB_SEEDS.find((s) => s.slug === spec.slug);
  if (!seed) throw new Error(`Unknown seed slug: ${spec.slug}`);

  if (spec.enrichment) {
    const ownerTargetId =
      spec.ownerTargetId ||
      spec.enrichment.ownerTargetId ||
      loadOwnerTargetIdByName(spec.enrichment.ownerName);
    return {
      ...spec.enrichment,
      ownerTargetId: ownerTargetId || spec.enrichment.ownerTargetId || null,
      enrichedAt: spec.enrichment.enrichedAt || new Date().toISOString().slice(0, 10),
      enrichedBy: spec.enrichment.enrichedBy || "p1_mx_research_2026-07",
      status: "ready",
    };
  }

  const ownerTargetId = spec.ownerTargetId || loadOwnerTargetIdByName(seed.ownerNameMatch[0]);
  return buildEnrichmentFromSeedContact(seed, {
    ownerTargetId,
    contactKey: spec.contactKey,
  });
}

/**
 * @returns {object[]}
 */
export function buildAllP1Enrichments() {
  return MX_P1_ENRICHMENT_SPECS.map((spec) => buildP1Enrichment(spec));
}
