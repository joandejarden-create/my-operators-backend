/**
 * P1 CALA brand-decision contact research (2026-07-04).
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildEnrichmentFromSeedContact } from "./mx-corporate-web-first.js";
import { CALA_CORPORATE_WEB_SEEDS } from "./cala-corporate-web-seeds.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..", "..", "..");
const CALA_QUEUE_JSON = join(ROOT, "reports", "gtm-brand-decision-enrichment-queue.json");
const P1_SPRINT_JSON = join(ROOT, "reports", "gtm-brand-decision-enrichment-queue-p1-sprint.json");

/** @typedef {{ ownerTargetId?: string, slug?: string, contactKey?: string, ownerName?: string, fileKey?: string, enrichment?: object }} P1ResearchSpec */

/** @type {P1ResearchSpec[]} */
export const CALA_P1_BATCH1_SPECS = [
  { slug: "gaviota", contactKey: "primary" },
  { slug: "essendi", contactKey: "primary" },
  { slug: "urbanova", contactKey: "primary" },
  { slug: "interlink-group", contactKey: "primary" },
  { slug: "ghl-hoteles", contactKey: "primary" },
  { slug: "atlantica-hotels", contactKey: "primary" },
  { slug: "grupo-martinon-grumasa", contactKey: "primary" },
  { slug: "grace-bay-resorts", contactKey: "primary" },
];

/** @type {P1ResearchSpec[]} */
export const CALA_P1_BATCH2_SPECS = [
  { slug: "globalia", contactKey: "be_live_dg", ownerTargetId: "recXTT5z1EkZnNK1A" },
  { slug: "real-hotels", contactKey: "primary", ownerTargetId: "recbkiwiCrlDpHi4Q" },
  {
    slug: "gran-caribe-cuba",
    contactKey: "primary",
    ownerTargetId: "recTVEtjkSYC095pD",
    ownerName: "Grupo Empresarial Hotelero Gran Caribe S.A",
  },
  { slug: "a3-property-investments", contactKey: "primary", ownerTargetId: "recWhfmyjxYKZSKRk" },
  { slug: "jhsf", contactKey: "ceo", ownerTargetId: "reccrXI0v1GDOCOYw" },
  { slug: "ich-administracao", contactKey: "primary", ownerTargetId: "rec8H9v3J8S6dN83g" },
  { slug: "mohari-gencom", contactKey: "primary", ownerTargetId: "recPRV3C1Llhl5BjQ" },
  {
    slug: "gran-caribe-cuba",
    contactKey: "primary",
    fileKey: "grupo",
    ownerTargetId: "recnzN14sYOJAIQYN",
    ownerName: "Gran Caribe Grupo Hotelero",
  },
];

function loadOwnerTargetIdByName(ownerName) {
  const sources = [CALA_QUEUE_JSON, P1_SPRINT_JSON];
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
export function buildCalaP1Enrichment(spec) {
  const seed = CALA_CORPORATE_WEB_SEEDS.find((s) => s.slug === spec.slug);
  if (!seed) throw new Error(`Unknown CALA seed slug: ${spec.slug}`);

  if (spec.enrichment) {
    const ownerTargetId =
      spec.ownerTargetId ||
      spec.enrichment.ownerTargetId ||
      loadOwnerTargetIdByName(spec.enrichment.ownerName);
    return {
      ...spec.enrichment,
      ownerTargetId: ownerTargetId || spec.enrichment.ownerTargetId || null,
      enrichedAt: spec.enrichment.enrichedAt || new Date().toISOString().slice(0, 10),
      enrichedBy: spec.enrichment.enrichedBy || "p1_cala_research_2026-07",
      status: "ready",
    };
  }

  const ownerTargetId = spec.ownerTargetId || loadOwnerTargetIdByName(seed.ownerNameMatch[0]);
  const enrichment = buildEnrichmentFromSeedContact(seed, {
    ownerTargetId,
    contactKey: spec.contactKey,
    enrichedBy: "p1_cala_research_2026-07",
  });
  if (spec.ownerName) enrichment.ownerName = spec.ownerName;
  return enrichment;
}
