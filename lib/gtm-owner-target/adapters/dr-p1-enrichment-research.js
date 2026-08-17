/**
 * P1 Dominican Republic brand-decision contact research (2026-07-04).
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildEnrichmentFromSeedContact } from "./mx-corporate-web-first.js";
import { DR_CORPORATE_WEB_SEEDS } from "./dr-corporate-web-seeds.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..", "..", "..");
const DR_QUEUE_JSON = join(
  ROOT,
  "reports",
  "gtm-brand-decision-enrichment-queue-mx-dominican-republic-dr-review.json"
);
const BRANDING_JSON = join(
  ROOT,
  "reports",
  "gtm-branding-decision-targets-mx-dominican-republic.json"
);

/** @typedef {{ ownerTargetId?: string, slug?: string, contactKey?: string, ownerName?: string, fileKey?: string, enrichment?: object }} DrP1ResearchSpec */

/** @type {DrP1ResearchSpec[]} */
export const DR_P1_BATCH1_SPECS = [
  { slug: "grupo-pinero", contactKey: "primary", ownerTargetId: "recuuSDdQC4NGfmaR" },
  { slug: "impressive-resorts-dr", contactKey: "primary", ownerTargetId: "rec8n5XCwP6bmXqut" },
  { slug: "zemi-hotels", contactKey: "primary", ownerTargetId: "recoODIcQn3x9ioaZ" },
  { slug: "hodelpa-hotels", contactKey: "primary", ownerTargetId: "recgYY2ewSAPx5OFP" },
  {
    slug: "central-romana",
    contactKey: "casa_de_campo",
    ownerTargetId: "reciTWCiMtYlZsNxf",
  },
  { slug: "majestic-resorts-dr", contactKey: "primary", ownerTargetId: "recmx5AqgkBgH72Vr" },
  {
    slug: "zafera-investments",
    contactKey: "primary",
    ownerTargetId: "recApdUerJmyVOHKI",
  },
  {
    slug: "green-earth-investments",
    contactKey: "primary",
    ownerTargetId: "recMRHH3Gjs0FEyGm",
    ownerName: "Delveccio Investments Ltd.",
  },
  { slug: "vh-hotels-resorts", contactKey: "primary", ownerTargetId: "recAoKBDcTz7rT1W6" },
];

/** ALIS CALA 2026 net-new DR leads — batch 2 (2026-07-04). */
/** @type {DrP1ResearchSpec[]} */
export const DR_ALIS_BATCH1_SPECS = [
  {
    slug: "noval-properties-dr",
    contactKey: "primary",
    ownerTargetId: "rec4u2fj8MYSFrWq2",
  },
  {
    slug: "grupo-puntacana-dr",
    contactKey: "primary",
    ownerTargetId: "recZOrnDfZxkzODCp",
  },
  {
    slug: "grupo-abrisa-dr",
    contactKey: "primary",
    ownerName: "Grupo Abrisa",
    fileKey: "abraham-hazoury",
  },
  {
    slug: "grupo-abrisa-dr",
    contactKey: "institutional",
    ownerName: "Grupo Abrisa",
    fileKey: "jorge-hazoury",
  },
  {
    slug: "ocama-boutique-dr",
    contactKey: "primary",
    ownerName: "Ocama Luxury Boutique Hotel",
    fileKey: "mark-andrus",
  },
];

/** DR P1 batch 3 — Santa Maria, Mullen, Rizek research (2026-07-05). */
/** @type {DrP1ResearchSpec[]} */
export const DR_P1_BATCH3_SPECS = [
  {
    slug: "grupo-santa-maria-dr",
    contactKey: "primary",
    ownerTargetId: "recQfvFY3dJN3KjB9",
  },
  {
    slug: "mullen-real-estate-capital",
    contactKey: "primary",
    ownerTargetId: "recInM1cvhNyluaOW",
  },
  {
    slug: "mullen-real-estate-capital",
    contactKey: "operations_president",
    ownerTargetId: "recInM1cvhNyluaOW",
    fileKey: "javier-coll",
  },
  {
    slug: "rizek-group-dr",
    contactKey: "primary",
    ownerTargetId: "recfr7SeBXSwJhCg4",
  },
];

function loadOwnerTargetIdByName(ownerName) {
  const sources = [DR_QUEUE_JSON, BRANDING_JSON];
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
 * @param {DrP1ResearchSpec} spec
 * @returns {object}
 */
export function buildDrP1Enrichment(spec) {
  const seed = DR_CORPORATE_WEB_SEEDS.find((s) => s.slug === spec.slug);
  if (!seed) throw new Error(`Unknown DR seed slug: ${spec.slug}`);

  if (spec.enrichment) {
    const ownerTargetId =
      spec.ownerTargetId ||
      spec.enrichment.ownerTargetId ||
      loadOwnerTargetIdByName(spec.enrichment.ownerName);
    return {
      ...spec.enrichment,
      ownerTargetId: ownerTargetId || spec.enrichment.ownerTargetId || null,
      enrichedAt: spec.enrichment.enrichedAt || new Date().toISOString().slice(0, 10),
      enrichedBy: spec.enrichment.enrichedBy || "p1_dr_research_2026-07",
      status: "ready",
    };
  }

  const ownerTargetId = spec.ownerTargetId || loadOwnerTargetIdByName(seed.ownerNameMatch[0]);
  const enrichment = buildEnrichmentFromSeedContact(seed, {
    ownerTargetId,
    contactKey: spec.contactKey || "primary",
    enrichedBy: spec.enrichedBy || "p1_dr_research_2026-07",
  });
  if (spec.ownerName) enrichment.ownerName = spec.ownerName;
  return enrichment;
}
