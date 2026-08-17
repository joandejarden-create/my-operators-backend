/**
 * Parent-company registry for Brand Explorer factory (thin slice — CHI + pilot references).
 * Human docs: docs/brand-explorer-factory.md
 */
import {
  listChoiceBrandManifest,
  CHOICE_P1_ENRICHMENT_QUEUE,
} from "../../scripts/lib/choice-brand-explorer-manifest.mjs";
import { resolveProfileForAirtableName } from "../../scripts/lib/choice-chi-brand-resolve.mjs";

/** @typedef {'choice-pipeline' | 'choice-restore-l2' | 'premium-split' | 'non-chi-split'} FactoryStrategy */

/**
 * @typedef {{
 *   parentCompany: string,
 *   referenceRootSubpath: string,
 *   sourceUrls?: Record<string, string>,
 *   brands: Array<{
 *     profileName: string,
 *     airtableName?: string,
 *     strategy: FactoryStrategy,
 *     goldReference?: string,
 *     notes?: string,
 *   }>,
 * }} ParentCompanyRegistryEntry
 */

/** @type {ParentCompanyRegistryEntry[]} */
export const PARENT_COMPANY_REGISTRY = [
  {
    parentCompany: "Choice Hotels International",
    referenceRootSubpath: "Choice Hotels International",
    sourceUrls: {
      development: "https://www.choicehotels.com/development",
      mediaCenter: "https://media.choicehotels.com/",
      fddInventory: "docs/choice-fdd-inventory.md",
    },
    brands: [], // filled dynamically from manifest
  },
  {
    parentCompany: "IHG Hotels & Resorts",
    referenceRootSubpath: "IHG Hotels & Resorts",
    brands: [
      {
        profileName: "Kimpton",
        strategy: "non-chi-split",
        goldReference: "fixtures/brand-explorer-presentation-kimpton-full.json",
        notes: "L2 complete — template for IHG soft brands",
      },
    ],
  },
  {
    parentCompany: "Hilton",
    referenceRootSubpath: "Hilton",
    brands: [
      {
        profileName: "Curio Collection by Hilton",
        strategy: "non-chi-split",
        goldReference: "fixtures/brand-explorer-presentation-curio-full.json",
        notes: "L2 complete — template for Hilton soft brands",
      },
    ],
  },
];

/**
 * Resolve factory strategy for a Choice manifest row.
 * @param {ReturnType<typeof listChoiceBrandManifest>[number]} manifestRow
 * @returns {FactoryStrategy}
 */
export function resolveChoiceFactoryStrategy(manifestRow) {
  if (manifestRow.premium?.applyScript) return "premium-split";
  if (manifestRow.enriched?.parity === "complete") return "choice-restore-l2";
  if (manifestRow.fullFixture || manifestRow.isTier1) return "choice-restore-l2";
  return "choice-pipeline";
}

/**
 * P1 enrichment queue as manifest rows.
 */
export function listChoiceP1ManifestRows() {
  const all = listChoiceBrandManifest();
  return CHOICE_P1_ENRICHMENT_QUEUE.map((airtableName) => {
    const direct = all.find((b) => b.airtableName === airtableName);
    if (direct) return direct;
    const profile = resolveProfileForAirtableName(airtableName).name;
    return all.find(
      (b) =>
        b.profileName === profile ||
        resolveProfileForAirtableName(b.airtableName).name === profile
    );
  }).filter(Boolean);
}

/**
 * @param {string} ref — brand profile name, airtable name, or queue id `p1`
 */
export function resolveFactoryTarget(ref) {
  const q = String(ref || "").trim().toLowerCase();
  if (q === "p1" || q === "choice-p1") {
    return { queue: "p1", brands: listChoiceP1ManifestRows() };
  }

  const chi = listChoiceBrandManifest().find(
    (b) =>
      b.airtableName.toLowerCase() === q ||
      b.profileName.toLowerCase() === q ||
      b.slug === q.replace(/\s+/g, "-")
  );
  if (chi) return { queue: null, brands: [chi] };

  for (const parent of PARENT_COMPANY_REGISTRY) {
    const hit = parent.brands.find((b) => b.profileName.toLowerCase() === q);
    if (hit) return { queue: null, brands: [hit], nonChi: true };
  }

  return { queue: null, brands: [] };
}

export function getParentEntry(parentCompany) {
  return PARENT_COMPANY_REGISTRY.find((p) => p.parentCompany === parentCompany) || null;
}
