/**
 * Dry diagnostic: warm family directory adapters and sample Address/Amenities/Coords
 * resolution for Hilton + Choice (no Airtable writes).
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  warmFamilyDirectoryCaches,
  resolveDirectoryAddressCandidate,
  resolveDirectoryAmenitiesCandidate,
  resolveDirectoryDescriptionCandidate,
  resolveDirectoryCoordinateCandidate,
  getFamilyAdapterCacheStats,
  getUnresolvedSourcePatterns,
  FAMILY_ADAPTER_VERSION,
} from "../lib/research-engine-v2/census-autopilot-family-directory-adapters.js";
import { buildWebhoundLearningCandidates } from "../lib/research-engine-v2/census-autopilot-source-yield-diagnostic.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

const samples = [
  {
    family: "Hilton",
    identityKey: "ind_hilton_mx_qrohwhw",
    fields: { "Property Identity Key": "ind_hilton_mx_qrohwhw" },
  },
  {
    family: "Choice",
    identityKey: "ind_choice_mx_mx165",
    fields: { "Property Identity Key": "ind_choice_mx_mx165" },
  },
  {
    family: "Marriott",
    identityKey: "ind_marriott_mx_mexcy",
    fields: {
      "Property Identity Key": "ind_marriott_mx_mexcy",
      "Official Property URL":
        "https://www.marriott.com/en-us/hotels/mexcy-courtyard-mexico-city-airport/overview/",
    },
  },
];

const warm = await warmFamilyDirectoryCaches({ delayMs: 80 });
const results = [];
for (const s of samples) {
  const address = await resolveDirectoryAddressCandidate(s);
  const amenities = await resolveDirectoryAmenitiesCandidate(s);
  const description = await resolveDirectoryDescriptionCandidate(s);
  const coordinates =
    s.family === "Marriott"
      ? await resolveDirectoryCoordinateCandidate(s)
      : await resolveDirectoryCoordinateCandidate(s);
  results.push({
    family: s.family,
    identity_key: s.identityKey,
    address: address.ok
      ? { ok: true, method: address.method, address: address.address, source_url: address.source_url }
      : { ok: false, reason: address.reason },
    amenities: amenities.ok
      ? {
          ok: true,
          method: amenities.method,
          tag_count: amenities.tags?.length || 0,
          tags_preview: (amenities.tags || []).slice(0, 6),
        }
      : { ok: false, reason: amenities.reason },
    description: description.ok
      ? { ok: true, method: description.method }
      : { ok: false, reason: description.reason },
    coordinates: coordinates.ok
      ? {
          ok: true,
          method: coordinates.method,
          lat: coordinates.lat,
          lng: coordinates.lng,
        }
      : { ok: false, reason: coordinates.reason },
  });
}

const report = {
  version: FAMILY_ADAPTER_VERSION,
  generated_at: new Date().toISOString(),
  status: "production_census_autopilot_family_directory_adapters_wired",
  airtable_writes: false,
  brand_explorer_writes: false,
  brand_setup_writes: false,
  webhound_run: false,
  warm,
  cache: getFamilyAdapterCacheStats(),
  sample_resolutions: results,
  webhound_candidates: buildWebhoundLearningCandidates({
    unresolved_patterns: getUnresolvedSourcePatterns({ minCount: 1 }),
  }),
  wiring: {
    address_confirmation: "resolveDirectoryAddressCandidate before property URL fetch",
    amenities_extraction: "Hilton amenityIds + Choice amenityGroups via directory before 403 pages",
    description_extraction:
      "Choice regional cards lack narrative — amenities only; deep signals when page fetch succeeds",
    coordinate_resolution: "Marriott HQV → Hilton/Choice directory geo → deep page signals → HTML",
  },
};

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });
writeFileSync(
  join(REPORTS, "production-census-autopilot-family-directory-adapters.json"),
  JSON.stringify(report, null, 2),
  "utf8"
);
const md = [
  `# Production Census Autopilot — Family Directory Adapters`,
  ``,
  `Status: **${report.status}**`,
  ``,
  `- Version: ${report.version}`,
  `- Airtable writes: false`,
  `- Hilton Mexico directory rows: ${warm.hilton_count}`,
  `- Choice Mexico regional rows: ${warm.choice_count}`,
  `- Warm errors: ${(warm.errors || []).length}`,
  ``,
  `## Sample resolutions`,
  ``,
  "```json",
  JSON.stringify(results, null, 2),
  "```",
  ``,
  `## Wiring`,
  ``,
  `- Address: Hilton locations + Choice regional cards before property URL`,
  `- Amenities: Hilton amenityIds + Choice amenity groups`,
  `- Descriptions: Choice regional narrative unsupported (amenities only)`,
  `- Coordinates: Marriott HQV + Hilton/Choice directory geo`,
  `- Deep page signals when official HTML fetch succeeds`,
  `- Webhound candidates only for repeated unresolved patterns (not run)`,
  ``,
].join("\n");
writeFileSync(
  join(REPORTS, "production-census-autopilot-family-directory-adapters.md"),
  md,
  "utf8"
);
writeFileSync(
  join(DOCS, "production-census-autopilot-family-directory-adapters.md"),
  md,
  "utf8"
);

console.log(
  JSON.stringify(
    {
      status: report.status,
      hilton: warm.hilton_count,
      choice: warm.choice_count,
      samples: results.map((r) => ({
        family: r.family,
        address: r.address.ok,
        amenities: r.amenities.ok,
        description: r.description.ok,
        coordinates: r.coordinates.ok,
      })),
    },
    null,
    2
  )
);
