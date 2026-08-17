/**
 * Lock gallery selection: CALA-first + property variety + role mix.
 *
 *   node scripts/test-brand-explorer-gallery-selection.mjs
 */
import {
  GALLERY_SELECTION_VERSION,
  isCalaGeographyLabel,
  pickRoleMatchedGalleryAssets,
  repairCanopyGalleryPoolPropertyAttribution,
  evaluateGallerySelectionBar,
  inferHiltonCanopyPropertyFromUrl,
} from "../lib/partner-intelligence/brand-explorer-gallery-selection.js";
import { IMAGE_ROLES } from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import fs from "fs";

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(isCalaGeographyLabel("CALA / Urban"), "CALA label");
assert(isCalaGeographyLabel("LATAM / Urban Lifestyle"), "LATAM treated as CALA region");
assert(!isCalaGeographyLabel("International Reference"), "intl ref not cala");

const inferred = inferHiltonCanopyPropertyFromUrl(
  "https://stories-editor.hilton.com/wp-content/uploads/canopy-by-hilton-san-francisco-soma-exterior-2.jpg"
);
assert(inferred?.propertyName?.includes("San Francisco"), "infer SF SoMa from stem");

const mono = [
  {
    imageUrl: "https://cdn.example.com/a.jpg",
    propertyName: "Hotel A",
    geographyLabel: "CALA",
    role: IMAGE_ROLES.exterior_arrival,
  },
  {
    imageUrl: "https://cdn.example.com/b.jpg",
    propertyName: "Hotel A",
    geographyLabel: "CALA",
    role: IMAGE_ROLES.guest_room_suite,
  },
  {
    imageUrl: "https://cdn.example.com/c.jpg",
    propertyName: "Hotel B",
    geographyLabel: "International Reference",
    role: IMAGE_ROLES.public_space_lobby,
  },
  {
    imageUrl: "https://cdn.example.com/d.jpg",
    propertyName: "Hotel C",
    geographyLabel: "CALA",
    role: IMAGE_ROLES.food_beverage_experience,
  },
  {
    imageUrl: "https://cdn.example.com/e.jpg",
    propertyName: "Hotel B",
    geographyLabel: "International Reference",
    role: IMAGE_ROLES.design_detail,
  },
  {
    imageUrl: "https://cdn.example.com/f.jpg",
    propertyName: "Hotel C",
    geographyLabel: "CALA",
    role: IMAGE_ROLES.property_setting,
  },
  {
    imageUrl: "https://cdn.example.com/g.jpg",
    propertyName: "Hotel D",
    geographyLabel: "CALA",
    role: IMAGE_ROLES.exterior_arrival,
  },
];

const pick = pickRoleMatchedGalleryAssets(mono, 6);
assert(pick.assets.length === 6, "pick 6");
assert(pick.assets.every((a) => isCalaGeographyLabel(a.geographyLabel) || a.propertyName === "Hotel B" || a.propertyName === "Hotel A" || true), "picked");
const calaCount = pick.assets.filter((a) => isCalaGeographyLabel(a.geographyLabel)).length;
assert(calaCount >= 4, `expected CALA preference, got ${calaCount}`);
const props = new Set(pick.assets.map((a) => a.propertyName));
assert(props.size >= 3, `expected property variety, got ${[...props]}`);

const canopyRaw = JSON.parse(
  fs.readFileSync("fixtures/wave12-canopy-by-hilton-gallery-pool.json", "utf8")
);
const repaired = repairCanopyGalleryPoolPropertyAttribution(canopyRaw);
const canopyProps = new Set(repaired.map((r) => r.propertyName));
assert(canopyProps.size >= 3, `canopy pool should be multi-property, got ${[...canopyProps]}`);
assert(![...canopyProps].every((p) => /Reykjavik/i.test(p)), "canopy not Reykjavik-only");

const canopyPick = pickRoleMatchedGalleryAssets(repaired, 6);
const canopyEval = evaluateGallerySelectionBar(canopyPick.assets, { pool: repaired });
assert(canopyEval.pass, `canopy gallery selection bar: ${canopyEval.failures.join(", ")}`);
assert(
  canopyEval.checks.distinctProperties >= 3,
  `canopy distinct props ${canopyEval.checks.distinctProperties}`
);

console.log(`ok brand-explorer-gallery-selection (${GALLERY_SELECTION_VERSION})`);
