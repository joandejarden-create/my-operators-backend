import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { overlayCurioCalaMaterials } from "../lib/curio-brand-explorer-cala-materials.js";
import { overlayCurioPresentationOverrides } from "../lib/curio-brand-explorer-presentation-overrides.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = path.join(ROOT, "fixtures", "brand-explorer-presentation-kimpton-full.json");
const out = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-full.json");

let j = fs.readFileSync(src, "utf8");
const reps = [
  ["Kimpton Hotels", "Curio Collection by Hilton"],
  ["Kimpton Hotels (", "Curio Collection by Hilton ("],
  ["Kimpton ", "Curio "],
  ["Kimpton", "Curio"],
  ["IHG Hotels & Resorts", "Hilton Worldwide"],
  ["IHG One Rewards", "Hilton Honors"],
  ["IHG ", "Hilton "],
  ["IHG", "Hilton"],
  ["Opera PMS and Hilton Concerto CRS", "OnQ PMS and Hilton CRS"],
  ["Concerto", "OnQ"],
  ["wine hour", "local rituals"],
  ["lifestyle F&B", "culinary-forward F&B"],
  ["upper-upscale lifestyle", "upper-upscale soft collection"],
  ["brand-explorer-presentation-kimpton-full", "brand-explorer-presentation-curio-full"],
  ["kimpton-brand-setup", "curio-brand-setup"],
];
for (const [a, b] of reps) j = j.split(a).join(b);

const obj = JSON.parse(j);
obj.targetBrandBasicsName = "Curio Collection by Hilton";
obj.brandNameFallback = "Curio Collection by Hilton";
obj.instructions =
  'Apply: node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name "Curio Collection by Hilton" --fixture fixtures/brand-explorer-presentation-curio-full.json --replace';
overlayCurioPresentationOverrides(obj);
obj.rows = overlayCurioCalaMaterials(obj.rows);
fs.writeFileSync(out, JSON.stringify(obj, null, 2));
console.log("Wrote", out, "rows:", obj.rows.length);
