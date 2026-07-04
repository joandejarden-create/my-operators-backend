import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures/brand-explorer-presentation-standards-radisson-choice.json"), "utf8")
);

function t(s) {
  return String(s || "")
    .replace(/Upscale brands/g, "Upper-upscale brands")
    .replace(/upscale positioning/gi, "upper-upscale positioning")
    .replace(/upscale urban/gi, "upper-upscale urban")
    .replace(/upscale refresh/gi, "upper-upscale refresh")
    .replace(/upscale consumers/gi, "upper-upscale consumers");
}

const out = {
  targetBrandBasicsName: "Radisson Blu (Choice)",
  brandNameFallback: "Radisson Blu (Choice)",
  instructions:
    'Standards tab (same slot keys as Radisson (Choice)). Push: npm run apply-brand-explorer-presentation -- --brand-name "Radisson Blu" --fixture fixtures/brand-explorer-presentation-standards-radisson-blu.json --replace-slot-prefix standards.',
  rows: src.rows.map((r) => ({
    ...r,
    title: r.title ? t(r.title) : r.title,
    body: t(r.body),
  })),
};

out.rows.find((r) => r.slotKey === "standards.intro").body =
  "Upper-upscale brands under the Choice portfolio share central reservations, loyalty, and reporting while maintaining distinct Nordic Nouveau guestroom and public-space standards. Owners should treat requirements as agreement- and prototype-specific: design-forward public spaces, signature F&B, and gallery-curator service rituals typically exceed core Radisson conversion scope—budget for upper-upscale FF&E, baths, and QA from LOI.";

fs.writeFileSync(
  path.join(ROOT, "fixtures/brand-explorer-presentation-standards-radisson-blu.json"),
  JSON.stringify(out, null, 2) + "\n"
);
console.log("standards rows", out.rows.length);
