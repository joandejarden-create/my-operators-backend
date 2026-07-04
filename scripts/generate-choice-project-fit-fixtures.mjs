/**
 * Generate fixtures/brand-project-fit-from-choice-materials/*.json
 *   node scripts/generate-choice-project-fit-fixtures.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { TARGET_BRANDS, buildProjectFitFormForBrand } from "./lib/choice-project-fit-profiles.mjs";
import { projectFitFormToAirtableFields } from "./lib/choice-project-fit-builder.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.resolve(__dirname, "../fixtures/brand-project-fit-from-choice-materials");

function slugify(name) {
  return name
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[()]/g, "")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

let count = 0;
for (const brandName of TARGET_BRANDS) {
  const form = buildProjectFitFormForBrand(brandName);
  if (!form) {
    console.warn(`Skip ${brandName}: no profile`);
    continue;
  }
  const fields = projectFitFormToAirtableFields(form);
  const fixture = {
    targetBrandBasicsName: brandName,
    formValues: form,
    fields,
  };
  const file = path.join(OUT_DIR, `${slugify(brandName)}.json`);
  fs.writeFileSync(file, `${JSON.stringify(fixture, null, 2)}\n`);
  console.log(`${brandName}: ${Object.keys(fields).length} Airtable fields → ${path.relative(process.cwd(), file)}`);
  count++;
}
console.log(`\nDone. ${count} fixture(s).`);
