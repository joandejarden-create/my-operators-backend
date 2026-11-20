#!/usr/bin/env node
/**
 * Gate: overview.scenario.1–3 must receive hotel property photographs
 * (exterior / interior / rendering) — never people/lifestyle, never blank IMAGE.
 */
import assert from "assert";
import {
  pickScenarioHotelImages,
  buildEmptyBrandCompletionBlocks,
  measureEmptyBrandCompletion,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import { buildDepthGapFillBlocks } from "../lib/brand-explorer/factory-2/be-factory-2-depth-candidate-builder-v1.js";
import {
  isPeopleOrLifestyleMarketingImage,
  isLikelyHotelPropertyPhotograph,
} from "../lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js";

const ROOT = process.cwd();

for (const [slug, brand] of [
  ["sonesta-select", "Sonesta Select"],
  ["ritz-carlton", "The Ritz-Carlton"],
  ["hampton-by-hilton", "Hampton by Hilton"],
  ["hyatt-regency", "Hyatt Regency"],
]) {
  const imgs = pickScenarioHotelImages(ROOT, slug, brand, 3);
  assert.ok(imgs.length >= 3, `${slug}: need 3 scenario hotel images, got ${imgs.length}`);
  const urls = new Set(imgs.map((i) => i.imageUrl));
  assert.equal(urls.size, imgs.length, `${slug}: scenario images must be distinct`);
  for (const img of imgs) {
    assert.ok(!isPeopleOrLifestyleMarketingImage(img.imageUrl), `${slug}: people image`);
    assert.ok(
      isLikelyHotelPropertyPhotograph(img.imageUrl, { role: img.scene }),
      `${slug}: not hotel photo ${img.imageUrl}`
    );
  }
  console.log(`OK pick ${slug}: ${imgs.length} distinct hotel scenes`);
}

const pack = {
  slug: "sonesta-select",
  brandName: "Sonesta Select",
  parentCompany: "Sonesta",
  loyaltyProgram: "Sonesta Travel Pass",
  positioning: "Upper-midscale select-service",
  scenarios: [
    { title: "Suburban Select-Service", body: "Suburban corridors needing Travel Pass." },
    { title: "Airport Edge Select-Service", body: "Airport edge sites with midweek corporate mix." },
    { title: "Conversion Into Select-Service", body: "Conversions that can fund Select product standards." },
  ],
  sources: { brandPage: "https://www.sonesta.com/" },
  operatingModel: "Franchise select-service",
  footprint: "Confirm live Sonesta Select footprint.",
  insight: "Select works when product clears IC.",
  feeNote: "Confirm current fee exhibits.",
};

const depth = buildDepthGapFillBlocks(pack);
for (const n of [1, 2, 3]) {
  const b = depth.find((x) => x.slotKey === `overview.scenario.${n}`);
  assert.ok(b, `depth missing overview.scenario.${n}`);
  assert.ok(/^https?:\/\//i.test(b.imageUrl || ""), `depth scenario.${n} missing imageUrl`);
  assert.ok(!isPeopleOrLifestyleMarketingImage(b.imageUrl), `depth scenario.${n} people`);
}
console.log("OK depth builder attaches scenario hotel images");

const completion = buildEmptyBrandCompletionBlocks(pack, new Set(), { repoRoot: ROOT });
const m = measureEmptyBrandCompletion([...depth.filter((b) => !/^overview\.scenario\./.test(b.slotKey)), ...completion]);
// measure on completion alone for scenario emit
const m2 = measureEmptyBrandCompletion(completion);
assert.ok(m2.scenarioWithImage >= 3 || completion.some((b) => b.slotKey === "overview.scenario.1" && b.imageUrl),
  "empty completion should emit scenario images");
const sc1 = completion.find((b) => b.slotKey === "overview.scenario.1");
assert.ok(sc1?.imageUrl, "empty completion scenario.1 imageUrl");
console.log("OK empty completion scenario images", { scenarioWithImage: m2.scenarioWithImage, mPass: m.scenarioImagePass });

console.log("test-be-factory-2-scenario-hotel-images-v1: PASS");
