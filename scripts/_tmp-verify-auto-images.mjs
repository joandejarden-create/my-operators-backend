import "dotenv/config";
import fs from "fs";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";

const apply = JSON.parse(
  fs.readFileSync("reports/brand-explorer-lane2-image-materialization.json", "utf8")
);
const auto = apply.applyResult?.resultsByBrand?.["autograph-collection"];
console.log("auto applied", auto?.applied);
console.log("errors", auto?.results?.errors);
console.log(
  "updated",
  auto?.results?.presentationUpdated?.length,
  "created",
  auto?.results?.presentationCreated?.length
);

const sampleIds = [
  ...(auto?.results?.presentationUpdated || []).slice(0, 2).map((r) => r.recordId),
  ...(auto?.results?.presentationCreated || []).slice(0, 2).map((r) => r.recordId),
  "reclHRYT05I4VkWJN",
];

for (const id of [...new Set(sampleIds)].filter(Boolean)) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const j = await res.json();
  console.log(
    id,
    j.fields?.["Slot Key"],
    "Image?",
    Boolean(j.fields?.Image),
    JSON.stringify(j.fields?.Image)?.slice(0, 160)
  );
}

// Force patch gallery.1 with pack URL
const pack = JSON.parse(
  fs.readFileSync("reports/brand-explorer-lane2-image-asset-pack.json", "utf8")
);
const brand = pack.brandResults.find((b) => b.brandSlug === "autograph-collection");
const g1 = brand.visualAssetPack.galleryCandidates[0];
console.log("pack g1", g1.imageUrl.slice(0, 100));
const patch = await fetch(
  `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/reclHRYT05I4VkWJN`,
  {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: { Image: [{ url: g1.imageUrl }] } }),
  }
);
const pj = await patch.json();
console.log("force patch", patch.status, Boolean(pj.fields?.Image), pj.error);
