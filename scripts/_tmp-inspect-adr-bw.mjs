import dotenv from "dotenv";
dotenv.config();
import Airtable from "airtable";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import fs from "node:fs";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const TABLE = "Brand Setup - Brand Explorer Presentation";

const quiet = JSON.parse(
  fs.readFileSync("reports/brand-explorer-public-visibility-quality-lock-quiet.json", "utf8")
);

const fails = quiet.brands.filter((b) => b.failures?.includes("forbidden_owner_facing_language"));
const out = [];
for (const b of fails) {
  for (const h of b.gateResults?.forbidden_owner_facing_language?.hits || []) {
    if (!h.recordId) continue;
    const r = await base(TABLE).find(h.recordId);
    const body = String(r.fields.Body || "");
    const i = body.search(/ADR/i);
    const slice = i >= 0 ? body.slice(Math.max(0, i - 30), i + 40) : body.slice(0, 120);
    const codes =
      i >= 0
        ? [...body.slice(Math.max(0, i - 1), i + 4)].map(
            (c) => `${c}=U+${c.codePointAt(0).toString(16)}`
          )
        : [];
    out.push({
      slug: b.slug,
      recordId: h.recordId,
      slotKey: h.slotKey,
      title: r.fields.Title || "",
      slice,
      codes,
      wordBoundary: /\bADR\b/.test(body),
      anyAdr: /ADR/i.test(body),
      bodyLen: body.length,
    });
  }
}
console.log(JSON.stringify(out, null, 2));

const res = {
  statusCode: 200,
  payload: null,
  setHeader() {},
  status(c) {
    this.statusCode = c;
    return this;
  },
  json(p) {
    this.payload = p;
  },
};
await getBrandLibraryBrandById({ query: { brandId: "recwXZ5gVZ8ZH8ekA" }, headers: {} }, res);
const brand = res.payload?.brand;
const be = brand?.brandExplorer || {};
console.log(
  "\nBW_PREMIER",
  JSON.stringify(
    {
      name: brand?.name,
      slug: brand?.slug,
      shouldRenderFullProfile: brand?.shouldRenderFullProfile ?? be.shouldRenderFullProfile,
      displayState: brand?.brandExplorerDisplayState || be.displayState || brand?.publicDisplayState,
      companyValidated: brand?.companyValidated ?? brand?.governance?.companyValidated,
      activeProfileApproved: brand?.activeProfileApproved,
      founderVisualReviewPass: brand?.founderVisualReviewPass,
      blocks: (be.blocks || brand?.blocks || []).length,
      gateHint: brand?.displayQuality || be.quality || be.displayMeta,
    },
    null,
    2
  )
);

// Find ADR in projected blocks for ascend
const ascendRes = {
  statusCode: 200,
  payload: null,
  setHeader() {},
  status(c) {
    this.statusCode = c;
    return this;
  },
  json(p) {
    this.payload = p;
  },
};
await getBrandLibraryBrandById({ query: { brandId: "ascend" }, headers: {} }, ascendRes);
const ab = ascendRes.payload?.brand;
const blocks = ab?.brandExplorer?.blocks || [];
const adrBlocks = blocks.filter((row) =>
  [row.title, row.body, row.caseSummaryOverview, row.caseSummaryBrandRelevance, row.caseSummaryOwnerObjective, row.caseSummaryInterpretation, row.caseSummaryTags]
    .filter(Boolean)
    .join("\n")
    .match(/\bADR\b/)
);
console.log(
  "\nASCEND_ADR_BLOCKS",
  adrBlocks.map((r) => ({
    slotKey: r.slotKey,
    recordId: r.recordId || r.id,
    snippet: String(r.body || "").match(/.{0,50}\bADR\b.{0,50}/)?.[0],
  }))
);
