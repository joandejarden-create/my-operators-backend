import dotenv from "dotenv";
dotenv.config();
import { getBrandLibraryBrandById } from "../api/brand-library.js";

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
const meta = brand?.brandExplorerDisplayMeta || brand?.displayMeta || brand?.brandExplorer?.displayMeta;
console.log("meta", JSON.stringify(meta, null, 2));
console.log(
  "flags",
  JSON.stringify(
    {
      shouldRenderFullProfile: brand.shouldRenderFullProfile,
      brandExplorerDisplayState: brand.brandExplorerDisplayState,
      publicDisplayState: brand.publicDisplayState,
      readiness: brand.readiness,
      displayQuality: brand.displayQuality,
      explorerOs: brand.explorerOs || brand.brandExplorerOs,
      gating: brand.gating,
    },
    null,
    2
  )
);
// dump any nested gate objects
function findKeys(o, pred, path = "", out = [], depth = 0) {
  if (!o || typeof o !== "object" || depth > 6) return out;
  for (const [k, v] of Object.entries(o)) {
    const p = path ? `${path}.${k}` : k;
    if (pred(k, v)) out.push({ p, v: typeof v === "object" ? JSON.stringify(v).slice(0, 200) : v });
    if (v && typeof v === "object" && !Array.isArray(v)) findKeys(v, pred, p, out, depth + 1);
  }
  return out;
}
const hits = findKeys(brand, (k) =>
  /visual|externalOwner|scenario|gallery|defect|gate|ready|approved|founder/i.test(k)
);
console.log(
  "interesting",
  hits.slice(0, 60).map((h) => `${h.p}=${String(h.v).slice(0, 120)}`)
);
