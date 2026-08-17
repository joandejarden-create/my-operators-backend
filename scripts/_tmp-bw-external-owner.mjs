import dotenv from "dotenv";
dotenv.config();
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { evaluateExternalOwnerReadinessRule } from "../lib/partner-intelligence/brand-explorer-external-owner-readiness-rules.js";

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
const blocks = res.payload.brand.brandExplorer?.blocks || [];
const rule = evaluateExternalOwnerReadinessRule(blocks);
console.log(JSON.stringify(rule, null, 2));
