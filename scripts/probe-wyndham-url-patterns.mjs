#!/usr/bin/env node
import { extractWyndhamPropertyUrls } from "../lib/wyndham-brand-directory-extract.js";

const result = await extractWyndhamPropertyUrls({ calaOnly: false, delayMs: 50, maxProperties: 200 });
console.log("total sample", result.propertyRows.length);
const regions = new Map();
for (const r of result.propertyRows) {
  regions.set(r.regionSlug, (regions.get(r.regionSlug) || 0) + 1);
}
console.log("top regions", [...regions.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30));
const calaHint = result.propertyRows.filter((r) =>
  /mexico|jamaica|puerto|rico|dominican|colombia|brazil|costa|panama|bahamas|barbados|aruba|curacao|trinidad|bermuda|cayman|peru|chile|argentina|ecuador|guatemala|honduras|salvador|uruguay|paraguay|bolivia|venezuela|cancun|caribbean|santo|domingo|san-juan/i.test(
    r.propertyUrl
  )
);
console.log("cala hint count", calaHint.length);
for (const r of calaHint.slice(0, 15)) console.log(r.propertyUrl);
