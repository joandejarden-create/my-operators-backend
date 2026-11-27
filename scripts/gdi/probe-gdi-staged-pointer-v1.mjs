#!/usr/bin/env node
import { createGdiPublicationPackageStore } from "../../lib/group-demand-intelligence/publication-package/index.js";

const hotelId = String(process.argv[2] || "").trim();
if (!/^rec[a-zA-Z0-9]+$/.test(hotelId)) {
  console.log(JSON.stringify({ ok: false, error: "hotelId_required" }));
  process.exit(2);
}
const store = createGdiPublicationPackageStore({});
const staged = store.readPointer(hotelId, "STAGED");
const current = store.readPointer(hotelId, "CURRENT");
let count = null;
if (staged?.publicationVersion) {
  const loaded = store.loadPackage(hotelId, staged.publicationVersion);
  count = loaded.ok
    ? loaded.package.opportunityCount ||
      (loaded.package.opportunities || []).length
    : null;
}
const payload = {
  ok: Boolean(staged?.publicationVersion) && Number(count) > 0,
  hotelId,
  stagedVersion: staged?.publicationVersion || null,
  stagedCount: count,
  currentVersion: current?.publicationVersion || null,
};
console.log(JSON.stringify(payload, null, 2));
process.exit(payload.ok ? 0 : 10);
