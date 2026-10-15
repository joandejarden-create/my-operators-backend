#!/usr/bin/env node
import { COLLECTION_CONTRACT } from "../lib/brand-explorer/canonical/product-shell.js";

console.log(JSON.stringify({
  version: COLLECTION_CONTRACT.version,
  collections: COLLECTION_CONTRACT.collections.map((c) => ({ id: c.id, mode: c.mode, count: c.count || null })),
  writes: 0,
}, null, 2));
