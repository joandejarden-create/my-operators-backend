#!/usr/bin/env node
import { runMutationSuite } from "../lib/brand-explorer/canonical/mutations.js";

const suite = runMutationSuite();
const needed = suite.cases.filter((row) => row.name === "collection_underpopulation" || row.name === "property_multi_identity");
const pass = suite.baselinePass && needed.every((row) => row.detected);
console.log(JSON.stringify({ pass, needed, writes: 0 }, null, 2));
process.exit(pass ? 0 : 1);
