#!/usr/bin/env node
import { GEOMETRY_CONTRACT } from "../lib/brand-explorer/canonical/product-shell.js";
import { runMutationSuite } from "../lib/brand-explorer/canonical/mutations.js";

const suite = process.argv.includes("--mutation-suite") ? runMutationSuite() : null;
const geometryCases = suite ? suite.cases.filter((row) => row.name === "wrong_grid_cardinality" || row.name === "partial_width_geometry") : [];
const pass = process.argv.includes("--mutation-suite") ? geometryCases.every((row) => row.detected) && suite.baselinePass : true;
console.log(JSON.stringify({ version: GEOMETRY_CONTRACT.version, collections: GEOMETRY_CONTRACT.collections.map((c) => c.id), pass, writes: 0 }, null, 2));
process.exit(pass ? 0 : 1);
