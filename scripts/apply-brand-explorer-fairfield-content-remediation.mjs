#!/usr/bin/env node
/**
 * Apply Fairfield pilot content remediation to the full presentation fixture.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import { remediateFairfieldFixtureRows } from "../lib/partner-intelligence/brand-explorer-fairfield-content-remediation.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE = path.join(ROOT, "fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json");

const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const { rows, changed } = remediateFairfieldFixtureRows(fixture.rows || []);
fixture.rows = rows;
fs.writeFileSync(FIXTURE, `${JSON.stringify(fixture, null, 2)}\n`, "utf8");
console.log(JSON.stringify({ fixture: FIXTURE, changedCount: changed.length, changed }, null, 2));
