#!/usr/bin/env node
import { runActiveUniverseBackscan } from "../lib/brand-explorer/canonical/backscan.js";

const report = await runActiveUniverseBackscan();
console.log(JSON.stringify({ alias: "brand-explorer-factory-backscan", writes: report.writes, activeCount: report.activeCount, cleanCount: report.cleanCount, failingCount: report.failingCount }, null, 2));
process.exit(report.ok ? 0 : 1);
