#!/usr/bin/env node
import { runBrandExplorerDoctor } from "../lib/brand-explorer/canonical/doctor.js";

const report = runBrandExplorerDoctor();
console.log(JSON.stringify({ pass: report.pass, auditEnabled: report.auditEnabled, materializationEnabled: report.materializationEnabled, failed: report.failed, lessons: report.lessons, mutations: report.mutations ? { pass: report.mutations.pass, failed: report.mutations.failed, baselinePass: report.mutations.baselinePass } : null, writes: 0 }, null, 2));
process.exit(report.pass ? 0 : 1);
