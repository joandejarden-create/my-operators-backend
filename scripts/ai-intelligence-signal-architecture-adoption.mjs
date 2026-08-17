#!/usr/bin/env node
/**
 * Write signal architecture adoption report (no holdout, no providers).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildSignalArchitectureAdoptionReport } from "../lib/ai-visibility/signal-architecture/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const outPath = path.join(
  ROOT,
  "data/ai-visibility/validation/signal-architecture-adoption.json"
);

const report = buildSignalArchitectureAdoptionReport();
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n", "utf8");
console.log(`Wrote ${outPath}`);
console.log(`status=${report.status}`);
console.log(`nextStep=${report.nextStep}`);
