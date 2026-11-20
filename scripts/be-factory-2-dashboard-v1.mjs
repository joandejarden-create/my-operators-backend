#!/usr/bin/env node
/**
 * Factory 2.0 — refresh portfolio dashboard from durable staging.
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  buildPortfolioDashboard,
  writeJson,
  reportsRoot,
} from "../lib/brand-explorer/factory-2/be-factory-2-staging-store-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

const dashboard = buildPortfolioDashboard(ROOT);
writeJson(join(reportsRoot(ROOT), "04-portfolio-dashboard.json"), dashboard);
console.log(JSON.stringify({ ok: true, summary: dashboard.summary, rows: dashboard.rows.length }, null, 2));
