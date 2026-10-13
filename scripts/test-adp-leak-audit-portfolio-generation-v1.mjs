#!/usr/bin/env node
/**
 * npm run test:adp-leak-audit-portfolio-generation-v1
 */
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import {
  createLeakAuditStore,
  generatePortfolioLeakAuditReport,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const base = join(process.cwd(), "data/ai-demand-positioning/leak-audit/_tmp");
mkdirSync(base, { recursive: true });
const tmp = mkdtempSync(join(base, "port-test-"));
const store = createLeakAuditStore({ root: tmp });
const result = generatePortfolioLeakAuditReport(store, {
  portfolioName: "Dovetail Demo Portfolio",
  hotels: [
    { hotelName: "Hotel A", city: "City A", country: "X" },
    { hotelName: "Hotel B", city: "City B", country: "Y" },
    { hotelName: "Hotel C", city: "City C", country: "Z" },
  ],
});

assert.equal(result.ok, true);
assert.equal(result.hotelLinks.length, 3);
assert.equal(result.portfolioRun.hotelCount, 3);
assert.match(result.shareUrl, /\/adp-leak-audit\/share\//);
assert.ok(result.portfolioReport.shareToken);

const byToken = store.getPortfolioReportByShareToken(result.portfolioReport.shareToken);
assert.equal(byToken.id, result.portfolioReport.id);
const blob = JSON.stringify(result.portfolioReport);
assert.equal(/\brec[A-Za-z0-9]{14}\b/.test(blob), false);
assert.equal(/fullPromptText/.test(blob), false);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_PORTFOLIO_GENERATION_V1",
    hotels: 3,
    shareToken: true,
  })
);
