#!/usr/bin/env node
/**
 * Guest Sentiment Airtable dry-run (no writes).
 * Usage: node scripts/he-guest-sentiment-airtable-dry-run-v1.mjs [--hotel rec…]
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildGuestSentimentAirtableDryRun,
} from "../lib/hotel-intelligence/guest-sentiment/guest-sentiment-staged-store-v1.js";
import {
  proposedAirtableSentimentSchema,
  buildGuestSentimentView,
} from "../lib/hotel-intelligence/guest-sentiment/guest-sentiment-model-v1.js";
import { loadStagedGuestSentimentPacket } from "../lib/hotel-intelligence/guest-sentiment/guest-sentiment-staged-store-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const hpcId =
  (process.argv.find((a) => a.startsWith("--hotel=")) || "").split("=")[1] ||
  "recLuxvwwxID7U2B8";

const dryRun = buildGuestSentimentAirtableDryRun(hpcId);
const schema = proposedAirtableSentimentSchema();
const loaded = loadStagedGuestSentimentPacket(hpcId);
const view = loaded
  ? buildGuestSentimentView(
      { id: hpcId, name: loaded.packet.hotelName },
      loaded.packet,
      { founderPreview: true }
    )
  : null;

const out = {
  generatedAt: new Date().toISOString(),
  applyAuthorized: false,
  paidProvidersCalled: false,
  hpcId,
  schema,
  dryRun,
  previewView: view
    ? {
        visible: view.visible,
        proposedPreview: view.proposedPreview,
        platformCount: (view.platforms || []).length,
        themeCount: (view.themes || []).length,
        trendsAvailable: view.trends?.available === true,
        rightsChip: view.rightsChip,
      }
    : null,
};

const outDir = path.join(ROOT, "reports/hotel-explorer");
fs.mkdirSync(outDir, { recursive: true });
const jsonPath = path.join(outDir, "HE-GUEST-SENTIMENT-AIRTABLE-DRY-RUN-V1.json");
fs.writeFileSync(jsonPath, JSON.stringify(out, null, 2));
console.log(JSON.stringify({ ok: true, jsonPath, applyAuthorized: false }, null, 2));
