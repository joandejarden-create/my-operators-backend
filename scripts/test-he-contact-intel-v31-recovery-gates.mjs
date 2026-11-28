#!/usr/bin/env node
/**
 * HE Contact Intelligence V3.1 recovery gates (static + chain contract).
 * Live browser click-through is certified separately via diagnose-he-contact-enrich-click-v31.mjs.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const js = fs.readFileSync(
  path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
  "utf8",
);
const css = fs.readFileSync(
  path.join(ROOT, "public/css/hotel-contact-intelligence.css"),
  "utf8",
);
const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");

const GATES = {
  HE_CONTACT_ENRICH_BUTTON_TWO_LINE: /FIND_LABEL_HTML|hci-enrich-btn__line|hci-btn--enrich-two-line/.test(
    js + css,
  ),
  HE_ENRICHMENT_TOAST_OUTSIDE_TABLE:
    /showEnrichToast|hci-enrich-loading-toast/.test(js) &&
    /hci-enrich-loading-toast/.test(css) &&
    !/slot\.innerHTML = waveHtml\("Finding Contact Details/.test(js),
  HE_SURFE_SESSION_RESULT_DISPLAY:
    /_sessionSurfeByPerson/.test(js) &&
    /Available for this session/.test(js) &&
    /gdi-list-session-tag/.test(js + css) &&
    /paintSurfeFallbackOffer/.test(js),
  HE_SURFE_NO_AIRTABLE_PERSISTENCE:
    /persist:\s*false/.test(js) &&
    /data-persist="forbidden"|persist_forbidden|NOT_WRITTEN/.test(js),
  HE_HYBRID_ROUTE_REGISTERED:
    /enrich-hybrid/.test(server) && /postContactIntelligenceHybridEnrich/.test(server),
  HE_SURFE_SUCCESS_CHAIN_VISIBLE:
    // Successful Surfe payload must refresh channel stack (not only a detached result slot).
    /_sessionSurfeByPerson[\s\S]{0,2000}renderVerifiedChannelSummary/.test(js) &&
    /paintSurfeFallbackOffer/.test(js) &&
    /findEnrichButton\(personId/.test(js),
  HE_CONTACT_DETAILS_FULL_TEXT_VISIBLE:
    /overflow-wrap:\s*anywhere/.test(css) &&
    !/gdi-list-contact-channels[\s\S]{0,120}overflow:\s*hidden/.test(css),
};

const failed = Object.entries(GATES).filter(([, ok]) => !ok);
const out = {
  at: new Date().toISOString(),
  version: "he-contact-intel-v31-recovery",
  gates: Object.fromEntries(
    Object.entries(GATES).map(([k, ok]) => [k, ok ? "PASS" : "FAIL"]),
  ),
};
fs.mkdirSync(
  path.join(ROOT, "reports/hotel-explorer/he-contact-intel-v31-recovery"),
  { recursive: true },
);
fs.writeFileSync(
  path.join(
    ROOT,
    "reports/hotel-explorer/he-contact-intel-v31-recovery/V31_RECOVERY_GATES.json",
  ),
  JSON.stringify(out, null, 2),
);
console.log(JSON.stringify(out, null, 2));
assert.equal(failed.length, 0, `Failed gates: ${failed.map(([k]) => k).join(", ")}`);
console.log("PASS — HE Contact Intelligence V3.1 recovery gates");
