#!/usr/bin/env node
/**
 * Unit tests for pilot semantic content-quality gates.
 */
import { evaluatePilotContentQuality } from "../lib/partner-intelligence/brand-explorer-pilot-content-quality.js";

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

const badRows = [
  {
    slotKey: "Brand Positioning",
    body: "Positioned as a efficient upper-midscale brand.",
    sort: 10,
  },
  {
    slotKey: "footprint.geo_intro",
    body: "International Reference.. Keep Fairfield by Marriott product and service responsibilities clear among owner, operator, and brand teams so the efficient upper-midscale Marriott select-service rooms brand stays deliverable after affiliation and through ongoing operations.",
    sort: 470,
  },
  {
    slotKey: "footprint.openings",
    title: "Fairfield Inn & Suites Cancun Airport Fairfield by Marriott — Cancún",
    body: "teaser",
    sort: 491,
  },
  {
    slotKey: "insight.similar",
    body: "Four Points... not Fairfield prototype SpringHill Suites...",
    sort: 701,
  },
  {
    slotKey: "Guest Psychographics Description",
    body: "Corporate / Business, Leisure, Family. Evaluate demand behavior in each target market rather than generic chain-loyalty averages.",
    sort: 11,
  },
];

const bad = evaluatePilotContentQuality(badRows);
assert(bad.pass === false, "expected bad fixture to fail content quality");
assert(bad.issueCount >= 4, `expected multiple issues, got ${bad.issueCount}`);

const goodRows = [
  {
    slotKey: "Brand Positioning",
    body: "Fairfield by Marriott is positioned as an efficient upper-midscale Marriott select-service rooms brand.",
    sort: 10,
  },
  {
    slotKey: "footprint.openings",
    title: "Fairfield Inn & Suites Cancun Airport — Cancún",
    body: "CALA\n\nCancún\n\nMexico\n\nTeaser",
    sort: 491,
  },
  {
    slotKey: "Guest Psychographics Description",
    body: "Fairfield by Marriott serves practical transient guests—business travelers, families, and leisure visitors who want reliable rooms, straightforward value, and Bonvoy consistency rather than lifestyle-hotel personality.",
    sort: 11,
  },
  {
    slotKey: "insight.similar",
    body: "Courtyard by Marriott sits adjacent but typically carries heavier F&B and meeting programming than Fairfield's rooms-first select-service lane.",
    sort: 700,
  },
];

const good = evaluatePilotContentQuality(goodRows);
assert(good.pass === true, `expected good rows to pass, issues: ${JSON.stringify(good.issues)}`);

console.log("ok brand-explorer-pilot-content-quality");
