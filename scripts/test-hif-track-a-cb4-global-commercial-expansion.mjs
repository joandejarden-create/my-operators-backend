#!/usr/bin/env node
/**
 * CB4 gate — research volume, incremental first-100, taxonomy, CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CB3_QUALIFICATION_CLASSES,
  qualifiesPrincipalFirst100,
  classifyPreferredVsOpenBrandDecision,
  HIF_TRACK_A_CB4,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-a-continuous-foundation-waves-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb4-global-commercial-expansion");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(HIF_TRACK_A_CB4.includes("cb4"), "CB4 wave id missing");

const inv = JSON.parse(fs.readFileSync(path.join(OUT, "INVESTIGATED_HOTELS.json"), "utf8"));
assert(inv.publicationState === "CUSTOMER_BLOCKED", "investigated must be CUSTOMER_BLOCKED");
assert(inv.newDistinctHotelsInvestigated >= 250, `need >=250 new hotels, got ${inv.newDistinctHotelsInvestigated}`);
assert(inv.cumulativeDeduplicatedHotelsInvestigated >= 250, "cumulative too low");
assert(inv.stopReason === "RESEARCH_VOLUME_TARGET_MET", "stop reason");

const cand = JSON.parse(fs.readFileSync(path.join(OUT, "RESEARCH_CANDIDATES.json"), "utf8"));
assert(cand.publicationState === "CUSTOMER_BLOCKED", "candidates CUSTOMER_BLOCKED");
assert(cand.researchCostUsd === 0, "no paid research");

const ahip = cand.candidates.find((c) => c.id === "ahip-remaining-portfolio-aimbridge-mct");
assert(ahip, "AHIP candidate required");
assert(ahip.countsTowardFirst100 === true, "AHIP counts toward first-100");
assert(ahip.opportunityType === "MCT", "AHIP is MCT");
assert(qualifiesPrincipalFirst100(ahip) === true, "AHIP must qualify principal first-100");

const rljBoston = cand.candidates.find((c) => c.id === "rlj-wyndham-boston-tapestry-committed");
assert(rljBoston, "RLJ Boston required");
const pref = classifyPreferredVsOpenBrandDecision(rljBoston);
assert(pref.countsTowardFirst100 === false, "committed Tapestry must not count first-100");
assert(
  pref.class === CB3_QUALIFICATION_CLASSES.INTRA_BRAND_FAMILY_DECISION ||
    rljBoston.qualificationClass === CB3_QUALIFICATION_CLASSES.COMMITTED_TRANSACTION,
  "RLJ Boston not open competitive",
);

const score = JSON.parse(fs.readFileSync(path.join(OUT, "FIRST_100_SCORECARD.json"), "utf8"));
assert(score.metrics.afterCb4 === 6, "cumulative first-100 must be 6");
assert(score.metrics.incrementalCb4Qualified === 1, "incremental qualified must be 1");
assert(score.incrementalQualifiedIds.includes("ahip-remaining-portfolio-aimbridge-mct"), "AHIP incremental id");

const handoff = JSON.parse(fs.readFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), "utf8"));
assert(handoff.handoffs.length >= 4, "Track C handoffs required");

const he = JSON.parse(fs.readFileSync(path.join(OUT, "HOTEL_EXPLORER_RESEARCH_PRIORITIES.json"), "utf8"));
assert(he.priorities.length >= 4, "HE priorities required");

console.log(
  JSON.stringify(
    {
      ok: true,
      newHotels: inv.newDistinctHotelsInvestigated,
      cumulative: inv.cumulativeDeduplicatedHotelsInvestigated,
      first100: score.metrics.afterCb4,
      incremental: score.metrics.incrementalCb4Qualified,
    },
    null,
    2,
  ),
);
