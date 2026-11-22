#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { HIF_TRACK_A_CB5 } from "../lib/hotel-intelligence/sec-edgar/hif-track-a-continuous-foundation-waves-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb5-trigger-first-portfolio");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(HIF_TRACK_A_CB5.includes("cb5"), "CB5 wave id");

const sig = JSON.parse(fs.readFileSync(path.join(OUT, "COMMERCIAL_SIGNALS.json"), "utf8"));
assert(sig.publicationState === "CUSTOMER_BLOCKED", "signals CUSTOMER_BLOCKED");
assert(sig.distinctCommercialSignalsInvestigated >= 100, "need >=100 signals");
assert(sig.stopReason === "RESEARCH_VOLUME_TARGET_MET", "stop reason");

const dossier = JSON.parse(fs.readFileSync(path.join(OUT, "AHIP_PORTFOLIO_MCT_DOSSIER.json"), "utf8"));
assert(dossier.portfolioVsProperty.doNotManufactureIndividualMcts === true, "no per-hotel MCT inflation");
assert(dossier.affectedHotels.length === 19, "19 affected hotels");
assert(dossier.counts.aifMinusConfirmedSold.rooms === 2018, "2018 rooms");
assert(dossier.operatingRelationships.replacementAnnouncement.includes("NONE_PUBLIC"), "no fake replacement");

const cand = JSON.parse(fs.readFileSync(path.join(OUT, "RESEARCH_CANDIDATES.json"), "utf8"));
assert(cand.researchCostUsd === 0, "cost 0");
assert(cand.batchMetrics.incrementalQualifiedFirst100 === 0, "no inflated first-100");
assert(cand.batchMetrics.cumulativeFirst100 === 6, "cumulative 6");

const trackBClosed = cand.candidates.filter((c) =>
  ["trackb-yucatan-resort-historical", "trackb-fairfield-villahermosa-committed", "trackb-hyatt-ziva-zilara-historical"].includes(
    c.id,
  ),
);
assert(trackBClosed.length === 3, "Track B Mexico closed classifications");
assert(trackBClosed.every((c) => c.persistEligible === false), "Track B not falsely persisted as open");

const score = JSON.parse(fs.readFileSync(path.join(OUT, "FIRST_100_SCORECARD.json"), "utf8"));
assert(score.metrics.afterCb5 === 6, "first-100 still 6");
assert(score.metrics.incrementalCb5Qualified === 0, "incremental 0");

const handoff = JSON.parse(fs.readFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), "utf8"));
assert(handoff.handoffs.some((h) => h.parentOpportunityId === "ahip-remaining-portfolio-aimbridge-mct"), "AHIP handoff");

console.log(
  JSON.stringify(
    {
      ok: true,
      signals: sig.distinctCommercialSignalsInvestigated,
      hotels: sig.distinctHotelsAffectedOrIdentified,
      first100: score.metrics.afterCb5,
      ahipRooms: dossier.counts.aifMinusConfirmedSold.rooms,
    },
    null,
    2,
  ),
);
