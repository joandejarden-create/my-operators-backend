#!/usr/bin/env node
/**
 * $0 reassessment of Checkpoint A WHO/HOW recoveries for future pathways.
 * No paid APIs. Preserves WHO/HOW intelligence. Does not mutate locked 26.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { evaluateCommercialViabilityPrecheck } from "../../lib/group-demand-intelligence/workflow/gdi-commercial-viability-precheck-v1.js";
import { COMMERCIAL_VIABILITY_DECISION } from "../../lib/group-demand-intelligence/workflow/gdi-commercial-viability-precheck-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "../..");
const outDir = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-b",
);
const wave2 = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a/wave2/CHECKPOINT_A_LIVE_RESULT.json",
);
const stage4 = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a/20-STAGE4_ZERO_DOLLAR_CHANNELS.json",
);
const stage5 = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a/21-STAGE5_FQO_REASSESSMENT.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

const live = JSON.parse(fs.readFileSync(wave2, "utf8"));
const s4 = JSON.parse(fs.readFileSync(stage4, "utf8"));
const s5 = JSON.parse(fs.readFileSync(stage5, "utf8"));

const stage4ById = Object.fromEntries(
  (s4.rows || []).map((r) => [r.opportunityId, r]),
);
const stage5ById = Object.fromEntries(
  (s5.rows || []).map((r) => [r.opportunityId, r]),
);

const FUTURE_HINT =
  /2027|2028|2029|annual|recurring|overflow|housing bureau|preferred hotel|partner hotel|next edition|training|regional meeting|continuation/i;

const rows = [];
for (const r of live.tranche.results) {
  const opp = {
    ...(r.opportunity || {}),
    id: r.opportunityId,
    whoHow: r.whoHow,
  };
  const evidenceText = [
    opp.title,
    opp.organizationName,
    ...(r.whoHow?.evidence || []).map((e) => e.claim || ""),
  ].join(" ");

  const pathwayHints = [];
  if (/2027|2028|2029/.test(evidenceText)) pathwayHints.push("FUTURE_EDITION_YEAR");
  if (/annual|recurring/i.test(evidenceText)) pathwayHints.push("RECURRING_ASSOCIATION");
  if (/regional|chapter/i.test(evidenceText)) pathwayHints.push("REGIONAL_MEETING");
  if (/training|corporate/i.test(evidenceText)) pathwayHints.push("CORPORATE_TRAINING");
  if (/overflow|comp(?:etitor)? hotel|displacement/i.test(evidenceText)) {
    pathwayHints.push("COMPETITIVE_OVERFLOW");
  }
  if (/preferred|housing|partner hotel|room block/i.test(evidenceText)) {
    pathwayHints.push("OFFICIAL_HOUSING_OR_PARTNER");
  }
  if (/project|construction|crew/i.test(evidenceText)) pathwayHints.push("LOCAL_PROJECT");

  if (pathwayHints.length) {
    opp.futureDemandPathway = true;
  }

  const viability = evaluateCommercialViabilityPrecheck(opp, {});
  const s4row = stage4ById[r.opportunityId] || {};
  const s5row = stage5ById[r.opportunityId] || {};

  rows.push({
    opportunityId: r.opportunityId,
    organizationName: opp.organizationName || r.input?.organizationName,
    title: opp.title,
    primaryWhoHowStatus: r.whoHow?.whoHowStatus,
    linkedInChannelPopulated: s4row.linkedInChannelPopulated === true,
    stage5Classification: s5row.classification || null,
    pastEventRisk: s5row.pastEventRisk === true,
    pathwayHints,
    hasFutureHintLanguage: FUTURE_HINT.test(evidenceText),
    viabilityDecision: viability.decision,
    viabilityReasons: viability.reasons,
    contactResearchWouldAdvance: viability.contactResearchWouldAdvance,
    preserveWhoHow: true,
  });
}

const linkedIn = rows.filter((r) => r.linkedInChannelPopulated);
const namedNoChannel = rows.filter(
  (r) => r.stage5Classification === "NAMED_NO_CHANNEL",
);
const linkedInAndNamedNoChannel = rows.filter(
  (r) => r.linkedInChannelPopulated && r.stage5Classification === "NAMED_NO_CHANNEL",
);
const proceed = rows.filter(
  (r) => r.viabilityDecision === COMMERCIAL_VIABILITY_DECISION.PROCEED_TO_WHO_HOW,
);
const skip = rows.filter(
  (r) =>
    r.viabilityDecision === COMMERCIAL_VIABILITY_DECISION.SKIP_PAID_CONTACT_RESEARCH,
);
const withPathwayHints = rows.filter((r) => r.pathwayHints.length > 0);

const report = {
  version: "gdi-checkpoint-b-future-pathway-reassess-v1",
  paid: false,
  lockedBaseline26Unchanged: true,
  uniqueOpportunities: rows.length,
  reconcile: {
    stage4LinkedInChannelsPopulated: linkedIn.length,
    stage5NamedNoChannel: namedNoChannel.length,
    overlapLinkedInAndNamedNoChannel: linkedInAndNamedNoChannel.length,
    note:
      "21 reachable (Stage4 LinkedIn) and 6 NAMED_NO_CHANNEL are overlapping layers — not additive. Intersection listed explicitly.",
    intersectionIds: linkedInAndNamedNoChannel.map((r) => r.opportunityId),
  },
  viability: {
    proceedToWhoHow: proceed.length,
    skipPaidContactResearch: skip.length,
    withPathwayHints: withPathwayHints.length,
  },
  rows,
};

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "24-FUTURE_PATHWAY_REASSESSMENT.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    {
      ok: true,
      ...report.reconcile,
      viability: report.viability,
      out: path.join(outDir, "24-FUTURE_PATHWAY_REASSESSMENT.json"),
    },
    null,
    2,
  ),
);
