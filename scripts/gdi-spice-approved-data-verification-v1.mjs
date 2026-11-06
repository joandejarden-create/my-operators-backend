#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const OUT_DIR = path.join("reports", "gdi-spice-presentation-parity-fix-v1");
const report = JSON.parse(
  fs.readFileSync(path.join(OUT_DIR, "01-dry-run-frozen-candidates.json"), "utf8"),
);
const HOTEL = "recKRJjcPnb4tVDDS";
const BASE = process.env.GDI_LOCAL_BASE_URL || "http://localhost:8080";

const drawers = [];
for (const cand of report.candidates) {
  const id = cand.opportunityId;
  const d = await fetch(
    `${BASE}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities/${id}`,
  ).then((r) => {
    if (!r.ok) throw new Error(`${r.status} ${id}`);
    return r.json();
  });
  const o = d.opportunity || d;
  const facts = (o.establishedFacts || o.facts || []).length;
  const unc = (o.uncertainties || o.establishedUncertainties || []).length;
  const qq = (o.qualificationQuestions || o.questions || []).length;
  const sources = (o.sources || o.evidence || []).length;
  const narrative = String(
    o.whatIsHappening || o.tileCustomerSummary || o.summaryWhat || "",
  ).trim();
  const action = String(
    o.recommendedAction || o.recommendedNextStep || "",
  ).trim();
  const primary = o.primaryContact?.name || o.tileContactName || null;
  drawers.push({
    id,
    title: o.displayTitle || o.title,
    frozenAction: cand.salesActionPillLabel,
    frozenPri: cand.priorityFilterKey,
    frozenContact: cand.tileContactName,
    frozenFacts: cand.facts,
    frozenSources: cand.sources,
    localFacts: facts,
    localUncertainties: unc,
    localQuestions: qq,
    localSources: sources,
    localPrimary: primary,
    hasNarrative: narrative.length >= 40,
    hasRecommendedAction: action.length >= 20,
    complete:
      cand.facts >= 3 &&
      cand.sources >= 3 &&
      !!cand.tileContactName &&
      !!cand.salesActionPillLabel &&
      !!cand.priorityFilterKey,
  });
}

const pri = {};
const qf = {};
for (const c of report.candidates) {
  pri[c.priorityFilterKey] = (pri[c.priorityFilterKey] || 0) + 1;
  for (const q of c.salesOpportunityQuickFilterKeys || []) {
    qf[q] = (qf[q] || 0) + 1;
  }
}
const carpha = report.candidates.find(
  (c) => c.opportunityId === "gdi_s4912_ba7b1cb8a3ab",
);
const savor = report.candidates.find(
  (c) => c.opportunityId === "gdi_s4912_03deda917778",
);
const ayesha = report.candidates.find(
  (c) => c.opportunityId === "gdi_s4912_cb2d71ab1b15",
);

const out = {
  ok:
    report.ok === true &&
    drawers.every((d) => d.complete) &&
    pri.MEDIUM_PRIORITY === 2 &&
    (pri.WATCHLIST || 0) === 5 &&
    (qf.HOTEL_SOURCING_NOT_CONFIRMED || 0) === 5 &&
    report.candidates.length === 7,
  dryRunOk: report.ok,
  counts: {
    opportunities: report.candidates.length,
    priority: pri,
    quick: qf,
  },
  gates: {
    carphaContactBuyer:
      carpha?.salesActionPillLabel === "Contact Buyer" &&
      carpha?.priorityFilterKey === "MEDIUM_PRIORITY",
    savorMediumBuildAccount:
      savor?.priorityFilterKey === "MEDIUM_PRIORITY" &&
      savor?.salesActionPillLabel === "Build Account",
    ayeshaPrimary: ayesha?.tileContactName === "A'yesha Gaetán",
    medium2Watch5:
      pri.MEDIUM_PRIORITY === 2 && (pri.WATCHLIST || 0) === 5,
    hotelUnconfirmed5: (qf.HOTEL_SOURCING_NOT_CONFIRMED || 0) === 5,
    sevenIds: report.candidates.length === 7,
    allDrawersComplete: drawers.every((d) => d.complete),
  },
  drawers,
};

fs.writeFileSync(
  path.join(OUT_DIR, "06-approved-data-verification.json"),
  JSON.stringify(out, null, 2),
);
console.log(JSON.stringify({ ok: out.ok, gates: out.gates, counts: out.counts }, null, 2));
process.exit(out.ok ? 0 : 1);
