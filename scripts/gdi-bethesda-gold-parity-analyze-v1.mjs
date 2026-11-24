#!/usr/bin/env node
/**
 * Post-replay Bethesda parity analysis + dual-pane HTML ($0 diagnostic).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports", "gdi-bethesda-gold-standard-replay-v1");
const HOTEL_ID = "recLuxvwwxID7U2B8";
const GOLD_VERSION = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const GOLD_PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL_ID,
  GOLD_VERSION,
  "package.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function extractUrlsFromBlob(opp) {
  const urls = new Set();
  const blob = JSON.stringify(opp);
  for (const m of blob.match(/https?:\/\/[^\\"'\s<>]+/g) || []) {
    const clean = m.replace(/[),.;]+$/, "");
    if (/^https?:\/\//i.test(clean)) urls.add(clean);
  }
  return [...urls];
}

function rehydrateForFqo(opp) {
  const urls = extractUrlsFromBlob(opp);
  const copy = {
    ...opp,
    sourceUrls: Array.isArray(opp.sourceUrls) && opp.sourceUrls.length
      ? opp.sourceUrls
      : urls.slice(0, 12),
    // Prefer presentation narrative fields for fit/action if thesis missing
    hotelOpportunityThesis:
      opp.hotelOpportunityThesis ||
      opp.whyThisHotel ||
      opp.summaryWhat ||
      opp.whatIsHappening ||
      "",
    recommendedNextStep:
      opp.recommendedNextStep ||
      opp.recommendedNextAction ||
      opp.recommendedAction ||
      opp.cardWhyNowLine ||
      "",
    eventStartDate: opp.eventStartDate || opp.tileEventStartDate || null,
    futureDemandPathway:
      opp.futureDemandPathway === true ||
      Boolean(opp.tileEventStartDate) ||
      /202[6-9]|2030|next|future|overflow|recurring/i.test(
        String(opp.summaryWhat || opp.cardWhyNowLine || ""),
      ),
  };
  return copy;
}

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}

const goldPkg = JSON.parse(fs.readFileSync(GOLD_PKG, "utf8"));
const hwSummary = JSON.parse(
  fs.readFileSync(path.join(OUT, "HOTEL_WIDE_REPLAY_SUMMARY.json"), "utf8"),
);
const candidatePath = hwSummary.stagedCandidate?.path;
const candidatePkg = candidatePath && fs.existsSync(candidatePath)
  ? JSON.parse(fs.readFileSync(candidatePath, "utf8"))
  : null;

const manifestPath = path.join(OUT, "hotel-wide-run", "HOTEL_COMPLETION_MANIFEST.json");
const manifest = fs.existsSync(manifestPath)
  ? JSON.parse(fs.readFileSync(manifestPath, "utf8"))
  : null;

const dispositionById = new Map();
for (const row of manifest?.opportunityDispositions || manifest?.dispositionsDetail || []) {
  const id = text(row.opportunityId || row.id || row.canonicalId);
  if (id) dispositionById.set(id, row);
}
// Fallback: scan accounted rows in manifest
if (dispositionById.size === 0 && Array.isArray(manifest?.rows)) {
  for (const row of manifest.rows) {
    dispositionById.set(text(row.opportunityId || row.id), row);
  }
}
if (dispositionById.size === 0 && manifest?.inventory?.records) {
  for (const row of manifest.inventory.records) {
    dispositionById.set(text(row.id || row.opportunityId), row);
  }
}

// Parse full manifest for per-opportunity records
let hwRecords = [];
if (Array.isArray(manifest?.opportunities)) hwRecords = manifest.opportunities;
else if (Array.isArray(manifest?.reconciliation?.records)) {
  hwRecords = manifest.reconciliation.records;
} else {
  // Search common shapes
  for (const [k, v] of Object.entries(manifest || {})) {
    if (Array.isArray(v) && v[0]?.disposition && (v[0]?.opportunityId || v[0]?.id)) {
      hwRecords = v;
      break;
    }
  }
}

for (const row of hwRecords) {
  dispositionById.set(text(row.opportunityId || row.id || row.canonicalId), row);
}

const candidateById = new Map();
for (const o of candidatePkg?.opportunities || []) {
  candidateById.set(text(o.id || o.opportunityId), o);
}

const fqoRaw = [];
const fqoRehydrated = [];
const parityRows = [];

for (const opp of goldPkg.opportunities) {
  const id = text(opp.id);
  const rawEval = evaluateFinalizedQualifiedOpportunity(opp);
  const reh = rehydrateForFqo(opp);
  const rehEval = evaluateFinalizedQualifiedOpportunity(reh);
  fqoRaw.push({
    id,
    pass: rawEval.finalizedQualified,
    reasons: rawEval.reasons,
  });
  fqoRehydrated.push({
    id,
    pass: rehEval.finalizedQualified,
    reasons: rehEval.reasons,
    gates: rehEval.gates,
    rehydratedUrlCount: reh.sourceUrls.length,
  });

  const hw = dispositionById.get(id);
  const cand = candidateById.get(id);
  const goldPrimary = text(opp.primaryContact?.name);
  const goldSecondary = text(opp.secondaryContact?.name);
  const candPrimary = text(cand?.primaryContact?.name);
  const candSecondary = text(cand?.secondaryContact?.name);
  const goldEmail = text(opp.primaryContact?.email);
  const candEmail = text(cand?.primaryContact?.email);
  const goldType = text(opp.opportunityType || opp.opportunityTypeLabel);
  const candType = text(cand?.opportunityType || cand?.opportunityTypeLabel);
  const goldPill = text(opp.salesActionPillLabel || opp.tileBadgePlan?.salesActionPillLabel);
  const candPill = text(cand?.salesActionPillLabel || cand?.salesActionKey);
  const goldAction = text(
    opp.recommendedNextAction || opp.recommendedNextStep || opp.recommendedAction,
  );
  const candAction = text(cand?.recommendedNextStep || cand?.recommendedNextAction);

  let disposition = "PARITY_PASS";
  const reasons = [];
  const notes = [];

  if (!rawEval.finalizedQualified && rawEval.reasons.includes("MISSING_EVIDENCE_URLS")) {
    reasons.push("FQO_TOPLEVEL_SOURCEURLS_MISSING_ON_GOLD_PACKAGE");
  }
  if (rehEval.finalizedQualified && !rawEval.finalizedQualified) {
    notes.push("REHYDRATION_WOULD_PASS_FQO");
  }
  if (!rehEval.finalizedQualified) {
    reasons.push(...(rehEval.reasons || []));
  }

  if (!cand && !hw) {
    disposition = "TECHNICAL_REGRESSION";
    reasons.push("GOLD_ID_UNACCOUNTED_IN_HOTEL_WIDE");
  } else if (!cand) {
    const hwDisp = text(hw?.disposition || hw?.auditDisposition || "");
    if (/PREVIOUSLY_APPROVED_REVIEW/i.test(hwDisp)) {
      disposition = "TECHNICAL_REGRESSION";
      reasons.push("PREVIOUSLY_APPROVED_NOW_FQO_FAIL");
      notes.push(hwDisp);
    } else if (/HISTORICAL/i.test(hwDisp)) {
      disposition = "GENUINE_COMMERCIAL_CHANGE";
      notes.push(hwDisp);
    } else if (/HOLD/i.test(hwDisp)) {
      disposition = rehEval.finalizedQualified
        ? "TECHNICAL_REGRESSION"
        : "RESEARCH_COMPLETION_REQUIRED";
      notes.push(hwDisp);
    } else if (/REJECT/i.test(hwDisp)) {
      disposition = "GENUINE_COMMERCIAL_CHANGE";
      notes.push(hwDisp);
    } else {
      disposition = "TECHNICAL_REGRESSION";
      notes.push(hwDisp || "NOT_IN_CANDIDATE");
    }
  } else {
    // In candidate
    if (goldPrimary && !candPrimary) {
      disposition = "TECHNICAL_REGRESSION";
      reasons.push("PRIMARY_LOST");
    } else if (
      goldPrimary &&
      candPrimary &&
      goldPrimary.toLowerCase() !== candPrimary.toLowerCase()
    ) {
      disposition = "GENUINE_COMMERCIAL_CHANGE";
      reasons.push("PRIMARY_CHANGED");
    }
    if (goldSecondary && !candSecondary) {
      reasons.push("SECONDARY_LOST");
      if (disposition === "PARITY_PASS") disposition = "TECHNICAL_REGRESSION";
    }
    if (goldEmail && !candEmail) {
      reasons.push("EMAIL_CHANNEL_LOST");
      if (disposition === "PARITY_PASS") disposition = "TECHNICAL_REGRESSION";
    }
    if (goldType && candType && goldType !== candType) {
      reasons.push("OPPORTUNITY_TYPE_CHANGED");
      if (/other|unknown|generic/i.test(candType)) {
        disposition = "TECHNICAL_REGRESSION";
        reasons.push("GENERIC_TYPE");
      } else if (disposition === "PARITY_PASS") {
        disposition = "GENUINE_COMMERCIAL_CHANGE";
      }
    }
    if (goldPill && candPill && goldPill !== candPill) {
      reasons.push("SALES_PILL_CHANGED");
      if (disposition === "PARITY_PASS") disposition = "IMPROVED";
    }
    if (goldAction && !candAction) {
      reasons.push("RECOMMENDATION_LOST");
      if (disposition === "PARITY_PASS") disposition = "TECHNICAL_REGRESSION";
    }
    if (
      disposition === "PARITY_PASS" &&
      notes.includes("REHYDRATION_WOULD_PASS_FQO") &&
      !rawEval.finalizedQualified
    ) {
      // Still in candidate via research twin / different path
      disposition = "IMPROVED";
      notes.push("CANDIDATE_PRESENT_DESPITE_GOLD_PACKAGE_FQO_FAIL");
    }
  }

  // If only evidence-url gate on raw gold but rehydrated passes and in candidate → IMPROVED/PARITY
  if (
    cand &&
    rawEval.reasons?.length === 1 &&
    rawEval.reasons[0] === "MISSING_EVIDENCE_URLS" &&
    rehEval.finalizedQualified
  ) {
    if (!reasons.includes("PRIMARY_LOST") && !reasons.includes("EMAIL_CHANNEL_LOST")) {
      disposition = disposition === "TECHNICAL_REGRESSION" && reasons.includes("FQO_TOPLEVEL_SOURCEURLS_MISSING_ON_GOLD_PACKAGE")
        ? "PARITY_PASS"
        : disposition;
      // Root cause is FQO evidence collection against presentation package shape
      if (disposition === "PARITY_PASS") {
        notes.push("GOLD_PRESENTATION_LACKS_TOPLEVEL_SOURCEURLS_BUT_CANDIDATE_RETAINS");
      }
    }
  }

  parityRows.push({
    canonicalId: id,
    title: text(opp.title || opp.displayTitle),
    goldPrimary,
    goldSecondary,
    candidatePrimary: candPrimary || null,
    candidateSecondary: candSecondary || null,
    goldType,
    candidateType: candType || null,
    goldSalesPill: goldPill,
    candidateSalesPill: candPill || null,
    goldHasEmail: Boolean(goldEmail),
    candidateHasEmail: Boolean(candEmail),
    inCandidate: Boolean(cand),
    hotelWideDisposition: text(hw?.disposition || hw?.auditDisposition) || null,
    fqoRawPass: rawEval.finalizedQualified,
    fqoRehydratedPass: rehEval.finalizedQualified,
    fqoRawReasons: rawEval.reasons,
    fqoRehydratedReasons: rehEval.reasons,
    parityDisposition: disposition,
    parityReasons: [...new Set(reasons)],
    notes,
  });
}

const counts = parityRows.reduce((a, r) => {
  a[r.parityDisposition] = (a[r.parityDisposition] || 0) + 1;
  return a;
}, {});

const metrics = {
  originalIdsReconciled: parityRows.length,
  expected67: parityRows.length === 67,
  newCandidateCount: candidatePkg?.opportunityCount ?? 0,
  candidateVersion: candidatePkg?.publicationVersion || null,
  hotelWideDispositions: hwSummary.dispositions,
  fqoOnGoldPresentation: {
    pass: fqoRaw.filter((r) => r.pass).length,
    fail: fqoRaw.filter((r) => !r.pass).length,
  },
  fqoOnGoldRehydratedUrls: {
    pass: fqoRehydrated.filter((r) => r.pass).length,
    fail: fqoRehydrated.filter((r) => !r.pass).length,
    reasonHistogram: fqoRehydrated.reduce((acc, r) => {
      for (const x of r.reasons || []) acc[x] = (acc[x] || 0) + 1;
      return acc;
    }, {}),
  },
  namedPrimaryRetention: {
    goldWithPrimary: goldPkg.opportunities.filter((o) => o.primaryContact?.name).length,
    goldIdsInCandidateWithPrimary: parityRows.filter(
      (r) => r.inCandidate && r.candidatePrimary,
    ).length,
    primaryLostWhenInCandidate: parityRows.filter((r) =>
      r.parityReasons.includes("PRIMARY_LOST"),
    ).length,
  },
  secondaryRetention: {
    goldWithSecondary: goldPkg.opportunities.filter((o) => o.secondaryContact?.name)
      .length,
    secondaryLost: parityRows.filter((r) => r.parityReasons.includes("SECONDARY_LOST"))
      .length,
  },
  publicChannelRetention: {
    goldWithEmail: parityRows.filter((r) => r.goldHasEmail).length,
    emailLostInCandidate: parityRows.filter((r) =>
      r.parityReasons.includes("EMAIL_CHANNEL_LOST"),
    ).length,
  },
  dispositionCounts: counts,
  technicalRegressionCount: counts.TECHNICAL_REGRESSION || 0,
  researchCompletionRequired: counts.RESEARCH_COMPLETION_REQUIRED || 0,
  genuineCommercialChange: counts.GENUINE_COMMERCIAL_CHANGE || 0,
  parityPass: counts.PARITY_PASS || 0,
  improved: counts.IMPROVED || 0,
};

writeJson("FQO_ON_GOLD_REHYDRATED.json", {
  pass: metrics.fqoOnGoldRehydratedUrls.pass,
  fail: metrics.fqoOnGoldRehydratedUrls.fail,
  reasonHistogram: metrics.fqoOnGoldRehydratedUrls.reasonHistogram,
  rows: fqoRehydrated,
});

writeJson("BETHESDA_67_REPLAY_PARITY_MATRIX.json", {
  hotelId: HOTEL_ID,
  goldVersion: GOLD_VERSION,
  candidateVersion: metrics.candidateVersion,
  metrics,
  rows: parityRows,
});

writeJson("REPLAY_STAGED_CANDIDATE_PACKAGE.json", {
  publicationVersion: candidatePkg?.publicationVersion,
  opportunityCount: candidatePkg?.opportunityCount,
  lifecycleState: candidatePkg?.lifecycleState,
  opportunityIds: (candidatePkg?.opportunities || []).map((o) => o.id),
  pointerActivated: false,
  path: candidatePath,
});

// Dual-pane HTML for isolated preview
const sampleIds = parityRows.slice(0, 12).map((r) => r.canonicalId);
const goldById = new Map(goldPkg.opportunities.map((o) => [text(o.id), o]));
const cards = sampleIds.map((id) => {
  const g = goldById.get(id);
  const c = candidateById.get(id);
  const p = parityRows.find((r) => r.canonicalId === id);
  return { id, gold: g, candidate: c || null, parity: p };
});

function cardHtml(side, opp, missing) {
  if (!opp) {
    return `<div class="card missing"><h3>${side}</h3><p class="warn">${missing || "Not in package"}</p></div>`;
  }
  const pc = opp.primaryContact || {};
  const sc = opp.secondaryContact || {};
  return `<div class="card">
    <h3>${side}</h3>
    <div class="title">${text(opp.title || opp.displayTitle)}</div>
    <div class="pills">
      <span>${text(opp.priorityPillLabel || opp.priority || opp.tileBadgePlan?.priorityPillLabel)}</span>
      <span>${text(opp.salesActionPillLabel || opp.tileBadgePlan?.salesActionPillLabel)}</span>
      <span>${text(opp.opportunityTypeLabel || opp.opportunityType)}</span>
    </div>
    <div class="meta"><b>Dates:</b> ${text(opp.eventDateDisplay || opp.tileEventStartDate)}</div>
    <div class="meta"><b>Primary:</b> ${text(pc.name)} — ${text(pc.role || pc.title)}</div>
    <div class="meta"><b>Channels:</b> ${text(pc.email) || "—"} · ${text(pc.phone) || "—"} · ${text(pc.linkedinUrl) || "—"}</div>
    <div class="meta"><b>Secondary:</b> ${text(sc.name) || "—"}</div>
    <div class="meta"><b>Action:</b> ${text(opp.recommendedNextAction || opp.recommendedNextStep || opp.recommendedAction || opp.cardWhyNowLine).slice(0, 280)}</div>
    <div class="meta"><b>Thesis:</b> ${text(opp.summaryWhat || opp.whatIsHappening).slice(0, 280)}</div>
  </div>`;
}

const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Bethesda Gold vs E2E Replay — Isolated Preview</title>
<style>
  :root { --ink:#1a1a1a; --muted:#5c5c5c; --line:#d8d4cc; --bg:#f7f4ef; --warn:#8a1c1c; --ok:#1c5a3a; }
  body { margin:0; font-family:"Source Serif 4", Georgia, serif; background:var(--bg); color:var(--ink); }
  header { padding:20px 28px; border-bottom:1px solid var(--line); background:#fff; }
  header h1 { margin:0 0 6px; font-size:22px; font-weight:600; }
  header p { margin:0; color:var(--muted); font-size:14px; }
  .metrics { display:flex; flex-wrap:wrap; gap:10px; padding:16px 28px; }
  .metric { background:#fff; border:1px solid var(--line); padding:10px 14px; font-size:13px; }
  .row { display:grid; grid-template-columns:1fr 1fr; gap:16px; padding:0 28px 28px; }
  .card { background:#fff; border:1px solid var(--line); padding:16px; min-height:220px; }
  .card.missing { border-color:#c9a0a0; }
  .card h3 { margin:0 0 8px; font-size:12px; letter-spacing:.06em; text-transform:uppercase; color:var(--muted); }
  .title { font-size:16px; font-weight:600; margin-bottom:10px; }
  .pills span { display:inline-block; border:1px solid var(--line); padding:2px 8px; margin:0 6px 6px 0; font-size:11px; }
  .meta { font-size:13px; color:var(--muted); margin:6px 0; line-height:1.35; }
  .meta b { color:var(--ink); font-weight:600; }
  .warn { color:var(--warn); }
  .parity { grid-column:1/-1; font-size:12px; color:var(--muted); margin:-8px 0 12px; }
  .pair { display:contents; }
</style>
</head>
<body>
<header>
  <h1>Bethesda Marriott — Gold 67 vs Current E2E Replay</h1>
  <p>Isolated diagnostic preview · port ≠ 8080 · pointers unchanged · $0 · candidate ${text(candidatePkg?.publicationVersion) || "(none)"}</p>
</header>
<section class="metrics">
  <div class="metric">Gold: 67</div>
  <div class="metric">Candidate: ${metrics.newCandidateCount}</div>
  <div class="metric">FQO raw pass: ${metrics.fqoOnGoldPresentation.pass}</div>
  <div class="metric">FQO rehydrated pass: ${metrics.fqoOnGoldRehydratedUrls.pass}</div>
  <div class="metric">PARITY_PASS: ${metrics.parityPass}</div>
  <div class="metric">TECHNICAL_REGRESSION: ${metrics.technicalRegressionCount}</div>
  <div class="metric">GENUINE_CHANGE: ${metrics.genuineCommercialChange}</div>
  <div class="metric">RESEARCH_REQUIRED: ${metrics.researchCompletionRequired}</div>
</section>
${cards
  .map(({ id, gold, candidate, parity }) => {
    return `<div class="row">
      <div class="parity"><b>${id}</b> · ${parity?.parityDisposition} · ${(parity?.parityReasons || []).join(", ") || "—"}</div>
      ${cardHtml("LEFT — Approved historical", gold)}
      ${cardHtml("RIGHT — Current E2E replay", candidate, "Absent from STAGED candidate (FQO fail / HOLD / review)")}
    </div>`;
  })
  .join("\n")}
</body>
</html>`;

fs.writeFileSync(path.join(OUT, "ISOLATED_DUAL_PANE_PREVIEW.html"), html);

console.log(JSON.stringify({ ok: true, metrics, html: path.join(OUT, "ISOLATED_DUAL_PANE_PREVIEW.html") }, null, 2));
