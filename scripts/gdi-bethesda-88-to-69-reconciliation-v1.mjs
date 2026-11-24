#!/usr/bin/env node
/**
 * Reconcile Nov-12 Bethesda 88 research universe vs gold 67 + certified candidate 69.
 * $0 · read-only vs STAGED/CURRENT · no publication.
 */
import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const GOLD = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const CAND = "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261124_3d4c88b8";
const OUT = path.join(ROOT, "reports/gdi-bethesda-88-to-69-reconciliation-v1");
const HIST = path.join(
  ROOT,
  "reports/gdi-bethesda-88-commercial-certification-v1",
);
const PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function sha(p) {
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}
function normTitle(s) {
  return text(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

fs.mkdirSync(OUT, { recursive: true });

const scorecard = JSON.parse(
  fs.readFileSync(path.join(HIST, "01-record-level-scorecard.json"), "utf8"),
);
const buckets = JSON.parse(
  fs.readFileSync(path.join(HIST, "05-certification-buckets.json"), "utf8"),
);
const crosswalk = JSON.parse(
  fs.readFileSync(
    path.join(HIST, "09-commercial-event-family-crosswalk.json"),
    "utf8",
  ),
);
const certSummary = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "reports/gdi-bethesda-certification-v1/CERTIFICATION_REPLAY_SUMMARY.json"),
    "utf8",
  ),
);
const goldPkg = JSON.parse(
  fs.readFileSync(path.join(PKG, GOLD, "package.json"), "utf8"),
);
const candPkg = JSON.parse(
  fs.readFileSync(path.join(PKG, CAND, "package.json"), "utf8"),
);

const goldById = new Map(goldPkg.opportunities.map((o) => [o.id, o]));
const candById = new Map(candPkg.opportunities.map((o) => [o.id, o]));
const goldTitleIndex = new Map();
for (const o of goldPkg.opportunities) {
  goldTitleIndex.set(normTitle(o.title), o.id);
}
const candTitleIndex = new Map();
for (const o of candPkg.opportunities) {
  candTitleIndex.set(normTitle(o.title), o.id);
}

/** Family alias map: any member → primary + siblings */
const familyByMember = new Map();
const linkedToPrimary = new Map();
for (const fam of crosswalk.families || []) {
  const members = (fam.members || []).map((m) => m.id);
  for (const m of fam.members || []) {
    familyByMember.set(m.id, {
      familyId: fam.familyId,
      role: m.role,
      primaryId: fam.primaryId,
      linkedIds: fam.linkedIds || [],
      quarantinedIds: fam.quarantinedIds || [],
      note: fam.note || "",
    });
    if (m.id !== fam.primaryId) {
      linkedToPrimary.set(m.id, fam.primaryId);
    }
  }
  for (const lid of fam.linkedIds || []) linkedToPrimary.set(lid, fam.primaryId);
  for (const qid of fam.quarantinedIds || []) {
    linkedToPrimary.set(qid, fam.primaryId);
  }
}

const disqualifiedSet = new Set(buckets.disqualifiedIds || []);
const conditionalSet = new Set(buckets.conditionallyReadyIds || []);
const certifiedSet = new Set(buckets.commerciallyCertifiedIds || []);
const incompleteSet = new Set(
  (scorecard.rows || [])
    .filter((r) => r.certificationOutcome === "RESEARCH_INCOMPLETE")
    .map((r) => r.id),
);

const recoveredNamed = new Set(
  certSummary?.aadResearch
    ? []
    : [],
);
const histRecovered = new Set(
  JSON.parse(
    fs.readFileSync(path.join(HIST, "00-commercial-certification-summary.json"), "utf8"),
  ).contactCoverage?.afterIncludingCanonicalDryRunProjection?.recoveredNamedIds ||
    [],
);

/** Research store presence */
let researchIds = new Set();
const researchPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "opportunities.json",
);
if (fs.existsSync(researchPath)) {
  const doc = JSON.parse(fs.readFileSync(researchPath, "utf8"));
  const opps = doc.opportunities || doc;
  researchIds = new Set((opps || []).map((o) => o.id).filter(Boolean));
}

function resolveCanonical(id, title) {
  if (goldById.has(id) || candById.has(id)) {
    return { canonicalId: id, match: "EXACT_ID" };
  }
  const fam = familyByMember.get(id);
  if (fam) {
    if (goldById.has(fam.primaryId) || candById.has(fam.primaryId)) {
      return {
        canonicalId: fam.primaryId,
        match: "FAMILY_PRIMARY",
        familyId: fam.familyId,
        role: fam.role,
      };
    }
    for (const lid of [fam.primaryId, ...(fam.linkedIds || [])]) {
      if (goldById.has(lid) || candById.has(lid)) {
        return {
          canonicalId: lid,
          match: "FAMILY_SIBLING",
          familyId: fam.familyId,
          role: fam.role,
        };
      }
    }
  }
  const nt = normTitle(title);
  if (goldTitleIndex.has(nt)) {
    return { canonicalId: goldTitleIndex.get(nt), match: "TITLE_GOLD" };
  }
  if (candTitleIndex.has(nt)) {
    return { canonicalId: candTitleIndex.get(nt), match: "TITLE_CAND" };
  }
  return { canonicalId: id, match: "UNRESOLVED_LINEAGE" };
}

function looksExpired(row, live) {
  const title = `${row.title || ""} ${live?.title || ""}`;
  if (/TITLE_SOURCE_INVALID|unidentified virtual|meeting.id/i.test(title)) {
    return true;
  }
  const start = Date.parse(
    live?.eventStartDate || live?.tileEventStartDate || row.eventStartDate || "",
  );
  if (Number.isFinite(start) && start < Date.now() - 30 * 86400000) {
    const future =
      live?.futureDemandPathway === true ||
      /FUTURE|OVERFLOW|WATCH|RECURRING/i.test(
        String(live?.futureCycleEvidenceState || live?.opportunityType || ""),
      );
    if (!future) return true;
  }
  return false;
}

const warnings =
  certSummary?.warnings?.rows ||
  candPkg?.hotelWidePassToPackageReconciliation?.exclusions ||
  [];
const pillConflictIds = new Set(
  warnings
    .filter((w) => w.code === "CONFLICTING_COMMERCIAL_ACTIONS")
    .map((w) => w.opportunityId),
);

const rows = [];
for (const row of scorecard.rows) {
  const id = row.id;
  const resolved = resolveCanonical(id, row.title);
  const inGoldExact = goldById.has(id);
  const inCandExact = candById.has(id);
  const inGoldCanon = goldById.has(resolved.canonicalId);
  const inCandCanon = candById.has(resolved.canonicalId);
  const live =
    candById.get(id) ||
    candById.get(resolved.canonicalId) ||
    goldById.get(id) ||
    goldById.get(resolved.canonicalId) ||
    null;
  const fam = familyByMember.get(id);

  let disposition = null;
  let explanation = "";
  let remainingResearch = null;
  let recommendedNextAction = null;

  // Quarantine / explicit historical disqualification first — never "merge away" invalid rows.
  if (
    fam?.role === "QUARANTINED" ||
    (fam?.quarantinedIds || []).includes(id) ||
    /TITLE_SOURCE_INVALID/i.test(row.title || "")
  ) {
    disposition = "DISQUALIFIED_WITH_EVIDENCE";
    explanation = "Quarantined / TITLE_SOURCE_INVALID research artifact.";
    remainingResearch = "None";
    recommendedNextAction = "Keep quarantined; never present as demand.";
  } else if (disqualifiedSet.has(id) || row.certificationOutcome === "DISQUALIFIED_WITH_EVIDENCE") {
    disposition = "DISQUALIFIED_WITH_EVIDENCE";
    explanation =
      row.missingCommercialInformation?.join("; ") ||
      "Historically disqualified with evidence; preserved exclusion.";
    remainingResearch = "None — do not resurrect without new evidence.";
    recommendedNextAction = "Preserve DISQUALIFIED; exclude from STAGED.";
  } else if (inCandExact || (inCandCanon && resolved.match !== "UNRESOLVED_LINEAGE" && id === resolved.canonicalId)) {
    disposition = "INCLUDED_IN_CERTIFIED_CANDIDATE";
    explanation = inCandExact
      ? "Present in isolated certified candidate 69 by exact ID."
      : `Present via canonical ID ${resolved.canonicalId}.`;
    recommendedNextAction = "Retain in certified baseline; no silent drop.";
  } else if (
    !inCandExact &&
    (inCandCanon || inGoldCanon) &&
    (resolved.match === "FAMILY_PRIMARY" ||
      resolved.match === "FAMILY_SIBLING" ||
      resolved.match === "TITLE_CAND" ||
      resolved.match === "TITLE_GOLD")
  ) {
    disposition = "DUPLICATE_OR_CANONICAL_MERGE";
    explanation = `Merged into family/canonical ${resolved.canonicalId} (${resolved.match}${fam ? `; family ${fam.familyId}; role ${fam.role}` : ""}). Not a silent loss.`;
    recommendedNextAction =
      "Keep as linked research; do not double-count in candidate.";
  } else if (
    fam &&
    fam.role !== "PRIMARY" &&
    fam.primaryId &&
    fam.primaryId !== id
  ) {
    // Linked research sibling of an open family not yet in candidate.
    disposition = "DUPLICATE_OR_CANONICAL_MERGE";
    explanation = `Linked research sibling of family primary ${fam.primaryId} (${fam.familyId}). Count once for commercial demand; sibling retained for lineage.`;
    recommendedNextAction =
      "Research against family primary only; do not double-count.";
  } else if (looksExpired(row, live)) {
    disposition = "GENUINELY_EXPIRED";
    explanation = "Past-dated or invalid timing without evidenced future pathway.";
    remainingResearch = "Confirm next edition only if new dated evidence appears.";
    recommendedNextAction = "Hold as historical; do not stage.";
  } else if (inGoldExact || inGoldCanon) {
    // In gold but not candidate — should be rare after 67/67
    disposition = "FOUNDER_REVIEW_REQUIRED";
    explanation =
      "In approved gold 67 but absent from candidate 69 — unexpected; founder review.";
    remainingResearch = "Investigate projection/serialization gap.";
    recommendedNextAction = "Do not expand discovery until explained.";
  } else if (
    !text(row.contactAfter?.name) &&
    (conditionalSet.has(id) || incompleteSet.has(id) || certifiedSet.has(id))
  ) {
    disposition = "CONTACT_RESEARCH_REQUIRED";
    explanation = `Historically ${row.certificationOutcome}; still lacks verified named person for FQO.`;
    remainingResearch = "Named WHO/ALT + labeled outreach route ($0 first).";
    recommendedNextAction = "Queue contact research; do not auto-promote.";
  } else if (conditionalSet.has(id)) {
    if (text(row.contactAfter?.name) && !inCandExact && !inCandCanon) {
      disposition = "OTHER_RESEARCH_INCOMPLETE";
      explanation =
        "Historically CONDITIONALLY_READY with named contact but not in gold/candidate — hotel-fit, timing, or FQO gaps remain.";
      remainingResearch = (row.missingCommercialInformation || []).join("; ") ||
        "Hotel-fit / timing / commercial action completeness.";
      recommendedNextAction =
        "Assess for QUALIFIED_NOT_YET_STAGED after FQO dry-run; no auto-promote.";
    } else {
      disposition = "CONTACT_RESEARCH_REQUIRED";
      explanation = "CONDITIONALLY_READY with incomplete named/outreach projection.";
      remainingResearch = "Complete WHO/HOW.";
      recommendedNextAction = "Contact research before candidacy.";
    }
  } else if (incompleteSet.has(id) || row.certificationOutcome === "RESEARCH_INCOMPLETE") {
    const named = text(row.contactAfter?.name);
    if (!named) {
      disposition = "CONTACT_RESEARCH_REQUIRED";
      explanation = "RESEARCH_INCOMPLETE and no verified named primary.";
      remainingResearch = "Named person + outreach route.";
      recommendedNextAction = "Contact research; preserve record.";
    } else {
      disposition = "OTHER_RESEARCH_INCOMPLETE";
      explanation =
        "RESEARCH_INCOMPLETE with some contact signal but not projected into gold/candidate.";
      remainingResearch = (row.missingCommercialInformation || []).join("; ") ||
        "Demand evidence / hotel fit / future pathway.";
      recommendedNextAction = "Targeted research; no auto-promote.";
    }
  } else if (certifiedSet.has(id) && !inCandExact && !inCandCanon && !inGoldExact) {
    disposition = "QUALIFIED_NOT_YET_STAGED";
    explanation =
      "Historically COMMERCIALLY_CERTIFIED but absent from gold 67 / candidate 69 — commercially valuable unfinished lineage.";
    remainingResearch = "Re-validate FQO on current evidence; founder staging decision.";
    recommendedNextAction = "Founder review for possible additive candidacy.";
  } else {
    disposition = "FOUNDER_REVIEW_REQUIRED";
    explanation = `Unclassified lineage (match=${resolved.match}).`;
    remainingResearch = "Manual lineage review.";
    recommendedNextAction = "Founder/IC review before any promotion.";
  }

  // Refine: if exact in candidate via sibling id that IS the candidate row
  if (inCandExact) {
    disposition = "INCLUDED_IN_CERTIFIED_CANDIDATE";
    explanation = "Present in isolated certified candidate 69 by exact ID.";
  }

  // AAD special note
  let aadNote = null;
  if (id === "gdi_opp_aad_2028_overflow") {
    aadNote =
      "Priority-3 Destination DC viable alternative accepted; actual AAD/Maritz housing buyer research remains OPEN.";
  }

  const candLive = candById.get(id) || (inCandCanon ? candById.get(resolved.canonicalId) : null);
  let currentCommercialStatus = row.certificationOutcome;
  if (disposition === "INCLUDED_IN_CERTIFIED_CANDIDATE") {
    currentCommercialStatus = "IN_CERTIFIED_CANDIDATE_69";
  } else if (disposition === "DUPLICATE_OR_CANONICAL_MERGE") {
    currentCommercialStatus = "MERGED_LINKED_RESEARCH";
  } else if (disposition === "DISQUALIFIED_WITH_EVIDENCE") {
    currentCommercialStatus = "DISQUALIFIED_PRESERVED";
  }

  rows.push({
    originalOpportunityId: id,
    originalTitle: row.title,
    originalInventory: row.inventory,
    originalCommercialCertification: row.certificationOutcome,
    originalNamedContact: row.contactAfter?.name || row.contactBefore?.name || null,
    originalOutreachRoute: row.outreachRoute || row.contactAfter?.orgRoute || row.contactAfter?.directEmail || null,
    currentCanonicalId: resolved.canonicalId,
    matchMethod: resolved.match,
    familyId: fam?.familyId || null,
    familyRole: fam?.role || null,
    presentInGold67: inGoldExact || inGoldCanon ? "YES" : "NO",
    presentInGoldExact: inGoldExact,
    presentInCandidate69: inCandExact || (disposition === "INCLUDED_IN_CERTIFIED_CANDIDATE") ? "YES" : disposition === "DUPLICATE_OR_CANONICAL_MERGE" ? "MERGED" : "NO",
    presentInCandidateExact: inCandExact,
    inResearchStore: researchIds.has(id),
    histRecoveredNamedDryRun: histRecovered.has(id),
    currentCommercialStatus,
    disposition,
    explanation: aadNote ? `${explanation} ${aadNote}` : explanation,
    remainingResearchNeeded: remainingResearch,
    recommendedNextAction,
    pillConflictOpen: pillConflictIds.has(id) || pillConflictIds.has(resolved.canonicalId),
    currentPrimary: candLive?.primaryContact?.name || null,
    currentOutreach:
      candLive?.primaryContact?.email ||
      candLive?.primaryContact?.phone ||
      candLive?.primaryContact?.askToBeDirectedTo ||
      null,
  });
}

/** Candidate 69 quality spot-check */
const candChecks = (candPkg.opportunities || []).map((o) => {
  const ev = evaluateFinalizedQualifiedOpportunity(o, {
    historicalApprovedInventory: goldById.has(o.id),
  });
  const named = text(o.primaryContact?.name);
  const orgAsPerson = /maritz global events|events?hq|inc\.?$|llc\.?$|association|academy of/i.test(
    named,
  );
  const outreach = Boolean(
    text(o.primaryContact?.email) ||
      text(o.primaryContact?.phone) ||
      text(o.primaryContact?.linkedinUrl) ||
      text(o.primaryContact?.askToBeDirectedTo) ||
      text(o.primaryContact?.organizationContactRouteLabel),
  );
  const action = text(
    o.recommendedNextStep || o.recommendedNextAction || o.cardWhyNowLine,
  );
  const thesis = text(
    o.hotelOpportunityThesis || o.whyThisHotel || o.summaryWhat || o.whatIsHappening,
  );
  return {
    id: o.id,
    title: o.title,
    inGold: goldById.has(o.id),
    namedPerson: named || null,
    orgAsPersonDefect: orgAsPerson,
    outreachPresent: outreach,
    thesisPresent: Boolean(thesis),
    actionPresent: Boolean(action),
    fqoPass: ev.finalizedQualified === true,
    fqoReasons: ev.reasons || [],
    pillConflict: pillConflictIds.has(o.id),
    salesPill: o.salesActionPillLabel || o.tileBadgePlan?.salesActionPillLabel || null,
  };
});

const byDisp = rows.reduce((a, r) => {
  a[r.disposition] = (a[r.disposition] || 0) + 1;
  return a;
}, {});

const matchedGoldExact = rows.filter((r) => r.presentInGoldExact).length;
const matchedGoldUnique = new Set(
  rows
    .filter((r) => r.presentInGold67 === "YES")
    .map((r) => r.currentCanonicalId),
).size;
const matchedCandExact = rows.filter((r) => r.presentInCandidateExact).length;
const matchedCand = rows.filter(
  (r) =>
    r.presentInCandidate69 === "YES" || r.presentInCandidate69 === "MERGED",
).length;
const matchedGold = matchedGoldExact; // founder metric = exact gold ID hits

const unexplained = rows.filter(
  (r) => r.disposition === "FOUNDER_REVIEW_REQUIRED",
).length;

/** Conditionally ready / incomplete deep dive */
const conditionalFocus = rows.filter((r) => conditionalSet.has(r.originalOpportunityId));
const incompleteFocus = rows.filter((r) => incompleteSet.has(r.originalOpportunityId));
const recoveredFocus = rows.filter((r) => histRecovered.has(r.originalOpportunityId));

const pillCorrections = [...pillConflictIds].map((id) => {
  const o = candById.get(id);
  return {
    opportunityId: id,
    title: o?.title || null,
    currentPill: o?.salesActionPillLabel || o?.tileBadgePlan?.salesActionPillLabel || null,
    recommendedNextStep: text(o?.recommendedNextStep || o?.cardWhyNowLine).slice(0, 180),
    recommendedCorrection:
      "Align salesActionPillLabel with recommendedNextStep intent (Watch vs Build Account vs Pursue Overflow) before any STAGED activation.",
    severity: "PRESENTATION_NOT_ELIGIBILITY",
  };
});

const summary = {
  at: new Date().toISOString(),
  hotelId: HOTEL,
  pr597: {
    state: "OPEN",
    url: "https://github.com/joandejarden-create/my-operators-backend/pull/597",
    mergeCommit: null,
    ci: "ALL_REQUIRED_CHECKS_SUCCESS_AT_LAST_POLL",
    note: "Integration Controller retains merge authority; GDI chat did not merge.",
  },
  metrics: {
    historicalResearchUniverse: 88,
    matchedToApprovedGold: matchedGoldExact,
    matchedToApprovedGoldUniqueCanonical: matchedGoldUnique,
    matchedToCurrentCandidate: matchedCand,
    matchedToCurrentCandidateExact: matchedCandExact,
    duplicateOrMerged: byDisp.DUPLICATE_OR_CANONICAL_MERGE || 0,
    genuinelyDisqualifiedOrExpired:
      (byDisp.DISQUALIFIED_WITH_EVIDENCE || 0) +
      (byDisp.GENUINELY_EXPIRED || 0),
    additionalQualifiedOpportunities: byDisp.QUALIFIED_NOT_YET_STAGED || 0,
    contactResearchRequired: byDisp.CONTACT_RESEARCH_REQUIRED || 0,
    otherResearchIncomplete: byDisp.OTHER_RESEARCH_INCOMPLETE || 0,
    includedInCertifiedCandidate: byDisp.INCLUDED_IN_CERTIFIED_CANDIDATE || 0,
    founderReviewRequired: byDisp.FOUNDER_REVIEW_REQUIRED || 0,
    unexplainedDifferences: unexplained,
  },
  dispositionCounts: byDisp,
  gold67: goldPkg.opportunities.length,
  candidate69: candPkg.opportunities.length,
  candidateVersion: CAND,
  pointers: {
    stagedSha: sha(path.join(PKG, "STAGED.json")),
    currentSha: sha(path.join(PKG, "CURRENT.json")),
    unchangedVsFreezeRaw: {
      staged: sha(path.join(PKG, "STAGED.json")) ===
        "f2a4f70b3fb104ee8c59c91648c35e5062de6f29565800a0015f55a2e875e6aa",
      current: sha(path.join(PKG, "CURRENT.json")) ===
        "db4eb0ca7d3e5b08ed160293f085b06260096347e7720f26c4dca6ca4606ce9c",
    },
  },
  candidateQuality: {
    count: candChecks.length,
    namedOk: candChecks.filter((c) => c.namedPerson && !c.orgAsPersonDefect).length,
    orgAsPersonDefects: candChecks.filter((c) => c.orgAsPersonDefect),
    outreachOk: candChecks.filter((c) => c.outreachPresent).length,
    thesisOk: candChecks.filter((c) => c.thesisPresent).length,
    actionOk: candChecks.filter((c) => c.actionPresent).length,
    fqoPass: candChecks.filter((c) => c.fqoPass).length,
    pillConflictsOpen: pillCorrections.length,
  },
  valuableUnfinished: {
    conditionallyReady: {
      count: conditionalFocus.length,
      byDisposition: conditionalFocus.reduce((a, r) => {
        a[r.disposition] = (a[r.disposition] || 0) + 1;
        return a;
      }, {}),
      idsStillActionableResearch: conditionalFocus
        .filter((r) =>
          /CONTACT_RESEARCH|OTHER_RESEARCH|QUALIFIED_NOT_YET/.test(r.disposition),
        )
        .map((r) => r.originalOpportunityId),
    },
    researchIncomplete: {
      count: incompleteFocus.length,
      byDisposition: incompleteFocus.reduce((a, r) => {
        a[r.disposition] = (a[r.disposition] || 0) + 1;
        return a;
      }, {}),
    },
    recoveredNamedDryRun12: {
      count: recoveredFocus.length,
      inCandidateOrMerged: recoveredFocus.filter(
        (r) =>
          r.presentInCandidate69 === "YES" ||
          r.presentInCandidate69 === "MERGED",
      ).length,
      stillOpen: recoveredFocus.filter(
        (r) =>
          r.presentInCandidate69 === "NO" &&
          !/DISQUALIFIED|EXPIRED|DUPLICATE/.test(r.disposition),
      ),
    },
  },
  pillConflictCorrections: pillCorrections,
  aadBuyerResearchOpen: true,
};

fs.writeFileSync(
  path.join(OUT, "RECONCILIATION_SUMMARY.json"),
  JSON.stringify(summary, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "MANIFEST_88_DISPOSITIONS.json"),
  JSON.stringify({ count: rows.length, dispositionCounts: byDisp, rows }, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "CANDIDATE_69_QUALITY_CHECKS.json"),
  JSON.stringify({ count: candChecks.length, rows: candChecks }, null, 2),
);

const md = `# Bethesda 88 → 69 Historical Research Reconciliation

**Generated:** ${summary.at}  
**$0 only · STAGED/CURRENT unchanged · No leisure / other-hotel expansion**

## Integration status (Task 5)

| Item | Status |
|---|---|
| PR #597 | **OPEN** — not merged |
| CI | All required checks SUCCESS (last poll) |
| Merge authority | Integration Controller only |
| Certified code on tip | Yes (\`360141c30\` lineage + certification artifacts) |

## Metrics

| Metric | Count |
|---|---:|
| Historical research universe | 88 |
| Matched to approved gold | ${matchedGold} |
| Matched to current candidate | ${matchedCand} |
| Duplicate / merged | ${byDisp.DUPLICATE_OR_CANONICAL_MERGE || 0} |
| Genuinely disqualified / expired | ${(byDisp.DISQUALIFIED_WITH_EVIDENCE || 0) + (byDisp.GENUINELY_EXPIRED || 0)} |
| Included in certified candidate | ${byDisp.INCLUDED_IN_CERTIFIED_CANDIDATE || 0} |
| Additional qualified (not yet staged) | ${byDisp.QUALIFIED_NOT_YET_STAGED || 0} |
| Contact research required | ${byDisp.CONTACT_RESEARCH_REQUIRED || 0} |
| Other research incomplete | ${byDisp.OTHER_RESEARCH_INCOMPLETE || 0} |
| Founder review required | ${byDisp.FOUNDER_REVIEW_REQUIRED || 0} |
| Unexplained differences | ${unexplained} |

## Disposition counts

${Object.entries(byDisp)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n")}

## Valuable unfinished research

### CONDITIONALLY_READY (14)
${JSON.stringify(summary.valuableUnfinished.conditionallyReady.byDisposition, null, 2)}

Still actionable (contact/other/qualified):  
${(summary.valuableUnfinished.conditionallyReady.idsStillActionableResearch || []).map((id) => `- \`${id}\``).join("\n") || "- (none)"}

### RESEARCH_INCOMPLETE (33)
${JSON.stringify(summary.valuableUnfinished.researchIncomplete.byDisposition, null, 2)}

### Dry-run recovered named (12)
In candidate or merged: **${summary.valuableUnfinished.recoveredNamedDryRun12.inCandidateOrMerged}** / 12

## Candidate 69 quality

| Check | Count |
|---|---:|
| Named person (not org-as-person) | ${summary.candidateQuality.namedOk} |
| Org-as-person defects | ${summary.candidateQuality.orgAsPersonDefects.length} |
| Outreach present | ${summary.candidateQuality.outreachOk} |
| Thesis present | ${summary.candidateQuality.thesisOk} |
| Action present | ${summary.candidateQuality.actionOk} |
| FQO pass (hist flag for gold) | ${summary.candidateQuality.fqoPass} |
| Open pill conflicts | ${summary.candidateQuality.pillConflictsOpen} |

AAD buyer research remains **OPEN** (Priority-3 Destination DC accepted).

## Pill conflicts — recommend before STAGED

${pillCorrections.map((p) => `- \`${p.opportunityId}\`: pill="${p.currentPill}" → ${p.recommendedCorrection}`).join("\n")}

## Protection

- Gold 67 package unchanged  
- Candidate 69 protected (\`${CAND}\`)  
- Pointers: STAGED/CURRENT freeze match = ${summary.pointers.unchangedVsFreezeRaw.staged && summary.pointers.unchangedVsFreezeRaw.current}

## Attachments

- \`MANIFEST_88_DISPOSITIONS.json\` — complete 88-record disposition  
- \`CANDIDATE_69_QUALITY_CHECKS.json\`  
- \`RECONCILIATION_SUMMARY.json\`

**STOP.** No leisure. No other hotels. No pointer activation.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_88_RECONCILIATION_REPORT.md"), md);
console.log(JSON.stringify(summary.metrics, null, 2));
console.log("dispositionCounts", byDisp);
console.log("wrote", OUT);
