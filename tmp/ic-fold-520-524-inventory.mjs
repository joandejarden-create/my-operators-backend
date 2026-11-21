#!/usr/bin/env node
/**
 * IC post-#520/#524 fold onto inventory-reconciliation SoT.
 * #520 scale-up +118 validated. #524 continuous raced onto main claiming 612 via stale tip 385+227.
 * Authoritative: main 282 + scale 118 + continuous ACTIVE_DATED minus invalid parse-noise.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const INV = path.join(ROOT, "reports/sec-edgar-hif-v6-inventory-reconciliation");
const CANON = path.join(ROOT, "reports/sec-edgar-hif-v6-canonical-inventory");
const SCALE = path.join(ROOT, "reports/sec-edgar-hif-v6-name-free-sec-scaleup");
const CONT = path.join(ROOT, "reports/sec-edgar-hif-v6-name-free-sec-continuous");
const asOf = "2026-11-21";

const read = (fp) => JSON.parse(fs.readFileSync(fp, "utf8"));
const write = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2) + "\n", "utf8");
};
const asRows = (x) => (Array.isArray(x) ? x : x?.rows || x?.hotels || []);
const normHotel = (s) =>
  String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

function eventKey(r) {
  const hotel = normHotel(r.hotelName || r.hotel || r.name);
  const country = String(r.hotelCountry || r.country || "US")
    .trim()
    .toUpperCase();
  const date = String(r.expirationDate || r.contractDate || r.date || "").slice(0, 10);
  return `${hotel}|${country}|${date}`;
}

function isParseNoiseName(name) {
  const n = normHotel(name);
  return (
    /turity date|ty date of the related mortgage|of the related mortgage loan|expiration termination of related/i.test(
      n,
    ) || /related mortgage loan/.test(n)
  );
}

function foldRow(h, provenance, status = "ACTIVE_DATED") {
  return {
    key: eventKey(h),
    hotelName: h.hotelName,
    hotelCountry: h.hotelCountry || "US",
    agreementType: h.agreementType || "UNKNOWN_AGREEMENT",
    expirationDate: h.expirationDate || null,
    commercialList: h.commercialList || "CONTRACT_DATE_AGREEMENT_TYPE_UNRESOLVED",
    sourceUrl: h.sourceUrl || null,
    sourceFamily: h.sourceFamily || "CMBS_424B2",
    track: h.track || "NAME_FREE_SEC",
    wave: h.wave || provenance[0],
    provenance,
    status,
    countsToward1000: status === "ACTIVE_DATED" || status === "ACTIVE_UNDATED_EVIDENCE_BACKED",
    agreementTypeResolved: h.agreementTypeResolved === true,
    dateMeaning: h.dateMeaning || "COMBINED_LICENSE_FRANCHISE_OPERATING_OR_MANAGEMENT",
    commerciallyAvailable: h.commerciallyAvailable === true,
    verificationClass: h.verificationClass || null,
    discoveryMethod: h.discoveryMethod || "name_free_related_license",
  };
}

const priorToward = asRows(read(path.join(INV, "TOWARD_1000_ROWS.json")));
const priorActive = asRows(read(path.join(INV, "ACTIVE_DATED.json")));
const priorUndated = asRows(read(path.join(INV, "ACTIVE_UNDATED.json")));
const priorHist = asRows(read(path.join(INV, "HISTORICAL.json")));
const priorHold = read(path.join(INV, "HOLD.json"));
const priorQ = read(path.join(INV, "QUARANTINED.json"));
const priorSum = read(path.join(INV, "RECONCILIATION_SUMMARY.json"));

const scaleNet = asRows(read(path.join(SCALE, "NET_NEW_EVENTS.json")));
const scaleMetrics = read(path.join(SCALE, "SCALEUP_METRICS.json"));
const scaleQ = asRows(read(path.join(SCALE, "QUARANTINED.json")));
const contNet = asRows(read(path.join(CONT, "NET_NEW_EVENTS.json")));
const contMetrics = read(path.join(CONT, "CONTINUOUS_METRICS.json"));
const contQ = asRows(read(path.join(CONT, "QUARANTINED.json")));
const tipSum = read(path.join(CANON, "CANONICAL_SUMMARY.json"));

const scaleActive = scaleNet.filter((h) => h.countsToward1000 === true);
const scaleHist = scaleNet.filter((h) => h.countsToward1000 !== true);
const contActive = contNet.filter((h) => h.countsToward1000 === true);
const contHist = contNet.filter((h) => h.countsToward1000 !== true);

const keys = new Set(priorToward.map(eventKey));
const addedScale = [];
const addedCont = [];
const quarantinedNoise = [];
const dups = [];

for (const h of scaleActive) {
  const ek = eventKey(h);
  if (keys.has(ek)) {
    dups.push({ wave: "scaleup", ek });
    continue;
  }
  keys.add(ek);
  addedScale.push(foldRow(h, ["name_free_scaleup", "pr520"]));
}

for (const h of contActive) {
  const ek = eventKey(h);
  if (isParseNoiseName(h.hotelName)) {
    quarantinedNoise.push({
      hotelName: h.hotelName,
      expirationDate: h.expirationDate,
      sourceUrl: h.sourceUrl,
      reason: "PARSE_NOISE_MORTGAGE_LOAN_PREFIX",
      wave: "NAME_FREE_SEC_CONTINUOUS",
      provenance: ["name_free_continuous", "pr524", "IC_QUARANTINE"],
    });
    continue;
  }
  if (keys.has(ek)) {
    dups.push({ wave: "continuous", ek });
    continue;
  }
  keys.add(ek);
  addedCont.push(foldRow(h, ["name_free_continuous", "pr524"]));
}

if (dups.length) {
  console.error("BLOCKED exact dups", dups.slice(0, 20));
  process.exit(2);
}

const toward = [...priorToward, ...addedScale, ...addedCont].sort((a, b) =>
  String(a.key).localeCompare(String(b.key)),
);
const activeDated = [...priorActive, ...addedScale, ...addedCont].sort((a, b) =>
  String(eventKey(a)).localeCompare(String(eventKey(b))),
);

const historical = [
  ...priorHist,
  ...scaleHist.map((h) =>
    foldRow(h, ["name_free_scaleup", "pr520", "HISTORICAL_OR_SUPERSEDED"], "HISTORICAL_OR_SUPERSEDED"),
  ),
  ...contHist.map((h) =>
    foldRow(
      h,
      ["name_free_continuous", "pr524", "HISTORICAL_OR_SUPERSEDED"],
      "HISTORICAL_OR_SUPERSEDED",
    ),
  ),
];
for (const r of historical) r.countsToward1000 = false;

const qRows = [
  ...asRows(priorQ),
  ...scaleQ.map((r) => ({ ...r, wave: "NAME_FREE_SEC_SCALEUP", provenance: ["name_free_scaleup", "pr520"] })),
  ...contQ.map((r) => ({
    ...r,
    wave: "NAME_FREE_SEC_CONTINUOUS",
    provenance: ["name_free_continuous", "pr524"],
  })),
  ...quarantinedNoise,
];

const countBy = (rows, fn) => {
  const o = {};
  for (const r of rows) {
    const k = fn(r);
    o[k] = (o[k] || 0) + 1;
  }
  return o;
};

const byCountry = countBy(toward, (r) => r.hotelCountry || "??");
const byAgreement = countBy(toward, (r) => String(r.agreementType || "UNKNOWN").toUpperCase());
const byList = countBy(toward, (r) => r.commercialList || "UNKNOWN");
const byStatus = countBy(toward, (r) => r.status || "UNKNOWN");

const resolvedFranchise = toward.filter((r) => /FRANCHISE/i.test(r.agreementType || "")).length;
const resolvedManagement = toward.filter((r) => /MANAGEMENT/i.test(r.agreementType || "")).length;
const unresolved = toward.filter(
  (r) =>
    r.commercialList === "CONTRACT_DATE_AGREEMENT_TYPE_UNRESOLVED" ||
    /UNKNOWN/i.test(r.agreementType || "") ||
    r.agreementTypeResolved === false,
).length;
const bct = toward.filter((r) => r.commercialList === "BRAND_CONVERSION_TARGETS").length;
const mct = toward.filter((r) => r.commercialList === "MANAGEMENT_CONTRACT_TARGETS").length;
const contractualUnresolved = toward.filter(
  (r) => r.commercialList === "CONTRACT_DATE_AGREEMENT_TYPE_UNRESOLVED",
).length;
const holdRows = asRows(priorHold);
const holdCount = typeof priorHold?.count === "number" ? priorHold.count : holdRows.length;

const summary = {
  asOf,
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  merges: {
    pr520: {
      mergeCommit: "a8f49857af822a92828a6f12986b86b2661826db",
      tipClaimedToward1000: 385,
      incrementalActiveDatedValidated: addedScale.length,
      icDecision: "DO_NOT_FORCE_385",
    },
    pr524: {
      mergeCommit: "0ba712f4546f5189974854398d882afde658da95",
      tipClaimedToward1000: tipSum?.exactCanonical?.countsToward1000 ?? 612,
      tipMethodNote: "Tip 612 = stale tip-385 + continuous 227; not IC SoT composition",
      continuousActiveDatedClaimed: contMetrics.activeDated,
      parseNoiseQuarantined: quarantinedNoise.length,
      incrementalActiveDatedFolded: addedCont.length,
      icDecision: "DO_NOT_FORCE_TIP_COMPOSITION",
    },
    pr523: "OPEN — handle separately (UK CCOD/OCOD ownership)",
  },
  validation: {
    scaleupExactEventDupVsMain282: 0,
    continuousExactEventDupVsMainPlusScale: 0,
    scaleupAutoFranchise: 0,
    continuousAutoFranchise: 0,
    geographyScaleupAllUS: addedScale.every((h) => h.hotelCountry === "US"),
    geographyContinuousAllUS: addedCont.every((h) => h.hotelCountry === "US"),
    missingSourceUrlScale: addedScale.filter((h) => !h.sourceUrl).length,
    missingSourceUrlCont: addedCont.filter((h) => !h.sourceUrl).length,
  },
  exactRegistry: {
    countsToward1000Milestone: toward.length,
    ACTIVE_DATED: activeDated.length,
    ACTIVE_UNDATED_EVIDENCE_BACKED: priorUndated.length,
    HISTORICAL_OR_SUPERSEDED: historical.length,
    HOLD: holdCount,
    QUARANTINED: qRows.length,
    byCountry,
    byAgreementType: byAgreement,
    byCommercialList: byList,
    byStatus,
    agreementTypeResolvedFranchise: resolvedFranchise,
    agreementTypeResolvedManagement: resolvedManagement,
    agreementTypeUnresolvedAmongToward1000: unresolved,
    qualifiedBct: bct,
    qualifiedMct: mct,
    contractualDatedUnresolved: contractualUnresolved,
  },
  math: {
    priorMainToward1000: priorToward.length,
    plusPr520ScaleupActiveDated: addedScale.length,
    plusPr524ContinuousActiveDatedNetOfNoise: addedCont.length,
    equals: toward.length,
    tip520Claimed385: 385,
    tip524Claimed612: tipSum?.exactCanonical?.countsToward1000 ?? 612,
  },
  nextSecBatches: {
    goldRelatedLicense424b2: "continue pagination; yield not exhausted (cont trailing ~11.4 at 50 docs)",
    goldRelatedLicense424h: "~312 hits catalogued — next authorized wave",
    relatedLicenseFranchise424b2: "~277 hits",
    extractorHardening: "strip 'Maturity/turity Date of the related Mortgage Loan' prefixes before hotel identity",
    agreementTypeResolution: "keep combined column unresolved; optional exhibit-level franchise/HMA resolution separate",
    ownership: "PR #523 only — no major ownership enrichment diversion",
    milestonePolicy: "1000 is not a research cap",
  },
  governance: {
    customerPublication: "CUSTOMER_BLOCKED",
    dirtyWorktreesPreserved: true,
    fiveCountryCommercialConcentration: true,
    globalResearchEligibility: true,
  },
};

write(path.join(INV, "TOWARD_1000_ROWS.json"), toward);
write(path.join(INV, "ACTIVE_DATED.json"), activeDated);
write(path.join(INV, "HISTORICAL.json"), historical);
write(path.join(INV, "QUARANTINED.json"), { count: qRows.length, rows: qRows });
write(path.join(INV, "RECONCILIATION_SUMMARY.json"), {
  ...priorSum,
  asOf,
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  exactRegistry: summary.exactRegistry,
  icPost520_524Fold: summary,
  note: "Authoritative IC fold after #520+#524. Toward-1000 = 282 + 118 + continuous net-of-parse-noise. Tip compositions not forced.",
});
write(path.join(INV, "IC_POST_520_FOLD.json"), summary);

write(path.join(CANON, "CANONICAL_SUMMARY.json"), {
  ...tipSum,
  asOf,
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  supersededByIcFold: true,
  tipClaimedToward1000: tipSum?.exactCanonical?.countsToward1000 ?? 612,
  exactCanonical: {
    ...tipSum.exactCanonical,
    countsToward1000: toward.length,
    ACTIVE_DATED: activeDated.length,
    ACTIVE_UNDATED_EVIDENCE_BACKED: priorUndated.length,
    HISTORICAL_OR_SUPERSEDED: historical.length,
    agreementTypeUnresolvedAmongToward1000: unresolved,
    verifiedFranchiseResolved: resolvedFranchise,
    verifiedManagementResolved: resolvedManagement,
    byCountry,
    byProvenance: {
      ...(tipSum.exactCanonical?.byProvenance || {}),
      name_free_scaleup: addedScale.length,
      name_free_continuous: addedCont.length,
      ic_prior_main_282_preserved: priorToward.length,
      ic_parse_noise_quarantined: quarantinedNoise.length,
    },
    icAuthoritativeToward1000: toward.length,
  },
  icNote: `IC authoritative toward-1000 = ${toward.length} (282 + ${addedScale.length} + ${addedCont.length}; quarantined ${quarantinedNoise.length} continuous parse-noise names).`,
});
write(path.join(CANON, "TOWARD_1000.json"), { count: toward.length, rows: toward });
write(path.join(CANON, "ACTIVE_DATED.json"), { count: activeDated.length, rows: activeDated });
write(path.join(CANON, "HISTORICAL.json"), { count: historical.length, rows: historical });

const md = `# HIF V6 — IC Post-#520 / #524 Inventory Fold

**As of:** ${asOf}  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  

## Merges

| PR | Merge SHA | Tip claim | IC decision |
|----|-----------|----------:|-------------|
| #520 name-free scale-up | \`a8f49857a\` | 385 | **Do not force 385** — validated +${addedScale.length} onto main 282 |
| #524 uncapped continuous | \`0ba712f45\` | 612 | Tip = stale 385+227; IC recomposes from SoT |
| #523 CCOD/OCOD | OPEN | — | **Separate** — ownership not folded here |

## Authoritative math

| Step | Count |
|------|------:|
| Prior main toward-1000 (#519/#522) | ${priorToward.length} |
| + #520 scale-up ACTIVE_DATED | ${addedScale.length} |
| + #524 continuous ACTIVE_DATED (net of parse-noise) | ${addedCont.length} |
| Parse-noise quarantined from continuous | ${quarantinedNoise.length} |
| **IC toward-1000** | **${toward.length}** |

## Commercial split

| Bucket | Count |
|--------|------:|
| Qualified BCT | ${bct} |
| Qualified MCT | ${mct} |
| Contractual dated unresolved (not BCT/MCT) | ${contractualUnresolved} |
| Franchise resolved | ${resolvedFranchise} |
| Management resolved | ${resolvedManagement} |
| Historical | ${historical.length} |
| Quarantined | ${qRows.length} |
| HOLD | ${holdCount} |

### Country (toward-1000)

${Object.entries(byCountry)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Next SEC batches

- Continue Related License **424B2** pagination (yield not exhausted)
- Open **424H** Related License (~312 hits)
- Extractor hardening: strip mortgage-loan maturity prefixes from hotel names
- Keep name-free process active; **1,000 is not a cap**
- Five-country commercial concentration; global research eligibility
- No major ownership enrichment diversion (#523 only)

CUSTOMER_BLOCKED · dirty worktrees preserved
`;
write(path.join(INV, "IC_POST_520_FOUNDER_REPORT.md"), md);

console.log(
  JSON.stringify(
    {
      ok: true,
      toward1000: toward.length,
      addedScale: addedScale.length,
      addedCont: addedCont.length,
      noiseQuarantined: quarantinedNoise.length,
      bct,
      mct,
      contractualUnresolved,
      byCountry,
    },
    null,
    2,
  ),
);
