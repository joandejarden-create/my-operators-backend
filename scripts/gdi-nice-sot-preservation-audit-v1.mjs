#!/usr/bin/env node
/**
 * NICE gdi_opp_nice_2027 SoT preservation audit (dry / no Airtable mutation).
 * Compares live fat payload vs previously rejected slim proposal vs lossless merge.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { mergeResearchSotLossless } from "../lib/group-demand-intelligence/workflow/gdi-research-sot-lossless-merge-v1.js";
import { GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1 } from "../lib/group-demand-intelligence/workflow/gdi-research-sot-field-ownership-v1.js";
import {
  mergeProposedResearchInventoryFields,
  RESEARCH_PAYLOAD_MAX_CHARS,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const IN = path.join(
  ROOT,
  "reports/gdi-validated-airtable-canary-prewrite-safety-v1",
);
const OUT = path.join(
  ROOT,
  "reports/gdi-nice-sot-preservation-audit-v1",
);
fs.mkdirSync(OUT, { recursive: true });

function sha256(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}

function writeJson(name, obj) {
  fs.writeFileSync(
    path.join(OUT, name),
    `${JSON.stringify(obj, null, 2)}\n`,
    "utf8",
  );
}

const liveRaw = fs.readFileSync(
  path.join(IN, "04b-ORIGINAL_researchPayloadJson.json"),
  "utf8",
);
const propRaw = fs.readFileSync(
  path.join(IN, "05b-PROPOSED_researchPayloadJson.json"),
  "utf8",
);
const live = JSON.parse(liveRaw);
const prop = JSON.parse(propRaw);

const merge = mergeResearchSotLossless({
  livePayload: live,
  proposedPayload: prop,
  maxChars: RESEARCH_PAYLOAD_MAX_CHARS,
});

const liveKeys = Object.keys(live).sort();
const mergedKeys = merge.ok ? Object.keys(merge.payload).sort() : [];
const propKeys = Object.keys(prop).sort();

const preservedCritical = {
  meetingHistory:
    Array.isArray(live.meetingHistory) &&
    merge.ok &&
    Array.isArray(merge.payload.meetingHistory) &&
    merge.payload.meetingHistory.length >= live.meetingHistory.length,
  competitors:
    Array.isArray(live.competitors) &&
    merge.ok &&
    Array.isArray(merge.payload.competitors) &&
    merge.payload.competitors.length >= live.competitors.length,
  evidenceFactCount: {
    live: (live.evidence || []).filter((e) => e?.claimKind === "FACT").length,
    merged: merge.ok
      ? (merge.payload.evidence || []).filter((e) => e?.claimKind === "FACT")
          .length
      : 0,
  },
  contactResearchAudit: Boolean(
    merge.ok && merge.payload.contactResearchAudit?.pass,
  ),
  commercialDemandType: merge.ok
    ? merge.payload.commercialDemandType === "BOTH"
    : false,
  revenueStreamClass: merge.ok
    ? merge.payload.revenueStreamClass === "COMBINED_GUESTROOMS_EVENT_FNB"
    : false,
  primaryContactGrade: merge.ok
    ? merge.payload.primaryContact?.contactGrade ===
      live.primaryContact?.contactGrade
    : false,
  backupContacts:
    merge.ok &&
    (merge.payload.backupContacts || []).length >=
      (live.backupContacts || []).length,
  summaryWhatPreserved:
    merge.ok &&
    String(merge.payload.summaryWhat || "").includes("NIST NICE"),
};

const storeMerge = mergeProposedResearchInventoryFields(
  {
    [F.opportunityId]: "gdi_opp_nice_2027",
    [F.hotelId]: "recLuxvwwxID7U2B8",
    [F.organizationName]: live.organizationName,
    [F.researchPayloadJson]: propRaw,
  },
  {
    [F.opportunityId]: "gdi_opp_nice_2027",
    [F.hotelId]: "recLuxvwwxID7U2B8",
    [F.organizationName]: live.organizationName,
    [F.researchPayloadJson]: liveRaw,
  },
);

const rejectedSlimChars = propRaw.length;
const liveChars = liveRaw.length;
const mergedChars = merge.ok ? merge.length : 0;

const audit = {
  opportunityId: "gdi_opp_nice_2027",
  airtableRecordId: "rec8QDHQEpGbi8kvW",
  ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  mergeOk: merge.ok,
  mergeCode: merge.code,
  classification: merge.classification,
  sizes: {
    liveChars,
    rejectedSlimChars,
    losslessMergedChars: mergedChars,
    storageLimit: RESEARCH_PAYLOAD_MAX_CHARS,
    storageLimitCompatible: merge.ok ? merge.storageLimitCompatible : false,
    unexplainedLossVsLive:
      merge.ok && mergedChars < liveChars * 0.9
        ? liveChars - mergedChars
        : 0,
  },
  liveSha256: sha256(liveRaw),
  rejectedSlimSha256: sha256(propRaw),
  mergedSha256: merge.ok ? sha256(merge.json) : null,
  existingKeys: liveKeys,
  proposedSlimKeys: propKeys,
  newlyAddedPaths: merge.diff?.added || [],
  legitimatelyUpdatedPaths: merge.diff?.updated || [],
  removedPaths: merge.diff?.removed || [],
  preservedEvidence: preservedCritical.evidenceFactCount,
  preservedContacts: {
    primary: merge.ok ? merge.payload.primaryContact?.name : null,
    primaryGrade: merge.ok ? merge.payload.primaryContact?.contactGrade : null,
    backupCount: merge.ok ? (merge.payload.backupContacts || []).length : 0,
    contactsCount: merge.ok ? (merge.payload.contacts || []).length : 0,
    auditPreserved: preservedCritical.contactResearchAudit,
  },
  preservedHistoricalIntelligence: {
    meetingHistoryCount: merge.ok
      ? (merge.payload.meetingHistory || []).length
      : 0,
    competitorsCount: merge.ok ? (merge.payload.competitors || []).length : 0,
    likelyCompetitor: merge.ok ? merge.payload.likelyCompetitor : null,
    likelyStrCompetitor: merge.ok ? merge.payload.likelyStrCompetitor : null,
  },
  commercialClassification: {
    commercialDemandType: merge.ok ? merge.payload.commercialDemandType : null,
    revenueStreamClass: merge.ok ? merge.payload.revenueStreamClass : null,
    changesFromLive: {
      commercialDemandType:
        live.commercialDemandType !==
        (merge.ok ? merge.payload.commercialDemandType : null),
      revenueStreamClass:
        live.revenueStreamClass !==
        (merge.ok ? merge.payload.revenueStreamClass : null),
    },
  },
  unresolvedConflicts: merge.unresolvedConflicts || [],
  autoResolvedConflicts: merge.autoResolvedConflicts || [],
  preservedCritical,
  storeMergeOk: storeMerge.ok,
  storeMergeCode: storeMerge.code,
  storeMergeClassification: storeMerge.classification,
  zeroUnexplainedLoss:
    merge.ok &&
    preservedCritical.meetingHistory &&
    preservedCritical.competitors &&
    preservedCritical.evidenceFactCount.merged >=
      preservedCritical.evidenceFactCount.live &&
    preservedCritical.commercialDemandType &&
    preservedCritical.revenueStreamClass &&
    preservedCritical.contactResearchAudit &&
    (merge.sizes?.unexplainedLossVsLive === 0 ||
      mergedChars >= liveChars * 0.9),
  verdict: null,
};

audit.verdict =
  audit.mergeOk && audit.zeroUnexplainedLoss && audit.storeMergeOk
    ? "NICE_LOSSLESS_MERGE_VERIFIED_DRY"
    : "NICE_LOSSLESS_MERGE_BLOCKED";

writeJson("00-NICE_PRESERVATION_AUDIT_SUMMARY.json", audit);
if (merge.ok) {
  fs.writeFileSync(
    path.join(OUT, "01-LOSSLESS_MERGED_researchPayloadJson.json"),
    `${merge.json}\n`,
  );
}
writeJson("02-SEMANTIC_PATH_DIFF.json", {
  added: merge.diff?.added || [],
  updated: merge.diff?.updated || [],
  removed: merge.diff?.removed || [],
  preservedCount: merge.diff?.preservedCount || 0,
  fullTopLevel: merge.diff?.full
    ? {
        added: merge.diff.full.added.filter((p) => !p.includes(".")),
        updated: merge.diff.full.updated.filter((p) => !p.includes(".")),
        removed: merge.diff.full.removed.filter((p) => !p.includes(".")),
      }
    : null,
});
writeJson("03-AUTO_RESOLVED_CONFLICTS.json", {
  autoResolved: merge.autoResolvedConflicts || [],
  unresolved: merge.unresolvedConflicts || [],
});

const md = `# NICE SoT Preservation Audit — gdi_opp_nice_2027

**Record:** rec8QDHQEpGbi8kvW  
**Verdict:** \`${audit.verdict}\`  
**Ownership:** \`${GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1}\`

## Sizes

| Representation | Chars |
|---|---|
| Live Airtable SoT | ${liveChars} |
| Rejected slim proposal | ${rejectedSlimChars} |
| Lossless merged | ${mergedChars} |
| Storage limit | ${RESEARCH_PAYLOAD_MAX_CHARS} |

## Preserved critical intelligence

- Meeting history: ${preservedCritical.meetingHistory}
- Competitors / STR set: ${preservedCritical.competitors}
- Evidence FACT count live→merged: ${preservedCritical.evidenceFactCount.live}→${preservedCritical.evidenceFactCount.merged}
- Contact research audit: ${preservedCritical.contactResearchAudit}
- commercialDemandType=BOTH: ${preservedCritical.commercialDemandType}
- revenueStreamClass=COMBINED_GUESTROOMS_EVENT_FNB: ${preservedCritical.revenueStreamClass}
- Primary contact grade: ${preservedCritical.primaryContactGrade}

## Semantic paths

- Added: ${(merge.diff?.added || []).join(", ") || "(none)"}
- Updated: ${(merge.diff?.updated || []).join(", ") || "(none)"}
- Removed (telemetry only expected): ${(merge.diff?.removed || []).join(", ") || "(none)"}

## Conflicts

Auto-resolved (keep live): ${(merge.autoResolvedConflicts || []).map((c) => c.code).join(", ") || "(none)"}  
Unresolved: ${(merge.unresolvedConflicts || []).map((c) => c.code).join(", ") || "(none)"}

## Mutation status

**No Airtable write executed.** Dry audit only.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_NICE_PRESERVATION_AUDIT.md"), md);

console.log(
  JSON.stringify(
    {
      out: OUT,
      verdict: audit.verdict,
      liveChars,
      rejectedSlimChars,
      mergedChars,
      zeroUnexplainedLoss: audit.zeroUnexplainedLoss,
    },
    null,
    2,
  ),
);
