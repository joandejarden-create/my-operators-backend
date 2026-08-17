/**
 * Outreach readiness summary for Pilot Target List (read-only).
 *
 *   node scripts/report-owner-targets-outreach-readiness.mjs
 *
 * Report: reports/owner-targets-outreach-readiness.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_PILOT_TARGET_LIST_TABLE,
  VAL_CALA_FIRST_PILOT_REGIONS,
  VAL_NON_CALA_FEEDBACK_REGIONS,
  MAP_PILOT_TARGET_LIST,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
  getGtmAirtableBase,
} from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function inc(map, key) {
  const k = key || "(blank)";
  map[k] = (map[k] || 0) + 1;
}

function hasText(value) {
  if (Array.isArray(value)) return value.length > 0;
  return Boolean(String(value || "").trim());
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const fields = [
    MAP_PILOT_TARGET_LIST.name,
    MAP_PILOT_TARGET_LIST.email,
    MAP_PILOT_TARGET_LIST.linkedInUrl,
    MAP_PILOT_TARGET_LIST.pilotRegion,
    MAP_PILOT_TARGET_LIST.region,
    MAP_PILOT_TARGET_LIST.outreachMessageAngle,
    MAP_PILOT_TARGET_LIST.whyTheyMatter,
    MAP_PILOT_TARGET_LIST.outreachSegment,
    MAP_PILOT_TARGET_LIST.pilotFit,
    MAP_PILOT_TARGET_LIST.category,
    MAP_PILOT_TARGET_LIST.priority,
    MAP_PILOT_TARGET_LIST.outreachStatus,
    MAP_PILOT_TARGET_LIST.readyForMailMerge,
    MAP_PILOT_TARGET_LIST.sendChannel,
    MAP_PILOT_TARGET_LIST.emailSubject,
    MAP_PILOT_TARGET_LIST.finalApprovedEmail,
    MAP_PILOT_TARGET_LIST.doNotContact,
  ];

  const records = await base(GTM_PILOT_TARGET_LIST_TABLE).select({ fields }).all();

  const byOutreachSegment = {};
  const byPilotFit = {};
  const byCategory = {};
  const byPriority = {};
  const byRegion = {};
  const byPilotRegion = {};
  const byOutreachStatus = {};
  let calaFirstCount = 0;
  let nonCalaFeedbackReferralCount = 0;
  let rowsMissingPilotRegion = 0;
  let rowsMissingOutreachSegment = 0;
  let rowsMissingPilotFit = 0;
  let rowsMissingOutreachMessageAngle = 0;
  let readyForMailMerge = 0;
  let rowsBlockedFromExport = 0;
  let missingEmail = 0;
  let missingLinkedInUrl = 0;
  let doNotContact = 0;
  const calaFirstRegions = new Set(VAL_CALA_FIRST_PILOT_REGIONS);
  const nonCalaRegions = new Set(VAL_NON_CALA_FEEDBACK_REGIONS);

  for (const rec of records) {
    const outreachStatus = String(rec.get(MAP_PILOT_TARGET_LIST.outreachStatus) || "").trim();
    const segment = String(
      rec.get(MAP_PILOT_TARGET_LIST.outreachSegment) || rec.get(MAP_PILOT_TARGET_LIST.category) || ""
    ).trim();
    const pilotFit = String(rec.get(MAP_PILOT_TARGET_LIST.pilotFit) || "").trim();
    const pilotRegion = String(rec.get(MAP_PILOT_TARGET_LIST.pilotRegion) || "").trim();
    const regionRaw = rec.get(MAP_PILOT_TARGET_LIST.region);
    const regionValues = Array.isArray(regionRaw)
      ? regionRaw
      : hasText(regionRaw)
        ? [String(regionRaw)]
        : [];
    const email = rec.get(MAP_PILOT_TARGET_LIST.email);
    const linkedIn = rec.get(MAP_PILOT_TARGET_LIST.linkedInUrl);
    const anglePick = rec.get(MAP_PILOT_TARGET_LIST.outreachMessageAngle);
    const angleText = rec.get(MAP_PILOT_TARGET_LIST.whyTheyMatter);
    const sendChannel = String(rec.get(MAP_PILOT_TARGET_LIST.sendChannel) || "").trim();
    const subject = String(rec.get(MAP_PILOT_TARGET_LIST.emailSubject) || "").trim();
    const finalApprovedEmail = String(rec.get(MAP_PILOT_TARGET_LIST.finalApprovedEmail) || "").trim();
    const isDnc = Boolean(rec.get(MAP_PILOT_TARGET_LIST.doNotContact));
    const isReady = Boolean(rec.get(MAP_PILOT_TARGET_LIST.readyForMailMerge));

    inc(byOutreachSegment, segment);
    inc(byPilotFit, pilotFit);
    inc(byCategory, rec.get(MAP_PILOT_TARGET_LIST.category));
    inc(byPriority, rec.get(MAP_PILOT_TARGET_LIST.priority));
    inc(byPilotRegion, pilotRegion);
    if (regionValues.length) {
      for (const region of regionValues) inc(byRegion, String(region || "").trim() || "(blank)");
    } else {
      inc(byRegion, "(blank)");
    }
    inc(byOutreachStatus, outreachStatus);

    if (calaFirstRegions.has(pilotRegion)) calaFirstCount += 1;
    if (
      nonCalaRegions.has(pilotRegion) &&
      (pilotFit === "Feedback / Referral Only" || pilotFit === "Follow-Up Later")
    ) {
      nonCalaFeedbackReferralCount += 1;
    }
    if (!hasText(pilotRegion)) rowsMissingPilotRegion += 1;
    if (!hasText(segment)) rowsMissingOutreachSegment += 1;
    if (!hasText(pilotFit)) rowsMissingPilotFit += 1;
    if (!hasText(anglePick) && !hasText(angleText)) rowsMissingOutreachMessageAngle += 1;
    if (!hasText(email)) missingEmail += 1;
    if (!hasText(linkedIn)) missingLinkedInUrl += 1;
    if (isReady) readyForMailMerge += 1;
    if (isDnc) doNotContact += 1;

    const candidateForExport = outreachStatus === "Approved" || isReady;
    if (candidateForExport) {
      let blocked = false;
      if (isDnc) blocked = true;
      if (outreachStatus !== "Approved") blocked = true;
      if (!isReady) blocked = true;
      if (!subject || !finalApprovedEmail) blocked = true;
      if (sendChannel === "Email" && !hasText(email)) blocked = true;
      if (blocked) rowsBlockedFromExport += 1;
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    tableName: GTM_PILOT_TARGET_LIST_TABLE,
    totalTargets: records.length,
    byOutreachSegment,
    byPilotFit,
    byCategory,
    byPriority,
    byPilotRegion,
    byRegion,
    byOutreachStatus,
    calaFirstCount,
    nonCalaFeedbackReferralCount,
    rowsMissingPilotRegion,
    rowsMissingOutreachSegment,
    rowsMissingPilotFit,
    rowsMissingOutreachMessageAngle,
    readyForMailMerge,
    rowsBlockedFromExport,
    doNotContact,
    missingEmail,
    missingLinkedInUrl,
  };

  const outPath = path.join(ROOT, "reports", "owner-targets-outreach-readiness.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  console.log("Pilot Target List outreach readiness");
  console.log("  Total targets:", report.totalTargets);
  console.log("  CALA-first count:", calaFirstCount);
  console.log("  Non-CALA feedback/referral count:", nonCalaFeedbackReferralCount);
  console.log("  Missing Pilot Region:", rowsMissingPilotRegion);
  console.log("  Missing Outreach Segment:", rowsMissingOutreachSegment);
  console.log("  Missing Pilot Fit:", rowsMissingPilotFit);
  console.log("  Missing Outreach Message Angle:", rowsMissingOutreachMessageAngle);
  console.log("  Ready for Mail Merge:", readyForMailMerge);
  console.log("  Rows blocked from export:", rowsBlockedFromExport);
  console.log("  Rows with Do Not Contact:", doNotContact);
  console.log("  Missing email:", missingEmail);
  console.log("  Missing LinkedIn URL:", missingLinkedInUrl);
  console.log("Wrote", outPath);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
