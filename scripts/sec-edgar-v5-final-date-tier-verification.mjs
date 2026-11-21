#!/usr/bin/env node
/**
 * Final V5 date/tier verification before IC merge of PR #503.
 * Explicit research as-of date. Offline. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const AS_OF = "2026-11-21";
const OUT = path.join(ROOT, "reports/sec-edgar-v5-final-verification");

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

/** Month delta — must match commercial-target-discovery-v1.monthsBetween (year/month only). */
function monthsBetween(asOf, end) {
  const a = new Date(asOf);
  const e = new Date(end);
  if (Number.isNaN(a.getTime()) || Number.isNaN(e.getTime())) return null;
  return (e.getUTCFullYear() - a.getUTCFullYear()) * 12 + (e.getUTCMonth() - a.getUTCMonth());
}

function assert(cond, msg, failures) {
  if (!cond) failures.push(msg);
}

function main() {
  const failures = [];
  const opp = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/OPPORTUNITIES.json"));
  const brand = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS.json"));
  const mgmt = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json"));
  const queue = readJson(
    path.join(ROOT, "reports/sec-edgar-commercial-inventory-reconcile-v5/TIER_B_ENRICHMENT_QUEUE.json"),
  );
  const recon = readJson(
    path.join(ROOT, "reports/sec-edgar-commercial-inventory-reconcile-v5/RECONCILIATION.json"),
  );

  assert(opp.asOf === AS_OF || recon.asOf === AS_OF, `asOf must be ${AS_OF}`, failures);

  const rows = opp.opportunities || [];
  const tierAB = rows.filter(
    (r) =>
      r.tier === "TIER_A_VERIFIED_DECISION_WINDOW" ||
      r.tier === "TIER_B_VERIFIED_CONTRACT_EXPIRATION",
  );
  const withEnd = tierAB.filter((r) => r.verifiedEndDate);
  const recomputed = withEnd.map((r) => {
    const m = monthsBetween(AS_OF, r.verifiedEndDate);
    return {
      hotel: r.hotelName,
      agreementType: r.agreementType,
      tier: r.tier,
      end: r.verifiedEndDate,
      storedMonths: r.monthsToExpiration,
      recomputedMonths: m,
      bucket: m < 0 ? "HISTORICAL" : "FUTURE",
      monthsMismatch: r.monthsToExpiration !== m,
      hints: r.appDateFilterHints || [],
    };
  });

  const historical = recomputed.filter((r) => r.bucket === "HISTORICAL");
  const future = recomputed.filter((r) => r.bucket === "FUTURE");
  const franchiseFuture = future.filter((r) => /FRANCHISE|LICENSE/i.test(r.agreementType || ""));
  const managementFuture = future.filter((r) => String(r.agreementType).toUpperCase() === "MANAGEMENT");

  assert(withEnd.length === 79, `expected 79 verified expirations, got ${withEnd.length}`, failures);
  assert(historical.length === 60, `expected 60 historical, got ${historical.length}`, failures);
  assert(future.length === 19, `expected 19 future, got ${future.length}`, failures);
  assert(franchiseFuture.length === 17, `expected 17 future franchise, got ${franchiseFuture.length}`, failures);
  assert(managementFuture.length === 2, `expected 2 future management, got ${managementFuture.length}`, failures);
  assert(
    historical.length + future.length === 79,
    "historical+future must equal verified expirations",
    failures,
  );
  assert(
    recomputed.every((r) => !r.monthsMismatch),
    "stored monthsToExpiration must match as-of recompute",
    failures,
  );

  const joplin = recomputed.find((r) => /Hilton Garden Inn Joplin/i.test(r.hotel || ""));
  assert(Boolean(joplin), "Hilton Garden Inn Joplin missing from Tier A/B verified set", failures);
  assert(joplin?.end === "2026-10-31", "Joplin end date must remain 2026-10-31", failures);
  assert(joplin?.bucket === "HISTORICAL", "Joplin must be HISTORICAL as of 2026-11-21", failures);
  assert(joplin?.recomputedMonths === -1, "Joplin monthsToExpiration must be -1", failures);
  assert(
    !(queue.futureDated || []).some((r) => /Hilton Garden Inn Joplin/i.test(r.hotel || "")),
    "Joplin must not appear in Tier B future enrichment queue",
    failures,
  );
  assert(
    (queue.historical || []).some((r) => /Hilton Garden Inn Joplin/i.test(r.hotel || "")),
    "Joplin must appear in Tier B historical enrichment queue",
    failures,
  );

  assert((queue.counts?.future ?? queue.futureDated?.length) === 17, "Tier B future queue count must be 17", failures);
  assert(
    (queue.counts?.historical ?? queue.historical?.length) === 60,
    "Tier B historical queue count must be 60",
    failures,
  );

  const brandHist = (brand.targets || []).filter(
    (t) => t.verifiedEndDate && monthsBetween(AS_OF, t.verifiedEndDate) < 0,
  );
  const brandFuture = (brand.targets || []).filter(
    (t) => t.verifiedEndDate && monthsBetween(AS_OF, t.verifiedEndDate) >= 0,
  );
  assert(brandHist.length === 60, `Brand list historical ends must be 60, got ${brandHist.length}`, failures);
  assert(brandFuture.length === 17, `Brand list future ends must be 17, got ${brandFuture.length}`, failures);

  const phl = rows.find((r) => /Hilton Philadelphia Airport/i.test(r.hotelName || ""));
  assert(Boolean(phl), "Hilton Philadelphia Airport missing", failures);
  assert(phl?.tier === "TIER_A_VERIFIED_DECISION_WINDOW", "PHL must remain Tier A", failures);
  assert(phl?.agreementType === "MANAGEMENT", "PHL agreementType MANAGEMENT", failures);
  assert(phl?.verifiedEndDate === "2035-03-30", "PHL end 2035-03-30 preserved", failures);
  assert(phl?.noticePeriodDays === 180, "PHL notice 180 days preserved", failures);
  assert(phl?.noticeDeadline === "2034-10-01", "PHL notice deadline = end − 180d → 2034-10-01", failures);
  assert(phl?.noticeDeadlinePassed === false, "PHL notice deadline not passed", failures);
  assert(phl?.nearTermOutreachPriority === false, "PHL far-horizon: near-term outreach false", failures);
  assert(phl?.researchEligible === true, "PHL remains researchEligible", failures);
  assert(
    phl?.renewalStatusClass === "STATUS_UNCONFIRMED" ||
      phl?.historicalApplicability === "HISTORICALLY_DISCLOSED_ONLY",
    "PHL current applicability not over-claimed",
    failures,
  );

  const hold = rows.filter((r) => r.tier === "HOLD_QUARANTINE");
  const brandIds = new Set((brand.targets || []).map((t) => t.researchId));
  const mgmtIds = new Set((mgmt.targets || []).map((t) => t.researchId));
  const holdLeak = hold.filter((h) => brandIds.has(h.researchId) || mgmtIds.has(h.researchId));
  assert(hold.length === 299, `HOLD count 299, got ${hold.length}`, failures);
  assert(holdLeak.length === 0, "HOLD must never enter Brand/Management projections", failures);
  assert((mgmt.targets || []).length === 4, `strict management list must be 4, got ${mgmt.targets?.length}`, failures);
  assert((brand.targets || []).length === 136, `strict brand list must be 136, got ${brand.targets?.length}`, failures);

  const report = {
    version: "v5-final-date-tier-verification",
    researchAsOf: AS_OF,
    auditedAt: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    routingDefaults: {
      discovery: "DETERMINISTIC_SEARCH_FIRST",
      exaAgent: "MANUAL_ESCALATION_ONLY",
      jev: "SHADOW_ONLY_DETERMINISTIC_DEFAULT",
    },
    identityMath: {
      verifiedExpirationsTierAB: withEnd.length,
      historical: historical.length,
      future: future.length,
      futureFranchise: franchiseFuture.length,
      futureManagement: managementFuture.length,
      identity:
        "79 verified = 60 historical + 19 future; 19 future = 17 franchise (Brand filter) + 2 management (PHL Tier A + Denver SpringHill Tier B)",
      tierBEnrichmentQueue: {
        future: queue.counts?.future ?? queue.futureDated?.length,
        historical: queue.counts?.historical ?? queue.historical?.length,
        note: "Queue is Tier B grain (77). Future 17 excludes management Tier A/B extras counted in the 19.",
      },
      brandListEnds: { historical: brandHist.length, future: brandFuture.length },
      strictLists: {
        brand: brand.targets?.length,
        management: mgmt.targets?.length,
        holdExcluded: holdLeak.length === 0,
      },
    },
    joplinCorrection: {
      hotel: "Hilton Garden Inn Joplin",
      verifiedEndDate: joplin?.end,
      bucketAsOfResearchDate: joplin?.bucket,
      monthsToExpiration: joplin?.recomputedMonths,
      narrativeFix:
        "Prior consolidated founder example incorrectly titled Joplin as Future franchise. Data already HISTORICAL as of 2026-11-21; narrative corrected.",
      datesPreserved: true,
    },
    hiltonPhiladelphiaAirport: {
      tier: phl?.tier,
      agreementType: phl?.agreementType,
      verifiedEndDate: phl?.verifiedEndDate,
      noticePeriodDays: phl?.noticePeriodDays,
      noticeDeadline: phl?.noticeDeadline,
      noticeRelevantTo2035Expiration: true,
      noticeRelevanceNote:
        "180-day notice is contractual timing for the 2035-03-30 MANAGEMENT expiration (deadline 2034-10-01). It is not a near-term outreach trigger as of 2026-11-21 (monthsToNoticeDeadline≈95).",
      currentApplicability: phl?.historicalApplicability,
      renewalStatusClass: phl?.renewalStatusClass,
      nearTermOutreachPriority: phl?.nearTermOutreachPriority,
      researchEligible: phl?.researchEligible,
      tierARetained: phl?.tier === "TIER_A_VERIFIED_DECISION_WINDOW",
      evidenceUrl: phl?.sourceUrl,
    },
    futureFranchiseRows: franchiseFuture.map((r) => ({ hotel: r.hotel, end: r.end, months: r.recomputedMonths })),
    futureManagementRows: managementFuture.map((r) => ({
      hotel: r.hotel,
      end: r.end,
      tier: r.tier,
      months: r.recomputedMonths,
    })),
    pass: failures.length === 0,
    failures,
  };

  writeJson(path.join(OUT, "DATE_TIER_VERIFICATION.json"), report);

  const md = [];
  md.push("# V5 Final Date/Tier Verification");
  md.push("");
  md.push(`Research as-of: **${AS_OF}**`);
  md.push(`Result: **${report.pass ? "PASS" : "FAIL"}**`);
  md.push("");
  md.push("## Identity math");
  md.push(`- Verified expirations (Tier A+B): **${withEnd.length}**`);
  md.push(`- Historical: **${historical.length}** · Future: **${future.length}**`);
  md.push(`- Future franchise: **${franchiseFuture.length}** · Future management: **${managementFuture.length}**`);
  md.push(`- ${report.identityMath.identity}`);
  md.push("");
  md.push("## Joplin correction");
  md.push(
    `- Hilton Garden Inn Joplin end **${joplin?.end}** → **${joplin?.bucket}** (months=${joplin?.recomputedMonths}). Dates preserved.`,
  );
  md.push("");
  md.push("## Hilton Philadelphia Airport");
  md.push(
    `- Tier A retained · MANAGEMENT · end 2035-03-30 · notice 180d → deadline 2034-10-01 · near-term outreach **false** · current status unconfirmed.`,
  );
  md.push("");
  md.push("## HOLD / routing");
  md.push(`- HOLD in projections: **${holdLeak.length}** (must be 0)`);
  md.push("- Routing: deterministic Search-first · Agent MANUAL_ESCALATION_ONLY · Jev shadow DETERMINISTIC_ONLY");
  md.push("");
  if (failures.length) {
    md.push("## Failures");
    for (const f of failures) md.push(`- ${f}`);
  }
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT_V5_FINAL_VERIFICATION.md"), md.join("\n"), "utf8");

  console.log(JSON.stringify({ pass: report.pass, failures, identity: report.identityMath }, null, 2));
  if (!report.pass) process.exitCode = 1;
}

main();
