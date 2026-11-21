#!/usr/bin/env node
/**
 * Founder-review table for CREATE_READY US candidates. No HPC creates.
 *
 *   node scripts/sec-edgar-contract-timing-v4-create-ready-founder-review.mjs
 *   node scripts/sec-edgar-contract-timing-v4-create-ready-founder-review.mjs --hpc-recheck
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const SRC = path.join(OUT_DIR, "US_PILOT_ENRICHED_MANIFEST.json");
const HPC_RECHECK = process.argv.includes("--hpc-recheck");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const enriched = JSON.parse(fs.readFileSync(SRC, "utf8"));
  const createReady = (enriched.candidates || []).filter((c) => c.classification === "CREATE_READY");
  const pending = (enriched.candidates || []).filter(
    (c) => c.classification === "CREATE_CANDIDATE_PENDING_VERIFICATION",
  );

  const findings = new Map();
  const v3p = path.join(ROOT, "reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json");
  if (fs.existsSync(v3p)) {
    for (const f of JSON.parse(fs.readFileSync(v3p, "utf8")).findings || []) {
      findings.set(f.findingId, f);
    }
  }

  let collisionMod = null;
  if (HPC_RECHECK) {
    collisionMod = await import("../lib/hotel-census/hpc-live-identity-collision-v1.js");
  }

  const rows = [];
  for (const c of createReady) {
    const f = findings.get(c.discoveryRecordIds?.[0]);
    let collision = c.existingHpcCollisionSearch || {};
    if (HPC_RECHECK && collisionMod) {
      try {
        const live = await collisionMod.performLiveHpcIdentityCollisionCheck({
          hotelName: c.proposedCanonicalHotelName,
          aliases: [],
          city: c.physicalAddressLocation?.city,
          country: "United States",
          brand: c.identityVerificationEvidence?.brandLabel || undefined,
        });
        collision = {
          outcome: live?.outcome || collision.outcome,
          hpcRecordId: live?.hpcRecordId || null,
          candidatesReviewed: (live?.candidates || []).slice(0, 5).map((x) => ({
            id: x.id || x.recordId,
            name: x.name || x.canonicalName,
          })),
          recheckedAt: new Date().toISOString(),
        };
        await sleep(100);
      } catch (err) {
        collision = { ...collision, recheckError: String(err.message || err).slice(0, 160) };
      }
    }

    let recommendation = "CREATE";
    if (collision.outcome === "HPC_EXISTING_RECORD_VERIFIED") recommendation = "LINK";
    else if (collision.outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW") recommendation = "HOLD";
    else if (collision.outcome !== "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT") recommendation = "HOLD";

    const brandOfficial = (c.sourceUrls || []).find((u) =>
      /marriott\.com|hilton\.com|hyatt\.com|ihg\.com/i.test(u),
    );
    const secSource = (c.sourceUrls || []).find((u) => /sec\.gov/i.test(u));

    rows.push({
      hotelName: c.proposedCanonicalHotelName,
      fullAddress: [
        c.physicalAddressLocation?.address,
        c.physicalAddressLocation?.city,
        c.physicalAddressLocation?.stateRegion,
        "United States",
      ]
        .filter(Boolean)
        .join(", "),
      brand:
        c.identityVerificationEvidence?.brandLabel ||
        c.identityVerificationEvidence?.brandFamily ||
        null,
      currentOperationEvidence: c.currentOperatingStatusEvidence?.class || null,
      officialHotelBrandSource: brandOfficial || null,
      historicalSecCmbsSource: secSource || null,
      relevantContractMilestone: {
        date: f?.primaryDate || null,
        meaning: f?.dateMeaning || null,
        accession: (c.filingAccessions || [])[0] || null,
      },
      existingHpcCollision: collision,
      intendedHpcFields: {
        "Property Name": c.proposedCanonicalHotelName,
        Country: "United States",
        City: c.physicalAddressLocation?.city || null,
        "State / Region": c.physicalAddressLocation?.stateRegion || null,
        "Street Address": c.physicalAddressLocation?.address || null,
        "Current Brand": c.identityVerificationEvidence?.brandLabel || null,
        "Publication State": "CUSTOMER_BLOCKED",
      },
      recommendation,
      founderCreateAuthorized: false,
      candidateId: c.candidateId,
    });
  }

  const report = {
    module: "V4_CREATE_READY_FOUNDER_REVIEW",
    status: "REVIEW_ONLY_NO_HPC_CREATE",
    generatedAt: new Date().toISOString(),
    hpcRecheck: HPC_RECHECK,
    createReadyCount: rows.length,
    pendingVerificationCount: pending.length,
    pendingVerificationHotelNames: pending.map((p) => p.proposedCanonicalHotelName),
    recommendationCounts: {
      CREATE: rows.filter((r) => r.recommendation === "CREATE").length,
      LINK: rows.filter((r) => r.recommendation === "LINK").length,
      HOLD: rows.filter((r) => r.recommendation === "HOLD").length,
    },
    rows,
    note: "Founder per-row approval required before any HPC CREATE. Do not pad CREATE_READY.",
  };

  const outJson = path.join(OUT_DIR, "CREATE_READY_FOUNDER_REVIEW.json");
  const outMd = path.join(OUT_DIR, "CREATE_READY_FOUNDER_REVIEW.md");
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2));
  fs.writeFileSync(
    outMd,
    `# CREATE_READY Founder Review (18 hotels)

**Status:** Review only · **No HPC creates** · HPC recheck: ${HPC_RECHECK}

| # | Hotel | Address | Brand | Status | Recommendation |
|---:|---|---|---|---|---|
${rows
  .map(
    (r, i) =>
      `| ${i + 1} | ${r.hotelName} | ${r.fullAddress} | ${r.brand || "—"} | ${r.currentOperationEvidence} | **${r.recommendation}** |`,
  )
  .join("\n")}

## Pending verification (research queue)

${pending.map((p) => `- ${p.proposedCanonicalHotelName}`).join("\n") || "_none_"}

## Recommendation counts

- CREATE: ${report.recommendationCounts.CREATE}
- LINK: ${report.recommendationCounts.LINK}
- HOLD: ${report.recommendationCounts.HOLD}
`,
  );

  console.log(
    JSON.stringify(
      {
        outJson,
        outMd,
        createReadyCount: rows.length,
        recommendationCounts: report.recommendationCounts,
        pendingVerificationCount: pending.length,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
