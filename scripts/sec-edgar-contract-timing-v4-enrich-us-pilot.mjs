#!/usr/bin/env node
/**
 * Enrich US pilot shortlist with Exa current-property corroboration (budget-capped).
 * Never creates HPC. Never pads CREATE_READY.
 *
 *   node scripts/sec-edgar-contract-timing-v4-enrich-us-pilot.mjs
 *   node scripts/sec-edgar-contract-timing-v4-enrich-us-pilot.mjs --limit=20 --max-spend=3
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  classifyAfterCollision,
  meetsCreateReadyEvidenceGates,
  PILOT_DISPOSITION,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v4-us-pilot-selection-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const SRC = path.join(OUT_DIR, "US_PILOT_SELECTION_DRY_RUN.json");
const OUT = path.join(OUT_DIR, "US_PILOT_ENRICHED_MANIFEST.json");
const RESEARCH_PILOT = path.join(OUT_DIR, "RESEARCH_MODE_PILOT_PACKET.json");

function argNum(name, def) {
  const a = process.argv.find((x) => x.startsWith(`--${name}=`));
  return a ? Number(a.split("=")[1]) : def;
}

function isBrandOfficialHotelUrl(url) {
  const u = String(url || "").toLowerCase();
  return (
    /marriott\.com\/.*\/hotels\//.test(u) ||
    /hilton\.com\/.*\/hotels\//.test(u) ||
    /hyatt\.com\/.*\/hotel\//.test(u) ||
    /ihg\.com\/.*\/hotels\//.test(u) ||
    /wyndhamhotels\.com\//.test(u) ||
    /choicehotels\.com\//.test(u) ||
    /radissonhotels\.com\//.test(u)
  );
}

function inferOperatingStatus(text, urls = []) {
  const t = String(text || "").toLowerCase();
  if (/\b(permanently closed|closed permanently|demolished|ceased operations)\b/.test(t)) {
    return "CLOSED";
  }
  if (/\b(now (a |an )?(hilton|marriott|hyatt|ihg)|reflagged|rebranded as)\b/.test(t)) {
    return "REBRANDED";
  }
  if (urls.some(isBrandOfficialHotelUrl)) return "OPERATING";
  if (
    /\b(book (a )?room|book now|check rates|guest rooms|hotel amenities|official site|rooms?\s*\||overview\s*\|)\b/.test(
      t,
    )
  ) {
    return "OPERATING";
  }
  if (/\b(coming soon|under construction|planned)\b/.test(t)) return "PIPELINE";
  return "UNKNOWN_REQUIRES_CORROBORATION";
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  if (!fs.existsSync(SRC)) {
    throw new Error(`Missing ${SRC} — run us-pilot-select first`);
  }
  const selection = JSON.parse(fs.readFileSync(SRC, "utf8"));
  const limit = argNum("limit", 25);
  const maxSpend = argNum("max-spend", 3);
  const shortlist = (selection.shortlist || []).slice(0, limit);

  const exa = createExaContractTimingClient({ env: process.env });
  let spent = 0;
  const enriched = [];

  for (const row of shortlist) {
    const query = [
      `"${row.hotelName}"`,
      row.city,
      row.stateRegion,
      "hotel official",
      row.brandLabel || row.brandFamily || "",
    ]
      .filter(Boolean)
      .join(" ");

    let corroboration = {
      independentCurrentEvidence: false,
      operatingStatus: "UNKNOWN_REQUIRES_CORROBORATION",
      sourceUrls: [...(row.sourceUrls || [])],
      filingAccessions: [...(row.filingAccessions || [])],
      streetAddress: row.addressHint || null,
      city: row.city,
      stateRegion: row.stateRegion,
      aliases: [],
      brandAffiliation: row.brandLabel || row.brandFamily || null,
      exaResults: [],
      note: null,
      spendUsd: 0,
    };

    if (spent < maxSpend && exa?.search) {
      try {
        const res = await exa.search({
          query,
          numResults: 5,
          type: "lite",
        });
        const cost = Number(res?.costUsd || res?.usd || 0.005) || 0.005;
        if (!res?.fromCache) spent += cost;
        corroboration.spendUsd = res?.fromCache ? 0 : cost;
        const results = res?.results || res?.data?.results || [];
        if (res?.ok && Array.isArray(results)) {
          const texts = [];
          for (const r of results.slice(0, 5)) {
            const url = r.url || r.link || null;
            if (url) corroboration.sourceUrls.push(url);
            const title = r.title || "";
            const snip = r.text || r.snippet || r.summary || "";
            texts.push(`${title} ${snip}`);
            corroboration.exaResults.push({
              url,
              title: title.slice(0, 120),
              score: r.score || null,
            });
          }
          const blob = texts.join(" \n ");
          const urls = corroboration.exaResults.map((x) => x.url).filter(Boolean);
          corroboration.operatingStatus = inferOperatingStatus(blob, urls);
          const nameHit = new RegExp(
            String(row.hotelName || "")
              .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
              .slice(0, 40),
            "i",
          );
          const geoHit =
            new RegExp(String(row.city || ""), "i").test(blob) ||
            new RegExp(String(row.stateRegion || ""), "i").test(blob) ||
            urls.some((u) =>
              new RegExp(String(row.city || "").replace(/\s+/g, "[-_]"), "i").test(u),
            );
          const brandOfficial = urls.some(isBrandOfficialHotelUrl);
          if (
            (nameHit.test(blob) || brandOfficial) &&
            geoHit &&
            corroboration.operatingStatus !== "UNKNOWN_REQUIRES_CORROBORATION" &&
            corroboration.exaResults.some((x) => x.url)
          ) {
            corroboration.independentCurrentEvidence = true;
            corroboration.note = brandOfficial
              ? "Brand official hotel page treated as independent current-property evidence"
              : null;
          } else {
            corroboration.note =
              "Exa returned results but independent current operating status not confirmed";
          }
        } else {
          corroboration.note = res?.error || "EXA_SEARCH_FAILED";
        }
      } catch (err) {
        corroboration.note = String(err.message || err).slice(0, 200);
      }
      await sleep(200);
    } else if (spent >= maxSpend) {
      corroboration.note = "SKIPPED_BUDGET_CEILING";
    } else {
      corroboration.note = "EXA_CLIENT_UNAVAILABLE";
    }

    // Dedupe urls
    corroboration.sourceUrls = [...new Set(corroboration.sourceUrls.filter(Boolean))];

    const collision = {
      ...(row.collision || {}),
      candidatesReviewed:
        row.collision?.candidatesReviewed || row.collision?.candidates || [],
      candidates: row.collision?.candidatesReviewed || row.collision?.candidates || [],
    };

    const hotel = {
      ...row,
      accessions: row.filingAccessions || [],
      sourceUrls: corroboration.sourceUrls,
      addressHint: row.addressHint || corroboration.streetAddress,
      operatingStatus: corroboration.operatingStatus,
    };

    let disposition = row.disposition;
    if (
      row.preDisposition === "CREATE_READY_PENDING_COLLISION" ||
      disposition === PILOT_DISPOSITION.CREATE_CANDIDATE_PENDING_VERIFICATION ||
      disposition === PILOT_DISPOSITION.CREATE_READY
    ) {
      disposition = classifyAfterCollision(
        "CREATE_READY_PENDING_COLLISION",
        collision,
        { ...hotel, ...corroboration },
      );
    }

    const gates = meetsCreateReadyEvidenceGates(hotel, collision, corroboration);

    enriched.push({
      candidateId: `hpc_pilot_${String(enriched.length + 1).padStart(3, "0")}`,
      discoveryRecordIds: row.findingIds || [],
      proposedCanonicalHotelName: row.hotelName,
      physicalAddressLocation: {
        address: row.addressHint || null,
        city: row.city,
        stateRegion: row.stateRegion,
        country: "United States",
      },
      currentOperatingStatusEvidence: {
        class: corroboration.operatingStatus,
        independentCurrentEvidence: corroboration.independentCurrentEvidence,
        sources: corroboration.exaResults.slice(0, 3),
        note: corroboration.note,
      },
      identityVerificationEvidence: {
        score: row.score,
        brandFamily: row.brandFamily,
        brandLabel: row.brandLabel,
        waves: row.waves,
      },
      existingHpcCollisionSearch: {
        outcome: collision.outcome || "NOT_RUN",
        hpcRecordId: collision.hpcRecordId || null,
        candidatesReviewed: collision.candidatesReviewed || [],
      },
      relatedContractsAndMilestones: {
        milestoneCount: row.milestoneCount,
        futureMilestoneCount: row.futureMilestoneCount,
      },
      sourceUrls: corroboration.sourceUrls,
      filingAccessions: corroboration.filingAccessions,
      classification: disposition,
      createReadyGates: gates,
      approvalDisposition:
        disposition === PILOT_DISPOSITION.CREATE_READY
          ? "PENDING_FOUNDER_CREATE_APPROVAL"
          : disposition === PILOT_DISPOSITION.LINK_EXISTING
            ? "PENDING_FOUNDER_LINK_APPROVAL"
            : "HOLD_RESEARCH",
      publicationRestrictions: {
        publicationState: "CUSTOMER_BLOCKED",
        createApplyAuthorized: false,
      },
      hotelExplorerEnrichmentQueueStatus: "QUEUED_PENDING_CREATE_OR_LINK",
      founderCreateAuthorized: false,
    });
  }

  const counts = {
    LINK_EXISTING: enriched.filter((c) => c.classification === "LINK_EXISTING").length,
    CREATE_READY: enriched.filter((c) => c.classification === "CREATE_READY").length,
    CREATE_CANDIDATE_PENDING_VERIFICATION: enriched.filter(
      (c) => c.classification === "CREATE_CANDIDATE_PENDING_VERIFICATION",
    ).length,
    RESEARCH_REQUIRED: enriched.filter((c) => c.classification === "RESEARCH_REQUIRED").length,
    REJECT_AMBIGUOUS: enriched.filter((c) => c.classification === "REJECT_AMBIGUOUS").length,
    HISTORICAL_ONLY: enriched.filter((c) => c.classification === "HISTORICAL_ONLY").length,
  };

  const body = {
    module: "V4_US_PILOT_ENRICHED_MANIFEST",
    status: "DRY_RUN_ENRICHED_STOP_FOR_FOUNDER",
    generatedAt: new Date().toISOString(),
    baseId: "appCCUsuGsE1ifoLk",
    policy: {
      noHpcCreateApply: true,
      doNotPadCreateReady: true,
      createReadyRequiresIndependentCorroboration: true,
      maxSpendUsd: maxSpend,
      spentUsd: Number(spent.toFixed(4)),
    },
    selectionUniverseSummary: selection.summary || {},
    enrichedCounts: counts,
    enrichedCount: enriched.length,
    meetsCreateReadyBandWithoutPadding: counts.CREATE_READY >= 25 && counts.CREATE_READY <= 50,
    actualCreateReady: counts.CREATE_READY,
    note:
      counts.CREATE_READY < 25
        ? `Only ${counts.CREATE_READY} CREATE_READY after corroboration — do not pad`
        : "CREATE_READY band met without padding",
    candidates: enriched,
    stop: { hpcCreate: "REQUIRES_PER_ROW_FOUNDER_MANIFEST_APPROVAL" },
  };
  const sha = crypto.createHash("sha256").update(JSON.stringify(body.candidates)).digest("hex");
  body.manifestSha256 = sha;
  fs.writeFileSync(OUT, JSON.stringify(body, null, 2));

  // Research Mode pilot: top VERIFIED-quality agreement candidates from enrichment with SEC provenance
  const pilotCandidates = enriched
    .filter(
      (c) =>
        (c.filingAccessions || []).length > 0 &&
        (c.sourceUrls || []).some((u) => /sec\.gov/i.test(u)) &&
        c.classification !== "REJECT_AMBIGUOUS",
    )
    .slice(0, 20)
    .map((c, i) => ({
      pilotRowId: `research_pilot_${String(i + 1).padStart(2, "0")}`,
      hotelName: c.proposedCanonicalHotelName,
      city: c.physicalAddressLocation.city,
      stateRegion: c.physicalAddressLocation.stateRegion,
      address: c.physicalAddressLocation.address,
      agreementType: "FRANCHISE",
      dateMeaning: "FRANCHISE_EXPIRATION",
      historicalApplicability: "HISTORICALLY_DISCLOSED_STATUS_UNCONFIRMED",
      publicationState: "CUSTOMER_BLOCKED",
      sourceUrls: c.sourceUrls.filter((u) => /sec\.gov/i.test(u)).slice(0, 2),
      filingAccessions: c.filingAccessions.slice(0, 2),
      discoveryRecordIds: c.discoveryRecordIds,
      classification: c.classification,
      requires: [
        "discovery_inventory_link",
        "contract_parent_link",
        "agreement_type",
        "source_provenance",
        "date_meaning",
        "historical_applicability",
        "publication_state_customer_blocked",
      ],
    }));

  const researchPacket = {
    module: "V4_RESEARCH_MODE_PILOT_PACKET",
    status: "PREPARED_PENDING_SCHEMA_AND_FOUNDER_PERSIST_AUTH",
    generatedAt: new Date().toISOString(),
    targetRows: "10-20",
    proposedRows: pilotCandidates.length,
    rows: pilotCandidates,
    gates: {
      schemaApplyAuthorized: false,
      researchPersistAuthorized: false,
      livePersistenceExecuted: false,
      readbackExecuted: false,
      idempotencyRerunExecuted: false,
    },
    instructions: [
      "1. Founder approve CORRECTED_SCHEMA_APPLY_PACKET.json + HIF_SCHEMA_APPLY=1",
      "2. Founder authorize Research Mode persist for this pilot only (not full 481)",
      "3. Engine: CONFIG→…→PERSIST_RESEARCH with CUSTOMER_BLOCKED",
      "4. Independent Airtable read-back per batch",
      "5. Rerun same batch for idempotency; then incremental change detection",
    ],
    spendUsd: Number(spent.toFixed(4)),
  };
  fs.writeFileSync(RESEARCH_PILOT, JSON.stringify(researchPacket, null, 2));

  console.log(
    JSON.stringify(
      {
        out: OUT,
        researchPilot: RESEARCH_PILOT,
        counts,
        actualCreateReady: counts.CREATE_READY,
        spentUsd: Number(spent.toFixed(4)),
        pilotRows: pilotCandidates.length,
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
