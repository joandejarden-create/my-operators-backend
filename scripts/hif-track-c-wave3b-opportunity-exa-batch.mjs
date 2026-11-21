#!/usr/bin/env node
/**
 * Track C Wave 3b — Opportunity-first HPC population + Exa benchmark V1.
 * Hard Exa spend cap: $10. Prefer free research. Lite Search + Contents only.
 * Exa Agent = MANUAL_ESCALATION_ONLY (not used). CUSTOMER_BLOCKED.
 *
 * --apply / --apply-hpc-create / --skip-exa
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  HPC_TABLE_ID,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_INVENTORY,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  buildDiscoveryInventoryFields,
  classifyWriteCategory,
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import {
  TRACK_C_STANDING_AUTH,
  MATCH_CLASS,
  PRIORITY_COUNTRY_LABELS,
  classifySecToHpcMatch,
  routeHandoff,
  buildTrackCDiscoveryHotel,
  dedupeDiscoveries,
  summarizeMatchClasses,
  summarizeCreateReadyAttrition,
  inferFamilySelect,
  normalizeCountry,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";
import {
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createContractTimingV2Budget } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const APPLY_HPC_CREATE = process.argv.includes("--apply-hpc-create");
const SKIP_EXA = process.argv.includes("--skip-exa");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-opportunity-exa-v1");
fs.mkdirSync(OUT, { recursive: true });
const EXA_CEILING = 10;
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const QUEUE = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures/hif-track-c/wave3b-opportunity-priority-queue-v1.json"), "utf8"),
);
const FREE = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures/hif-track-c/wave3b-free-enrichment-v1.json"), "utf8"),
);

const BRAND_URL_RE =
  /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|wyndham|choicehotels|bestwestern|accor|fairmont|fourseasons\.com|radissonhotels/i;
const INDEPENDENT_OFFICIAL_RE = /gideonputnam\.com|honeycreekresort\.com|visitlakeville\.org/i;

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
async function airtableGet(url) {
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json;
}
async function airtablePost(tableId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function airtablePatch(tableId, recordId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function findByFormula(tableId, formula) {
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const json = await airtableGet(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`);
  return json.records?.[0] || null;
}
async function countCountry(country) {
  let offset;
  let n = 0;
  do {
    const qs = new URLSearchParams({ pageSize: "100", filterByFormula: `{Country}="${country}"` });
    qs.append("fields[]", "Country");
    if (offset) qs.set("offset", offset);
    const json = await airtableGet(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`);
    n += (json.records || []).length;
    offset = json.offset;
  } while (offset);
  return n;
}

function isOfficialBrandUrl(url) {
  return Boolean(url && BRAND_URL_RE.test(url));
}
function isIndependentOfficialUrl(url) {
  return Boolean(url && INDEPENDENT_OFFICIAL_RE.test(url));
}

async function collide(hotel) {
  try {
    return await performLiveHpcIdentityCollisionCheck({
      hotelName: hotel.hotelName,
      aliases: [hotel.hotelName, hotel.historicalBrand].filter(Boolean),
      city: hotel.city,
      brand: hotel.currentBrand,
      address: hotel.address,
      country: hotel.physicalCountry || hotel.country,
      officialUrl: hotel.sourceUrl || hotel.officialUrl,
      adpPropertyId: hotel.discoveryPropertyId,
    });
  } catch (e) {
    return { outcome: "HPC_LIVE_LOOKUP_UNAVAILABLE", hpcRecordId: null, error: e.message };
  }
}

function evaluate(hotel, collision) {
  const url = hotel.sourceUrl || hotel.officialUrl || "";
  const hasExactAddress = Boolean(hotel.address && String(hotel.address).trim().length > 4);
  const hasOfficialBrandSource = isOfficialBrandUrl(url);
  const hasIndependentOfficial = isIndependentOfficialUrl(url);
  let matchClass = classifySecToHpcMatch({
    collisionOutcome: collision?.outcome,
    identityClass: hotel.identityClass || "VERIFIED_PROPERTY",
    hasExactAddress,
    hasOfficialBrandSource,
  });
  if (collision?.hpcRecordId) matchClass = MATCH_CLASS.MATCHED_EXISTING_HPC;
  if (hotel.projectLevelNoPhysicalHotelYet) matchClass = MATCH_CLASS.RESEARCH_REQUIRED;
  // Independent with DMO/official site: inventory-eligible but not CREATE_READY under current brand-domain gate
  if (
    matchClass === MATCH_CLASS.CREATE_CANDIDATE &&
    hasExactAddress &&
    hasIndependentOfficial &&
    !hasOfficialBrandSource
  ) {
    matchClass = MATCH_CLASS.RESEARCH_REQUIRED;
    hotel.gateRecommendation =
      "FOUNDER_RECOMMENDATION: accept verified independent official hotel site / DMO listing as Official Property URL equivalent when brand directory unavailable (Lakeville Inn / Gideon Putnam pattern).";
  }
  return { matchClass, hasExactAddress, hasOfficialBrandSource, hasIndependentOfficial };
}

function loadSecCohort(limit = 30) {
  const cmbs = JSON.parse(
    fs.readFileSync(path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cmbs-owner/DISTINCT_HOTELS.json"), "utf8"),
  );
  const deferred = new Set(["Courtyard Silver Spring North", "Courtyard Denver Stapleton"]);
  const us = (cmbs.hotels || []).filter((h) => String(h.hotelCountry || "").toUpperCase() === "US");
  const wave3Enrich = JSON.parse(
    fs.readFileSync(path.join(ROOT, "fixtures/hif-track-c/wave3-official-enrichment-v1.json"), "utf8"),
  );
  const enriched = wave3Enrich.byHotelName || {};
  // Prefer unresolved / not already enriched for Exa cohort C
  const unresolved = us.filter((h) => deferred.has(h.hotelName) || !enriched[h.hotelName]).slice(0, limit);
  return unresolved.map((h) => ({
    hotelName: h.hotelName,
    city: h.hotelMarket || null,
    country: "United States",
    priority: deferred.has(h.hotelName) ? "P3" : "P2",
    cohort: "C_SEC_UNRESOLVED",
    discoveryPropertyId: `disc_tc_opp_exa_${String(h.hotelName)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 40)}`,
  }));
}

async function loadControlUs(limit = 50) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    for (const f of ["Country", "Hotel Name (raw)", "City", "State / Region", "Address", "Current Brand", "Source Filing URL", "Discovery Property ID", "HPC Match Status"]) {
      qs.append("fields[]", f);
    }
    if (offset) qs.set("offset", offset);
    const json = await airtableGet(
      `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${inventoryTableId}?${qs}`,
    );
    for (const r of json.records || []) {
      if (!/united states|^us$/i.test(String(r.fields?.Country || ""))) continue;
      if (r.fields["HPC Match Status"] === "MATCHED") continue;
      const name = r.fields["Hotel Name (raw)"];
      if (!name) continue;
      out.push({
        hotelName: name,
        city: r.fields.City || null,
        stateRegion: r.fields["State / Region"] || null,
        address: r.fields.Address || null,
        currentBrand: r.fields["Current Brand"] || null,
        sourceUrl: r.fields["Source Filing URL"] || null,
        discoveryPropertyId: r.fields["Discovery Property ID"] || null,
        priority: "P4",
        cohort: "E_CONTROL_CENSUS",
        country: "United States",
      });
      if (out.length >= limit) return out;
    }
    offset = json.offset;
  } while (offset);
  return out;
}

async function main() {
  const started = Date.now();
  console.log(JSON.stringify({ wave: "3b-opportunity-exa", apply: APPLY, applyHpcCreate: APPLY_HPC_CREATE, skipExa: SKIP_EXA, exaCeiling: EXA_CEILING }, null, 2));

  const usBefore = await countCountry(PRIORITY_COUNTRY_LABELS.US);
  const mx = await countCountry(PRIORITY_COUNTRY_LABELS.MX);
  const br = await countCountry(PRIORITY_COUNTRY_LABELS.BR);

  const budget = createContractTimingV2Budget({
    ceilingUsd: EXA_CEILING,
    ledgerPath: path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-budget-ledger.json"),
    cwd: ROOT,
  });
  // Reset spent for this pilot ledger if fresh file — createContractTiming loads existing; use dedicated path
  const exaClient = createExaContractTimingClient({
    budget,
    cacheDir: path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-cache"),
  });

  const methodStats = {
    FREE_SOURCE: { investigated: 0, verifiedIdentity: 0, officialUrl: 0, brandVerified: 0, createReady: 0, matched: 0, creates: 0, requests: 0, spendUsd: 0 },
    EXA_ASSISTED: { investigated: 0, verifiedIdentity: 0, officialUrl: 0, brandVerified: 0, createReady: 0, matched: 0, creates: 0, requests: 0, spendUsd: 0 },
    HYBRID: { investigated: 0, verifiedIdentity: 0, officialUrl: 0, brandVerified: 0, createReady: 0, matched: 0, creates: 0, requests: 0, spendUsd: 0 },
  };

  const freeMap = FREE.byHotelName || {};
  const p0p1 = QUEUE.priorityQueue || [];
  const secCohort = loadSecCohort(30);
  const control = await loadControlUs(50);
  // Cohort B: near-term contractual = first 30 SEC US with markets (overlap with C handled by dedupe)
  const nearTerm = loadSecCohort(30).map((h) => ({ ...h, cohort: "B_NEAR_TERM_CONTRACTUAL", priority: "P2" }));

  const rawPool = dedupeDiscoveries([...p0p1, ...nearTerm, ...secCohort, ...control]);
  console.log(`[pool] distinct=${rawPool.length} p0p1=${p0p1.length} sec=${secCohort.length} control=${control.length}`);

  const rows = [];
  const createCandidates = [];
  const inventoryHotels = [];
  const heHandoff = [];
  const missingHpcLinks = [];

  for (const raw of rawPool) {
    const enrich = freeMap[raw.hotelName] || {};
    const hotel = buildTrackCDiscoveryHotel({
      hotelName: raw.hotelName,
      city: enrich.city || raw.city,
      stateRegion: enrich.stateRegion || raw.stateRegion,
      country: enrich.country || raw.country || "United States",
      address: enrich.address || raw.address || null,
      currentBrand: enrich.currentBrand || raw.currentBrand || null,
      historicalBrand: enrich.historicalBrand || null,
      sourceUrl: enrich.officialUrl || raw.sourceUrl || raw.officialUrlHint || null,
      discoveryPropertyId: raw.discoveryPropertyId,
      discoveryMethod: "TRACK_C_WAVE3B_OPPORTUNITY_FIRST",
      identityClass: raw.projectLevelNoPhysicalHotelYet ? "PROJECT_LEVEL" : "VERIFIED_PROPERTY",
      projectLevelNoPhysicalHotelYet: raw.projectLevelNoPhysicalHotelYet,
      notes: `CUSTOMER_BLOCKED | Track C wave3b | priority=${raw.priority || "P4"} | cohort=${raw.cohort || ""} | opp=${raw.opportunityType || ""} | ic=${raw.icStatus || ""}`,
      handoffs: routeHandoff({
        hasFranchiseOrManagementSignal: /BCT|MCT|CONCESSION|MANAGEMENT/i.test(String(raw.opportunityType || "")),
        hasContractualAgreementEvidence: raw.cohort === "B_NEAR_TERM_CONTRACTUAL" || raw.cohort === "C_SEC_UNRESOLVED",
      }),
      roomCount: enrich.rooms || null,
    });

    methodStats.FREE_SOURCE.investigated += 1;
    methodStats.HYBRID.investigated += 1;
    if (hotel.address) methodStats.FREE_SOURCE.verifiedIdentity += 1;
    if (isOfficialBrandUrl(hotel.sourceUrl)) {
      methodStats.FREE_SOURCE.officialUrl += 1;
      methodStats.FREE_SOURCE.brandVerified += 1;
    }

    let methodUsed = "FREE_SOURCE";
    let exaEvidence = null;

    const needsExa =
      !SKIP_EXA &&
      !raw.projectLevelNoPhysicalHotelYet &&
      (!hotel.address || !isOfficialBrandUrl(hotel.sourceUrl)) &&
      ["P0", "P1", "P2", "P3"].includes(raw.priority || "") &&
      budget.remaining() > 0.01;

    if (needsExa) {
      const q = `${hotel.hotelName} ${hotel.city || ""} hotel official address site:marriott.com OR site:hilton.com OR site:ihg.com OR site:hyatt.com OR site:fourseasons.com`;
      const search = await exaClient.search({
        query: q,
        type: "lite",
        numResults: 5,
        includeDomains: ["marriott.com", "hilton.com", "ihg.com", "hyatt.com", "fourseasons.com", "gideonputnam.com"],
      });
      methodStats.EXA_ASSISTED.investigated += 1;
      methodStats.EXA_ASSISTED.requests += 1;
      methodStats.HYBRID.requests += 1;
      if (search.ok) {
        methodUsed = "HYBRID";
        const results = search.results || [];
        const top = results.find((r) => isOfficialBrandUrl(r.url)) || results[0];
        if (top?.url && isOfficialBrandUrl(top.url) && !hotel.sourceUrl) {
          hotel.sourceUrl = top.url;
          methodStats.EXA_ASSISTED.officialUrl += 1;
          methodStats.EXA_ASSISTED.brandVerified += 1;
          methodStats.HYBRID.officialUrl += 1;
        }
        exaEvidence = {
          ok: true,
          topUrl: top?.url || null,
          title: top?.title || null,
          fromCache: Boolean(search.fromCache),
          costUsd: search.costDollars?.total ?? null,
        };
        // Contents only if still missing address and budget allows
        if (!hotel.address && top?.url && budget.remaining() > 0.002) {
          const contents = await exaClient.contents([top.url]);
          methodStats.EXA_ASSISTED.requests += 1;
          if (contents.ok) {
            const text = JSON.stringify(contents.results || contents).slice(0, 4000);
            const addr = text.match(
              /\d{1,5}\s+[A-Za-z0-9 .'-]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Highway|Hwy|Parkway|Pkwy)[^,]{0,40}/i,
            );
            if (addr && !hotel.address) {
              hotel.address = addr[0].trim().slice(0, 120);
              methodStats.EXA_ASSISTED.verifiedIdentity += 1;
              methodStats.HYBRID.verifiedIdentity += 1;
            }
          }
        }
      } else {
        exaEvidence = { ok: false, error: search.error || "EXA_FAIL" };
      }
    }

    const collision = await collide(hotel);
    const { matchClass, hasExactAddress, hasOfficialBrandSource } = evaluate(hotel, collision);
    if (collision?.hpcRecordId) {
      hotel.hpcRecordId = collision.hpcRecordId;
      hotel.hpcMatchStatus = "MATCHED";
      methodStats[methodUsed === "HYBRID" ? "HYBRID" : "FREE_SOURCE"].matched += 1;
    }
    if (matchClass === MATCH_CLASS.CREATE_READY) {
      methodStats[methodUsed === "HYBRID" ? "HYBRID" : "FREE_SOURCE"].createReady += 1;
      if (methodUsed === "HYBRID") methodStats.EXA_ASSISTED.createReady += 1;
    }

    const row = {
      priority: raw.priority || "P4",
      cohort: raw.cohort || "E_CONTROL_CENSUS",
      hotelName: hotel.hotelName,
      city: hotel.city,
      country: hotel.physicalCountry,
      address: hotel.address,
      currentBrand: hotel.currentBrand,
      sourceUrl: hotel.sourceUrl,
      matchClass,
      collisionOutcome: collision?.outcome || null,
      hpcRecordId: collision?.hpcRecordId || null,
      opportunityType: raw.opportunityType || null,
      qualificationTier: raw.qualificationTier || null,
      findingId: raw.findingId || null,
      discoveryPropertyId: hotel.discoveryPropertyId,
      methodUsed,
      exaEvidence,
      projectLevel: Boolean(raw.projectLevelNoPhysicalHotelYet),
      gateRecommendation: hotel.gateRecommendation || null,
      doNotPromote: Boolean(raw.doNotPromoteToQualifiedCompetitive),
    };
    rows.push(row);
    inventoryHotels.push({ ...hotel, matchClass });

    const isCommercial = ["P0", "P1"].includes(raw.priority);
    if (isCommercial && !collision?.hpcRecordId) {
      missingHpcLinks.push({
        hotelName: hotel.hotelName,
        reason: matchClass,
        opportunityType: raw.opportunityType,
        qualificationTier: raw.qualificationTier,
        findingId: raw.findingId,
      });
    }

    if (isCommercial) {
      heHandoff.push({
        priority: raw.priority,
        hotelName: hotel.hotelName,
        market: [hotel.city, hotel.stateProvince, hotel.physicalCountry].filter(Boolean).join(", "),
        hpcRecordId: collision?.hpcRecordId || null,
        discoveryFindingId: raw.findingId || null,
        discoveryPropertyId: hotel.discoveryPropertyId,
        agreementType: raw.opportunityType || null,
        opportunityType: raw.opportunityType || null,
        opportunityQualificationTier: raw.qualificationTier || null,
        decisionTiming: raw.notes || null,
        commercialStatus: raw.icStatus || raw.qualificationTier || null,
        identityEvidence: hotel.sourceUrl || null,
        ownershipOperatorEvidence: enrich.roomsEvidence || null,
        hotelExplorerProfileStatus: "UNKNOWN_NOT_QUERIED_FROM_TRACK_C",
        missingIntelligence: [
          !hotel.address ? "address" : null,
          !hotel.sourceUrl ? "official_url" : null,
          !collision?.hpcRecordId ? "hpc_link" : null,
        ].filter(Boolean),
        recommendedResearchPriority: raw.priority === "P0" ? "IMMEDIATE" : "NEXT",
        matchClass,
        doNotPromoteToQualifiedCompetitive: Boolean(raw.doNotPromoteToQualifiedCompetitive),
      });
    }

    if (matchClass === MATCH_CLASS.CREATE_READY && hasExactAddress && hasOfficialBrandSource && !collision?.hpcRecordId) {
      createCandidates.push({
        discoveryPropertyId: hotel.discoveryPropertyId,
        hotelName: hotel.hotelName,
        priority: raw.priority,
        methodUsed,
        hpcFields: {
          "Property Name": hotel.hotelName,
          "Canonical Property Name": hotel.hotelName,
          Country: hotel.physicalCountry,
          City: hotel.city,
          "State / Region": hotel.stateProvince,
          Address: hotel.address,
          "Current Brand": hotel.currentBrand,
          "Affiliation Status": hotel.currentBrand && !/independent/i.test(hotel.currentBrand) ? "Branded" : "Independent",
          "Family / Source Family": inferFamilySelect(hotel.currentBrand),
          "Official Property URL": hotel.sourceUrl,
        },
      });
    }
    await sleep(60);
  }

  writeJson("OPPORTUNITY_IDENTITY_ROWS.json", {
    asOf: new Date().toISOString(),
    distinctInvestigated: rows.length,
    summary: summarizeMatchClasses(rows),
    attrition: summarizeCreateReadyAttrition(rows),
    rows,
  });
  writeJson("MISSING_HPC_LINKS.json", { asOf: new Date().toISOString(), count: missingHpcLinks.length, items: missingHpcLinks });
  writeJson("HOTEL_EXPLORER_HANDOFF.json", {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    note: "Track C prepares handoff only — does not modify HE worktree or publish",
    items: heHandoff,
  });

  const exaSpend = Number(budget.spentUsd() || 0);
  methodStats.EXA_ASSISTED.spendUsd = exaSpend;
  methodStats.HYBRID.spendUsd = exaSpend;

  writeJson("BENCHMARK_SCORECARD.json", {
    asOf: new Date().toISOString(),
    exaHardCapUsd: EXA_CEILING,
    actualExaSpendUsd: exaSpend,
    remainingUsd: Math.max(0, EXA_CEILING - exaSpend),
    methodStats,
    elapsedMs: Date.now() - started,
    recommendation:
      exaSpend > 0
        ? "HYBRID: free official directories first; Exa Lite only for missing brand URL/address on P0–P3. Agent not needed for identity URL recovery in this pilot."
        : "FREE_SOURCE sufficient for most P0 brand-directory hotels; Exa unused or cached. Prefer free-first hybrid at scale.",
  });

  // Persistence
  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC_CREATE ? "APPLY_WITH_HPC_CREATE" : "APPLY_INVENTORY") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    stream: "TRACK_C_WAVE3B_OPPORTUNITY_EXA",
    standingAuth: TRACK_C_STANDING_AUTH,
    exaSpendUsd: exaSpend,
    operations: [],
    summary: { creates: 0, noops: 0, conflicts: 0, staged: 0, hpcCreates: 0, hpcCreateFailures: 0, hpcReusedExisting: 0, existingEnriched: 0 },
    readBack: [],
    hpcCreateResults: [],
  };

  // Persist inventory for commercial + CREATE_READY only (not entire control cohort)
  const persistPool = dedupeDiscoveries(
    inventoryHotels.filter((h) => {
      const r = rows.find((x) => x.discoveryPropertyId === h.discoveryPropertyId);
      return r && (["P0", "P1"].includes(r.priority) || r.matchClass === MATCH_CLASS.CREATE_READY);
    }),
  );

  for (const h of persistPool) {
    const cls = classifyWriteCategory({
      destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
      verified: h.verified !== false,
      evidenceSupported: Boolean(h.sourceUrl),
      sourceUrl: h.sourceUrl,
      identityClass: h.identityClass === "PROJECT_LEVEL" ? "PROBABLE_PROPERTY" : h.identityClass,
    });
    if (cls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
      ledger.operations.push({ id: h.discoveryPropertyId, action: "STAGED", reason: cls.reason });
      ledger.summary.staged += 1;
      continue;
    }
    const fields = buildDiscoveryInventoryFields(h);
    fields[MAP_DISCOVERY_INVENTORY.discoveryMethod] = h.discoveryMethod;
    if (h.notes) fields[MAP_DISCOVERY_INVENTORY.notesQuarantine] = h.notes;
    const existing = await findByFormula(
      inventoryTableId,
      `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${h.discoveryPropertyId}"`,
    );
    if (!APPLY) {
      ledger.operations.push({ id: h.discoveryPropertyId, action: existing ? "WOULD_NOOP" : "WOULD_CREATE" });
      if (existing) ledger.summary.noops += 1;
      else ledger.summary.creates += 1;
      continue;
    }
    if (existing) {
      if (h.hpcRecordId) {
        await airtablePatch(inventoryTableId, existing.id, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [h.hpcRecordId],
        });
        ledger.summary.existingEnriched += 1;
      }
      ledger.operations.push({ id: h.discoveryPropertyId, action: "NOOP_EXISTING", airtableId: existing.id });
      ledger.summary.noops += 1;
      continue;
    }
    const created = await airtablePost(inventoryTableId, fields);
    if (!created.ok) {
      ledger.operations.push({ id: h.discoveryPropertyId, action: "CREATE_FAILED", error: created.json });
      ledger.summary.conflicts += 1;
      continue;
    }
    ledger.operations.push({ id: h.discoveryPropertyId, action: "CREATED", airtableId: created.json.id });
    ledger.summary.creates += 1;
    ledger.readBack.push({ id: created.json.id, discoveryPropertyId: h.discoveryPropertyId });
    await sleep(70);
  }

  let founderAuth = null;
  if (APPLY_HPC_CREATE && createCandidates.length) {
    let gitName = "";
    let gitEmail = "";
    try {
      gitName = execSync("git config user.name", { cwd: ROOT, encoding: "utf8" }).trim();
      gitEmail = execSync("git config user.email", { cwd: ROOT, encoding: "utf8" }).trim();
    } catch {
      /* ignore */
    }
    founderAuth = resolveAuthenticatedFounderIdentity({
      envValue: process.env.HPC_CREATE_APPROVER_IDENTITY || "",
      gitName,
      gitEmail,
    });
  }

  // Prefer P0 then P1 creates
  createCandidates.sort((a, b) => String(a.priority).localeCompare(String(b.priority)));
  const batch = createCandidates.slice(0, TRACK_C_STANDING_AUTH.maxHpcCreateBatch);
  for (const row of batch) {
    const fields = { ...row.hpcFields };
    for (const k of Object.keys(fields)) {
      if (fields[k] == null || fields[k] === "") delete fields[k];
    }
    if (!APPLY_HPC_CREATE || !founderAuth?.ok) {
      ledger.operations.push({ id: row.discoveryPropertyId, action: "HPC_CREATE_STAGED", hotelName: row.hotelName });
      ledger.summary.staged += 1;
      continue;
    }
    const auth = persistHpcCreateAuthorization({
      runId: `track_c_wave3b_${new Date().toISOString().slice(0, 10)}`,
      draftPropertyId: row.discoveryPropertyId,
      hotelName: row.hotelName,
      fields,
      approverIdentity: founderAuth.approverIdentity,
      authorizationSource: `${TRACK_C_STANDING_AUTH.source}_WAVE3B_OPPORTUNITY_EXA`,
    });
    if (!auth.ok) {
      ledger.hpcCreateResults.push({ draftPropertyId: row.discoveryPropertyId, ok: false, error: auth.error });
      ledger.summary.hpcCreateFailures += 1;
      continue;
    }
    const exec = await executeAuthorizedHpcCreate({
      runId: `track_c_wave3b_${new Date().toISOString().slice(0, 10)}`,
      authorization: auth.record,
      dryRun: false,
      apply: true,
      simulation: false,
    });
    ledger.hpcCreateResults.push({
      draftPropertyId: row.discoveryPropertyId,
      hotelName: row.hotelName,
      priority: row.priority,
      methodUsed: row.methodUsed,
      ok: exec.ok,
      state: exec.state,
      hpcRecordId: exec.hpcRecordId || null,
      reusedExisting: exec.reusedExisting || false,
      error: exec.error || null,
    });
    if (exec.ok && exec.reusedExisting) ledger.summary.hpcReusedExisting += 1;
    else if (exec.ok && exec.hpcRecordId) {
      ledger.summary.hpcCreates += 1;
      const inv = await findByFormula(
        inventoryTableId,
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${row.discoveryPropertyId}"`,
      );
      if (inv?.id) {
        await airtablePatch(inventoryTableId, inv.id, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [exec.hpcRecordId],
        });
        ledger.summary.existingEnriched += 1;
      }
    } else ledger.summary.hpcCreateFailures += 1;
    await sleep(180);
  }

  writeJson(APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json", ledger);

  const usAfter = await countCountry(PRIORITY_COUNTRY_LABELS.US);
  const mxAfter = await countCountry(PRIORITY_COUNTRY_LABELS.MX);
  const brAfter = await countCountry(PRIORITY_COUNTRY_LABELS.BR);

  const p0Linked = heHandoff.filter((h) => h.priority === "P0" && h.hpcRecordId).length;
  const p0Total = heHandoff.filter((h) => h.priority === "P0").length;

  const commercialScorecard = {
    qualifiedOpenBctMct: heHandoff.filter((h) => /FIRST_100/.test(String(h.opportunityQualificationTier || ""))).length,
    emergingAdjacent: heHandoff.filter((h) => h.priority === "P1").length,
    distinctHotelsInHandoff: heHandoff.length,
    withHpcLink: heHandoff.filter((h) => h.hpcRecordId).length,
    missingHpcLink: heHandoff.filter((h) => !h.hpcRecordId).length,
    p0HpcLinkageRate: p0Total ? Number((p0Linked / p0Total).toFixed(4)) : 0,
    heProfilesKnown: 0,
    heProfilesUnknown: heHandoff.length,
  };
  writeJson("COMMERCIAL_CRITICAL_MASS_SCORECARD.json", commercialScorecard);

  const report = {
    asOf: new Date().toISOString(),
    stream: "TRACK_C_WAVE3B_OPPORTUNITY_EXA",
    publicationState: "CUSTOMER_BLOCKED",
    baselineCommercial: {
      first100OpenBct: ["Lakeville Inn", "Kimpton Shorebreak Huntington Beach Resort", "Courtyard by Marriott Denver Downtown"],
      first100OpenMct: ["Hilton Trinidad & Conference Centre", "Four Seasons Hotel Bengaluru at Embassy ONE"],
      legacyNotOpenCompetitive: ["Legacy Hotel Green Bay"],
      source: "Track A CB2/CB3 IC qualification (PR #544 lineage)",
    },
    hpcBeforeAfter: { US: { before: usBefore, after: usAfter }, Mexico: { before: mx, after: mxAfter }, Brazil: { before: br, after: brAfter } },
    investigatedDistinct: rows.length,
    createCandidates: createCandidates.length,
    persistence: ledger.summary,
    exaSpendUsd: exaSpend,
    exaHardCapUsd: EXA_CEILING,
    commercialScorecard,
    missingHpcLinks: missingHpcLinks.length,
    calaPreserved: mxAfter === mx && brAfter === br,
    elapsedMs: Date.now() - started,
    founderGateRecommendation:
      "Accept verified independent official hotel website (and optionally DMO listing with corroborating address) as Official Property URL for independents such as Lakeville Inn and Gideon Putnam — without weakening brand-directory requirement for branded hotels.",
    nextCheckpoint: "Resolve Lakeville/Gideon CREATE after gate decision; Exa Lite batch for remaining SEC P2/P3 within residual budget; HE research on P0 linked hotels",
  };
  writeJson("FOUNDER_TRACK_C_WAVE3B_OPPORTUNITY_EXA_REPORT.json", report);
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_TRACK_C_WAVE3B_OPPORTUNITY_EXA_REPORT.md"),
    `# Track C Wave 3b — Opportunity-First + Exa Benchmark V1

US HPC ${usBefore}→${usAfter} · Exa spend $${exaSpend.toFixed(4)} / $${EXA_CEILING} cap · Distinct investigated ${rows.length}  
P0/P1 handoff hotels: ${heHandoff.length} · Missing HPC links: ${missingHpcLinks.length} · HPC creates: ${ledger.summary.hpcCreates}  
CALA preserved: ${report.calaPreserved} · CUSTOMER_BLOCKED · Agent not used
`,
  );

  console.log(JSON.stringify({ done: true, report, ledger: ledger.summary, createCandidates: createCandidates.length }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
