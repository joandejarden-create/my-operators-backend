#!/usr/bin/env node
/**
 * HIF Track C — Wave orchestrator (C1–C5).
 *
 * Default: research + dry-run persistence ledger.
 * --apply              : persist Discovery Inventory / Findings
 * --apply-hpc-create   : authorized HPC creates for CREATE_READY (max 25)
 * --audit-only         : Wave C1 only
 * --skip-audit         : reuse prior WAVE_C1_HPC_AUDIT.json
 *
 * CUSTOMER_BLOCKED. No invented rooms/operators/addresses.
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
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  buildDiscoveryInventoryFields,
  buildCommercialFindingFields,
  classifyWriteCategory,
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import {
  TRACK_C_STANDING_AUTH,
  MATCH_CLASS,
  PRIORITY_COUNTRY_LABELS,
  AIMBRIDGE_PUBLIC_US_PORTFOLIO_SEED,
  AIMBRIDGE_PUBLIC_UK_PORTFOLIO_SEED,
  normalizeCountry,
  normalizeHotelKey,
  isUsCountry,
  classifySecToHpcMatch,
  routeHandoff,
  buildTrackCDiscoveryHotel,
  mapCreateReadyRowToTrackC,
  aggregateHpcAudit,
  dedupeDiscoveries,
  summarizeMatchClasses,
  buildCalaPreservationCheck,
  inferBrandFamilyFromLabel,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";
import {
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
  HPC_CREATE_REUSE_STATE,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const APPLY_HPC_CREATE = process.argv.includes("--apply-hpc-create");
const AUDIT_ONLY = process.argv.includes("--audit-only");
const SKIP_AUDIT = process.argv.includes("--skip-audit");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-universe-expansion");
fs.mkdirSync(OUT, { recursive: true });

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findingsTableId = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

function writeJson(name, obj) {
  const fp = path.join(OUT, name);
  fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`);
  return fp;
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
  const json = await res.json();
  return { ok: res.ok, json };
}

async function airtablePatch(tableId, recordId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

async function findByFormula(tableId, formula) {
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const json = await airtableGet(
    `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`,
  );
  return json.records?.[0] || null;
}

async function getRecord(tableId, recordId) {
  return airtableGet(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`);
}

/** Wave C1 — exact HPC audit */
async function waveC1ExactHpcAudit() {
  const fields = [
    "Property Name",
    "Canonical Property Name",
    "Country",
    "State / Region",
    "City",
    "Current Brand",
    "Brand Family",
    "Operator / Management Company",
    "Production Use Status",
    "Enrichment Status",
    "Affiliation Status",
    "Property Identity Key",
  ];
  const records = [];
  let offset;
  let scanned = 0;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    for (const f of fields) qs.append("fields[]", f);
    if (offset) qs.set("offset", offset);
    const json = await airtableGet(
      `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`,
    );
    for (const r of json.records || []) {
      records.push(r);
      scanned += 1;
    }
    offset = json.offset;
    if (scanned % 2000 === 0) process.stderr.write(`[C1] scanned ${scanned}...\n`);
    await sleep(50);
  } while (offset);

  const audit = aggregateHpcAudit(records);
  const usRecords = records.filter((r) => isUsCountry(r.fields?.Country));
  const result = {
    asOf: new Date().toISOString(),
    wave: "C1",
    baseId: GRAPH_BASE_ID_CANONICAL,
    hpcTableId: HPC_TABLE_ID,
    ...audit,
    usHotelsBefore: {
      count: usRecords.length,
      hotels: usRecords.map((r) => ({
        id: r.id,
        name: r.fields?.["Property Name"] || null,
        city: r.fields?.City || null,
        state: r.fields?.["State / Region"] || null,
        brand: r.fields?.["Current Brand"] || null,
        operator:
          r.fields?.["Operator / Management Company"] ||
          r.fields?.["Management Company"] ||
          null,
      })),
    },
    verifiedVsUnresolved: {
      verifiedIdentities: audit.verifiedIdentity,
      unresolvedIdentities: audit.unresolvedIdentity,
    },
  };
  writeJson("WAVE_C1_HPC_AUDIT.json", result);
  return result;
}

function loadCreateReadyRows() {
  const fp = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/CREATE_READY_FOUNDER_REVIEW.json");
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return (j.rows || []).filter((r) => r.recommendation === "CREATE");
}

function loadCmbsUsHotels() {
  const fp = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cmbs-owner/DISTINCT_HOTELS.json");
  if (!fs.existsSync(fp)) return [];
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return (j.hotels || [])
    .filter((h) => String(h.hotelCountry || "").toUpperCase() === "US")
    .map((h) =>
      buildTrackCDiscoveryHotel({
        hotelName: h.hotelName,
        country: "United States",
        city: h.hotelMarket || null,
        currentBrand: null,
        identityClass: h.identityClass || "PROBABLE_PROPERTY",
        evidenceConfidence: "MEDIUM",
        sourceUrl: h.sourceUrl,
        discoveryMethod: "TRACK_C_SEC_CMBS_CARRY",
        matchClass: MATCH_CLASS.RESEARCH_REQUIRED,
        handoffs: routeHandoff({
          hasFranchiseOrManagementSignal: Boolean(h.agreementType),
          hasContractualAgreementEvidence: true,
        }),
        notes: `CUSTOMER_BLOCKED | Track C CMBS carry | agreement=${h.agreementType || ""} | exp=${h.expirationDate || ""} | operator_not_inferred`,
        operatorName: h.operatorName || null,
      }),
    );
}

/** Wave C2 — US discovery pool */
function waveC2UsDiscovery(createReadyMapped) {
  const fromCreateReady = createReadyMapped.map((m) => m.hotel);
  const fromAimbridge = AIMBRIDGE_PUBLIC_US_PORTFOLIO_SEED.map((h) =>
    buildTrackCDiscoveryHotel({
      ...h,
      brandFamily: inferBrandFamilyFromLabel(h.currentBrand),
      discoveryMethod: "TRACK_C_OPERATOR_PORTFOLIO_PUBLIC",
      matchClass:
        h.identityClass === "VERIFIED_PROPERTY"
          ? MATCH_CLASS.UNMATCHED_VERIFIED
          : MATCH_CLASS.CREATE_CANDIDATE,
      handoffs: routeHandoff({
        hasFranchiseOrManagementSignal: Boolean(h.operatorName),
        hasContractualAgreementEvidence: false,
      }),
    }),
  );
  const fromCmbs = loadCmbsUsHotels();
  const all = dedupeDiscoveries([...fromCreateReady, ...fromAimbridge, ...fromCmbs]);
  const result = {
    asOf: new Date().toISOString(),
    wave: "C2",
    sources: {
      createReadyPilot: fromCreateReady.length,
      aimbridgePublicPortfolio: fromAimbridge.length,
      secCmbsDistinctUs: fromCmbs.length,
    },
    distinctUsHotelsIdentified: all.length,
    hotels: all,
  };
  writeJson("WAVE_C2_US_DISCOVERY.json", result);
  return result;
}

/** Wave C3 — SEC-to-HPC reconciliation (live collision on high-confidence subset) */
async function waveC3SecToHpcReconciliation(createReadyMapped, discovery) {
  const rows = [];
  let i = 0;
  for (const m of createReadyMapped) {
    i += 1;
    process.stderr.write(`[C3] collision ${i}/${createReadyMapped.length}: ${m.hotel.hotelName}\n`);
    let collision;
    try {
      collision = await performLiveHpcIdentityCollisionCheck({
        hotelName: m.hotel.hotelName,
        aliases: [m.hpcFields["Property Name"]].filter(Boolean),
        city: m.hotel.city,
        brand: m.hotel.currentBrand,
        address: m.hotel.address,
        country: m.hotel.physicalCountry,
        officialUrl: m.officialHotelBrandSource,
        adpPropertyId: m.hotel.discoveryPropertyId,
      });
    } catch (e) {
      collision = {
        outcome: "HPC_LIVE_LOOKUP_UNAVAILABLE",
        hpcRecordId: null,
        candidates: [],
        error: e.message,
      };
    }
    const matchClass = classifySecToHpcMatch({
      collisionOutcome: collision.outcome,
      identityClass: m.hotel.identityClass,
      operatingStatus: m.hotel.operatingStatus,
      hasExactAddress: Boolean(m.hotel.address),
      hasOfficialBrandSource: Boolean(m.officialHotelBrandSource),
    });
    rows.push({
      candidateId: m.candidateId,
      hotelName: m.hotel.hotelName,
      discoveryPropertyId: m.hotel.discoveryPropertyId,
      collisionOutcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      candidateCount: (collision.candidates || []).length,
      matchClass,
      handoffs: m.hotel.handoffs,
      hpcFields: m.hpcFields,
      officialHotelBrandSource: m.officialHotelBrandSource,
      historicalSecCmbsSource: m.historicalSecCmbsSource,
    });
    await sleep(120);
  }

  // Mark remaining discovery hotels without live collision as research/uncertain classes
  const createReadyIds = new Set(rows.map((r) => r.discoveryPropertyId));
  for (const h of discovery.hotels || []) {
    if (createReadyIds.has(h.discoveryPropertyId)) continue;
    rows.push({
      candidateId: null,
      hotelName: h.hotelName,
      discoveryPropertyId: h.discoveryPropertyId,
      collisionOutcome: "NOT_RECHECKED_LIVE",
      hpcRecordId: null,
      candidateCount: 0,
      matchClass: h.matchClass || MATCH_CLASS.RESEARCH_REQUIRED,
      handoffs: h.handoffs,
      liveCollisionSkipped: true,
      reason: "Defer live collision to next resumable wave; inventory persist still eligible",
    });
  }

  const summary = summarizeMatchClasses(rows);
  const result = {
    asOf: new Date().toISOString(),
    wave: "C3",
    liveCollisionRechecked: createReadyMapped.length,
    summary,
    rows,
  };
  writeJson("WAVE_C3_SEC_TO_HPC_RECONCILIATION.json", result);
  return result;
}

/** Wave C5 seeds — UK/Canada */
function waveC5UkCanadaSeeds() {
  const uk = AIMBRIDGE_PUBLIC_UK_PORTFOLIO_SEED.map((h) =>
    buildTrackCDiscoveryHotel({
      ...h,
      brandFamily: inferBrandFamilyFromLabel(h.currentBrand),
      discoveryMethod: "TRACK_C_UK_OPERATOR_PORTFOLIO",
      matchClass: MATCH_CLASS.CREATE_CANDIDATE,
      handoffs: routeHandoff({ hasFranchiseOrManagementSignal: true }),
    }),
  );
  // Canada carry from Track B public research (names already in inventory — re-stage for Track C coverage)
  const ca = [
    buildTrackCDiscoveryHotel({
      discoveryPropertyId: "disc_tc_yorkville_royal_sonesta_toronto_ca",
      hotelName: "Yorkville Royal Sonesta Toronto",
      city: "Toronto",
      stateRegion: "ON",
      country: "Canada",
      address: "220 Bloor Street West",
      currentBrand: "Royal Sonesta",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      sourceUrl: "https://www.sonesta.com/",
      discoveryMethod: "TRACK_C_CA_IDENTITY_STRENGTHEN",
      matchClass: MATCH_CLASS.UNMATCHED_VERIFIED,
      notes: "CUSTOMER_BLOCKED | Track C CA strengthen | cross-ref Track B disc_tb_yorkville_royal_sonesta_toronto_2025",
    }),
    buildTrackCDiscoveryHotel({
      discoveryPropertyId: "disc_tc_sonesta_es_toronto_thornhill_ca",
      hotelName: "Sonesta ES Suites Toronto Markham",
      city: "Thornhill",
      stateRegion: "ON",
      country: "Canada",
      currentBrand: "Sonesta ES Suites",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      sourceUrl: "https://www.sonesta.com/",
      discoveryMethod: "TRACK_C_CA_IDENTITY_STRENGTHEN",
      matchClass: MATCH_CLASS.UNMATCHED_VERIFIED,
    }),
  ];
  const result = {
    asOf: new Date().toISOString(),
    wave: "C5",
    uk: uk.length,
    canada: ca.length,
    hotels: [...uk, ...ca],
  };
  writeJson("WAVE_C5_UK_CANADA_EXPANSION.json", result);
  return result;
}

async function persistDiscoveryBatch(hotels, ledger) {
  for (const h of hotels) {
    const payload = {
      destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
      verified: h.verified !== false,
      evidenceSupported: Boolean(h.sourceUrl),
      sourceUrl: h.sourceUrl,
      identityClass: h.identityClass,
    };
    const cls = classifyWriteCategory(payload);
    if (cls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
      ledger.operations.push({
        id: h.discoveryPropertyId,
        action: "STAGED",
        category: cls.category,
        reason: cls.reason,
      });
      ledger.summary.staged += 1;
      continue;
    }

    const fields = buildDiscoveryInventoryFields(h);
    fields[MAP_DISCOVERY_INVENTORY.discoveryMethod] = h.discoveryMethod || "TRACK_C_UNIVERSE_EXPANSION";
    if (h.brandFamily) fields[MAP_DISCOVERY_INVENTORY.brandFamily] = h.brandFamily;
    if (h.notes) fields[MAP_DISCOVERY_INVENTORY.notesQuarantine] = h.notes;

    const existing = await findByFormula(
      inventoryTableId,
      `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${h.discoveryPropertyId}"`,
    );

    if (!APPLY) {
      ledger.operations.push({
        id: h.discoveryPropertyId,
        action: existing ? "WOULD_NOOP_OR_UPDATE" : "WOULD_CREATE",
        existingId: existing?.id || null,
        fields,
      });
      if (existing) ledger.summary.noops += 1;
      else ledger.summary.creates += 1;
      continue;
    }

    if (existing) {
      ledger.operations.push({
        id: h.discoveryPropertyId,
        action: "NOOP_EXISTING",
        airtableId: existing.id,
      });
      ledger.summary.noops += 1;
      const rb = await getRecord(inventoryTableId, existing.id);
      ledger.readBack.push({
        table: "inventory",
        id: existing.id,
        ok: Boolean(rb?.id),
        fields: {
          "Discovery Property ID": rb?.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId],
          Country: rb?.fields?.[MAP_DISCOVERY_INVENTORY.country],
          "Publication State": rb?.fields?.[MAP_DISCOVERY_INVENTORY.publicationState],
        },
      });
      continue;
    }

    const created = await airtablePost(inventoryTableId, fields);
    if (!created.ok) {
      ledger.operations.push({
        id: h.discoveryPropertyId,
        action: "CREATE_FAILED",
        error: created.json,
      });
      ledger.summary.conflicts += 1;
      continue;
    }
    ledger.operations.push({
      id: h.discoveryPropertyId,
      action: "CREATED",
      airtableId: created.json.id,
    });
    ledger.summary.creates += 1;
    const rb = await getRecord(inventoryTableId, created.json.id);
    ledger.readBack.push({
      table: "inventory",
      id: created.json.id,
      ok: Boolean(rb?.id),
      fields: {
        "Discovery Property ID": rb?.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId],
        Country: rb?.fields?.[MAP_DISCOVERY_INVENTORY.country],
        "Publication State": rb?.fields?.[MAP_DISCOVERY_INVENTORY.publicationState],
        "Hotel Name (normalized)": rb?.fields?.[MAP_DISCOVERY_INVENTORY.hotelNameNormalized],
      },
    });
    await sleep(80);

    // Commercial handoff finding when franchise/management signal present
    if ((h.handoffs || []).includes("TRACK_A_COMMERCIAL") && h.sourceUrl) {
      const findingId = `find_tc_${h.discoveryPropertyId.replace(/^disc_tc_/, "")}`.slice(0, 64);
      const existingFind = await findByFormula(
        findingsTableId,
        `{${MAP_DISCOVERY_FINDINGS.findingId}}="${findingId}"`,
      );
      if (!existingFind) {
        const fFields = buildCommercialFindingFields(
          {
            findingId,
            commercialCategory: "BCT",
            commercialThesis: `Track C handoff: franchise/management signal for ${h.hotelName}`,
            commercialRelevance: "IDENTITY_LINKED_COMMERCIAL_SIGNAL",
            evidencePathway: "TRACK_C_TO_TRACK_A",
            sourceUrl: h.sourceUrl,
            confidence: h.evidenceConfidence || "MEDIUM",
          },
          created.json.id,
        );
        fFields[MAP_DISCOVERY_FINDINGS.findingId] = findingId;
        fFields[MAP_DISCOVERY_FINDINGS.notes] = "CUSTOMER_BLOCKED | Track C → Track A handoff";
        const fCreated = await airtablePost(findingsTableId, fFields);
        ledger.operations.push({
          id: findingId,
          action: fCreated.ok ? "FINDING_CREATED" : "FINDING_FAILED",
          airtableId: fCreated.json?.id || null,
        });
        if (fCreated.ok) ledger.summary.findingsCreated = (ledger.summary.findingsCreated || 0) + 1;
      }
    }
  }
}

async function waveC4ControlledExpansion(c3, createReadyMapped, discoveryHotels, ukCaHotels) {
  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC_CREATE ? "APPLY_WITH_HPC_CREATE" : "APPLY_INVENTORY") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    stream: "TRACK_C",
    standingAuth: TRACK_C_STANDING_AUTH,
    researchCostUsd: 0,
    operations: [],
    summary: {
      creates: 0,
      updates: 0,
      noops: 0,
      conflicts: 0,
      staged: 0,
      researchOnly: 0,
      hpcCreates: 0,
      hpcCreateFailures: 0,
      hpcReusedExisting: 0,
      findingsCreated: 0,
      existingEnriched: 0,
    },
    schemaGaps: [],
    readBack: [],
    hpcCreateResults: [],
  };

  const inventoryTargets = dedupeDiscoveries([
    ...discoveryHotels,
    ...ukCaHotels,
    ...createReadyMapped.map((m) => m.hotel),
  ]);

  await persistDiscoveryBatch(inventoryTargets, ledger);

  // Controlled HPC creates for live CREATE_READY that remain absent
  const createEligible = (c3.rows || []).filter(
    (r) =>
      r.matchClass === MATCH_CLASS.CREATE_READY &&
      !r.liveCollisionSkipped &&
      String(r.collisionOutcome || "").includes("NEW_RECORD"),
  );

  const mappedById = new Map(createReadyMapped.map((m) => [m.candidateId, m]));
  const maxBatch = TRACK_C_STANDING_AUTH.maxHpcCreateBatch;
  const batch = createEligible.slice(0, maxBatch);

  let founderAuth = null;
  if (APPLY_HPC_CREATE && batch.length) {
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
    if (!founderAuth.ok) {
      ledger.summary.hpcCreateAuthBlocked = founderAuth.reason;
      ledger.operations.push({
        action: "HPC_CREATE_AUTH_BLOCKED",
        reason: founderAuth.reason,
      });
    }
  }

  for (const row of batch) {
    const mapped = mappedById.get(row.candidateId);
    if (!mapped) continue;
    const draftPropertyId = mapped.hotel.discoveryPropertyId;
    const fields = { ...mapped.hpcFields };

    if (!APPLY_HPC_CREATE || !founderAuth?.ok) {
      ledger.operations.push({
        id: draftPropertyId,
        action: "HPC_CREATE_STAGED",
        candidateId: row.candidateId,
        fields,
        collisionOutcome: row.collisionOutcome,
      });
      ledger.summary.staged += 1;
      continue;
    }

    const auth = persistHpcCreateAuthorization({
      runId: `track_c_${new Date().toISOString().slice(0, 10)}`,
      draftPropertyId,
      hotelName: mapped.hotel.hotelName,
      fields,
      approverIdentity: founderAuth.approverIdentity,
      authorizationSource: TRACK_C_STANDING_AUTH.source,
    });
    if (!auth.ok) {
      ledger.hpcCreateResults.push({ draftPropertyId, ok: false, error: auth.error });
      ledger.summary.hpcCreateFailures += 1;
      continue;
    }

    const exec = await executeAuthorizedHpcCreate({
      runId: `track_c_${new Date().toISOString().slice(0, 10)}`,
      authorization: auth.record,
      dryRun: false,
      apply: true,
      simulation: false,
    });
    ledger.hpcCreateResults.push({
      draftPropertyId,
      candidateId: row.candidateId,
      ok: exec.ok,
      state: exec.state,
      hpcRecordId: exec.hpcRecordId || null,
      reusedExisting: exec.reusedExisting || false,
      error: exec.error || null,
      airtableWrites: exec.airtableWrites || 0,
    });
    if (exec.ok && exec.reusedExisting) ledger.summary.hpcReusedExisting += 1;
    else if (exec.ok && exec.hpcRecordId) {
      ledger.summary.hpcCreates += 1;
      // Link discovery inventory to HPC when possible
      const inv = await findByFormula(
        inventoryTableId,
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${draftPropertyId}"`,
      );
      if (inv?.id) {
        const link = await airtablePatch(inventoryTableId, inv.id, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [exec.hpcRecordId],
        });
        if (link.ok) ledger.summary.existingEnriched += 1;
      }
    } else ledger.summary.hpcCreateFailures += 1;
    await sleep(200);
  }

  // Matched existing → enrich discovery match status only (non-destructive)
  for (const row of c3.rows || []) {
    if (row.matchClass !== MATCH_CLASS.MATCHED_EXISTING_HPC || !row.hpcRecordId) continue;
    if (!APPLY) {
      ledger.operations.push({
        id: row.discoveryPropertyId,
        action: "WOULD_LINK_EXISTING_HPC",
        hpcRecordId: row.hpcRecordId,
      });
      continue;
    }
    const inv = await findByFormula(
      inventoryTableId,
      `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${row.discoveryPropertyId}"`,
    );
    if (!inv?.id) continue;
    const link = await airtablePatch(inventoryTableId, inv.id, {
      [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
      [MAP_DISCOVERY_INVENTORY.hpcHotel]: [row.hpcRecordId],
    });
    ledger.operations.push({
      id: row.discoveryPropertyId,
      action: link.ok ? "LINKED_EXISTING_HPC" : "LINK_FAILED",
      hpcRecordId: row.hpcRecordId,
    });
    if (link.ok) ledger.summary.existingEnriched += 1;
  }

  writeJson(APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json", ledger);
  return ledger;
}

async function main() {
  console.log(
    JSON.stringify(
      {
        start: true,
        apply: APPLY,
        applyHpcCreate: APPLY_HPC_CREATE,
        standingAuth: TRACK_C_STANDING_AUTH.source,
      },
      null,
      2,
    ),
  );

  let c1;
  if (SKIP_AUDIT && fs.existsSync(path.join(OUT, "WAVE_C1_HPC_AUDIT.json"))) {
    c1 = JSON.parse(fs.readFileSync(path.join(OUT, "WAVE_C1_HPC_AUDIT.json"), "utf8"));
    console.log(`[C1] reused prior audit scanned=${c1.scanned}`);
  } else {
    console.log("[C1] live HPC audit starting...");
    c1 = await waveC1ExactHpcAudit();
    console.log(
      `[C1] scanned=${c1.scanned} US=${c1.priorityCountries.US} MX=${c1.priorityCountries.Mexico} BR=${c1.priorityCountries.Brazil} UK=${c1.priorityCountries.UK} CA=${c1.priorityCountries.Canada}`,
    );
  }

  if (AUDIT_ONLY) {
    writeJson("FOUNDER_TRACK_C_REPORT.json", {
      status: "AUDIT_ONLY",
      waveC1: c1.priorityCountries,
    });
    return;
  }

  const createReadyRows = loadCreateReadyRows();
  const createReadyMapped = createReadyRows.map(mapCreateReadyRowToTrackC);
  writeJson(
    "CREATE_READY_TRACK_C_MAPPED.json",
    createReadyMapped.map((m) => ({
      candidateId: m.candidateId,
      hotelName: m.hotel.hotelName,
      discoveryPropertyId: m.hotel.discoveryPropertyId,
      hpcFields: m.hpcFields,
    })),
  );

  const c2 = waveC2UsDiscovery(createReadyMapped);
  console.log(`[C2] distinct US discoveries=${c2.distinctUsHotelsIdentified}`);

  console.log("[C3] SEC→HPC live reconciliation...");
  const c3 = await waveC3SecToHpcReconciliation(createReadyMapped, c2);
  console.log(
    `[C3] matchRate=${c3.summary.matchRate} matched=${c3.summary.matchedExistingHpc} unmatchedVerified=${c3.summary.unmatchedVerified} uncertain=${c3.summary.uncertain}`,
  );

  const c5 = waveC5UkCanadaSeeds();
  console.log(`[C5] UK=${c5.uk} CA=${c5.canada}`);

  console.log(`[C4] persistence mode=${APPLY ? "APPLY" : "DRY_RUN"} hpcCreate=${APPLY_HPC_CREATE}`);
  const ledger = await waveC4ControlledExpansion(c3, createReadyMapped, c2.hotels, c5.hotels);

  // Post-create US count (only if we applied HPC creates)
  let usAfter = c1.priorityCountries.US;
  if (APPLY_HPC_CREATE && ledger.summary.hpcCreates > 0) {
    // Lightweight US recount
    let offset;
    let count = 0;
    do {
      const qs = new URLSearchParams({
        pageSize: "100",
        filterByFormula: `{Country}="${PRIORITY_COUNTRY_LABELS.US}"`,
      });
      qs.append("fields[]", "Country");
      if (offset) qs.set("offset", offset);
      const json = await airtableGet(
        `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`,
      );
      count += (json.records || []).length;
      offset = json.offset;
    } while (offset);
    usAfter = count;
  } else {
    usAfter = c1.priorityCountries.US + (ledger.summary.hpcCreates || 0);
  }

  const cala = buildCalaPreservationCheck(
    c1.priorityCountries,
    c1.calaTotal,
    c1.calaTotal,
  );

  const brandCoverage = {
    topBrandsInHpc: (c1.byBrandTop100 || []).slice(0, 25),
    discoveryBrandFamilies: Object.entries(
      (c2.hotels || []).reduce((acc, h) => {
        const b = h.brandFamily || inferBrandFamilyFromLabel(h.currentBrand) || "UNKNOWN";
        acc[b] = (acc[b] || 0) + 1;
        return acc;
      }, {}),
    ).sort((a, b) => b[1] - a[1]),
    operatorsDisclosedNotInferred: (c2.hotels || []).filter((h) => h.operatorName).map((h) => ({
      hotel: h.hotelName,
      operator: h.operatorName,
    })),
  };

  const report = {
    asOf: new Date().toISOString(),
    stream: "TRACK_C",
    publicationState: "CUSTOMER_BLOCKED",
    mergeOwner: "Integration Controller",
    verifiedHpcCountryDistribution: c1.byCountry,
    priorityCountriesVerified: c1.priorityCountries,
    usHotelsBefore: c1.priorityCountries.US,
    usHotelsAfter: usAfter,
    distinctNewUsHotelsIdentified: c2.distinctUsHotelsIdentified,
    newHpcRecordsPersisted: ledger.summary.hpcCreates || 0,
    existingHotelRecordsEnriched: ledger.summary.existingEnriched || 0,
    discoveryInventoryCreates: ledger.summary.creates || 0,
    discoveryInventoryNoops: ledger.summary.noops || 0,
    secToHpcMatchRate: c3.summary.matchRate,
    secToHpcSummary: c3.summary,
    unresolvedCases: (c3.rows || []).filter((r) =>
      [MATCH_CLASS.AMBIGUOUS, MATCH_CLASS.UNCERTAIN, MATCH_CLASS.RESEARCH_REQUIRED].includes(
        r.matchClass,
      ),
    ).length,
    brandOperatorCoverage: brandCoverage,
    ukCanadaExpansion: { uk: c5.uk, canada: c5.canada },
    calaPreservation: cala,
    coverageGaps: {
      usHpcVsDiscovered: {
        hpcUs: c1.priorityCountries.US,
        discoveredDistinctUs: c2.distinctUsHotelsIdentified,
        note: "Do not invent external universe denominators; gap is relative to Track C discovery pool only",
      },
      ukHpc: c1.priorityCountries.UK,
      canadaHpc: c1.priorityCountries.Canada,
      marketsSparseOutsideCala: (c1.byCountry || [])
        .filter(([, n]) => n > 0 && n < 20)
        .map(([c, n]) => ({ country: c, count: n })),
    },
    persistenceLedgerFile: APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json",
    researchCostUsd: 0,
    standingAuth: TRACK_C_STANDING_AUTH,
  };
  writeJson("FOUNDER_TRACK_C_REPORT.json", report);

  const md = `# HIF Track C — US Universe Expansion Report

**As of:** ${report.asOf}  
**Publication:** CUSTOMER_BLOCKED  
**Merge owner:** Integration Controller  

## 1. Verified HPC country distribution (live)

| Country | Count |
|---------|------:|
| Brazil | ${c1.priorityCountries.Brazil} |
| Mexico | ${c1.priorityCountries.Mexico} |
| United States | ${c1.priorityCountries.US} |
| United Kingdom | ${c1.priorityCountries.UK} |
| Canada | ${c1.priorityCountries.Canada} |
| **HPC total scanned** | **${c1.scanned}** |
| CALA total | ${c1.calaTotal} |

## 2–4. US hotels

- **Before research (HPC):** ${report.usHotelsBefore}
- **After research (HPC):** ${report.usHotelsAfter}
- **Distinct new US hotels identified (discovery pool):** ${report.distinctNewUsHotelsIdentified}
- **New HPC records persisted:** ${report.newHpcRecordsPersisted}
- **Discovery Inventory creates:** ${report.discoveryInventoryCreates}

## 5. Existing enriched

- Discovery↔HPC links / enrichments: ${report.existingHotelRecordsEnriched}

## 6. SEC→HPC match

- Match rate (live CREATE_READY subset + deferred rows): ${report.secToHpcMatchRate}
- Matched existing: ${c3.summary.matchedExistingHpc}
- Unmatched verified / create-ready: ${c3.summary.unmatchedVerified}
- Uncertain / ambiguous / research: ${c3.summary.uncertain}

## 7. Brand / operator coverage

- Operators only when source-disclosed (Aimbridge portfolio + press). Brand≠operator.
- See \`WAVE_C2_US_DISCOVERY.json\` / report JSON \`brandOperatorCoverage\`.

## 8. UK / Canada

- UK discovery seeds: ${c5.uk}
- Canada identity strengthen: ${c5.canada}
- HPC UK=${c1.priorityCountries.UK} · CA=${c1.priorityCountries.Canada}

## 9. CALA preservation

- CALA total: ${cala.calaTotal}
- Preserved: ${cala.preserved}

## 10. Coverage gaps

- Sparse countries (<20 HPC): see report JSON
- US HPC remains thin vs discovery pool — continue resumable waves

## 11. Persistence ledger

- File: \`${report.persistenceLedgerFile}\`
- Mode: ${ledger.mode}

## 12. Cost / status

- Research cost USD: 0
- HPC create auth: ${ledger.summary.hpcCreateAuthBlocked || (APPLY_HPC_CREATE ? "ATTEMPTED" : "NOT_REQUESTED")}
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_TRACK_C_REPORT.md"), md);
  console.log(JSON.stringify({ done: true, report: path.join(OUT, "FOUNDER_TRACK_C_REPORT.json"), ledger: ledger.summary }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
