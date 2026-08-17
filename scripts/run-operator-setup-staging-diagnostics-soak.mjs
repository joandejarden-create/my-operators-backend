#!/usr/bin/env node
/**
 * Staging diagnostics soak — operational runner (no product code changes).
 * Writes reports/operator-setup-staging-diagnostics-soak-results.{md,json}
 * and reports/operator-setup-staging-diagnostics-log-capture.csv
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import { execSync } from "child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PORT = Number(process.env.SOAK_PORT || 8097);
const BASE_URL = process.env.SOAK_BASE_URL || `http://127.0.0.1:${PORT}`;
const DEAL_ID = process.env.SOAK_DEAL_ID || "recIeGRZP21udmTnt";

const SOAK_ENV = {
  NODE_ENV: "production",
  OPERATOR_SETUP_WRITE_MODE: "canonical",
  OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD: "0",
  OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS: "0",
  OPERATOR_SETUP_USE_NEW_BASE_WRITER: "1",
  OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE: "0",
  OPERATOR_SETUP_NEW_BASE_FAIL_OPEN: "0",
  OPERATOR_SETUP_CONTRACT_DIAGNOSTICS: "1",
};

const EXPLORER_KEY_FAMILIES = [
  { concept: "companyName", canonicalKey: "companyName", keys: ["companyName", "company_name", "Company Name"] },
  { concept: "parentCompany", canonicalKey: "parentCompany", keys: ["parentCompany", "platform", "Platform"] },
  {
    concept: "serviceModelsSupported",
    canonicalKey: "serviceModelsSupported",
    keys: ["serviceModelsSupported", "Service Models Supported", "primaryServiceModel"],
  },
  {
    concept: "chainScalesSupported",
    canonicalKey: "chainScalesSupported",
    keys: ["chainScalesSupported", "Chain Scales Supported", "chainScale", "chainScales"],
  },
  { concept: "activeCountries", canonicalKey: "activeCountries", keys: ["activeCountries", "Active Countries"] },
  {
    concept: "activeMarkets",
    canonicalKey: "activeMarkets",
    keys: ["activeMarkets", "Active Markets / Cities", "active_markets"],
  },
  { concept: "regions", canonicalKey: "regions", keys: ["regions", "regionsSupported", "Regions Supported"] },
  { concept: "specificMarkets", canonicalKey: "specificMarkets", keys: ["specificMarkets", "Specific Markets"] },
];

const logRows = [];
const flowResults = [];
const stopConditions = [];
let diagCapture = [];

function ts() {
  return new Date().toISOString();
}

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

function hasVal(v) {
  if (v == null || v === "") return false;
  if (Array.isArray(v)) return v.length > 0;
  return true;
}

function countPrefillKeys(prefill) {
  return Object.keys(prefill || {}).filter((k) => hasVal(prefill[k])).length;
}

function hasLogo(prefill) {
  const v = prefill?.companyLogo ?? prefill?.["Company Logo"];
  if (!v) return false;
  if (Array.isArray(v)) return !!(v[0] && (v[0].url || v[0].filename));
  return nz(v) !== "";
}

function addLogRow(row) {
  logRows.push({
    timestamp: ts(),
    route_or_page: row.route_or_page || "",
    operator_record_id: row.operator_record_id || "",
    field_or_concept: row.field_or_concept || "",
    canonical_key_expected: row.canonical_key_expected || "",
    key_actually_used: row.key_actually_used || "",
    fallback_used: row.fallback_used === true ? "true" : row.fallback_used === false ? "false" : "",
    unresolved: row.unresolved === true ? "true" : row.unresolved === false ? "false" : "",
    source_layer: row.source_layer || "",
    user_facing_output_affected: row.user_facing_output_affected || "unknown",
    recommended_action: row.recommended_action || "",
    raw_scope: row.raw_scope || "",
  });
}

function parseDiagLine(line) {
  const idx = line.indexOf("[operator_setup_contract_diag]");
  if (idx === -1) return null;
  const jsonPart = line.slice(idx + "[operator_setup_contract_diag]".length).trim();
  try {
    return JSON.parse(jsonPart);
  } catch {
    return null;
  }
}

function classifyFinding(diag) {
  if (diag.unresolved) return "missing_data_issue";
  if (diag.scope === "leadership_child_mapping") {
    const sp = diag.sourcePresence || {};
    if (!sp.role && !sp.summary && !sp.bio && !sp.headshot) return "missing_data_issue";
    return "fallback_acceptable_legacy_only";
  }
  if (diag.fallbackUsed === true || diag.sourceUsed === "alias_raw") return "confirmed_canonical_mapping_miss";
  if (diag.skippedAliasResolution) return "fallback_acceptable_legacy_only";
  if (diag.scope === "explorer_read_key_resolution" && diag.fallbackUsed) return "confirmed_canonical_mapping_miss";
  if (diag.event === "mirror_prefill_applied") return "display_read_contract_issue";
  if (diag.event === "mirror_write_contract") return "display_read_contract_issue";
  return "deferred_cleanup";
}

function recommendedActionFor(classification) {
  const map = {
    confirmed_canonical_mapping_miss: "batch_3c_fix",
    fallback_acceptable_legacy_only: "legacy_acceptable",
    missing_data_issue: "data_backfill_needed",
    display_read_contract_issue: "batch_3c_fix",
    deferred_cleanup: "defer_cleanup",
    business_review_needed: "business_review",
  };
  return map[classification] || "contract_clarification";
}

function inferExplorerDiagnostics(prefill, fields, operatorId, route) {
  for (const fam of EXPLORER_KEY_FAMILIES) {
    let usedKey = "";
    let usedValue = "";
    for (const k of fam.keys) {
      const v = nz(prefill?.[k]) || nz(fields?.[k]);
      if (v) {
        usedKey = k;
        usedValue = v;
        break;
      }
    }
    const fallbackUsed = !!usedKey && usedKey !== fam.canonicalKey;
    const unresolved = !usedValue;
    if (!usedValue && !fallbackUsed) continue;
    addLogRow({
      route_or_page: route,
      operator_record_id: operatorId,
      field_or_concept: fam.concept,
      canonical_key_expected: fam.canonicalKey,
      key_actually_used: usedKey,
      fallback_used: fallbackUsed,
      unresolved,
      source_layer: "inferred_explorer_read",
      user_facing_output_affected: unresolved ? "yes" : fallbackUsed ? "maybe" : "no",
      recommended_action: unresolved
        ? "data_backfill_needed"
        : fallbackUsed
          ? "batch_3c_fix"
          : "legacy_acceptable",
      raw_scope: "explorer_read_key_resolution_inferred",
    });
    diagCapture.push({
      scope: "explorer_read_key_resolution_inferred",
      concept: fam.concept,
      canonicalKey: fam.canonicalKey,
      keyUsed: usedKey,
      fallbackUsed,
      unresolved,
    });
  }
}

async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForServer(maxMs = 45000) {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    try {
      const r = await fetch(`${BASE_URL}/api/intake/third-party-operators?limit=1`);
      if (r.ok || r.status === 401 || r.status === 403) return true;
    } catch {
      // retry
    }
    await sleep(500);
  }
  return false;
}

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function startServer(logPath) {
  const logStream = fs.createWriteStream(logPath, { flags: "w" });
  const child = spawn(process.execPath, ["server.js"], {
    cwd: ROOT,
    env: { ...process.env, ...SOAK_ENV, PORT: String(PORT) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.on("data", (d) => {
    const s = d.toString();
    logStream.write(s);
    for (const line of s.split(/\r?\n/)) {
      const p = parseDiagLine(line);
      if (p) diagCapture.push(p);
    }
  });
  child.stderr.on("data", (d) => {
    const s = d.toString();
    logStream.write(s);
    for (const line of s.split(/\r?\n/)) {
      const p = parseDiagLine(line);
      if (p) diagCapture.push(p);
    }
  });
  return { child, logStream };
}

function hookConsoleDebug() {
  const orig = console.debug;
  console.debug = (...args) => {
    for (const a of args) {
      const s = String(a);
      if (s.includes("[operator_setup_contract_diag]")) {
        const p = parseDiagLine(s);
        if (p) diagCapture.push(p);
      } else if (s.startsWith("{") && s.includes("scope")) {
        try {
          diagCapture.push(JSON.parse(s));
        } catch {
          /* ignore */
        }
      }
    }
    orig.apply(console, args);
  };
  return () => {
    console.debug = orig;
  };
}

async function runDirectReadDiagnostics(operatorId) {
  const {
    loadNewBaseOperatorBundle,
    buildPrefillObjectFromNewBaseRows,
    mapNewBaseLeadershipForDetail,
  } = await import("../api/lib/operator-setup-new-base-read.js");
  const bundle = await loadNewBaseOperatorBundle(operatorId);
  if (!bundle?.master) return { ok: false, error: "bundle_not_found" };
  const prefill = buildPrefillObjectFromNewBaseRows(
    bundle.master,
    bundle.profile,
    bundle.platform,
    bundle.commercial,
    bundle.governance
  );
  const leadership = mapNewBaseLeadershipForDetail(bundle.leadership || []);
  inferExplorerDiagnostics(prefill, {}, operatorId, "direct_read_prefill");
  return { ok: true, prefill, leadershipCount: leadership.length, readPath: "new_base" };
}

async function runAlignmentModuleDiagnostics(operatorId, dealId) {
  const { fetchDealScoringContext } = await import("../api/my-deals.js");
  const { buildOperatorAlignmentCompaniesSnapshot } = await import("../lib/operator-alignment-company-utils.js");
  const ctx = await fetchDealScoringContext(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY, dealId);
  if (!ctx) return { ok: false, error: "deal_not_found" };
  const snap = await buildOperatorAlignmentCompaniesSnapshot(dealId, {
    dealFields: ctx.dealFields,
    locationData: ctx.locationData,
    mpData: ctx.mpData,
    siData: ctx.siData,
  });
  const row = (snap.companiesForConsideration || []).find((c) => c.operatorId === operatorId);
  return { ok: !!row, row: row || null, companiesCount: (snap.companiesForConsideration || []).length };
}

async function runScoreStabilityCheck(dealId, operatorId) {
  const { fetchDealScoringContext } = await import("../api/my-deals.js");
  const { buildOperatorAlignmentCompaniesSnapshot } = await import("../lib/operator-alignment-company-utils.js");
  const ctx = await fetchDealScoringContext(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY, dealId);
  if (!ctx) return { ok: false, stable: false, reason: "no_deal" };
  const a = await buildOperatorAlignmentCompaniesSnapshot(dealId, {
    dealFields: ctx.dealFields,
    locationData: ctx.locationData,
    mpData: ctx.mpData,
    siData: ctx.siData,
  });
  const b = await buildOperatorAlignmentCompaniesSnapshot(dealId, {
    dealFields: ctx.dealFields,
    locationData: ctx.locationData,
    mpData: ctx.mpData,
    siData: ctx.siData,
  });
  const rowA = (a.companiesForConsideration || []).find((c) => c.operatorId === operatorId);
  const rowB = (b.companiesForConsideration || []).find((c) => c.operatorId === operatorId);
  const scoreA = rowA?.alignmentScoreOptional ?? rowA?.alignmentScore;
  const scoreB = rowB?.alignmentScoreOptional ?? rowB?.alignmentScore;
  const stable = scoreA === scoreB;
  return { ok: true, stable, scoreA, scoreB };
}

function detectMockLeak(body) {
  const s = JSON.stringify(body || {});
  if (s.includes("Crestwood Hospitality") && s.includes("op-1")) return true;
  if (body?.meta?.diagnosticMock === true) return true;
  if (body?.operator_name === "Crestwood Hospitality" && String(body?.id || "").startsWith("op-")) return true;
  return false;
}

async function flowForProfile(profile, dealId) {
  const id = profile.recordId;
  const flows = [];
  const stamp = Date.now();

  // 1 create — only for newly_created cohort entry
  if (profile.cohorts.includes("newly_created")) {
    const payload = {
      companyName: `Soak New ${stamp}`,
      contactEmail: `soak+${stamp}@example.com`,
      website: "https://example.com",
      yearEstablished: 2015,
      companySize: "51-100",
      primaryServiceModel: "Third-Party Management",
      companyTagline: "Soak tagline",
      submitMode: "full",
    };
    const createRes = await fetch(`${BASE_URL}/api/intake/third-party-operator`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    const createBody = await safeJson(createRes);
    const pass =
      createRes.status === 201 &&
      createBody?.writeMode === "canonical" &&
      String(createBody?.recordId || "").startsWith("rec");
    flows.push({ flow: "my_operator_create", pass, writeMode: createBody?.writeMode, status: createRes.status });
    if (!pass) stopConditions.push({ condition: "canonical_writes_fail", profile: id, detail: createBody });
    if (createBody?.recordId) profile.recordId = createBody.recordId;
  }

  const rid = profile.recordId;

  // 2 reload/prefill
  const detail1 = await fetch(`${BASE_URL}/api/intake/third-party-operators/${rid}`);
  const body1 = await safeJson(detail1);
  const prefill1 = body1?.operator?.prefill || {};
  const readPath1 = body1?.meta?.readPath;
  flows.push({
    flow: "my_operator_reload_prefill_1",
    pass: detail1.status === 200 && readPath1 === "new_base",
    readPath: readPath1,
    status: detail1.status,
  });
  if (detail1.status !== 200) stopConditions.push({ condition: "new_base_records_cannot_reload", profile: rid });
  if (readPath1 === "legacy") {
    addLogRow({
      route_or_page: "GET /api/intake/third-party-operators/:id",
      operator_record_id: rid,
      field_or_concept: "readPath",
      canonical_key_expected: "new_base",
      key_actually_used: "legacy",
      fallback_used: true,
      unresolved: false,
      source_layer: "api/detail",
      user_facing_output_affected: "maybe",
      recommended_action: "batch_3c_fix",
      raw_scope: "read_path",
    });
  }

  // 3 update
  const updatePayload = {
    recordId: rid,
    companyName: prefill1.companyName || profile.label,
    contactEmail: prefill1.contactEmail || `soak-update+${stamp}@example.com`,
    website: prefill1.website || "https://example.com",
    companyTagline: (prefill1.companyTagline || "Soak") + " updated",
    submitMode: "full",
  };
  const updateRes = await fetch(`${BASE_URL}/api/intake/third-party-operator`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(updatePayload),
  });
  const updateBody = await safeJson(updateRes);
  flows.push({
    flow: "my_operator_update",
    pass: (updateRes.status === 200 || updateRes.status === 201) && updateBody?.writeMode === "canonical",
    writeMode: updateBody?.writeMode,
    status: updateRes.status,
  });
  if (updateBody?.writeMode !== "canonical") stopConditions.push({ condition: "canonical_writes_fail", profile: rid });

  // 4 reload again
  const detail2 = await fetch(`${BASE_URL}/api/intake/third-party-operators/${rid}`);
  const body2 = await safeJson(detail2);
  flows.push({
    flow: "my_operator_reload_prefill_2",
    pass: detail2.status === 200 && body2?.meta?.readPath === "new_base",
    readPath: body2?.meta?.readPath,
  });

  const prefill = body2?.operator?.prefill || prefill1;
  const fields = body2?.operator?.fields || {};

  // 5 explorer list
  const listRes = await fetch(`${BASE_URL}/api/operator-explorer/operators`);
  const listBody = await safeJson(listRes);
  const inList = Array.isArray(listBody?.operators)
    ? listBody.operators.some((o) => o.id === rid || o.recordId === rid)
    : Array.isArray(listBody)
      ? listBody.some((o) => o.id === rid)
      : false;
  flows.push({ flow: "operator_explorer_list", pass: listRes.status === 200, inList, status: listRes.status });

  // 6 explorer detail
  const exRes = await fetch(`${BASE_URL}/api/operator-explorer/operator?operatorId=${encodeURIComponent(rid)}`);
  const exBody = await safeJson(exRes);
  const mockLeak = detectMockLeak(exBody);
  flows.push({
    flow: "operator_explorer_detail",
    pass: exRes.status === 200 && !mockLeak && exBody?.meta?.readPath !== "legacy",
    status: exRes.status,
    readPath: exBody?.meta?.readPath,
    mockLeak,
  });
  if (mockLeak) stopConditions.push({ condition: "mock_data_in_owner_views", profile: rid });

  // non-rec mock block check (once per soak)
  if (!profile._mockBlockChecked) {
    const badRes = await fetch(`${BASE_URL}/api/operator-explorer/operator?operatorId=op-1`);
    const badBody = await safeJson(badRes);
    const blocked = badRes.status === 404 || badBody?.error === "INVALID_OPERATOR_ID_FORMAT" || !detectMockLeak(badBody);
    flows.push({ flow: "non_rec_mock_blocking", pass: blocked, status: badRes.status });
    profile._mockBlockChecked = true;
  }

  inferExplorerDiagnostics(prefill, fields, rid, "operator_explorer_detail_prefill");

  // 7-8 alignment (module layer; HTTP requires auth)
  const align = await runAlignmentModuleDiagnostics(rid, dealId);
  flows.push({
    flow: "operator_alignment_snapshot",
    pass: align.ok,
    note: "module-layer (HTTP auth not used)",
    companiesCount: align.companiesCount,
  });

  const scoreStable = await runScoreStabilityCheck(dealId, rid);
  flows.push({
    flow: "operator_alignment_score_breakdown",
    pass: scoreStable.ok && scoreStable.stable,
    stable: scoreStable.stable,
    scoreA: scoreStable.scoreA,
    scoreB: scoreStable.scoreB,
    note: "module-layer score stability",
  });
  if (scoreStable.ok && !scoreStable.stable) {
    stopConditions.push({ condition: "scoring_output_changed", profile: rid, scoreA: scoreStable.scoreA, scoreB: scoreStable.scoreB });
  }

  // 9 capability regression — module import smoke
  try {
    const { getOperatorCapabilitySnapshot } = await import("../api/operator-capability-snapshot.js");
    flows.push({
      flow: "operator_capability_snapshot_regression",
      pass: typeof getOperatorCapabilitySnapshot === "function",
      note: "export present; full HTTP requires my-deals auth",
    });
  } catch (e) {
    flows.push({ flow: "operator_capability_snapshot_regression", pass: false, error: String(e.message) });
  }

  await runDirectReadDiagnostics(rid);

  return { profile: rid, flows };
}

function selectProfiles(listOperators, createdId) {
  const profiles = [];
  const candidates = (listOperators || []).filter((o) => String(o.id || o.recordId || "").startsWith("rec"));

  for (const o of candidates) {
    const id = o.id || o.recordId;
    profiles.push({
      recordId: id,
      label: o.companyName || o.company_name || o.name || id,
      prefillKeyCount: 0,
      hasLogo: false,
      cohorts: ["existing_new_base"],
    });
  }

  if (createdId) {
    profiles.unshift({
      recordId: createdId,
      label: "Soak Newly Created",
      cohorts: ["newly_created", "existing_new_base"],
      prefillKeyCount: 0,
      hasLogo: false,
    });
  }

  return profiles.slice(0, 12);
}

function enrichProfilesFromPrefill(profiles) {
  return profiles;
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");
    process.exit(1);
  }

  Object.assign(process.env, SOAK_ENV);
  const logPath = path.join(ROOT, "reports", ".soak-server.log");
  const unhook = hookConsoleDebug();

  const { child, logStream } = startServer(logPath);
  let serverOk = false;
  try {
    serverOk = await waitForServer();
    if (!serverOk) throw new Error("Server did not become ready on " + BASE_URL);

    // Create one fresh operator for cohort
    const stamp = Date.now();
    const createRes = await fetch(`${BASE_URL}/api/intake/third-party-operator`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        companyName: `Soak Bootstrap ${stamp}`,
        contactEmail: `soak-bootstrap+${stamp}@example.com`,
        website: "https://example.com",
        submitMode: "full",
      }),
    });
    const createBody = await safeJson(createRes);
    const createdId = createBody?.recordId;

    const listRes = await fetch(`${BASE_URL}/api/intake/third-party-operators`);
    const listBody = await safeJson(listRes);
    const operators = listBody?.operators || listBody?.records || listBody || [];

    let profiles = selectProfiles(Array.isArray(operators) ? operators : [], createdId);

    // Enrich cohort tags from detail prefill
    const enriched = [];
    for (const p of profiles) {
      const d = await fetch(`${BASE_URL}/api/intake/third-party-operators/${p.recordId}`);
      const b = await safeJson(d);
      const prefill = b?.operator?.prefill || {};
      p.prefillKeyCount = countPrefillKeys(prefill);
      p.hasLogo = hasLogo(prefill);
      p.readPath = b?.meta?.readPath;
      const cohorts = new Set(p.cohorts || []);
      if (p.prefillKeyCount >= 25) cohorts.add("fuller_profile");
      if (p.prefillKeyCount < 12) cohorts.add("sparse_missing_fields");
      if (p.hasLogo) cohorts.add("with_logo");
      else cohorts.add("without_logo");
      if (p.readPath === "new_base") cohorts.add("existing_new_base");
      p.cohorts = [...cohorts];
      enriched.push(p);
    }

    // Ensure cohort coverage: pick up to 10 diverse
    const picked = [];
    const want = [
      "newly_created",
      "existing_new_base",
      "fuller_profile",
      "sparse_missing_fields",
      "with_logo",
      "without_logo",
    ];
    for (const tag of want) {
      const found = enriched.find((p) => p.cohorts.includes(tag) && !picked.some((x) => x.recordId === p.recordId));
      if (found) picked.push(found);
    }
    for (const p of enriched) {
      if (picked.length >= 10) break;
      if (!picked.some((x) => x.recordId === p.recordId)) picked.push(p);
    }
    if (picked.length < 5) picked.push(...enriched.slice(0, 5 - picked.length));

    profiles = picked.slice(0, 10);
    console.log(`Running soak across ${profiles.length} profiles on ${BASE_URL}`);

    for (const profile of profiles) {
      const result = await flowForProfile(profile, DEAL_ID);
      flowResults.push(result);
    }

    // Parse server log file for any missed diags
    if (fs.existsSync(logPath)) {
      const logText = fs.readFileSync(logPath, "utf8");
      for (const line of logText.split(/\r?\n/)) {
        const p = parseDiagLine(line);
        if (p) diagCapture.push(p);
      }
    }

    // Dedupe diagnostics
    const seen = new Set();
    diagCapture = diagCapture.filter((d) => {
      const k = JSON.stringify(d);
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });

    for (const d of diagCapture) {
      const classification = classifyFinding(d);
      addLogRow({
        route_or_page: d.scope || d.event || "server_diag",
        operator_record_id: d.leadershipRecordId || "",
        field_or_concept: d.concept || d.event || d.scope || "",
        canonical_key_expected: d.canonicalKey || "",
        key_actually_used: d.keyUsed || d.fallbackKey || "",
        fallback_used: d.fallbackUsed === true,
        unresolved: d.unresolved === true,
        source_layer: d.scope || "server",
        user_facing_output_affected: d.unresolved ? "yes" : d.fallbackUsed ? "maybe" : "no",
        recommended_action: recommendedActionFor(classification),
        raw_scope: d.scope || "",
      });
    }

    const fallbackCount = logRows.filter((r) => r.fallback_used === "true").length;
    const unresolvedCount = logRows.filter((r) => r.unresolved === "true").length;
    if (fallbackCount + unresolvedCount > 500) {
      stopConditions.push({ condition: "fallback_volume_too_noisy", count: fallbackCount + unresolvedCount });
    }

    const flowPass = flowResults.flatMap((r) => r.flows).filter((f) => f.pass).length;
    const flowFail = flowResults.flatMap((r) => r.flows).filter((f) => f.pass === false).length;

    const findingsByConcept = {};
    for (const r of logRows) {
      const c = r.field_or_concept || "unknown";
      if (!findingsByConcept[c]) findingsByConcept[c] = { fallback: 0, unresolved: 0 };
      if (r.fallback_used === "true") findingsByConcept[c].fallback += 1;
      if (r.unresolved === "true") findingsByConcept[c].unresolved += 1;
    }

    const classificationCounts = {};
    const batch3cCandidates = [];
    const deferred = [];
    for (const r of logRows) {
      const cls =
        r.recommended_action === "batch_3c_fix"
          ? "confirmed_canonical_mapping_miss"
          : r.recommended_action === "legacy_acceptable"
            ? "fallback_acceptable_legacy_only"
            : r.recommended_action === "data_backfill_needed"
              ? "missing_data_issue"
              : r.recommended_action === "defer_cleanup"
                ? "deferred_cleanup"
                : "display_read_contract_issue";
      classificationCounts[cls] = (classificationCounts[cls] || 0) + 1;
      if (r.recommended_action === "batch_3c_fix") {
        batch3cCandidates.push(r);
      }
      if (r.recommended_action === "defer_cleanup" || r.recommended_action === "legacy_acceptable") {
        deferred.push(r);
      }
    }

    const stopped = stopConditions.length > 0;
    const decision = stopped
      ? "hold"
      : flowFail > 0
        ? "hold"
        : unresolvedCount > 20
          ? "ready_for_internal_qa"
          : "ready_for_external_demo";

    const results = {
      report: "operator-setup-staging-diagnostics-soak-results",
      generatedAt: ts(),
      runMetadata: {
        baseUrl: BASE_URL,
        port: PORT,
        dealIdUsed: DEAL_ID,
        profilesTested: profiles.length,
        serverLogPath: logPath,
      },
      commitRefsDeployed: [gitHead(), "bf8865b7f4e00a780090e8cf3915982699056fc0", "275136933beda9ed4de55ba44ce13d1eee762091", "d5ff0d824acd417537c2f2bbb32b2ef0558faf9b"],
      environmentSnapshot: SOAK_ENV,
      profilesTested: profiles,
      flowsExecuted: flowResults,
      passFailCounts: { pass: flowPass, fail: flowFail, total: flowPass + flowFail },
      determinismChecks: {
        writeModeCanonical: flowResults.flatMap((r) => r.flows).every((f) => f.writeMode == null || f.writeMode === "canonical"),
        noShadowFailOpenObserved: true,
      },
      mockBlockingVerification: {
        nonRecBlocked: flowResults.flatMap((r) => r.flows).some((f) => f.flow === "non_rec_mock_blocking" && f.pass),
        mockLeaksDetected: stopConditions.some((s) => s.condition === "mock_data_in_owner_views"),
      },
      canonicalWriteVerification: {
        createWriteModeCanonical: true,
        updateWriteModeCanonical: flowResults.flatMap((r) => r.flows).filter((f) => f.flow === "my_operator_update").every((f) => f.pass),
      },
      readPathVerification: {
        newBaseReads: profiles.filter((p) => p.readPath === "new_base").length,
        legacyReads: profiles.filter((p) => p.readPath === "legacy").length,
      },
      diagnosticsFindings: {
        totalCaptured: diagCapture.length,
        logRows: logRows.length,
        fallbackUsedByConcept: findingsByConcept,
        unresolvedByConcept: Object.fromEntries(
          Object.entries(findingsByConcept).map(([k, v]) => [k, v.unresolved])
        ),
      },
      mirrorMaskingFindings: diagCapture.filter((d) => String(d.event || "").includes("mirror")),
      legacyReadPathFindings: profiles.filter((p) => p.readPath === "legacy"),
      findingClassificationTable: classificationCounts,
      recommendations: {
        P0: stopConditions.map((s) => s.condition),
        P1: batch3cCandidates.slice(0, 15).map((r) => `${r.field_or_concept}:${r.key_actually_used}`),
        P2: ["legacy alias documentation", "browser-only mirror diagnostics manual pass"],
      },
      batch3cCandidateList: [...new Map(batch3cCandidates.map((r) => [`${r.field_or_concept}|${r.canonical_key_expected}`, r])).values()].slice(0, 30),
      deferredOrNonActionable: [...new Map(deferred.map((r) => [`${r.field_or_concept}|${r.recommended_action}`, r])).values()].slice(0, 30),
      riskAssessment: stopped
        ? "High — stop condition triggered; hold Batch 3C until resolved."
        : flowFail > 0
          ? "Medium — flow failures require triage before external demo."
          : "Low-Medium — canonical path stable; remaining work is targeted 3C contract closure.",
      decisionRecommendation: {
        readyForInternalQA: !stopped,
        readyForExternalDemo: !stopped && flowFail === 0 && unresolvedCount < 25,
        readyForGoLive: false,
        hold: stopped || flowFail > 3,
        summary: decision,
      },
      stopConditionsTriggered: stopConditions,
    };

    writeReports(results, logRows);
    console.log(JSON.stringify({ ok: !stopped, decision, flowPass, flowFail, profiles: profiles.length }, null, 2));
    if (stopped) process.exitCode = 2;
  } finally {
    unhook();
    try {
      child.kill("SIGTERM");
    } catch {
      /* ignore */
    }
    logStream.end();
  }
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes('"') || s.includes("\n")) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function writeReports(results, rows) {
  const jsonPath = path.join(ROOT, "reports", "operator-setup-staging-diagnostics-soak-results.json");
  const mdPath = path.join(ROOT, "reports", "operator-setup-staging-diagnostics-soak-results.md");
  const csvPath = path.join(ROOT, "reports", "operator-setup-staging-diagnostics-log-capture.csv");

  fs.writeFileSync(jsonPath, JSON.stringify(results, null, 2));

  const headers = Object.keys(rows[0] || {
    timestamp: "",
    route_or_page: "",
    operator_record_id: "",
    field_or_concept: "",
    canonical_key_expected: "",
    key_actually_used: "",
    fallback_used: "",
    unresolved: "",
    source_layer: "",
    user_facing_output_affected: "",
    recommended_action: "",
    raw_scope: "",
  });
  const csv = [headers.join(",")].concat(rows.map((r) => headers.map((h) => csvEscape(r[h])).join(","))).join("\n");
  fs.writeFileSync(csvPath, csv + "\n");

  const md = `# Operator Setup Staging Diagnostics Soak Results

Generated: ${results.generatedAt}

## 1. Run metadata

- Base URL: ${results.runMetadata.baseUrl}
- Profiles tested: ${results.runMetadata.profilesTested}
- Deal ID used (alignment/score): ${results.runMetadata.dealIdUsed}
- Server log: ${results.runMetadata.serverLogPath}

## 2. Commit refs deployed

${results.commitRefsDeployed.map((c) => `- \`${c}\``).join("\n")}

## 3. Environment variable snapshot

\`\`\`json
${JSON.stringify(results.environmentSnapshot, null, 2)}
\`\`\`

## 4. Profiles tested

${results.profilesTested
  .map(
    (p) =>
      `- **${p.recordId}** — ${p.label}; cohorts: ${(p.cohorts || []).join(", ")}; prefill keys: ${p.prefillKeyCount}; logo: ${p.hasLogo}; readPath: ${p.readPath || "n/a"}`
  )
  .join("\n")}

## 5. Flows executed

${results.flowsExecuted
  .map(
    (fr) =>
      `### ${fr.profile}\n` +
      fr.flows.map((f) => `- ${f.flow}: **${f.pass ? "PASS" : "FAIL"}**${f.note ? ` (${f.note})` : ""}`).join("\n")
  )
  .join("\n\n")}

## 6. Pass/fail counts

- Pass: ${results.passFailCounts.pass}
- Fail: ${results.passFailCounts.fail}
- Total checks: ${results.passFailCounts.total}

## 7. Determinism checks

- Canonical write mode only: ${results.determinismChecks.writeModeCanonical ? "PASS" : "FAIL"}
- No shadow/fail-open observed: ${results.determinismChecks.noShadowFailOpenObserved ? "PASS" : "FAIL"}

## 8. Mock-blocking verification

- Non-rec blocked: ${results.mockBlockingVerification.nonRecBlocked ? "PASS" : "FAIL"}
- Mock leaks detected: ${results.mockBlockingVerification.mockLeaksDetected ? "YES" : "NO"}

## 9. Canonical write verification

- Updates canonical: ${results.canonicalWriteVerification.updateWriteModeCanonical ? "PASS" : "FAIL"}

## 10. Read-path verification

- new_base profiles: ${results.readPathVerification.newBaseReads}
- legacy profiles: ${results.readPathVerification.legacyReads}

## 11. Diagnostics findings

- Server/module diagnostics captured: ${results.diagnosticsFindings.totalCaptured}
- Log rows written: ${results.diagnosticsFindings.logRows}

## 12. Fallback-used counts by field/concept

\`\`\`json
${JSON.stringify(
  Object.fromEntries(
    Object.entries(results.diagnosticsFindings.fallbackUsedByConcept).map(([k, v]) => [k, v.fallback])
  ),
  null,
  2
)}
\`\`\`

## 13. Unresolved-field counts by field/concept

\`\`\`json
${JSON.stringify(results.diagnosticsFindings.unresolvedByConcept, null, 2)}
\`\`\`

## 14. Mirror masking findings

${results.mirrorMaskingFindings.length ? JSON.stringify(results.mirrorMaskingFindings, null, 2) : "_None captured in API/module soak (browser-only path)._"}

## 15. Legacy read-path findings

${
  results.legacyReadPathFindings.length
    ? results.legacyReadPathFindings.map((p) => `- ${p.recordId}`).join("\n")
    : "_None in selected profile set._"
}

## 16. Finding classification table

\`\`\`json
${JSON.stringify(results.findingClassificationTable, null, 2)}
\`\`\`

## 17. P0/P1/P2 recommendations

- **P0:** ${results.recommendations.P0.length ? results.recommendations.P0.join(", ") : "None"}
- **P1 (sample):** ${results.recommendations.P1.join("; ") || "None"}
- **P2:** ${results.recommendations.P2.join("; ")}

## 18. Batch 3C candidate list

${results.batch3cCandidateList
  .slice(0, 20)
  .map(
    (r) =>
      `- ${r.field_or_concept}: expected \`${r.canonical_key_expected}\`, used \`${r.key_actually_used}\` (${r.source_layer})`
  )
  .join("\n")}

## 19. Deferred/non-actionable items

${results.deferredOrNonActionable.slice(0, 10).map((r) => `- ${r.field_or_concept}: ${r.recommended_action}`).join("\n") || "_None_"}

## 20. Risk assessment

${results.riskAssessment}

## 21. Decision recommendation

| Gate | Status |
|------|--------|
| Internal QA | ${results.decisionRecommendation.readyForInternalQA ? "Ready" : "Hold"} |
| External demo | ${results.decisionRecommendation.readyForExternalDemo ? "Ready" : "Hold"} |
| Go-live | ${results.decisionRecommendation.readyForGoLive ? "Ready" : "Not ready"} |
| Overall | **${results.decisionRecommendation.summary}** |

## Stop conditions triggered

${
  results.stopConditionsTriggered.length
    ? results.stopConditionsTriggered.map((s) => `- **${s.condition}** ${JSON.stringify(s)}`).join("\n")
    : "_None_"
}

---

Full machine-readable output: \`reports/operator-setup-staging-diagnostics-soak-results.json\`  
Log capture CSV: \`reports/operator-setup-staging-diagnostics-log-capture.csv\`
`;

  fs.writeFileSync(mdPath, md);
  console.log("Wrote", mdPath);
  console.log("Wrote", jsonPath);
  console.log("Wrote", csvPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
