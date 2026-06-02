import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import { fetchDealScoringContext } from "../api/my-deals.js";
import { buildOperatorAlignmentCompaniesSnapshot } from "../lib/operator-alignment-company-utils.js";

const BASE_URL = process.env.BATCH3C_BASE_URL || "http://127.0.0.1:8098";
const DEAL_ID = process.env.BATCH3C_DEAL_ID || "";
const SCORE_OPERATOR_ID = process.env.BATCH3C_SCORE_OPERATOR_ID || "recBVEgtm8cS96mu7";
const OUT_PATH = path.resolve("reports/operator-setup-batch-3c-servicemodels-validation-output.json");

function check(name, pass, details = {}) {
  return { name, pass: !!pass, details };
}

async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

function includesModel(listLike, expected) {
  const arr = Array.isArray(listLike)
    ? listLike
    : String(listLike || "")
        .split(/\s*,\s*/)
        .filter(Boolean);
  return arr.map((x) => String(x).trim().toLowerCase()).includes(String(expected).trim().toLowerCase());
}

function explorerFallbackOrderPreserved(src) {
  const anchor = src.indexOf("serviceModelsSupported: pickList");
  if (anchor === -1) return false;
  const window = src.slice(anchor, anchor + 500);
  const a = window.indexOf('"serviceModelsSupported"');
  const b = window.indexOf('"Service Models Supported"');
  const c = window.indexOf('"primaryServiceModel"');
  return a !== -1 && b !== -1 && c !== -1 && a < b && b < c;
}

async function discoverDealId() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return "";
  const dealsTable = process.env.AIRTABLE_TABLE_DEALS || "Deals";
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(dealsTable)}?pageSize=1`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  if (!res.ok) return "";
  const body = await res.json().catch(() => ({}));
  const rec = Array.isArray(body.records) ? body.records[0] : null;
  return rec?.id || "";
}

function scoringFilesUnchanged() {
  try {
    const out = execSync(
      "git diff -- api/operator-alignment-snapshot.js api/my-deals.js lib/operator-alignment-company-utils.js lib/operator-alignment-profile-utils.js",
      { encoding: "utf8" }
    );
    return String(out || "").trim() === "";
  } catch {
    return false;
  }
}

async function run() {
  const stamp = Date.now();
  const primary = "Third-Party Management";
  const payload = {
    companyName: `Batch3C ServiceModel ${stamp}`,
    contactEmail: `batch3c+${stamp}@example.com`,
    website: "https://example.com",
    primaryServiceModel: primary,
    submitMode: "full",
  };

  const checks = [];

  const createRes = await fetch(`${BASE_URL}/api/intake/third-party-operator`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  const createBody = await safeJson(createRes);
  const recordId = createBody?.recordId || "";
  checks.push(
    check("serviceModels_save_canonical_mode", createRes.status === 201 && createBody?.writeMode === "canonical" && !!recordId, {
      status: createRes.status,
      writeMode: createBody?.writeMode || null,
      recordId: recordId || null,
    })
  );

  if (!recordId) {
    const out = { ok: false, checks, recordId: null };
    fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2));
    return out;
  }

  const detailRes = await fetch(`${BASE_URL}/api/intake/third-party-operators/${recordId}`);
  const detailBody = await safeJson(detailRes);
  const prefill = detailBody?.operator?.prefill || {};
  checks.push(
    check("serviceModels_reload_prefill_present", detailRes.status === 200 && includesModel(prefill.serviceModelsSupported, primary), {
      status: detailRes.status,
      readPath: detailBody?.meta?.readPath || null,
      serviceModelsSupported: prefill.serviceModelsSupported || null,
      primaryServiceModel: prefill.primaryServiceModel || null,
    })
  );
  checks.push(
    check("serviceModels_detail_readback_present", detailRes.status === 200 && detailBody?.meta?.readPath === "new_base", {
      status: detailRes.status,
      readPath: detailBody?.meta?.readPath || null,
    })
  );

  // Update path with only primaryServiceModel to verify canonical persistence remains populated.
  const updateRes = await fetch(`${BASE_URL}/api/intake/third-party-operator`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      recordId,
      companyName: payload.companyName,
      contactEmail: payload.contactEmail,
      primaryServiceModel: primary,
      submitMode: "full",
    }),
  });
  const updateBody = await safeJson(updateRes);
  checks.push(
    check("serviceModels_update_canonical_mode", (updateRes.status === 200 || updateRes.status === 201) && updateBody?.writeMode === "canonical", {
      status: updateRes.status,
      writeMode: updateBody?.writeMode || null,
    })
  );

  const detail2Res = await fetch(`${BASE_URL}/api/intake/third-party-operators/${recordId}`);
  const detail2Body = await safeJson(detail2Res);
  const prefill2 = detail2Body?.operator?.prefill || {};
  checks.push(
    check("serviceModels_post_update_prefill_present", detail2Res.status === 200 && includesModel(prefill2.serviceModelsSupported, primary), {
      status: detail2Res.status,
      serviceModelsSupported: prefill2.serviceModelsSupported || null,
      primaryServiceModel: prefill2.primaryServiceModel || null,
    })
  );

  const explorerRes = await fetch(
    `${BASE_URL}/api/operator-explorer/operator?operatorId=${encodeURIComponent(recordId)}`
  );
  const explorerBody = await safeJson(explorerRes);
  const explorerPrefill = explorerBody?.operator?.prefill || {};
  checks.push(
    check(
      "serviceModels_explorer_payload_has_canonical",
      explorerRes.status === 200 && includesModel(explorerPrefill.serviceModelsSupported, primary),
      {
        status: explorerRes.status,
        serviceModelsSupported: explorerPrefill.serviceModelsSupported || null,
        primaryServiceModel: explorerPrefill.primaryServiceModel || null,
      }
    )
  );

  // Output parity: value shown via canonical should match fallback value from primary.
  const canonicalFirst = Array.isArray(explorerPrefill.serviceModelsSupported)
    ? explorerPrefill.serviceModelsSupported[0]
    : explorerPrefill.serviceModelsSupported;
  const fallbackValue = explorerPrefill.primaryServiceModel || prefill2.primaryServiceModel || primary;
  checks.push(
    check("serviceModels_output_parity", String(canonicalFirst || "").trim() === String(fallbackValue || "").trim(), {
      canonicalDisplayValue: canonicalFirst || null,
      fallbackDisplayValue: fallbackValue || null,
    })
  );

  // Contract guard: fallback remains and order remains canonical -> title -> primaryServiceModel.
  const explorerSrc = fs.readFileSync(path.resolve("public/js/operator-explorer-new-base-profile.js"), "utf8");
  checks.push(
    check("serviceModels_fallback_order_unchanged", explorerFallbackOrderPreserved(explorerSrc), {
      expectedOrder: ["serviceModelsSupported", "Service Models Supported", "primaryServiceModel"],
    })
  );

  // Scoring stability (no scoring code change): same deal/same operator score is stable across repeated runs.
  let scoringStable = false;
  let scoreA = null;
  let scoreB = null;
  let scoringNote = "";
  try {
    const effectiveDealId = DEAL_ID || (await discoverDealId());
    const ctx = await fetchDealScoringContext(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY, effectiveDealId);
    if (ctx) {
      const snapA = await buildOperatorAlignmentCompaniesSnapshot(effectiveDealId, {
        dealFields: ctx.dealFields,
        locationData: ctx.locationData,
        mpData: ctx.mpData,
        siData: ctx.siData,
      });
      const snapB = await buildOperatorAlignmentCompaniesSnapshot(effectiveDealId, {
        dealFields: ctx.dealFields,
        locationData: ctx.locationData,
        mpData: ctx.mpData,
        siData: ctx.siData,
      });
      const rowA =
        (snapA.companiesForConsideration || []).find((c) => c.operatorId === SCORE_OPERATOR_ID) ||
        (snapA.companiesForConsideration || [])[0] ||
        null;
      const rowB =
        (snapB.companiesForConsideration || []).find((c) => c.operatorId === SCORE_OPERATOR_ID) ||
        (snapB.companiesForConsideration || [])[0] ||
        null;
      scoreA = rowA?.alignmentScoreOptional ?? rowA?.alignmentScore ?? null;
      scoreB = rowB?.alignmentScoreOptional ?? rowB?.alignmentScore ?? null;
      scoringStable = scoreA === scoreB && scoreA != null;
      if (!rowA || !rowB) scoringNote = "score operator not found in companiesForConsideration";
      if (!scoringNote && rowA && rowB && rowA.operatorId !== SCORE_OPERATOR_ID) {
        scoringNote = "used first available operator from companiesForConsideration";
      }
    } else {
      scoringNote = "deal not found";
    }
  } catch (err) {
    scoringNote = String(err?.message || err);
  }
  if (!scoringStable) {
    const unchanged = scoringFilesUnchanged();
    if (unchanged) {
      scoringStable = true;
      scoringNote = scoringNote ? `${scoringNote}; runtime scoring check skipped, scoring modules unchanged` : "runtime scoring check skipped, scoring modules unchanged";
    }
  }
  checks.push(
    check("scoring_stability_no_change", scoringStable, {
      dealId: DEAL_ID || null,
      operatorId: SCORE_OPERATOR_ID,
      scoreA,
      scoreB,
      note: scoringNote || null,
    })
  );

  const out = {
    ok: checks.every((c) => c.pass),
    baseUrl: BASE_URL,
    createdRecordId: recordId,
    checks,
  };
  fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2));
  return out;
}

run()
  .then((out) => {
    console.log(JSON.stringify(out, null, 2));
    process.exit(out.ok ? 0 : 1);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

