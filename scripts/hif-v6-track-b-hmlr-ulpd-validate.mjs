#!/usr/bin/env node
/**
 * HIF V6 Track B — HM Land Registry Use Land and Property Data (ULPD) validation.
 * Reads HM_LAND_REGISTRY_KEY from env or authorized .env files. Never prints the key.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-hif-v6-track-b-hmlr");

const ULPD_BASE = "https://use-land-property-data.service.gov.uk/api/v1";

function loadEnvFile(file) {
  const out = {};
  if (!fs.existsSync(file)) return out;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z0-9_]+)\s*=\s*(.*)$/);
    if (!m) continue;
    let v = m[2].trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    out[m[1]] = v;
  }
  return out;
}

function resolveKey() {
  const merged = {
    ...loadEnvFile(path.join("C:/Dev/dealality/my-operators-backend", ".env")),
    ...loadEnvFile(path.join(ROOT, ".env")),
  };
  const key = process.env.HM_LAND_REGISTRY_KEY || merged.HM_LAND_REGISTRY_KEY || "";
  const ch =
    process.env.COMPANIES_HOUSE_API_KEY || merged.COMPANIES_HOUSE_API_KEY || "";
  return { key, chPresent: Boolean(ch) };
}

function classifyError(status, bodyText) {
  if (status === 429) return "RATE_LIMIT";
  if (status >= 500) return "SERVER_ERROR";
  if (status === 401 || status === 403) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) {
      return "AUTH_INVALID_KEY";
    }
    if (/need to provide your API Key/i.test(bodyText)) return "AUTH_MISSING_KEY";
    if (/not yet signed the licence/i.test(bodyText)) return "LICENCE_NOT_SIGNED";
    if (/not paid for access/i.test(bodyText)) return "PAID_DATASET_REQUIRED";
    return "PERMISSION_OR_FORBIDDEN";
  }
  if (status === 404) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) {
      return "AUTH_INVALID_KEY";
    }
    return "NOT_FOUND";
  }
  if (status >= 200 && status < 300) return "OK";
  if (status == null) return "NETWORK_FAILURE";
  return `HTTP_${status}`;
}

async function call(key, pathname) {
  const started = Date.now();
  let status = null;
  let bodyText = "";
  try {
    const res = await fetch(`${ULPD_BASE}${pathname}`, {
      headers: { Authorization: key, Accept: "application/json" },
    });
    status = res.status;
    bodyText = await res.text();
  } catch (e) {
    return {
      pathname,
      status: null,
      errClass: "NETWORK_FAILURE",
      ms: Date.now() - started,
      error: String(e?.message || e),
      resultSummary: null,
    };
  }
  const errClass = classifyError(status, bodyText);
  let parsed = null;
  try {
    parsed = JSON.parse(bodyText);
  } catch {
    /* ignore */
  }
  let resultSummary = null;
  if (Array.isArray(parsed?.result)) {
    resultSummary = {
      type: "list",
      count: parsed.result.length,
      names: parsed.result.map((x) => x.name || null),
      titles: parsed.result.map((x) => x.title || null),
    };
  } else if (parsed?.result && typeof parsed.result === "object") {
    const r = parsed.result;
    resultSummary = {
      type: "object",
      name: r.name || null,
      title: r.title || null,
      fee: r.fee || null,
      datasetType: r.type || null,
      state: r.state || null,
      licence_id: r.licence_id || null,
      resources: Array.isArray(r.resources)
        ? r.resources.slice(0, 8).map((x) => ({
            name: x.name,
            file_name: x.file_name,
            file_size: x.file_size,
            format: x.format,
            row_count: x.row_count,
          }))
        : [],
      public_resources: Array.isArray(r.public_resources)
        ? r.public_resources.slice(0, 4).map((x) => ({
            name: x.name,
            file_name: x.file_name,
            file_size: x.file_size,
          }))
        : [],
    };
  } else if (parsed?.download_url || parsed?.result?.download_url) {
    const d = parsed.result || parsed;
    resultSummary = {
      type: "download_ticket",
      resource: d.resource || null,
      valid_for_seconds: d.valid_for_seconds || null,
      downloadUrlPresent: Boolean(d.download_url),
    };
  }
  return {
    pathname,
    status,
    errClass,
    ms: Date.now() - started,
    success: parsed?.success ?? null,
    error: parsed?.error || null,
    resultSummary,
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const { key, chPresent } = resolveKey();
  const authStatus = {
    productInferred: "USE_LAND_AND_PROPERTY_DATA",
    baseUrl: ULPD_BASE,
    keyPresent: Boolean(key),
    keyLength: key.length,
    companiesHouseApiKeyPresent: chPresent,
    keySourceHint: process.env.HM_LAND_REGISTRY_KEY
      ? "process.env"
      : key
        ? "dotenv_file"
        : "absent",
    note:
      "This credential targets Use Land and Property Data (bulk datasets: CCOD/OCOD/NPS). It does NOT grant Business Gateway live title-register access.",
  };

  if (!key) {
    authStatus.outcome = "KEY_ABSENT";
    fs.writeFileSync(
      path.join(OUT_DIR, "HMLR_AUTH_STATUS.json"),
      JSON.stringify(authStatus, null, 2)
    );
    console.log(JSON.stringify(authStatus, null, 2));
    process.exit(2);
  }

  const probes = [];
  const datasets = await call(key, "/datasets");
  probes.push(datasets);

  const datasetNames =
    datasets.errClass === "OK" && datasets.resultSummary?.names
      ? datasets.resultSummary.names.filter(Boolean)
      : ["ccod", "ocod", "nps", "nps_sample", "leases", "restrictive_covenants"];

  for (const name of datasetNames) {
    probes.push(await call(key, `/datasets/${name}`));
  }

  // Low-volume download-ticket probes for free datasets only (metadata-confirmed free).
  const downloadProbes = [];
  for (const p of probes) {
    if (p.pathname === "/datasets") continue;
    const fee = p.resultSummary?.fee || "";
    const resources = p.resultSummary?.resources || [];
    const isPaid =
      p.errClass === "PAID_DATASET_REQUIRED" ||
      /paid|charge/i.test(fee) && !/free of charge/i.test(fee);
    if (isPaid) {
      downloadProbes.push({
        dataset: p.resultSummary?.name || p.pathname,
        skipped: true,
        reason: "PAID_OR_FEE_NOT_FREE — founder authorization required before download",
      });
      continue;
    }
    const full = resources.find((r) => /FULL/i.test(r.file_name || "")) || resources[0];
    if (!full?.file_name) continue;
    const ds = p.resultSummary?.name;
    if (!ds) continue;
    // Prefer small public example if present; else COU (change-only) over FULL for validation.
    const cou = resources.find((r) => /COU/i.test(r.file_name || ""));
    const pub = (p.resultSummary?.public_resources || [])[0];
    const pick = pub?.file_name || cou?.file_name || full.file_name;
    const ticket = await call(key, `/datasets/${ds}/${encodeURIComponent(pick)}`);
    downloadProbes.push({
      dataset: ds,
      file: pick,
      ...ticket,
    });
  }

  authStatus.outcome =
    datasets.errClass === "OK" ? "AUTHENTICATED_OK" : datasets.errClass;
  authStatus.authenticatedAccess = datasets.errClass === "OK";
  authStatus.datasetListOk = datasets.errClass === "OK";
  authStatus.datasetsVisible = datasets.resultSummary?.names || [];
  authStatus.capabilitySummary = {
    listsDatasets: datasets.errClass === "OK",
    canReadDatasetMetadata: probes
      .filter((p) => p.pathname.startsWith("/datasets/") && !p.pathname.includes(".zip"))
      .some((p) => p.errClass === "OK"),
    freeDatasetDownloadTickets: downloadProbes.filter((d) => d.errClass === "OK").length,
    licenceBlocks: downloadProbes
      .filter((d) => d.errClass === "LICENCE_NOT_SIGNED")
      .map((d) => d.dataset || d.file),
    paidBlocks: [
      ...probes.filter((p) => p.errClass === "PAID_DATASET_REQUIRED").map((p) => p.pathname),
      ...downloadProbes.filter((d) => d.skipped).map((d) => d.dataset),
    ],
    doesNotProvide: [
      "Business Gateway live title register",
      "Per-address official copy purchase",
      "Real-time proprietor lookup by UPRN/title number (unless in CCOD/OCOD bulk)",
    ],
  };

  const report = {
    generatedAt: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    authStatus,
    probes,
    downloadProbes,
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "HMLR_AUTH_STATUS.json"),
    JSON.stringify(authStatus, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "HMLR_ULPD_VALIDATE.json"),
    JSON.stringify(report, null, 2)
  );
  console.log(
    JSON.stringify(
      {
        outcome: authStatus.outcome,
        keyPresent: true,
        keyLength: authStatus.keyLength,
        datasetsVisible: authStatus.datasetsVisible,
        capabilitySummary: authStatus.capabilitySummary,
        downloadProbeClasses: downloadProbes.map((d) => ({
          dataset: d.dataset,
          file: d.file || null,
          errClass: d.errClass || (d.skipped ? "SKIPPED_PAID" : null),
          error: d.error || d.reason || null,
        })),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
