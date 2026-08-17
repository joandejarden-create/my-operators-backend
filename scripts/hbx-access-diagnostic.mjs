/**
 * HBX access diagnostic — read-only, no Airtable, no discovery resume.
 * Never prints key, secret, signature, or credential prefixes.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  resolveHbxConfig,
  buildHbxSignature,
  buildHbxHeaders,
  contentUrl,
  apiUrl,
} from "../lib/research-engine-v2/hbx-content-api-client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-access-diagnostic-v1.json"
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function presence(name) {
  const v = String(process.env[name] || "").trim();
  return v.length > 0;
}

function extractError(json, text) {
  if (json == null) {
    return {
      code: null,
      message: text ? String(text).slice(0, 300).replace(/\s+/g, " ") : null,
    };
  }
  const err = json.error;
  if (typeof err === "string") return { code: null, message: err };
  if (err && typeof err === "object") {
    return {
      code: err.code != null ? String(err.code) : null,
      message:
        err.message != null
          ? String(err.message)
          : err.description != null
            ? String(err.description)
            : null,
    };
  }
  if (json.message) return { code: null, message: String(json.message) };
  return { code: null, message: null };
}

function pickHeaders(res) {
  const interesting = [
    "retry-after",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "x-request-id",
    "x-correlation-id",
    "cf-ray",
    "date",
  ];
  const out = {};
  for (const h of interesting) {
    const v = res.headers.get(h);
    if (v) out[h] = v;
  }
  return out;
}

async function oneGet(url, cfg) {
  const { headers, auth_meta } = buildHbxHeaders(cfg);
  // Strip credentials from returned meta — only timestamp unit checks
  const safeMeta = {
    timestamp_unix_seconds: auth_meta.timestamp,
    signature_hex_length: 64, // never echo fingerprint that contains prefix of signature
  };
  const started = Date.now();
  const res = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Accept-Encoding": "gzip",
      "Api-key": cfg.apiKey,
      "X-Signature": headers["X-Signature"],
    },
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  const err = extractError(json, text);
  return {
    http_status: res.status,
    ok: res.ok,
    elapsed_ms: Date.now() - started,
    auth_meta: safeMeta,
    response_headers: pickHeaders(res),
    sanitized_error_code: err.code,
    sanitized_error_message: err.message,
    // never persist hotel content
    body_top_keys: json && typeof json === "object" ? Object.keys(json).slice(0, 12) : [],
    audit_data_present: Boolean(json?.auditData),
  };
}

function signatureSelfCheck(cfg) {
  const nowMs = Date.now();
  const ts = Math.floor(nowMs / 1000);
  const built = buildHbxSignature(cfg.apiKey, cfg.apiSecret, ts);
  const checks = {
    timestamp_is_seconds: Number.isInteger(built.timestamp) && built.timestamp === ts,
    timestamp_not_milliseconds: built.timestamp < 1e12,
    timestamp_current_within_120s: Math.abs(built.timestamp - ts) <= 120,
    sha256_hex_length_64: /^[0-9a-f]{64}$/.test(built.signature),
    lowercase_hex: built.signature === built.signature.toLowerCase(),
    concatenation_order_apiKey_secret_ts: true, // verified by reading buildHbxSignature source
  };
  // Independent recompute with known order; compare equality only (no values logged)
  const independent = crypto
    .createHash("sha256")
    .update(`${cfg.apiKey}${cfg.apiSecret}${ts}`)
    .digest("hex");
  checks.independent_recompute_matches = independent === built.signature;
  // Ensure no whitespace injected into key/secret as used
  checks.key_no_surrounding_whitespace =
    cfg.apiKey === String(process.env.HBX_API_KEY || "").trim();
  checks.secret_no_surrounding_whitespace =
    cfg.apiSecret === String(process.env.HBX_API_SECRET || "").trim();
  const fail = Object.entries(checks).filter(([, v]) => !v).map(([k]) => k);
  return { result: fail.length ? "FAIL" : "PASS", checks, failed: fail };
}

function clockCheck() {
  const nowMs = Date.now();
  const nowSec = Math.floor(nowMs / 1000);
  const iso = new Date(nowMs).toISOString();
  return {
    result:
      nowSec > 1_700_000_000 && nowSec < 2_000_000_000 && nowMs > 1e12 ? "PASS" : "FAIL",
    local_iso: iso,
    unix_seconds: nowSec,
    note: "Compared local Date.now(); external NTP not queried to avoid extra network",
  };
}

function classify(results) {
  const msgs = [
    results.test_status?.sanitized_error_message,
    results.content_api?.sanitized_error_message,
    results.live_status?.sanitized_error_message,
  ]
    .filter(Boolean)
    .map((s) => String(s).toLowerCase());

  const hasQuota = msgs.some((m) => /quota/.test(m));
  const has429 =
    results.test_status?.http_status === 429 ||
    results.content_api?.http_status === 429 ||
    results.live_status?.http_status === 429;
  const contentOk = results.content_api?.ok === true;
  const testOk = results.test_status?.ok === true;

  if (!results.api_key_present || !results.secret_present) {
    return {
      primary: "CREDENTIALS_MISSING",
      secondary: null,
      credential_replacement: "UNKNOWN",
      account_action: "YES",
      safe_resume: "NO",
    };
  }
  if (results.signature_self_check?.result === "FAIL") {
    return {
      primary: "SIGNATURE_IMPLEMENTATION_ERROR",
      secondary: null,
      credential_replacement: "NO",
      account_action: "NO",
      safe_resume: "NO",
    };
  }
  if (results.clock_check?.result === "FAIL") {
    return {
      primary: "CLOCK_TIMESTAMP_ERROR",
      secondary: null,
      credential_replacement: "NO",
      account_action: "NO",
      safe_resume: "NO",
    };
  }
  if (contentOk && testOk) {
    return {
      primary: "ACCESS_OK",
      secondary: has429 ? "RATE_LIMIT_ONLY" : null,
      credential_replacement: "NO",
      account_action: "NO",
      safe_resume: has429 ? "NO" : "YES",
    };
  }
  if (hasQuota) {
    return {
      primary: "TEST_DAILY_QUOTA_EXHAUSTED",
      secondary: has429 ? "RATE_LIMIT_ONLY" : "ORCHESTRATOR_OVER_AGGRESSIVE_REQUEST_RATE",
      credential_replacement: "NO",
      account_action: "YES",
      safe_resume: "NO",
      next:
        "Wait for TEST daily quota reset OR obtain entitled LIVE Content API credentials (separate from TEST). Do not resume full-CALA discovery on TEST. Apply rate-limit patch before any future run.",
    };
  }
  if (has429 && !hasQuota) {
    return {
      primary: "RATE_LIMIT_ONLY",
      secondary: null,
      credential_replacement: "NO",
      account_action: "NO",
      safe_resume: "NO",
      next: "Wait / back off; apply rate-limit patch; retry one Content API probe before resume.",
    };
  }

  const msg = msgs.join(" | ");
  if (/inactive|disabled|revok|invalid.*(key|api|sign)/i.test(msg)) {
    return {
      primary: "ACCOUNT_OR_CREDENTIAL_INACTIVE",
      secondary: null,
      credential_replacement: "YES",
      account_action: "YES",
      safe_resume: "NO",
    };
  }
  if (/access.*(disallow|denied)|not entitled|permission/i.test(msg)) {
    return {
      primary: "CONTENT_API_ACCESS_DISALLOWED",
      secondary: null,
      credential_replacement: "UNKNOWN",
      account_action: "YES",
      safe_resume: "NO",
    };
  }
  if (
    results.content_api?.http_status === 403 &&
    !hasQuota &&
    results.content_api?.sanitized_error_message
  ) {
    return {
      primary: "UNKNOWN_HBX_ACCESS_FAILURE",
      secondary: "SEE_SANITIZED_ERROR_MESSAGE",
      credential_replacement: "UNKNOWN",
      account_action: "YES",
      safe_resume: "NO",
    };
  }
  return {
    primary: "UNKNOWN_HBX_ACCESS_FAILURE",
    secondary: has429 ? "RATE_LIMIT_ONLY" : null,
    credential_replacement: "UNKNOWN",
    account_action: "UNKNOWN",
    safe_resume: "NO",
    next: "Use sanitized error message with Hotelbeds support; do not resume discovery.",
  };
}

async function main() {
  const apiKeyPresent = presence("HBX_API_KEY");
  const secretPresent = presence("HBX_API_SECRET");
  const liveKeySeparate = presence("HBX_API_KEY_LIVE") || presence("HBX_LIVE_API_KEY");
  const liveSecretSeparate =
    presence("HBX_API_SECRET_LIVE") || presence("HBX_LIVE_API_SECRET");
  const testKeySeparate = presence("HBX_API_KEY_TEST") || presence("HBX_TEST_API_KEY");

  const cfg = resolveHbxConfig(process.env);
  const sig = cfg.ok ? signatureSelfCheck(cfg) : { result: "FAIL", checks: {}, failed: ["missing_credentials"] };
  const clock = clockCheck();

  const report = {
    objective: "hbx-access-diagnostic-v1",
    generated_at: new Date().toISOString(),
    HBX_DIAGNOSTIC_STATUS: "running",
    API_KEY_PRESENT: apiKeyPresent ? "YES" : "NO",
    SECRET_PRESENT: secretPresent ? "YES" : "NO",
    TEST_CREDENTIALS_PRESENT: apiKeyPresent && secretPresent
      ? testKeySeparate
        ? "YES"
        : "NOT_SEPARATE"
      : "NO",
    LIVE_CREDENTIALS_PRESENT:
      liveKeySeparate && liveSecretSeparate
        ? "YES"
        : apiKeyPresent && secretPresent
          ? "NOT_SEPARATE"
          : "NO",
    configured_hbx_env: cfg.hbxEnv || null,
    configured_api_host: cfg.apiBase || null,
    configured_content_host: cfg.contentBase || null,
    SIGNATURE_SELF_CHECK: sig.result,
    signature_check_detail: { failed: sig.failed, checks: sig.checks },
    CLOCK_CHECK: clock.result,
    clock_detail: clock,
    implementation_notes: {
      env_vars: ["HBX_API_KEY", "HBX_API_SECRET", "HBX_ENV", "HBX_API_BASE_URL", "HBX_CONTENT_API_BASE_URL"],
      signature: "SHA256 hex of apiKey+secret+unixSeconds",
      wave1_client: "lib/research-engine-v2/hbx-content-api-client.js (unchanged auth path)",
      prior_wave1_delay_ms: 150,
      geography_wave_default_delay_ms: 150,
      geography_wave_concurrency: 1,
      known_client_bug:
        "hbxFetchJson error_code ignored string json.error (e.g. Quota exceeded) — diagnostic extracts string errors correctly",
      prior_inventory_evidence: "reports show HTTP 403 with error Quota exceeded on TEST Content API",
    },
    TEST_REQUEST_HTTP_STATUS: null,
    TEST_SANITIZED_ERROR: null,
    LIVE_REQUEST_HTTP_STATUS: null,
    LIVE_SANITIZED_ERROR: null,
    CONTENT_API_HTTP_STATUS: null,
    CONTENT_API_SANITIZED_ERROR: null,
    CONTENT_API_TEST_STATUS: null,
  };

  if (!cfg.ok) {
    const c = classify({
      api_key_present: apiKeyPresent,
      secret_present: secretPresent,
      signature_self_check: sig,
      clock_check: clock,
    });
    Object.assign(report, finalize(report, c));
    writeReport(report);
    console.log(JSON.stringify(publicSummary(report), null, 2));
    return;
  }

  // STEP 5 — ONE TEST hotel-api status
  await sleep(2000);
  const testStatus = await oneGet(apiUrl(cfg, "hotel-api/1.0/status"), cfg);
  report.test_status = testStatus;
  report.TEST_REQUEST_HTTP_STATUS = testStatus.http_status;
  report.TEST_SANITIZED_ERROR = {
    code: testStatus.sanitized_error_code,
    message: testStatus.sanitized_error_message,
    headers: testStatus.response_headers,
  };

  // STEP 7 — ONE minimal Content API (Wave1 operation shape, tiny page)
  await sleep(5000);
  const content = await oneGet(
    contentUrl(
      cfg,
      "hotels?fields=code,name,countryCode&language=ENG&from=1&to=1&useSecondaryLanguage=false"
    ),
    cfg
  );
  report.content_api = {
    ...content,
    // never store hotel payloads
    hotel_rows_returned: content.ok ? "present_nonzero_or_empty_omitted" : 0,
  };
  // Re-fetch count without logging names: only whether hotels array length > 0
  // (already omitted body). Mark access only.
  report.CONTENT_API_HTTP_STATUS = content.http_status;
  report.CONTENT_API_SANITIZED_ERROR = {
    code: content.sanitized_error_code,
    message: content.sanitized_error_message,
    headers: content.response_headers,
  };
  report.CONTENT_API_TEST_STATUS = content.ok
    ? "ACCESS_OK"
    : content.sanitized_error_message && /quota/i.test(content.sanitized_error_message)
      ? "QUOTA_EXCEEDED"
      : "FAILED";

  // STEP 6 — LIVE only if separate credentials exist
  if (liveKeySeparate && liveSecretSeparate) {
    await sleep(5000);
    const liveEnv = {
      ...process.env,
      HBX_API_KEY: process.env.HBX_API_KEY_LIVE || process.env.HBX_LIVE_API_KEY,
      HBX_API_SECRET: process.env.HBX_API_SECRET_LIVE || process.env.HBX_LIVE_API_SECRET,
      HBX_ENV: "live",
      HBX_API_BASE_URL: "",
      HBX_CONTENT_API_BASE_URL: "",
    };
    const liveCfg = resolveHbxConfig(liveEnv);
    const liveStatus = await oneGet(apiUrl(liveCfg, "hotel-api/1.0/status"), liveCfg);
    report.live_status = liveStatus;
    report.LIVE_REQUEST_HTTP_STATUS = liveStatus.http_status;
    report.LIVE_SANITIZED_ERROR = {
      code: liveStatus.sanitized_error_code,
      message: liveStatus.sanitized_error_message,
      headers: liveStatus.response_headers,
    };
  } else {
    report.LIVE_REQUEST_HTTP_STATUS = "SKIPPED_NO_SEPARATE_LIVE_CREDENTIALS";
    report.LIVE_SANITIZED_ERROR = {
      code: null,
      message:
        "Only single HBX_API_KEY/HBX_API_SECRET pair present; refused to send TEST key to LIVE host",
      headers: {},
    };
  }

  const c = classify({
    api_key_present: apiKeyPresent,
    secret_present: secretPresent,
    signature_self_check: sig,
    clock_check: clock,
    test_status: testStatus,
    content_api: content,
    live_status: report.live_status || null,
  });
  Object.assign(report, finalize(report, c));
  writeReport(report);
  console.log(JSON.stringify(publicSummary(report), null, 2));
}

function finalize(report, c) {
  return {
    HBX_DIAGNOSTIC_STATUS: "complete",
    PRIMARY_ROOT_CAUSE: c.primary,
    SECONDARY_ROOT_CAUSE: c.secondary,
    IS_CREDENTIAL_REPLACEMENT_REQUIRED: c.credential_replacement,
    IS_HBX_ACCOUNT_ACTION_REQUIRED: c.account_action,
    IS_RATE_LIMIT_PATCH_REQUIRED: "YES",
    SAFE_TO_RESUME_DISCOVERY: c.safe_resume,
    NEXT_ACTION: c.next || null,
  };
}

function publicSummary(r) {
  return {
    HBX_DIAGNOSTIC_STATUS: r.HBX_DIAGNOSTIC_STATUS,
    API_KEY_PRESENT: r.API_KEY_PRESENT,
    SECRET_PRESENT: r.SECRET_PRESENT,
    TEST_CREDENTIALS_PRESENT: r.TEST_CREDENTIALS_PRESENT,
    LIVE_CREDENTIALS_PRESENT: r.LIVE_CREDENTIALS_PRESENT,
    configured_hbx_env: r.configured_hbx_env,
    SIGNATURE_SELF_CHECK: r.SIGNATURE_SELF_CHECK,
    CLOCK_CHECK: r.CLOCK_CHECK,
    TEST_REQUEST_HTTP_STATUS: r.TEST_REQUEST_HTTP_STATUS,
    TEST_SANITIZED_ERROR: r.TEST_SANITIZED_ERROR,
    LIVE_REQUEST_HTTP_STATUS: r.LIVE_REQUEST_HTTP_STATUS,
    LIVE_SANITIZED_ERROR: r.LIVE_SANITIZED_ERROR,
    CONTENT_API_HTTP_STATUS: r.CONTENT_API_HTTP_STATUS,
    CONTENT_API_SANITIZED_ERROR: r.CONTENT_API_SANITIZED_ERROR,
    CONTENT_API_TEST_STATUS: r.CONTENT_API_TEST_STATUS,
    PRIMARY_ROOT_CAUSE: r.PRIMARY_ROOT_CAUSE,
    SECONDARY_ROOT_CAUSE: r.SECONDARY_ROOT_CAUSE,
    IS_CREDENTIAL_REPLACEMENT_REQUIRED: r.IS_CREDENTIAL_REPLACEMENT_REQUIRED,
    IS_HBX_ACCOUNT_ACTION_REQUIRED: r.IS_HBX_ACCOUNT_ACTION_REQUIRED,
    IS_RATE_LIMIT_PATCH_REQUIRED: r.IS_RATE_LIMIT_PATCH_REQUIRED,
    SAFE_TO_RESUME_DISCOVERY: r.SAFE_TO_RESUME_DISCOVERY,
    NEXT_ACTION: r.NEXT_ACTION,
  };
}

function writeReport(report) {
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  // Ensure no accidental secret fields
  const cleaned = JSON.parse(
    JSON.stringify(report, (k, v) => {
      if (/secret|apiKey|api_key|signature$/i.test(k)) return undefined;
      return v;
    })
  );
  fs.writeFileSync(OUT, JSON.stringify(cleaned, null, 2));
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      HBX_DIAGNOSTIC_STATUS: "failed",
      error: String(err?.message || err).slice(0, 200),
    })
  );
  process.exitCode = 1;
});
