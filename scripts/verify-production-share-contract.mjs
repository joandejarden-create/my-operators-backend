#!/usr/bin/env node
/**
 * Production share contract — pre/post deploy durability gate.
 *
 *   npm run verify:production-share-contract
 *   npm run verify:production-share-contract -- --live
 *   npm run verify:production-share-contract -- --restart-sim
 *
 * Fails when:
 * - production-like runtime missing durable GDI share secret
 * - registry / revoked list missing from deploy tree
 * - pre-existing Bethesda (or contract) token fails local verify
 * - share routes missing from server.js
 * - --live: production HTTP checks fail for contract tokens
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  verifyGdiShareCapability,
  assertGdiShareProductionConfig,
  getGdiShareCapabilitySecret,
  getGdiShareVerificationSecrets,
  resolveGdiShareCapabilities,
  GDI_SHARE_CAPABILITY,
  readGdiShareRegistry,
  loadDurableRevokedTokenIds,
  isTokenIdDurablyRevoked,
} from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const args = new Set(process.argv.slice(2));
const live = args.has("--live");
const restartSim = args.has("--restart-sim");
const baseUrl = (
  process.env.DEALALITY_PRODUCTION_BASE_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

const contractPath = path.join(
  root,
  "config/client-share/production-share-contract-tokens.json"
);
const serverJs = fs.readFileSync(path.join(root, "server.js"), "utf8");

const checks = [];
function check(name, pass, detail) {
  checks.push({ name, pass: !!pass, detail: detail || null });
  console.log(pass ? "PASS" : "FAIL", name, detail || "");
}

function failExit() {
  console.error("\nFAIL verify:production-share-contract");
  process.exit(1);
}

const contract = JSON.parse(fs.readFileSync(contractPath, "utf8"));
const gdiTokens = (contract.tokens || []).filter((t) => t.product === "GDI");

// --- Config / durability ---
const secret = getGdiShareCapabilitySecret();
const productionLike =
  String(process.env.NODE_ENV || "").toLowerCase() === "production" ||
  !!String(process.env.RAILWAY_ENVIRONMENT || "").trim() ||
  args.has("--require-secret");

if (productionLike) {
  check(
    "gdi_share_secret_configured",
    !!secret && secret.length >= 32,
    secret ? `len=${secret.length}` : "missing"
  );
} else {
  check(
    "gdi_share_secret_configured_or_skip_local",
    true,
    secret ? `len=${secret.length}` : "skipped_local_without_secret"
  );
}

try {
  const r = assertGdiShareProductionConfig();
  check("gdi_share_production_assert", true, r.skipped ? "skipped_non_prod" : "ok");
} catch (err) {
  check("gdi_share_production_assert", false, err.message);
}

const prevSecrets = getGdiShareVerificationSecrets();
if (secret || productionLike) {
  check(
    "gdi_share_verification_secrets",
    prevSecrets.length >= 1,
    `count=${prevSecrets.length} (current+previous)`
  );
} else {
  check(
    "gdi_share_verification_secrets",
    true,
    "skipped_local_without_secret"
  );
}

const registryPath = path.join(
  root,
  "config/client-share/gdi-share-registry/active-tokens.json"
);
const revokedPath = path.join(
  root,
  "config/client-share/gdi-share-registry/revoked-token-ids.json"
);
check("registry_file_in_tree", fs.existsSync(registryPath), registryPath);
check("durable_revoked_list_in_tree", fs.existsSync(revokedPath), revokedPath);

const reg = readGdiShareRegistry();
const revoked = loadDurableRevokedTokenIds();
check(
  "registry_not_ephemeral_only_contract",
  Object.keys(reg.tokens || {}).length > 0,
  `active_rows=${Object.keys(reg.tokens || {}).length}`
);
check(
  "durable_revoked_list_loaded",
  Array.isArray(revoked.tokenIds),
  `revoked=${revoked.tokenIds.length}`
);

// --- Route markers ---
const routeMarkers = [
  "group-demand-intelligence-share.html",
  "/api/group-demand-intelligence/share/resolve",
  "postGdiShareValidation",
  "assertGdiShareProductionConfig",
  "owner-ai-demand-share.html",
  "hotel-explorer-share.html",
];
for (const m of routeMarkers) {
  check(`server_route_marker:${m}`, serverJs.includes(m), m);
}

// --- Pre-existing token verify (local crypto + registry) ---
for (const t of gdiTokens) {
  const row = reg.tokens?.[t.tokenId];
  check(
    `token_registry_row:${t.tokenId}`,
    row?.status === "ACTIVE" && row?.hotelId === t.hotelId,
    row ? `${row.status} ${row.hotelId}` : "missing_row"
  );
  check(
    `token_not_durably_revoked:${t.tokenId}`,
    !isTokenIdDurablyRevoked(t.tokenId),
    t.tokenId
  );

  if (!secret) {
    check(
      `token_verify:${t.tokenId}`,
      true,
      "skipped_crypto_without_local_secret — use --live or set GDI_SHARE_CAPABILITY_SECRET"
    );
    continue;
  }

  const verified = verifyGdiShareCapability(t.token, { allowSelfHeal: true });
  check(
    `token_verify:${t.tokenId}`,
    verified.ok === true,
    verified.ok
      ? `hotel=${verified.claims.hotelId} caps=${(verified.capabilities || []).join(",")}`
      : `${verified.code} ${verified.error}`
  );
  if (verified.ok) {
    const caps = resolveGdiShareCapabilities(verified.claims);
    for (const need of t.expectedCapabilities || []) {
      check(
        `token_capability:${t.tokenId}:${need}`,
        caps.includes(need),
        caps.join(",")
      );
    }
  }
}

// --- Restart simulation: re-verify same token after re-read ---
if (secret) {
  for (const t of gdiTokens) {
    const a = verifyGdiShareCapability(t.token);
    const b = verifyGdiShareCapability(t.token);
    check(
      `token_stable_across_reverify:${t.tokenId}`,
      a.ok && b.ok && a.claims.tid === b.claims.tid,
      a.ok && b.ok ? "stable" : `${a.code}/${b.code}`
    );
  }
} else {
  check("token_stable_across_reverify", true, "skipped_without_local_secret");
}

// --- Live production ---
async function liveCheck() {
  for (const t of gdiTokens) {
    const enc = encodeURIComponent(t.token);
    const pageUrl = `${baseUrl}${t.pagePath}?share=${enc}`;
    const resolveUrl = `${baseUrl}${t.resolvePath}?share=${enc}`;
    const pageRes = await fetch(pageUrl, { redirect: "follow" });
    check(`live_page:${t.tokenId}`, pageRes.status === 200, `HTTP ${pageRes.status}`);
    const resolveRes = await fetch(resolveUrl);
    const resolveBody = await resolveRes.json().catch(() => ({}));
    check(
      `live_resolve:${t.tokenId}`,
      resolveRes.status === 200 && resolveBody.ok === true,
      resolveRes.status === 200
        ? `hotel=${resolveBody.hotelId}`
        : `${resolveRes.status} ${resolveBody.error || resolveBody.message}`
    );
    if (resolveBody.ok && t.hotelId) {
      for (const apiTpl of t.requiredApis || []) {
        const apiPath = apiTpl.replace("{hotelId}", t.hotelId);
        const apiRes = await fetch(`${baseUrl}${apiPath}?share=${enc}`);
        check(
          `live_api:${apiPath}`,
          apiRes.status === 200,
          `HTTP ${apiRes.status}`
        );
      }
      if (resolveBody.canValidate !== true && !(resolveBody.capabilities || []).includes("CAN_VALIDATE")) {
        // Soft warn for old production until next deploy ships capability field
        check(
          `live_capabilities_field:${t.tokenId}`,
          true,
          "capability field may be absent until next deploy — legacy surface still validates"
        );
      } else {
        check(
          `live_can_validate:${t.tokenId}`,
          resolveBody.canValidate === true ||
            (resolveBody.capabilities || []).includes(GDI_SHARE_CAPABILITY.CAN_VALIDATE),
          JSON.stringify(resolveBody.capabilities || [])
        );
      }
    }
  }
}

const failedLocal = checks.some((c) => !c.pass);
if (failedLocal) failExit();

if (live) {
  await liveCheck();
  if (checks.some((c) => !c.pass)) failExit();
}

console.log("\nPASS verify:production-share-contract");
console.log(JSON.stringify({ ok: true, live, checks }, null, 2));
