/**
 * Issue production-signed ADP client share for Renaissance New York Times Square.
 * Pulls ADP_SHARE_CAPABILITY_SECRET from Railway (in-memory only — never printed).
 * Seals URL into production-share-contract-tokens for Admin Open/Copy.
 * Revokes known DEV-signed Renaissance tokens from the 2026-10-16 local issue.
 */
import { spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  issueShareCapability,
  revokeShareCapability,
  markAdpTokenIdDurablyRevoked,
  readShareRegistry,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveExternalClientLinks } from "../lib/admin/report-external-client-links-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_renaissance_times_square";
const HPC = "recG66DQJKP2c0UNh";
const PROPERTY_NAME = "Renaissance New York Times Square Hotel";
const PUBLIC_BASE = "https://my-operators-backend-production.up.railway.app";
const TOKEN_ID = `sht_${randomBytes(12).toString("hex")}`;
const OUT_DIR = path.join(ROOT, "reports/adp-six-hotel-e2e/renaissance");
const EXPECTED_PERIOD =
  "adp_period_adp_renaissance_times_square_20261016083441_2ff16a";

/** DEV-signed token ids known from 2026-10-16 local issue attempts (revoke; do not resurrect). */
const DEV_SIGNED_TOKEN_IDS_TO_REVOKE = [
  "sht_691d7f09c7f5b91876c63584",
  "sht_57ee8b2c0875eab153fcda13",
];

function loadRailwayVars() {
  const r = spawnSync("railway", ["variable", "list", "--json"], {
    encoding: "utf8",
    cwd: ROOT,
    shell: true,
  });
  if (r.status !== 0) {
    throw new Error(`railway variable list failed: ${r.stderr || r.stdout}`);
  }
  return JSON.parse(r.stdout);
}

function sealContractToken({ tokenId, token }) {
  const p = path.join(
    ROOT,
    "config/client-share/production-share-contract-tokens.json"
  );
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  doc.tokens = Array.isArray(doc.tokens) ? doc.tokens : [];
  doc.tokens = doc.tokens.filter(
    (t) =>
      !(
        t.product === "ADP" &&
        String(t.propertyId || "") === PROPERTY_ID
      )
  );
  doc.tokens.push({
    product: "ADP",
    label: `${PROPERTY_NAME} — client share`,
    hotelId: HPC,
    propertyId: PROPERTY_ID,
    tokenId,
    token,
    pagePath: "/owner-ai-demand-share.html",
    resolvePath: "/api/ai-demand-positioning/share/resolve",
    reportScope: "current_published",
  });
  doc.updatedAt = new Date().toISOString().slice(0, 10);
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
}

function redactBearerFiles() {
  const targets = [
    path.join(
      ROOT,
      "data/ai-demand-positioning/share-registry/issued-share-urls.ad-hoc.2026-10-16T09-42-12.json"
    ),
    path.join(
      ROOT,
      "reports/client-share-links/AD_HOC_CLIENT_SHARE_LINKS_2026-10-16T09-42-12.json"
    ),
    path.join(OUT_DIR, "SHARE_ISSUE.log"),
  ];
  const redacted = [];
  for (const p of targets) {
    if (!fs.existsSync(p)) continue;
    const raw = fs.readFileSync(p, "utf8");
    if (!/adpshare\.v1\./.test(raw)) continue;
    const safe = {
      warning:
        "REDACTED — usable bearer share URLs removed. Production tokens live in protected delivery + sealed contract; do not re-commit bearers.",
      redactedAt: new Date().toISOString(),
      propertyId: PROPERTY_ID,
      note: "Credential validity addressed by revoke + production re-issue. Removing from tip does not erase Git history.",
      priorHadBearerCapability: true,
    };
    fs.writeFileSync(p, `${JSON.stringify(safe, null, 2)}\n`, "utf8");
    redacted.push(path.relative(ROOT, p).replace(/\\/g, "/"));
  }
  return redacted;
}

function writeOut(name, content) {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(path.join(OUT_DIR, name), content, "utf8");
}

async function main() {
  const man = loadPublishedManifest(PROPERTY_ID);
  if (!man?.latestPeriodId) {
    throw new Error(`No published ADP snapshot for ${PROPERTY_ID}`);
  }
  if (man.latestPeriodId !== EXPECTED_PERIOD) {
    throw new Error(
      `Unexpected published period ${man.latestPeriodId}; expected ${EXPECTED_PERIOD}`
    );
  }

  const vars = loadRailwayVars();
  const secret = String(vars.ADP_SHARE_CAPABILITY_SECRET || "").trim();
  if (secret.length < 32) {
    throw new Error("Railway ADP_SHARE_CAPABILITY_SECRET missing/short");
  }
  process.env.ADP_SHARE_CAPABILITY_SECRET = secret;
  delete process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET;

  const revoked = [];
  for (const tid of DEV_SIGNED_TOKEN_IDS_TO_REVOKE) {
    const r = revokeShareCapability(tid, "dev_signed_local_issue_superseded");
    markAdpTokenIdDurablyRevoked(tid, "dev_signed_local_issue_superseded");
    revoked.push({ tokenId: tid, revokeOk: r.ok === true, error: r.error || null });
  }

  const issued = issueShareCapability({
    propertyId: PROPERTY_ID,
    tokenId: TOKEN_ID,
    label: `renaissance-client-share:${PROPERTY_ID}:prod`,
    reportScope: "current_published",
  });

  sealContractToken({ tokenId: issued.tokenId, token: issued.token });
  const redactedPaths = redactBearerFiles();

  const url = `${PUBLIC_BASE}${issued.sharePath}`;
  const sha12 = createHash("sha256").update(issued.token).digest("hex").slice(0, 12);

  const links = resolveExternalClientLinks(PROPERTY_ID, { createIfMissing: false });
  const linksByHpc = resolveExternalClientLinks(HPC, { createIfMissing: false });
  const reg = readShareRegistry();
  const active = reg.tokens?.[issued.tokenId];

  const pack = {
    ok: true,
    propertyId: PROPERTY_ID,
    hotelCensusId: HPC,
    propertyName: PROPERTY_NAME,
    latestPeriodId: man.latestPeriodId,
    demandCaptureRate: man.demandCaptureRate ?? null,
    tokenId: issued.tokenId,
    tokenSha12: sha12,
    publicBase: PUBLIC_BASE,
    sharePath: issued.sharePath,
    // shareUrl retained in protected local pack only — founder handoff; do not broaden commit surface
    shareUrlPresent: true,
    adminResolveAdpAvailable: Boolean(links.adp?.available),
    adminResolveAdpUrlMatches: links.adp?.url === url,
    adminResolveViaHpcAdpAvailable: Boolean(linksByHpc.adp?.available),
    registryStatus: active?.status || null,
    revokedDevTokens: revoked,
    redactedTrackedBearerFiles: redactedPaths,
    mintedAt: new Date().toISOString(),
    secretSource: "railway_production_env",
    note: "Deploy active-tokens.json + sealed contract before treating Admin Open as production-synced. Production verify uses Railway secret; self-heal may activate valid HMAC without registry row.",
  };

  // Protected delivery: URL file is local/ops; tracked proof is redacted.
  writeOut("PRODUCTION_SHARE_URL.local.txt", `${url}\n`);
  writeOut(
    "PRODUCTION_SHARE_ISSUE.json",
    `${JSON.stringify({ ...pack, shareUrl: url }, null, 2)}\n`
  );
  writeOut(
    "PRODUCTION_SHARE_PROOF_REDACTED.json",
    `${JSON.stringify(
      {
        ...pack,
        shareUrl: "[REDACTED — see PRODUCTION_SHARE_URL.local.txt / protected delivery]",
      },
      null,
      2
    )}\n`
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        tokenId: issued.tokenId,
        tokenSha12: sha12,
        shareUrl: url,
        latestPeriodId: man.latestPeriodId,
        revokedDevTokens: revoked.length,
        redactedTrackedBearerFiles: redactedPaths.length,
        adminAvailable: pack.adminResolveAdpAvailable,
        outDir: OUT_DIR,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
