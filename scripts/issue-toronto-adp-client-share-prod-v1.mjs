/**
 * Issue production-signed ADP client share for Radisson Blu Toronto Downtown.
 * Pulls ADP_SHARE_CAPABILITY_SECRET from Railway (in-memory only — never printed).
 * Seals URL into production-share-contract-tokens for Admin Open/Copy.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { issueShareCapability } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const HPC = "recH9h3McgMNFQEla";
const PROPERTY_NAME = "Radisson Blu Toronto Downtown";
const PUBLIC_BASE = "https://my-operators-backend-production.up.railway.app";
const TOKEN_ID = "sht_a07147cf287fc81fa9afca26"; // keep stable token id; re-sign with prod secret
const OUT_DIR = path.join(
  ROOT,
  "reports/adp/radisson-blu-toronto-customer-release-v1"
);

function loadRailwayVars() {
  const r = spawnSync("railway", ["variables", "--json"], {
    encoding: "utf8",
    cwd: ROOT,
    shell: true,
  });
  if (r.status !== 0) {
    throw new Error(`railway variables failed: ${r.stderr || r.stdout}`);
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
      !(t.product === "ADP" && String(t.propertyId || "") === PROPERTY_ID)
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

async function main() {
  const man = loadPublishedManifest(PROPERTY_ID);
  if (!man?.latestPeriodId) {
    throw new Error(`No published ADP snapshot for ${PROPERTY_ID}`);
  }

  const vars = loadRailwayVars();
  const secret = String(vars.ADP_SHARE_CAPABILITY_SECRET || "").trim();
  if (secret.length < 32) {
    throw new Error("Railway ADP_SHARE_CAPABILITY_SECRET missing/short");
  }
  process.env.ADP_SHARE_CAPABILITY_SECRET = secret;
  delete process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET;

  const issued = issueShareCapability({
    propertyId: PROPERTY_ID,
    tokenId: TOKEN_ID,
    label: `production-distribution:${PROPERTY_ID}:toronto-customer-release-v1`,
    reportScope: "current_published",
  });

  sealContractToken({ tokenId: issued.tokenId, token: issued.token });

  const url = `${PUBLIC_BASE}${issued.sharePath}`;
  const pack = {
    ok: true,
    propertyId: PROPERTY_ID,
    hotelCensusId: HPC,
    propertyName: PROPERTY_NAME,
    latestPeriodId: man.latestPeriodId,
    portfolioPeriodId: man.portfolioPeriodId || null,
    tokenId: issued.tokenId,
    tokenSha12: createHash("sha256").update(issued.token).digest("hex").slice(0, 12),
    publicBase: PUBLIC_BASE,
    shareUrl: url,
    sharePath: issued.sharePath,
    mintedAt: new Date().toISOString(),
    secretSource: "railway_production_env",
    note:
      "Production verify requires this tokenId ACTIVE in deployed adp-share-registry.",
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUT_DIR, "CUSTOMER_SHARE.json"),
    `${JSON.stringify(pack, null, 2)}\n`
  );
  fs.writeFileSync(path.join(OUT_DIR, "CUSTOMER_SHARE_URL.txt"), `${url}\n`);
  console.log(
    JSON.stringify(
      {
        ok: true,
        tokenId: pack.tokenId,
        shareUrl: pack.shareUrl,
        latestPeriodId: pack.latestPeriodId,
        portfolioPeriodId: pack.portfolioPeriodId,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
