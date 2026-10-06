/**
 * Issue production-signed ADP client share for Hilton Times Square.
 * Pulls ADP_SHARE_CAPABILITY_SECRET from Railway (in-memory only — never printed).
 * Seals URL into production-share-contract-tokens for Admin Open/Copy.
 */
import { spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { issueShareCapability } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveExternalClientLinks } from "../lib/admin/report-external-client-links-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_hilton_times_square";
const HPC = "rec35fExUxCClpOP6";
const PROPERTY_NAME = "Hilton New York Times Square";
const PUBLIC_BASE = "https://my-operators-backend-production.up.railway.app";
const TOKEN_ID = `sht_${randomBytes(12).toString("hex")}`;
const OUT_DIR = path.join(ROOT, "reports/hilton-adp-client-share");

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

function upsertCensusLink() {
  const p = path.join(ROOT, "fixtures/ai-demand-positioning/census-links-v1.json");
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  doc.links = doc.links || {};
  doc.links[PROPERTY_ID] = {
    censusRecordId: HPC,
    propertyName: PROPERTY_NAME,
    linkStatus: "linked",
    notes: "ADP_GDI_HPC_CANONICALIZE · Hilton New York Times Square client share",
  };
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
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

function writeOut(name, content) {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(path.join(OUT_DIR, name), content, "utf8");
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
  // Sign with production secret only for this process.
  process.env.ADP_SHARE_CAPABILITY_SECRET = secret;
  delete process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET;

  const issued = issueShareCapability({
    propertyId: PROPERTY_ID,
    tokenId: TOKEN_ID,
    label: `hilton-client-share:${PROPERTY_ID}:prod`,
    reportScope: "current_published",
  });

  upsertCensusLink();
  sealContractToken({ tokenId: issued.tokenId, token: issued.token });

  const url = `${PUBLIC_BASE}${issued.sharePath}`;
  const sha12 = createHash("sha256").update(issued.token).digest("hex").slice(0, 12);

  // Admin resolver should now hydrate from sealed contract (preferProductionShareCatalog).
  const links = resolveExternalClientLinks(PROPERTY_ID, { createIfMissing: false });
  const linksByHpc = resolveExternalClientLinks(HPC, { createIfMissing: false });

  const pack = {
    ok: true,
    propertyId: PROPERTY_ID,
    hotelCensusId: HPC,
    propertyName: PROPERTY_NAME,
    latestPeriodId: man.latestPeriodId,
    tokenId: issued.tokenId,
    tokenSha12: sha12,
    publicBase: PUBLIC_BASE,
    shareUrl: url,
    sharePath: issued.sharePath,
    adminResolveAdpAvailable: Boolean(links.adp?.available),
    adminResolveAdpUrlMatches: links.adp?.url === url,
    adminResolveViaHpcAdpAvailable: Boolean(linksByHpc.adp?.available),
    note:
      "Production verify requires this tokenId ACTIVE in deployed adp-share-registry. Deploy before client use if SHARE_UNKNOWN.",
    mintedAt: new Date().toISOString(),
    secretSource: "railway_production_env",
  };

  writeOut("HILTON_ADP_CLIENT_SHARE.json", `${JSON.stringify(pack, null, 2)}\n`);
  writeOut(
    "HILTON_ADP_CLIENT_URL.txt",
    `${url}\n`
  );
  writeOut(
    "FOUNDER_HANDOFF.md",
    `# Hilton ADP Client Share

**Hotel:** ${PROPERTY_NAME}  
**ADP subject:** \`${PROPERTY_ID}\`  
**HPC:** \`${HPC}\`  
**Token id:** \`${issued.tokenId}\`  
**Published period:** \`${man.latestPeriodId}\`

## Client URL

${url}

## Admin

Sealed into \`config/client-share/production-share-contract-tokens.json\` so local Admin ADP Client Open/Copy can hydrate without minting.

Census link added: \`${PROPERTY_ID}\` → \`${HPC}\`.

## Deploy note

Production share verify reads \`config/client-share/adp-share-registry/active-tokens.json\`.  
After mint, deploy so Railway has token \`${issued.tokenId}\` ACTIVE, then smoke the URL logged-out.
`
  );

  // Never print the secret. Print URL for founder.
  console.log(JSON.stringify({
    ok: true,
    tokenId: issued.tokenId,
    tokenSha12: sha12,
    shareUrl: url,
    adminAvailable: pack.adminResolveAdpAvailable,
    latestPeriodId: man.latestPeriodId,
    outDir: OUT_DIR,
  }, null, 2));
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
