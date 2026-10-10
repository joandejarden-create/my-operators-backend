/**
 * Issue/seal production ADP client shares for:
 *   - AC Hotel A Coruña (mint if missing)
 *   - Radisson Santo Domingo (preserve existing ACTIVE token if present)
 *
 * Pulls ADP_SHARE_CAPABILITY_SECRET from Railway (never printed).
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
const PUBLIC_BASE = "https://my-operators-backend-production.up.railway.app";
const OUT_DIR = path.join(
  ROOT,
  "reports/adp/ac-coruna-radisson-santo-domingo-public-links"
);

const TARGETS = [
  {
    propertyId: "adp_ac_hotel_a_coruna",
    hpc: "rec2PVBDavppGpenm",
    propertyName: "AC Hotel A Coruña",
    preserveTokenId: null, // mint new if absent
  },
  {
    propertyId: "adp_radisson_santo_domingo",
    hpc: "recUOyzOXn2Zdp98I",
    propertyName: "Radisson Hotel Santo Domingo",
    preserveTokenId: "sht_9f9b35057ffc4bd879f9eee1",
    preservedShareUrl:
      "https://my-operators-backend-production.up.railway.app/owner-ai-demand-share.html?share=adpshare.v1.eyJ2IjoxLCJ0aWQiOiJzaHRfOWY5YjM1MDU3ZmZjNGJkODc5ZjllZWUxIiwicHJvcGVydHlJZCI6ImFkcF9yYWRpc3Nvbl9zYW50b19kb21pbmdvIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwcm9wZXJ0aWVzIiwicHVibGljYXRpb25fbWV0YSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzg4ODc3MTIzLCJleHAiOm51bGx9.ZL3O2WJeaHkyAbN_2wReAts2Pn3YURf9396X7BOgB98",
  },
];

function loadRailwayVars() {
  const railwayCwdCandidates = [
    process.env.RAILWAY_WORKING_DIR,
    path.join(path.dirname(ROOT), "deal-capture-proxy-or-share"),
    ROOT,
  ].filter(Boolean);
  let lastErr = "";
  for (const cwd of railwayCwdCandidates) {
    if (!fs.existsSync(cwd)) continue;
    const r = spawnSync("railway", ["variable", "list", "--json"], {
      encoding: "utf8",
      cwd,
      shell: true,
    });
    if (r.status === 0) {
      return JSON.parse(r.stdout);
    }
    lastErr = r.stderr || r.stdout || `status=${r.status}`;
  }
  throw new Error(`railway variable list failed: ${lastErr}`);
}

function readRegistry() {
  const p = path.join(
    ROOT,
    "config/client-share/adp-share-registry/active-tokens.json"
  );
  return { path: p, doc: JSON.parse(fs.readFileSync(p, "utf8")) };
}

function upsertCensusLink(propertyId, hpc, propertyName) {
  const p = path.join(ROOT, "fixtures/ai-demand-positioning/census-links-v1.json");
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  doc.links = doc.links || {};
  doc.links[propertyId] = {
    censusRecordId: hpc,
    propertyName,
    linkStatus: "linked",
    notes: "AC Coruña + Radisson SD public ADP link pass",
  };
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
}

function sealContractToken({ propertyId, hpc, propertyName, tokenId, token }) {
  const p = path.join(
    ROOT,
    "config/client-share/production-share-contract-tokens.json"
  );
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  doc.tokens = Array.isArray(doc.tokens) ? doc.tokens : [];
  doc.tokens = doc.tokens.filter(
    (t) => !(t.product === "ADP" && String(t.propertyId || "") === propertyId)
  );
  doc.tokens.push({
    product: "ADP",
    label: `${propertyName} — client share`,
    hotelId: hpc,
    propertyId,
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
  const vars = loadRailwayVars();
  const secret = String(vars.ADP_SHARE_CAPABILITY_SECRET || "").trim();
  if (secret.length < 32) {
    throw new Error("Railway ADP_SHARE_CAPABILITY_SECRET missing/short");
  }
  process.env.ADP_SHARE_CAPABILITY_SECRET = secret;
  delete process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET;

  const { path: regPath, doc: registry } = readRegistry();
  registry.tokens = registry.tokens || {};

  const results = [];

  for (const t of TARGETS) {
    const man = loadPublishedManifest(t.propertyId);
    if (!man?.latestPeriodId) {
      throw new Error(`No published ADP snapshot for ${t.propertyId}`);
    }

    let tokenId = t.preserveTokenId;
    let token = null;
    let shareUrl = t.preservedShareUrl || null;
    let action = "PRESERVED";

    const existing =
      tokenId && registry.tokens[tokenId] ? registry.tokens[tokenId] : null;

    if (existing && existing.status === "ACTIVE" && shareUrl) {
      token = shareUrl.includes("share=")
        ? shareUrl.split("share=")[1]
        : null;
      if (!token) {
        // Re-issue same tokenId with production secret so Admin seal has token
        const issued = issueShareCapability({
          propertyId: t.propertyId,
          tokenId,
          label: existing.label || `preserve:${t.propertyId}:prod`,
          reportScope: "current_published",
        });
        token = issued.token;
        shareUrl = `${PUBLIC_BASE}${issued.sharePath}`;
        action = "RESEALED_EXISTING_TOKEN";
      }
    } else {
      tokenId = `sht_${randomBytes(12).toString("hex")}`;
      const issued = issueShareCapability({
        propertyId: t.propertyId,
        tokenId,
        label: `client-share:${t.propertyId}:prod`,
        reportScope: "current_published",
      });
      token = issued.token;
      shareUrl = `${PUBLIC_BASE}${issued.sharePath}`;
      action = "MINTED";
    }

    upsertCensusLink(t.propertyId, t.hpc, t.propertyName);
    sealContractToken({
      propertyId: t.propertyId,
      hpc: t.hpc,
      propertyName: t.propertyName,
      tokenId,
      token,
    });

    const links = resolveExternalClientLinks(t.propertyId, {
      createIfMissing: false,
    });

    results.push({
      propertyId: t.propertyId,
      hotelCensusId: t.hpc,
      propertyName: t.propertyName,
      latestPeriodId: man.latestPeriodId,
      certificationStatus:
        man.certificationStatus || (man.certified ? "CERTIFIED" : null),
      publishStatus: man.publishStatus || null,
      demandCaptureRate: man.demandCaptureRate ?? null,
      tokenId,
      action,
      shareUrl,
      sharePath: shareUrl.replace(PUBLIC_BASE, ""),
      tokenSha12: createHash("sha256").update(token).digest("hex").slice(0, 12),
      adminResolvePresent: Boolean(
        links?.adpUrl || links?.adp?.url || links?.externalAdpUrl
      ),
      registryPath: regPath,
    });
  }

  writeOut(
    "SHARE_ISSUE_RESULT.json",
    `${JSON.stringify({ ok: true, issuedAt: new Date().toISOString(), results }, null, 2)}\n`
  );
  writeOut(
    "PUBLIC_LINKS.md",
    results
      .map(
        (r) =>
          `## ${r.propertyName}\n\n- subjectId: \`${r.propertyId}\`\n- period: \`${r.latestPeriodId}\`\n- tokenId: \`${r.tokenId}\`\n- action: ${r.action}\n- URL: ${r.shareUrl}\n`
      )
      .join("\n")
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        results: results.map((r) => ({
          propertyId: r.propertyId,
          tokenId: r.tokenId,
          action: r.action,
          latestPeriodId: r.latestPeriodId,
          shareUrl: r.shareUrl,
        })),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err?.stack || err);
  process.exit(1);
});
