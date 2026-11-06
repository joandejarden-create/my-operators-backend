/**
 * Canonical deployment sealing for ADP durable-publication packages.
 *
 * Fixes the defective seal that used a literal "\\n" join and invalid JSON suffix.
 */

import { createHash } from "crypto";
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";

export const ADP_DEPLOY_SEAL_V1 = "ADP_DEPLOY_SEAL_V1";

export function walkFilesCanonical(dir, base = dir, acc = []) {
  if (!existsSync(dir)) return acc;
  for (const ent of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, ent.name);
    if (ent.isDirectory()) walkFilesCanonical(p, base, acc);
    else {
      // Always forward slashes for reproducibility across OS.
      const rel = p.slice(base.length + 1).split(/[/\\]/).join("/");
      const buf = readFileSync(p);
      acc.push({
        path: rel,
        bytes: buf.length,
        sha256: createHash("sha256").update(buf).digest("hex"),
      });
    }
  }
  return acc;
}

export function inventorySha256(files) {
  const sorted = [...files].sort((a, b) => a.path.localeCompare(b.path));
  // Real newline separator — never a literal backslash-n.
  const body = sorted.map((f) => `${f.sha256}:${f.path}`).join("\n");
  return {
    sha256: createHash("sha256").update(body, "utf8").digest("hex"),
    canonicalBody: body,
    files: sorted,
  };
}

export function buildDeploymentManifest({
  schema = "ADP_DEPLOYMENT_MANIFEST_V1",
  note = "",
  files,
  forbiddenInArtifact = [
    "sealed.local.json",
    "SHARE_SIGNING_SECRET",
    ".env",
    "production-share-contract-tokens.sealed",
  ],
  extra = {},
} = {}) {
  const inv = inventorySha256(files);
  const manifest = {
    schema,
    seal: ADP_DEPLOY_SEAL_V1,
    generatedAt: new Date().toISOString(),
    note,
    forbiddenInArtifact,
    inventorySha256: inv.sha256,
    fileCount: inv.files.length,
    totalBytes: inv.files.reduce((s, f) => s + f.bytes, 0),
    files: inv.files,
    ...extra,
  };
  const body = `${JSON.stringify(manifest, null, 2)}\n`;
  // Validate JSON round-trip before sealing.
  JSON.parse(body);
  const manifestSha256 = createHash("sha256").update(body, "utf8").digest("hex");
  return { manifest, body, manifestSha256, inventorySha256: inv.sha256 };
}

export function writeSealedDeploymentPackage(outDir, deployStageDir, meta = {}) {
  mkdirSync(outDir, { recursive: true });
  const files = walkFilesCanonical(deployStageDir);
  const sealed = buildDeploymentManifest({
    note: meta.note || "Canonical ADP deploy seal v1",
    files,
    extra: meta.extra || {},
  });
  writeFileSync(join(outDir, "DEPLOYMENT_MANIFEST.json"), sealed.body);
  mkdirSync(join(outDir, "IMMUTABLE_ARTIFACT"), { recursive: true });
  writeFileSync(
    join(outDir, "IMMUTABLE_ARTIFACT/MANIFEST.json"),
    `${JSON.stringify(
      {
        artifactId: meta.artifactId || "adp_deploy_seal_v1",
        generatedAt: new Date().toISOString(),
        seal: ADP_DEPLOY_SEAL_V1,
        deploymentManifestSha256: sealed.manifestSha256,
        fileInventorySha256: sealed.inventorySha256,
        fileCount: sealed.manifest.fileCount,
        totalBytes: sealed.manifest.totalBytes,
        containsBearerSecrets: false,
      },
      null,
      2
    )}\n`
  );
  writeFileSync(
    join(outDir, "IMMUTABLE_ARTIFACT/FILE_INVENTORY.json"),
    `${JSON.stringify(sealed.manifest.files, null, 2)}\n`
  );
  return sealed;
}

export function verifySealedDeploymentPackage(outDir) {
  const manPath = join(outDir, "DEPLOYMENT_MANIFEST.json");
  const immPath = join(outDir, "IMMUTABLE_ARTIFACT/MANIFEST.json");
  if (!existsSync(manPath) || !existsSync(immPath)) {
    return { ok: false, code: "MISSING_SEAL_FILES" };
  }
  const manBuf = readFileSync(manPath);
  let manifest;
  try {
    manifest = JSON.parse(manBuf.toString("utf8"));
  } catch (err) {
    return { ok: false, code: "INVALID_MANIFEST_JSON", message: err.message };
  }
  const manSha = createHash("sha256").update(manBuf).digest("hex");
  const imm = JSON.parse(readFileSync(immPath, "utf8"));
  const recomputed = inventorySha256(manifest.files || []);
  const problems = [];
  if (manSha !== imm.deploymentManifestSha256) {
    problems.push("manifest_sha_mismatch");
  }
  if (recomputed.sha256 !== imm.fileInventorySha256) {
    problems.push("inventory_sha_mismatch");
  }
  if (manifest.inventorySha256 && manifest.inventorySha256 !== recomputed.sha256) {
    problems.push("manifest_embedded_inventory_mismatch");
  }
  // Reject the known-defective historical seal pattern
  const buggy = createHash("sha256")
    .update((manifest.files || []).map((f) => `${f.sha256}:${f.path}`).join("\\n"), "utf8")
    .digest("hex");
  if (imm.fileInventorySha256 === buggy && buggy !== recomputed.sha256) {
    problems.push("defective_literal_backslash_n_seal");
  }
  return {
    ok: problems.length === 0,
    problems,
    manifestSha256: manSha,
    inventorySha256: recomputed.sha256,
    immutable: imm,
  };
}
