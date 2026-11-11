/**
 * ADP_ADMIN_RUNTIME_IDENTITY_V1
 *
 * Every Admin surface must identify the exact code version and authoritative
 * data version it is serving, and fail readiness if incompatible.
 */

import { createHash } from "crypto";
import { execSync } from "child_process";
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  resolveAdpDurablePublishedMode,
  resolveAdpDurablePublishedRoot,
} from "../../durable-publication/resolve-durable-root-v1.js";
import {
  loadPublishedCurrentRegistry,
  resolvePublishedCurrentRegistryPath,
} from "../../published-current-registry-v1.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../../certification/adp-unprompted-independent-discovery-standard-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../../governance/adp-operational-registry-hygiene-v1.js";

export const ADP_ADMIN_RUNTIME_IDENTITY_V1 = "ADP_ADMIN_RUNTIME_IDENTITY_V1";
export const ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY = 38;
export const ADP_ADMIN_METHODOLOGY_V1_1_1 =
  CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID;
export const ADP_ADMIN_METHODOLOGY_FINGERPRINT_SHA256 =
  "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "../../../..");

function gitSha() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function sha256File(path) {
  if (!existsSync(path)) return null;
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

/**
 * Build runtime identity + readiness for Admin HTML/API.
 */
export function buildAdpAdminRuntimeIdentityV1(env = process.env, coverage = null) {
  const mode = resolveAdpDurablePublishedMode(env);
  const durableRoot = resolveAdpDurablePublishedRoot(env);
  const staticRegistryPath = resolvePublishedCurrentRegistryPath(env);
  const durableRegistryPath = join(
    durableRoot,
    "registry",
    "published-current-registry-v1.json"
  );
  const matrixPath = join(
    ROOT,
    "reports/dealality-adp-core-loyalty-execution-v1/FINAL_31_HOTEL_CERTIFICATION_MATRIX.json"
  );

  let registry = { properties: [] };
  try {
    registry = loadPublishedCurrentRegistry(env);
  } catch {
    registry = { properties: [] };
  }
  // Prefer durable registry file when present under prefer/required
  if (
    (mode === "prefer" || mode === "required") &&
    existsSync(durableRegistryPath)
  ) {
    try {
      registry = JSON.parse(readFileSync(durableRegistryPath, "utf8"));
    } catch {
      /* keep static */
    }
  }

  const properties = (registry.properties || []).filter(
    (p) => p?.propertyId && !isAdpSyntheticOrTestPropertyId(p.propertyId)
  );
  const inventoryCount = properties.length;
  const synthInRegistry = (registry.properties || []).filter((p) =>
    isAdpSyntheticOrTestPropertyId(p?.propertyId)
  );

  const coverageCount = coverage?.properties
    ? coverage.properties.filter(
        (p) => !isAdpSyntheticOrTestPropertyId(p.propertyId)
      ).length
    : null;

  const defects = [];
  if (mode !== "prefer" && mode !== "required") {
    defects.push({
      code: "DURABLE_AUTHORITY_BYPASSED",
      detail: `ADP_DURABLE_PUBLISHED_MODE=${mode || "off"} — Admin must use prefer|required for 38-hotel published-current`,
    });
  }
  if (!existsSync(durableRegistryPath) && mode === "prefer") {
    defects.push({
      code: "DURABLE_REGISTRY_MISSING",
      detail: durableRegistryPath,
    });
  }
  if (!existsSync(matrixPath)) {
    defects.push({
      code: "CANONICAL_31_MATRIX_MISSING",
      detail: matrixPath,
    });
  }
  if (inventoryCount !== ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY) {
    defects.push({
      code: "INVENTORY_COUNT_REGRESSION",
      detail: `expected=${ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY} actual=${inventoryCount}`,
    });
  }
  if (synthInRegistry.length) {
    defects.push({
      code: "SYNTHETIC_IN_PUBLISHED_REGISTRY",
      detail: synthInRegistry.map((p) => p.propertyId).join(","),
    });
  }
  if (
    coverageCount != null &&
    coverageCount !== ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY
  ) {
    defects.push({
      code: "ADMIN_COVERAGE_COUNT_REGRESSION",
      detail: `expected=${ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY} actual=${coverageCount}`,
    });
  }
  if (coverage?.properties) {
    const ready = coverage.properties.filter(
      (p) =>
        !isAdpSyntheticOrTestPropertyId(p.propertyId) &&
        p.coverageStatus === "READY"
    ).length;
    if (ready !== ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY) {
      defects.push({
        code: "ADMIN_READY_COUNT_REGRESSION",
        detail: `expected=${ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY} ready=${ready}`,
      });
    }
  }

  const sha = gitSha();
  return {
    contract: ADP_ADMIN_RUNTIME_IDENTITY_V1,
    ok: defects.length === 0,
    readiness: defects.length === 0 ? "READY" : "INCOMPATIBLE",
    defects,
    code: {
      gitSha: sha,
      gitShaShort: sha ? sha.slice(0, 12) : null,
      entrypointHtml: "/app/admin/ai-demand-admin.html",
      adminJs: "/js/admin-ai-demand-reviews.js",
      adminShellJs: "/js/admin-ai-demand-admin.js",
      apiCatalog: "/api/admin/ai-demand-positioning/monthly-reviews",
      apiIdentity: "/api/admin/ai-demand-positioning/admin-runtime-identity",
    },
    environment: {
      nodeEnv: env.NODE_ENV || null,
      port: env.PORT || null,
      durableMode: mode,
      durableRoot,
    },
    publicationAuthority: {
      mode,
      primary: "DURABLE_PUBLISHED_CURRENT_REGISTRY",
      durableRegistryPath,
      durableRegistrySha256: sha256File(durableRegistryPath),
      staticRegistryPath,
      staticRegistrySha256: sha256File(staticRegistryPath),
      registryUpdatedAt: registry.updatedAt || null,
    },
    methodology: {
      id: ADP_ADMIN_METHODOLOGY_V1_1_1,
      label: "Independent Unprompted V1.1.1",
      fingerprintSha256: ADP_ADMIN_METHODOLOGY_FINGERPRINT_SHA256,
    },
    inventory: {
      expected: ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY,
      registryCount: inventoryCount,
      coverageCount,
      syntheticExcluded: true,
      syntheticInRegistry: synthInRegistry.map((p) => p.propertyId),
    },
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Assert Admin readiness — throws or returns fail-closed result.
 */
export function assertAdpAdminRuntimeCompatibleV1(identity) {
  const id = identity || buildAdpAdminRuntimeIdentityV1();
  return {
    ok: id.ok === true,
    readiness: id.readiness,
    defects: id.defects || [],
    identity: id,
  };
}
