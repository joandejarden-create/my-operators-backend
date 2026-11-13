/**
 * Durable filesystem store for GDI Customer Publication Packages.
 *
 * Layout:
 *   {root}/{hotelId}/{publicationVersion}/package.json
 *   {root}/{hotelId}/CURRENT.json   — pointer when CUSTOMER_PUBLISHED
 *   {root}/{hotelId}/STAGED.json    — pointer when PRODUCTION_STAGED
 *
 * Atomic writes. No silent Airtable fallback when package is required.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CUSTOMER_PUBLICATION_PACKAGE_V1,
  verifyPublicationPackageIntegrity,
} from "./gdi-customer-publication-package-v1.js";
import {
  GDI_PUBLICATION_LIFECYCLE_STATE,
  isCustomerVisibleLifecycleState,
} from "./gdi-publication-lifecycle-v1.js";

export const GDI_PUBLICATION_PACKAGE_STORE_V1 =
  "gdi-publication-package-store-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v == null ? "" : v).trim();
}

function atomicWriteJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  try {
    fs.renameSync(tmp, filePath);
  } catch {
    fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
    try {
      fs.unlinkSync(tmp);
    } catch {
      /* best-effort */
    }
  }
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

/**
 * Prefer durable data/ store; fall back to deploy-bundled lib copy when
 * Railway lean uploads omit data/ trees (same pattern as Bethesda reconstruction bag).
 */
export function resolvePublicationPackageRoot(env = process.env, options = {}) {
  if (options.root) return path.resolve(options.root);
  const explicit = text(env.GDI_PUBLICATION_PACKAGE_ROOT);
  if (explicit) return path.resolve(explicit);

  const dataRoot = path.join(
    REPO_ROOT,
    "data/group-demand-intelligence/publication-packages",
  );
  const bundledRoot = path.join(
    REPO_ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  );

  // Prefer a root that already has at least one hotel CURRENT pointer.
  const dataHasCurrent = (() => {
    try {
      if (!fs.existsSync(dataRoot)) return false;
      for (const name of fs.readdirSync(dataRoot)) {
        if (fs.existsSync(path.join(dataRoot, name, "CURRENT.json"))) return true;
      }
      return false;
    } catch {
      return false;
    }
  })();
  if (dataHasCurrent) return dataRoot;
  if (fs.existsSync(bundledRoot)) return bundledRoot;
  return dataRoot;
}

export function createGdiPublicationPackageStore(options = {}) {
  const env = options.env || process.env;
  const root = resolvePublicationPackageRoot(env, options);

  function hotelDir(hotelId) {
    return path.join(root, text(hotelId));
  }

  function versionDir(hotelId, publicationVersion) {
    return path.join(hotelDir(hotelId), text(publicationVersion));
  }

  function packagePath(hotelId, publicationVersion) {
    return path.join(versionDir(hotelId, publicationVersion), "package.json");
  }

  function writePackage(pkg) {
    const integrity = verifyPublicationPackageIntegrity(pkg);
    if (!integrity.ok) {
      return {
        ok: false,
        error: "PACKAGE_INTEGRITY_FAILED",
        fails: integrity.fails,
      };
    }
    const hotelId = text(pkg.hotelId);
    const publicationVersion = text(pkg.publicationVersion);
    const file = packagePath(hotelId, publicationVersion);
    atomicWriteJson(file, pkg);
    return {
      ok: true,
      path: file,
      publicationId: pkg.publicationId,
      publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
    };
  }

  function loadPackage(hotelId, publicationVersion) {
    const file = packagePath(hotelId, publicationVersion);
    const pkg = readJson(file);
    if (!pkg) return { ok: false, error: "PACKAGE_NOT_FOUND", path: file };
    const integrity = verifyPublicationPackageIntegrity(pkg);
    if (!integrity.ok) {
      return {
        ok: false,
        error: "PACKAGE_INTEGRITY_FAILED",
        fails: integrity.fails,
        path: file,
      };
    }
    return { ok: true, package: pkg, path: file };
  }

  function writePointer(hotelId, kind, pointer) {
    const name = kind === "CURRENT" ? "CURRENT.json" : "STAGED.json";
    const file = path.join(hotelDir(hotelId), name);
    atomicWriteJson(file, {
      schema: GDI_PUBLICATION_PACKAGE_STORE_V1,
      hotelId: text(hotelId),
      kind,
      updatedAt: new Date().toISOString(),
      ...pointer,
    });
    return file;
  }

  function readPointer(hotelId, kind) {
    const name = kind === "CURRENT" ? "CURRENT.json" : "STAGED.json";
    return readJson(path.join(hotelDir(hotelId), name));
  }

  function stageForProduction(pkg) {
    const written = writePackage({
      ...pkg,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
    });
    if (!written.ok) return written;
    writePointer(pkg.hotelId, "STAGED", {
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
    });
    return { ...written, staged: true };
  }

  /**
   * Activate customer-visible pointer. Requires FOUNDER_APPROVED+ fingerprint match.
   * Does not delete prior packages (rollback via pointer swap).
   */
  function publishCustomerVisible(pkg, opts = {}) {
    if (opts.requireFounderApproval !== false) {
      if (pkg.approvalStatus !== "FOUNDER_APPROVED" && !opts.force) {
        return {
          ok: false,
          error: "FOUNDER_APPROVAL_REQUIRED",
          approvalStatus: pkg.approvalStatus || null,
        };
      }
    }
    const written = writePackage({
      ...pkg,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
      approvedAt: pkg.approvedAt || new Date().toISOString(),
      approvalStatus: "FOUNDER_APPROVED",
    });
    if (!written.ok) return written;
    writePointer(pkg.hotelId, "CURRENT", {
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
    });
    return { ...written, published: true };
  }

  /**
   * Withhold / rollback customer visibility without deleting feedback or package bytes.
   */
  function withholdCustomerPublication(hotelId, reason = "WITHHELD") {
    const prior = readPointer(hotelId, "CURRENT");
    writePointer(hotelId, "CURRENT", {
      publicationId: prior?.publicationId || null,
      publicationVersion: prior?.publicationVersion || null,
      contentFingerprintSha256: prior?.contentFingerprintSha256 || null,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.WITHHELD,
      withheld: true,
      withholdReason: text(reason) || "WITHHELD",
    });
    return { ok: true, withheld: true, prior };
  }

  function loadCustomerVisiblePackage(hotelId) {
    const pointer = readPointer(hotelId, "CURRENT");
    if (!pointer || pointer.withheld === true) {
      return {
        ok: false,
        error: pointer?.withheld ? "PUBLICATION_WITHHELD" : "NO_CURRENT_PUBLICATION",
        pointer,
      };
    }
    if (!isCustomerVisibleLifecycleState(pointer.lifecycleState)) {
      return {
        ok: false,
        error: "CURRENT_NOT_CUSTOMER_PUBLISHED",
        pointer,
      };
    }
    const loaded = loadPackage(hotelId, pointer.publicationVersion);
    if (!loaded.ok) return loaded;
    if (
      text(loaded.package.contentFingerprintSha256).toLowerCase() !==
      text(pointer.contentFingerprintSha256).toLowerCase()
    ) {
      return {
        ok: false,
        error: "POINTER_FINGERPRINT_MISMATCH",
        pointer,
        packageFingerprint: loaded.package.contentFingerprintSha256,
      };
    }
    return { ok: true, package: loaded.package, pointer, path: loaded.path };
  }

  function listVersions(hotelId) {
    const dir = hotelDir(hotelId);
    if (!fs.existsSync(dir)) return [];
    return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => d.name)
      .sort();
  }

  return {
    version: GDI_PUBLICATION_PACKAGE_STORE_V1,
    packageSchema: GDI_CUSTOMER_PUBLICATION_PACKAGE_V1,
    root,
    writePackage,
    loadPackage,
    stageForProduction,
    publishCustomerVisible,
    withholdCustomerPublication,
    loadCustomerVisiblePackage,
    readPointer,
    writePointer,
    listVersions,
  };
}
