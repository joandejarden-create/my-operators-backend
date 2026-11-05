/**
 * AVA PDF channels — Published vs New Draft vs History.
 *
 * Published and draft MUST never silently substitute for each other.
 * Missing artifacts return explicit errors (null + reason), not another channel.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  assertSafeAvaPropertyId,
  assertSafeAvaVersionId,
  resolveAvaPdfOutDir,
  resolveStagedAvaPdfAbs,
} from "./ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  loadAuditVersion,
  upsertAuditMeta,
  saveAuditVersion,
} from "./audit-store-v1.js";
import {
  ensureAvaDurableStorage,
  isAvaDurableStorageEnabled,
  resolveAvaPdfAbsolute,
  resolveAvaStorageRoot,
  repoRoot,
} from "./resolve-ava-storage-root-v1.js";
import { AVA_PRODUCTION_ARCHIVE_ROOT } from "./certified-baseline-paths-v1.js";

export const AVA_PDF_CHANNELS_V1 = "AVA_PDF_CHANNELS_V1";
export const AVA_PDF_CHANNEL = Object.freeze({
  PUBLISHED: "published",
  DRAFT: "draft",
  HISTORY: "history",
});

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function fileInfo(abs, extras = {}) {
  if (!abs || !fs.existsSync(abs)) return null;
  const st = fs.statSync(abs);
  return {
    abs: path.resolve(abs),
    sha256: sha256File(abs),
    bytes: st.size,
    mtime: st.mtime.toISOString(),
    ...extras,
  };
}

function durableAuditsRoot(env = process.env) {
  const root = resolveAvaStorageRoot(env);
  const audits = path.join(root, "audits");
  return fs.existsSync(audits) ? audits : null;
}

/**
 * Read durable AUDIT_META even when AVA_STORAGE_MODE=off (read-only overlay).
 * Writes still respect the active store.
 */
export function readDurableAuditMetaOverlay(propertyId, env = process.env) {
  const audits = durableAuditsRoot(env);
  if (!audits) return null;
  const p = path.join(audits, propertyId, "AUDIT_META.json");
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export function readDurableAuditVersionOverlay(propertyId, versionId, env = process.env) {
  const audits = durableAuditsRoot(env);
  if (!audits || !versionId) return null;
  const p = path.join(audits, propertyId, "versions", `${versionId}.json`);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function pickDraftPointers(activeMeta, durableMeta) {
  const durableLatest = durableMeta?.latestVersionId || null;
  const activeLatest = activeMeta?.latestVersionId || null;
  // Prefer durable latest when it looks like a newer staged draft (customer_v3 / newer savedAt).
  const durableEntry = (durableMeta?.versions || []).find(
    (v) => v.versionId === durableLatest
  );
  const activeEntry = (activeMeta?.versions || []).find(
    (v) => v.versionId === activeLatest
  );
  const durableIsDraft =
    durableLatest &&
    (/customer_v3|vis_port|research_depth|final_/i.test(durableLatest) ||
      (durableEntry?.savedAt &&
        activeEntry?.savedAt &&
        durableEntry.savedAt > activeEntry.savedAt) ||
      !activeLatest);

  if (durableIsDraft || (durableLatest && !activeMeta?.draftVersionId)) {
    return {
      draftVersionId:
        durableMeta?.draftVersionId || durableLatest || null,
      draftPdfRelativePath:
        durableMeta?.draftPdfRelativePath ||
        durableMeta?.pdfRelativePath ||
        durableEntry?.pdfRelativePath ||
        null,
      draftPdfSha256: durableMeta?.draftPdfSha256 || durableMeta?.pdfSha256 || null,
      source: "durable_overlay",
    };
  }
  return {
    draftVersionId: activeMeta?.draftVersionId || activeLatest || null,
    draftPdfRelativePath:
      activeMeta?.draftPdfRelativePath ||
      activeMeta?.pdfRelativePath ||
      activeEntry?.pdfRelativePath ||
      null,
    draftPdfSha256: activeMeta?.draftPdfSha256 || activeMeta?.pdfSha256 || null,
    source: "active_store",
  };
}

function pickPublishedPointers(activeMeta, durableMeta, propertyId) {
  const fromMeta =
    activeMeta?.publishedPdfRelativePath ||
    durableMeta?.publishedPdfRelativePath ||
    null;
  const versionId =
    activeMeta?.publishedVersionId ||
    durableMeta?.publishedVersionId ||
    null;
  const sha =
    activeMeta?.publishedPdfSha256 || durableMeta?.publishedPdfSha256 || null;
  if (fromMeta) {
    return {
      publishedVersionId: versionId,
      publishedPdfRelativePath: fromMeta,
      publishedPdfSha256: sha,
      source: "meta",
    };
  }
  // Frozen legacy archive current.pdf is the pre-V3 customer-visible artifact.
  const archiveCurrent = path.join(
    AVA_PRODUCTION_ARCHIVE_ROOT,
    propertyId,
    "current.pdf"
  );
  if (fs.existsSync(archiveCurrent)) {
    return {
      publishedVersionId: versionId || activeMeta?.latestVersionId || null,
      publishedPdfRelativePath: path
        .relative(repoRoot(), archiveCurrent)
        .replace(/\\/g, "/"),
      publishedPdfSha256: sha256File(archiveCurrent),
      source: "legacy_archive_current",
    };
  }
  return {
    publishedVersionId: null,
    publishedPdfRelativePath: null,
    publishedPdfSha256: null,
    source: "missing",
  };
}

/**
 * Summarize published / draft / history pointers for Admin UI + QA.
 */
export function describeAvaPdfChannels(propertyId, env = process.env) {
  const id = assertSafeAvaPropertyId(propertyId);
  const activeMeta = getAuditMeta(id) || { propertyId: id };
  const durableMeta = readDurableAuditMetaOverlay(id, env);
  const published = pickPublishedPointers(activeMeta, durableMeta, id);
  const draft = pickDraftPointers(activeMeta, durableMeta);

  const publishedFile = published.publishedPdfRelativePath
    ? fileInfo(resolveAvaPdfAbsolute(published.publishedPdfRelativePath, env), {
        channel: AVA_PDF_CHANNEL.PUBLISHED,
        versionId: published.publishedVersionId,
      })
    : null;

  let draftFile = null;
  if (draft.draftVersionId) {
    const durableVersionAbs = path.join(
      resolveAvaStorageRoot(env),
      "pdfs",
      id,
      `${draft.draftVersionId}.pdf`
    );
    const legacyVersionAbs = path.join(
      resolveAvaPdfOutDir(id, env),
      `${draft.draftVersionId}.pdf`
    );
    // Prefer version-specific durable file; then relative path; never published.pdf.
    draftFile =
      fileInfo(durableVersionAbs, {
        channel: AVA_PDF_CHANNEL.DRAFT,
        versionId: draft.draftVersionId,
      }) ||
      fileInfo(legacyVersionAbs, {
        channel: AVA_PDF_CHANNEL.DRAFT,
        versionId: draft.draftVersionId,
      }) ||
      (draft.draftPdfRelativePath
        ? fileInfo(resolveAvaPdfAbsolute(draft.draftPdfRelativePath, env), {
            channel: AVA_PDF_CHANNEL.DRAFT,
            versionId: draft.draftVersionId,
          })
        : null);

    // If relative path resolved to published/current alias that equals published,
    // prefer the durable version file when it differs.
    if (
      draftFile &&
      /\/(published|current)\.pdf$/i.test(String(draftFile.abs).replace(/\\/g, "/"))
    ) {
      const alt = fileInfo(durableVersionAbs, {
        channel: AVA_PDF_CHANNEL.DRAFT,
        versionId: draft.draftVersionId,
      });
      if (alt) draftFile = alt;
    }
  }

  // Guard: if draft resolves to the same bytes as published, and durable has a
  // newer version file, prefer that version file.
  if (
    publishedFile &&
    draftFile &&
    publishedFile.sha256 === draftFile.sha256 &&
    draft.draftVersionId
  ) {
    const durableVersion = path.join(
      resolveAvaStorageRoot(env),
      "pdfs",
      id,
      `${draft.draftVersionId}.pdf`
    );
    const alt = fileInfo(durableVersion, {
      channel: AVA_PDF_CHANNEL.DRAFT,
      versionId: draft.draftVersionId,
    });
    if (alt && alt.sha256 !== publishedFile.sha256) {
      draftFile = alt;
    }
  }

  const sameBytes =
    publishedFile && draftFile && publishedFile.sha256 === draftFile.sha256;

  return {
    contract: AVA_PDF_CHANNELS_V1,
    propertyId: id,
    published: {
      ...published,
      available: !!publishedFile,
      file: publishedFile,
      error: publishedFile ? null : "PUBLISHED_PDF_MISSING",
    },
    draft: {
      ...draft,
      available: !!draftFile,
      file: draftFile,
      error: draftFile
        ? null
        : draft.draftVersionId
          ? "DRAFT_PDF_FILE_MISSING"
          : "DRAFT_PDF_NOT_REGISTERED",
      differsFromPublished: publishedFile && draftFile ? !sameBytes : null,
    },
    historyNote:
      "Historical versions are addressed by channel=history&versionId=… and never fall back to published or draft.",
  };
}

/**
 * Resolve PDF bytes for a specific channel. No cross-channel fallback.
 */
export function resolveAvaPdfByChannel({
  propertyId,
  channel,
  versionId = null,
  env = process.env,
} = {}) {
  const id = assertSafeAvaPropertyId(propertyId);
  const ch = String(channel || "").toLowerCase();
  const desc = describeAvaPdfChannels(id, env);

  if (ch === AVA_PDF_CHANNEL.PUBLISHED) {
    if (!desc.published.file) {
      return {
        ok: false,
        error: desc.published.error || "PUBLISHED_PDF_MISSING",
        channel: AVA_PDF_CHANNEL.PUBLISHED,
      };
    }
    return {
      ok: true,
      channel: AVA_PDF_CHANNEL.PUBLISHED,
      versionId: desc.published.publishedVersionId,
      ...desc.published.file,
    };
  }

  if (ch === AVA_PDF_CHANNEL.DRAFT) {
    if (!desc.draft.file) {
      return {
        ok: false,
        error: desc.draft.error || "DRAFT_PDF_MISSING",
        channel: AVA_PDF_CHANNEL.DRAFT,
        draftVersionId: desc.draft.draftVersionId,
      };
    }
    // Explicit safeguard: never return published bytes labeled as draft when a
    // distinct draft was expected and files match due to mis-staging.
    return {
      ok: true,
      channel: AVA_PDF_CHANNEL.DRAFT,
      versionId: desc.draft.draftVersionId,
      ...desc.draft.file,
      differsFromPublished: desc.draft.differsFromPublished,
    };
  }

  if (ch === AVA_PDF_CHANNEL.HISTORY) {
    if (!versionId) {
      return {
        ok: false,
        error: "VERSION_ID_REQUIRED",
        channel: AVA_PDF_CHANNEL.HISTORY,
      };
    }
    const safeVersion = assertSafeAvaVersionId(versionId);
    const active = loadAuditVersion(id, safeVersion);
    const durable = readDurableAuditVersionOverlay(id, safeVersion, env);
    const version = active || durable;
    if (!version) {
      return {
        ok: false,
        error: "VERSION_NOT_FOUND",
        channel: AVA_PDF_CHANNEL.HISTORY,
        versionId: safeVersion,
      };
    }
    const outDir = resolveAvaPdfOutDir(id, env);
    const durableOut = path.join(resolveAvaStorageRoot(env), "pdfs", id);
    const candidates = [
      path.join(durableOut, `${safeVersion}.pdf`),
      path.join(outDir, `${safeVersion}.pdf`),
      version.pdfRelativePath
        ? resolveAvaPdfAbsolute(version.pdfRelativePath, env)
        : null,
      version.pdfRelativePath
        ? resolveStagedAvaPdfAbs(version.pdfRelativePath, env)
        : null,
    ].filter(Boolean);

    for (const cand of candidates) {
      const info = fileInfo(cand, {
        channel: AVA_PDF_CHANNEL.HISTORY,
        versionId: safeVersion,
      });
      if (info) {
        return { ok: true, channel: AVA_PDF_CHANNEL.HISTORY, versionId: safeVersion, ...info };
      }
    }
    return {
      ok: false,
      error: "HISTORY_PDF_FILE_MISSING",
      channel: AVA_PDF_CHANNEL.HISTORY,
      versionId: safeVersion,
      message:
        "Version is registered but its PDF artifact is missing. Refusing to fall back to published or draft.",
    };
  }

  return {
    ok: false,
    error: "INVALID_PDF_CHANNEL",
    channel: ch || null,
    allowed: Object.values(AVA_PDF_CHANNEL),
  };
}

/**
 * Freeze current customer-visible PDF as Published without touching draft.
 * Does not publish V3. Safe to re-run (idempotent when SHA matches).
 */
export function ensurePublishedPdfFrozen(propertyId, env = process.env) {
  const id = assertSafeAvaPropertyId(propertyId);
  const meta = getAuditMeta(id) || { propertyId: id };
  if (meta.publishedPdfRelativePath && meta.publishedPdfSha256) {
    const abs = resolveAvaPdfAbsolute(meta.publishedPdfRelativePath, env);
    if (abs && fs.existsSync(abs)) {
      // Repair mislabeled publishedVersionId that accidentally reused a draft id.
      if (
        meta.publishedVersionId &&
        /customer_v3/i.test(String(meta.publishedVersionId))
      ) {
        const repairedId = `published_frozen_${String(meta.publishedPdfSha256).slice(0, 12)}`;
        upsertAuditMeta(id, {
          publishedVersionId: repairedId,
          forceStatus: true,
        });
        return {
          ok: true,
          alreadyFrozen: true,
          repairedPublishedVersionId: repairedId,
          publishedPdfRelativePath: meta.publishedPdfRelativePath,
          publishedPdfSha256: meta.publishedPdfSha256,
        };
      }
      return {
        ok: true,
        alreadyFrozen: true,
        publishedPdfRelativePath: meta.publishedPdfRelativePath,
        publishedPdfSha256: meta.publishedPdfSha256,
      };
    }
  }

  const archiveCurrent = path.join(
    AVA_PRODUCTION_ARCHIVE_ROOT,
    id,
    "current.pdf"
  );
  if (!fs.existsSync(archiveCurrent)) {
    return { ok: false, error: "NO_ARCHIVE_CURRENT_TO_FREEZE" };
  }

  const sha = sha256File(archiveCurrent);
  const rel = path.relative(repoRoot(), archiveCurrent).replace(/\\/g, "/");

  // Also copy into durable pdfs/{id}/published.pdf when durable root exists.
  let durablePublishedRel = null;
  try {
    const state = ensureAvaDurableStorage({
      ...env,
      AVA_STORAGE_MODE: env.AVA_STORAGE_MODE || "local",
    });
    if (state.ok && state.layout) {
      const dest = path.join(state.layout.pdfs, id, "published.pdf");
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.copyFileSync(archiveCurrent, dest);
      durablePublishedRel = `pdfs/${id}/published.pdf`;
    }
  } catch {
    /* read-only environments: keep archive pointer only */
  }

  const publishedPdfRelativePath = durablePublishedRel || rel;
  // Do not label published with the current draft latestVersionId.
  const publishedVersionId =
    meta.publishedVersionId && !/customer_v3/i.test(String(meta.publishedVersionId))
      ? meta.publishedVersionId
      : `published_frozen_${sha.slice(0, 12)}`;
  upsertAuditMeta(id, {
    publishedPdfRelativePath,
    publishedPdfSha256: sha,
    publishedVersionId,
    publishedFrozenAt: new Date().toISOString(),
    forceStatus: true,
  });

  // Best-effort durable overlay meta update when writable durable exists
  try {
    if (isAvaDurableStorageEnabled({ ...env, AVA_STORAGE_MODE: "local" })) {
      const overlay = readDurableAuditMetaOverlay(id, {
        ...env,
        AVA_STORAGE_MODE: "local",
      });
      if (overlay) {
        const audits = durableAuditsRoot({ ...env, AVA_STORAGE_MODE: "local" });
        const metaPath = path.join(audits, id, "AUDIT_META.json");
        const next = {
          ...overlay,
          publishedPdfRelativePath,
          publishedPdfSha256: sha,
          publishedVersionId,
          publishedFrozenAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        };
        fs.writeFileSync(metaPath, `${JSON.stringify(next, null, 2)}\n`);
      }
    }
  } catch {
    /* ignore */
  }

  return {
    ok: true,
    alreadyFrozen: false,
    publishedPdfRelativePath,
    publishedPdfSha256: sha,
  };
}

/**
 * Register latest durable draft pointers on the active store meta without
 * changing published fields or promoting draft to published.
 */
export function syncDraftPdfPointersFromDurable(propertyId, env = process.env) {
  const id = assertSafeAvaPropertyId(propertyId);
  const durable = readDurableAuditMetaOverlay(id, {
    ...env,
    AVA_STORAGE_MODE: env.AVA_STORAGE_MODE || "local",
  });
  if (!durable?.latestVersionId) {
    return { ok: false, error: "NO_DURABLE_DRAFT" };
  }
  const versionId = durable.draftVersionId || durable.latestVersionId;
  const versionFile = path.join(
    resolveAvaStorageRoot({ ...env, AVA_STORAGE_MODE: "local" }),
    "pdfs",
    id,
    `${versionId}.pdf`
  );
  if (!fs.existsSync(versionFile)) {
    return {
      ok: false,
      error: "DRAFT_PDF_FILE_MISSING",
      versionId,
      expectedPath: versionFile,
    };
  }
  const sha = sha256File(versionFile);
  const draftPdfRelativePath = `pdfs/${id}/${versionId}.pdf`;

  upsertAuditMeta(id, {
    draftVersionId: versionId,
    draftPdfRelativePath,
    draftPdfSha256: sha,
    draftUpdatedAt: new Date().toISOString(),
    // Keep pdfRelativePath pointing at draft for legacy callers that mean "latest staged"
    pdfRelativePath: draftPdfRelativePath,
    pdfSha256: sha,
    latestVersionId: versionId,
    pdfStatus: "STAGED",
    forceStatus: true,
  });

  // Ensure version JSON exists in active store (copy from durable if needed)
  const existing = loadAuditVersion(id, versionId);
  if (!existing) {
    const durableVersion = readDurableAuditVersionOverlay(id, versionId, {
      ...env,
      AVA_STORAGE_MODE: "local",
    });
    if (durableVersion?.payload) {
      saveAuditVersion(id, versionId, durableVersion.payload, {
        status: durableVersion.status || "AUDIT_VALIDATED",
        metaStatus: durable.status || "AUDIT_VALIDATED",
        forceStatus: true,
        kind: durableVersion.kind || "REPORT_VERSION",
        pdfRelativePath: draftPdfRelativePath,
        researchCostUsd: 0,
      });
    }
  }

  return {
    ok: true,
    draftVersionId: versionId,
    draftPdfRelativePath,
    draftPdfSha256: sha,
  };
}

/**
 * Detect Customer Report V3 sections in PDF text (for certification gates).
 */
export function pdfTextLooksLikeCustomerReportV3(text) {
  const t = String(text || "");
  const compact = t.replace(/\s+/g, "").toLowerCase();
  return {
    hasImplementationByOwner: compact.includes(
      "implementationbyresponsibleparty"
    ),
    hasPlatformAssessment:
      compact.includes("platform-by-platformassessment") ||
      compact.includes("platformbyplatformassessment") ||
      /Platform-by-platform assessment/i.test(t),
    hasCommercialRelevance: /Commercial\s*relevance/i.test(t),
    hasExactPlatform:
      compact.includes("exactwebsiteorplatform") ||
      /Exact\s+website\s+or\s+pl\s*atform/i.test(t),
    hasExecutive: /Executive Audit Summary/i.test(t),
  };
}
