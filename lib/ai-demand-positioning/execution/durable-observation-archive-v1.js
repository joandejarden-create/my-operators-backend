/**
 * Durable ADP observation archive V1.
 * Every paid (or completed) monitoring period must write + readback-verify
 * original provider responses before measurement certification may advance.
 *
 * Index-only pointers are NOT sufficient — this writes a verified archive
 * of observation records (including rawResponse when present).
 */

import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";

export const DURABLE_OBSERVATION_ARCHIVE_V1 = "DURABLE_OBSERVATION_ARCHIVE_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
export const ARCHIVE_ROOT = path.join(
  REPO_ROOT,
  "data/ai-demand-positioning/observation-archive"
);

function sha(value) {
  return createHash("sha256")
    .update(typeof value === "string" ? value : JSON.stringify(value))
    .digest("hex");
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

/**
 * Normalize one observation into the durable archive shape.
 */
export function normalizeArchivedObservation(obs, ctx = {}) {
  const raw =
    obs?.rawResponse ??
    obs?.response ??
    obs?.rawText ??
    obs?.providerResponse ??
    null;
  const citationUrls = [
    ...(Array.isArray(obs?.sourcesCited) ? obs.sourcesCited : []),
    ...(Array.isArray(obs?.providerCitations) ? obs.providerCitations : []),
    ...(Array.isArray(obs?.citations) ? obs.citations : []),
  ]
    .map((c) => (typeof c === "string" ? c : c?.url || c?.href || null))
    .filter(Boolean);

  const body = {
    hotelId: ctx.hotelId || obs?.propertyId || null,
    propertyId: obs?.propertyId || ctx.propertyId || null,
    runId: ctx.runId || ctx.periodId || obs?.periodId || null,
    measurementPeriod: ctx.periodId || obs?.periodId || null,
    scenarioId: obs?.scenarioId || null,
    prompt: obs?.prompt || obs?.query || ctx.promptsByScenario?.[obs?.scenarioId] || null,
    provider: obs?.provider || null,
    model: obs?.model || null,
    originalResponse: raw,
    timestamp: obs?.timestamp || null,
    citationUrls,
    providerJobProvenance: {
      provider: obs?.provider || null,
      model: obs?.model || null,
      jobId: obs?.jobId || obs?.providerJobId || null,
      error: obs?.error || null,
    },
    observationHash: null,
    processingState: obs?.parsed ? "PARSED" : obs?.error ? "ERROR" : "CAPTURED",
    certificationState: ctx.certificationState || "UNCERTIFIED",
    observationId: obs?.observationId || null,
  };
  body.observationHash = sha({
    observationId: body.observationId,
    scenarioId: body.scenarioId,
    provider: body.provider,
    originalResponse: body.originalResponse,
    timestamp: body.timestamp,
  }).slice(0, 32);
  return body;
}

export function archiveDirForPeriod(propertyId, periodId) {
  return path.join(ARCHIVE_ROOT, propertyId, periodId);
}

/**
 * Pre-archive corpus validation (fail-closed before write).
 */
export function validatePeriodCorpusForArchive(period, options = {}) {
  const observations = Array.isArray(period?.observations) ? period.observations : [];
  const issues = [];
  if (!observations.length) {
    return { ok: false, code: "EMPTY_OBSERVATIONS", issues: ["no observations"] };
  }

  const requireOriginal =
    options.requireOriginalResponse === true ||
    period?.status === "EXECUTION_COMPLETE" ||
    period?.officialPeriod === true;

  const providers = new Set();
  const scenarios = new Set();
  const keys = new Set();
  let duplicates = 0;
  let missingProvider = 0;
  let missingScenario = 0;
  let interrupted = 0;
  let missingOriginal = 0;

  for (const o of observations) {
    if (!o?.provider) missingProvider += 1;
    else providers.add(String(o.provider).toLowerCase());
    if (!o?.scenarioId) missingScenario += 1;
    else scenarios.add(o.scenarioId);
    const key = `${o?.scenarioId || ""}::${o?.provider || ""}`;
    if (keys.has(key) && o?.scenarioId && o?.provider) duplicates += 1;
    keys.add(key);
    if (o?.error && !o?.rawResponse && !o?.response) interrupted += 1;
    const raw = o?.rawResponse ?? o?.response ?? o?.rawText ?? null;
    if (requireOriginal && (raw == null || raw === "")) missingOriginal += 1;
  }

  if (missingProvider) {
    issues.push(`MISSING_PROVIDER:${missingProvider}`);
  }
  if (missingScenario) {
    issues.push(`MISSING_SCENARIO:${missingScenario}`);
  }
  if (duplicates) {
    issues.push(`DUPLICATE_OBSERVATION_KEYS:${duplicates}`);
  }
  if (interrupted) {
    issues.push(`INTERRUPTED_PROVIDER_RESPONSE:${interrupted}`);
  }
  if (requireOriginal && missingOriginal) {
    issues.push(`MISSING_ORIGINAL_RESPONSE:${missingOriginal}`);
  }

  const expectedProviders = options.expectedProviders || null;
  const expectedScenarios = options.expectedScenarios || null;
  if (Array.isArray(expectedProviders) && expectedProviders.length) {
    for (const p of expectedProviders) {
      if (!providers.has(String(p).toLowerCase())) {
        issues.push(`MISSING_EXPECTED_PROVIDER:${p}`);
      }
    }
  }
  if (Array.isArray(expectedScenarios) && expectedScenarios.length) {
    for (const s of expectedScenarios) {
      if (!scenarios.has(s)) issues.push(`MISSING_EXPECTED_SCENARIO:${s}`);
    }
  }

  return {
    ok: issues.length === 0,
    code: issues.length ? "CORPUS_VALIDATION_FAILED" : "CORPUS_OK",
    issues,
    providers: [...providers].sort(),
    scenarios: [...scenarios].sort(),
    observationCount: observations.length,
    duplicates,
    interrupted,
    missingOriginal,
  };
}

/**
 * Write durable archive for a period. Returns manifest + readback result.
 */
export function writeDurableObservationArchive(period, options = {}) {
  const propertyId = String(period?.propertyId || "").trim();
  const periodId = String(period?.periodId || "").trim();
  if (!propertyId || !periodId) {
    return {
      ok: false,
      error: "MISSING_PROPERTY_OR_PERIOD_ID",
    };
  }
  const observations = Array.isArray(period.observations) ? period.observations : [];
  if (observations.length === 0) {
    return {
      ok: false,
      error: "EMPTY_OBSERVATIONS",
      propertyId,
      periodId,
    };
  }

  if (options.simulateWriteFailure === true) {
    return {
      ok: false,
      error: "SIMULATED_ARCHIVE_WRITE_FAILURE",
      propertyId,
      periodId,
    };
  }

  if (options.validateCorpus !== false) {
    const validation = validatePeriodCorpusForArchive(period, options);
    if (!validation.ok && options.failClosedOnCorpusIssues !== false) {
      // Soft issues (duplicates) block certification; interrupted without raw blocks
      const hard = validation.issues.some((i) =>
        /MISSING_PROVIDER|MISSING_SCENARIO|MISSING_ORIGINAL|INTERRUPTED|MISSING_EXPECTED/.test(
          i
        )
      );
      if (hard) {
        return {
          ok: false,
          error: validation.code,
          validation,
          propertyId,
          periodId,
        };
      }
    }
  }

  const dir = archiveDirForPeriod(propertyId, periodId);
  const obsDir = path.join(dir, "observations");
  ensureDir(obsDir);

  const archived = observations.map((o) =>
    normalizeArchivedObservation(o, {
      propertyId,
      periodId,
      runId: period.runId || periodId,
      hotelId: options.hotelId || propertyId,
      promptsByScenario: options.promptsByScenario || null,
      certificationState: period.certificationStatus || "UNCERTIFIED",
    })
  );

  for (const row of archived) {
    const file = path.join(
      obsDir,
      `${row.observationId || row.observationHash}.json`
    );
    fs.writeFileSync(file, JSON.stringify(row, null, 2) + "\n", "utf8");
  }

  const corpusHash = sha(
    archived.map((a) => a.observationHash).sort().join("|")
  ).slice(0, 32);

  const manifest = {
    contractVersion: DURABLE_OBSERVATION_ARCHIVE_V1,
    propertyId,
    periodId,
    runId: period.runId || periodId,
    writtenAt: new Date().toISOString(),
    expectedObservationCount: observations.length,
    archivedObservationCount: archived.length,
    providers: [...new Set(archived.map((a) => a.provider).filter(Boolean))].sort(),
    scenarioIds: [...new Set(archived.map((a) => a.scenarioId).filter(Boolean))].sort(),
    corpusHash,
    withOriginalResponse: archived.filter((a) => a.originalResponse != null && a.originalResponse !== "").length,
    archiveRelative: path.relative(REPO_ROOT, dir).replace(/\\/g, "/"),
  };

  fs.writeFileSync(
    path.join(dir, "manifest.json"),
    JSON.stringify(manifest, null, 2) + "\n",
    "utf8"
  );

  // Also keep a compact period pointer alongside runtime SoT
  fs.writeFileSync(
    path.join(dir, "period-pointer.json"),
    JSON.stringify(
      {
        propertyId,
        periodId,
        runtimeRelative: `data/ai-demand-positioning/runtime/${periodId}.json`,
        corpusHash,
      },
      null,
      2
    ) + "\n",
    "utf8"
  );

  const readback = readbackDurableObservationArchive(propertyId, periodId);
  const ok =
    readback.ok &&
    readback.actualCount === manifest.expectedObservationCount &&
    readback.corpusHash === corpusHash;

  return {
    ok,
    propertyId,
    periodId,
    manifest,
    readback,
    error: ok ? null : "READBACK_MISMATCH",
  };
}

/**
 * Read back archive and reconcile counts/hashes.
 */
export function readbackDurableObservationArchive(propertyId, periodId) {
  const dir = archiveDirForPeriod(propertyId, periodId);
  const manifestPath = path.join(dir, "manifest.json");
  if (!fs.existsSync(manifestPath)) {
    return {
      ok: false,
      error: "MANIFEST_MISSING",
      propertyId,
      periodId,
      actualCount: 0,
    };
  }
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const obsDir = path.join(dir, "observations");
  const files = fs.existsSync(obsDir)
    ? fs.readdirSync(obsDir).filter((f) => f.endsWith(".json"))
    : [];
  const rows = [];
  for (const f of files) {
    try {
      rows.push(JSON.parse(fs.readFileSync(path.join(obsDir, f), "utf8")));
    } catch {
      /* skip corrupt */
    }
  }
  const corpusHash = sha(
    rows.map((a) => a.observationHash).filter(Boolean).sort().join("|")
  ).slice(0, 32);

  const countMatch = rows.length === Number(manifest.expectedObservationCount);
  const hashMatch = corpusHash === manifest.corpusHash;
  return {
    ok: countMatch && hashMatch && rows.length > 0,
    propertyId,
    periodId,
    expectedCount: manifest.expectedObservationCount,
    actualCount: rows.length,
    corpusHash,
    manifestCorpusHash: manifest.corpusHash,
    countMatch,
    hashMatch,
    withOriginalResponse: rows.filter(
      (a) => a.originalResponse != null && a.originalResponse !== ""
    ).length,
    readAt: new Date().toISOString(),
  };
}

/**
 * Fail-closed gate: period may not certify without durable archive.
 */
export function assertDurableArchiveOrBlock(period, options = {}) {
  const propertyId = period?.propertyId;
  const periodId = period?.periodId;
  const obsCount = Array.isArray(period?.observations)
    ? period.observations.length
    : 0;
  if (!propertyId || !periodId || obsCount === 0) {
    return {
      ok: false,
      state: "BLOCKED_EVIDENCE",
      code: "NO_OBSERVATIONS_TO_ARCHIVE",
      detail: "Measurement certification requires a non-empty observation corpus",
    };
  }

  const requireOriginal =
    options.requireOriginalResponse === true ||
    period?.status === "EXECUTION_COMPLETE" ||
    period?.officialPeriod === true;
  const validation = validatePeriodCorpusForArchive(period, {
    requireOriginalResponse: requireOriginal,
    expectedProviders: options.expectedProviders,
    expectedScenarios: options.expectedScenarios,
  });
  if (!validation.ok) {
    const hard = validation.issues.some((i) =>
      /MISSING_PROVIDER|MISSING_SCENARIO|MISSING_ORIGINAL|INTERRUPTED|MISSING_EXPECTED/.test(
        i
      )
    );
    if (hard) {
      return {
        ok: false,
        state: "BLOCKED_EVIDENCE",
        code: "CORPUS_VALIDATION_FAILED",
        detail: validation.issues.join("; "),
        validation,
      };
    }
  }

  const rb = readbackDurableObservationArchive(propertyId, periodId);
  if (!rb.ok) {
    // Attempt write then re-read
    const wrote = writeDurableObservationArchive(period, {
      ...options,
      failClosedOnCorpusIssues: true,
    });
    if (!wrote.ok) {
      return {
        ok: false,
        state: "BLOCKED_TECHNICAL",
        code: "DURABLE_ARCHIVE_REQUIRED",
        detail:
          "Completed provider job without verified durable observation archive — cannot advance to measurement certification",
        writeError: wrote.error,
        readback: wrote.readback || rb,
        validation: wrote.validation || validation,
      };
    }
    return {
      ok: true,
      state: "ARCHIVED",
      code: "ARCHIVE_WRITTEN_AND_VERIFIED",
      manifest: wrote.manifest,
      readback: wrote.readback,
    };
  }
  // Readback OK but reconcile observation count vs live period
  if (rb.actualCount !== obsCount) {
    return {
      ok: false,
      state: "BLOCKED_TECHNICAL",
      code: "ARCHIVE_PERIOD_COUNT_MISMATCH",
      detail: `Archive has ${rb.actualCount} obs; period has ${obsCount}`,
      readback: rb,
    };
  }
  return {
    ok: true,
    state: "ARCHIVED",
    code: "ARCHIVE_READBACK_OK",
    readback: rb,
  };
}

/**
 * Inventory all durable archives on disk (hotel × period coverage).
 */
export function inventoryDurableArchiveCoverage() {
  if (!fs.existsSync(ARCHIVE_ROOT)) {
    return {
      hotels: [],
      hotelCount: 0,
      periodCount: 0,
      successfulReadbacks: 0,
      failedReadbacks: 0,
      withOriginalResponseTotal: 0,
    };
  }
  const hotels = [];
  let successfulReadbacks = 0;
  let failedReadbacks = 0;
  let withOriginalResponseTotal = 0;
  let periodCount = 0;
  for (const propertyId of fs.readdirSync(ARCHIVE_ROOT)) {
    const hotelDir = path.join(ARCHIVE_ROOT, propertyId);
    if (!fs.statSync(hotelDir).isDirectory()) continue;
    const periods = [];
    for (const periodId of fs.readdirSync(hotelDir)) {
      const pDir = path.join(hotelDir, periodId);
      if (!fs.statSync(pDir).isDirectory()) continue;
      periodCount += 1;
      const rb = readbackDurableObservationArchive(propertyId, periodId);
      if (rb.ok) {
        successfulReadbacks += 1;
        withOriginalResponseTotal += rb.withOriginalResponse || 0;
      } else {
        failedReadbacks += 1;
      }
      periods.push({
        periodId,
        ok: rb.ok,
        expectedCount: rb.expectedCount ?? null,
        actualCount: rb.actualCount ?? 0,
        withOriginalResponse: rb.withOriginalResponse ?? 0,
        corpusHash: rb.corpusHash || null,
        error: rb.error || null,
      });
    }
    hotels.push({
      propertyId,
      periodCount: periods.length,
      periods,
      allOk: periods.every((p) => p.ok),
    });
  }
  return {
    hotels,
    hotelCount: hotels.length,
    periodCount,
    successfulReadbacks,
    failedReadbacks,
    withOriginalResponseTotal,
    inventoriedAt: new Date().toISOString(),
  };
}

/**
 * Archive all COMPLETE_RAW_CORPUS runtime periods for a property (idempotent).
 */
export function archiveExistingRuntimePeriodsForProperty(propertyId, runtimeDir) {
  const dir =
    runtimeDir ||
    path.join(REPO_ROOT, "data/ai-demand-positioning/runtime");
  if (!fs.existsSync(dir)) return [];
  const out = [];
  for (const f of fs.readdirSync(dir)) {
    if (!f.includes(propertyId) || !f.endsWith(".json")) continue;
    try {
      const period = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
      if (!period?.observations?.length) continue;
      out.push(writeDurableObservationArchive(period));
    } catch (err) {
      out.push({ ok: false, file: f, error: err.message });
    }
  }
  return out;
}
