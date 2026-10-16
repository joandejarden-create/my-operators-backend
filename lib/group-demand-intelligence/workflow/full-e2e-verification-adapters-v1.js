/**
 * Full E2E local/production verification adapters.
 *
 * Delivery proof requires: valid schema, hotel identity, publication reconcile,
 * list/detail parity, no provider-metadata leaks, and observed runtime identity
 * when an intended release is asserted. HTTP 200, empty JSON fallback, asset
 * presence, or a caller-supplied SHA alone must not prove delivery.
 *
 * Budget reconcile reads authorization identity from the ledger first, then
 * compares to the requested authorization. Never mint allowance from CLI args.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

function text(v) {
  return String(v || "").trim();
}

function finiteNumber(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

const PROVIDER_LEAK_RE =
  /\btrun_[a-z0-9]+|enrichmentId|creditsConsumed|parallel\.ai|surfe[_-]?enrich|webhound|providerRunId\b/i;

const BUILD_HEADER_KEYS = [
  "x-dealality-build-sha",
  "x-dealality-git-sha",
  "x-railway-git-commit",
  "x-git-commit",
];

/**
 * Strict JSON parse — invalid body must not become {}.
 */
export function parseJsonBodyStrict(rawText) {
  const raw = String(rawText ?? "");
  if (!raw.trim()) {
    return { ok: false, reason: "empty_body", body: null };
  }
  try {
    const body = JSON.parse(raw);
    if (body === null || typeof body !== "object" || Array.isArray(body)) {
      return { ok: false, reason: "non_object_json", body: null };
    }
    return { ok: true, reason: null, body };
  } catch (err) {
    return {
      ok: false,
      reason: "invalid_json",
      body: null,
      error: String(err?.message || err),
    };
  }
}

export function detectProviderMetadataLeaks(value) {
  const blob = typeof value === "string" ? value : JSON.stringify(value ?? "");
  const match = blob.match(PROVIDER_LEAK_RE);
  return {
    leaked: Boolean(match),
    match: match ? match[0] : null,
  };
}

export function normalizePublicationManifest(input = null) {
  if (!input) return null;
  if (Array.isArray(input)) {
    return {
      hotelId: null,
      runId: null,
      publishedIds: input.map((id) => text(id)).filter(Boolean),
      legacyPreservedIds: [],
    };
  }
  const publishedIds = (
    input.publishedIds ||
    input.expectedPublishedIds ||
    input.opportunityIds ||
    []
  )
    .map((id) => text(id))
    .filter(Boolean);
  const legacyPreservedIds = (input.legacyPreservedIds || input.legacyIds || [])
    .map((id) => text(id))
    .filter(Boolean);
  return {
    hotelId: text(input.hotelId) || null,
    runId: text(input.runId) || null,
    publishedIds: [...new Set(publishedIds)],
    legacyPreservedIds: [...new Set(legacyPreservedIds)],
  };
}

export function loadPublicationManifestFromRun(runDir) {
  if (!runDir) return null;
  const candidates = [
    path.join(runDir, "full_e2e", "publication-manifest.json"),
    path.join(runDir, "publication-manifest.json"),
    path.join(runDir, "full_e2e", "apply-publish-result.json"),
  ];
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    try {
      const doc = JSON.parse(fs.readFileSync(p, "utf8"));
      const normalized = normalizePublicationManifest(
        doc.publicationManifest || doc.manifest || doc
      );
      if (normalized?.publishedIds?.length) {
        return { ...normalized, sourcePath: p };
      }
      const fromResult = [
        ...(doc.createdIds || []),
        ...(doc.updatedIds || []),
        ...(doc.publishedIds || []),
        ...(doc.opportunityIds || []),
      ]
        .map((id) => text(id))
        .filter(Boolean);
      if (fromResult.length) {
        return {
          hotelId: text(doc.hotelId) || null,
          runId: text(doc.runId) || null,
          publishedIds: [...new Set(fromResult)],
          legacyPreservedIds: (doc.legacyPreservedIds || [])
            .map((id) => text(id))
            .filter(Boolean),
          sourcePath: p,
        };
      }
    } catch {
      /* try next */
    }
  }
  return null;
}

export function computeOpportunityListFingerprint(body = {}) {
  const ids = (body.opportunities || [])
    .map((o) => text(o?.id))
    .filter(Boolean)
    .sort();
  const material = [
    text(body.hotelId),
    text(body.updatedAt),
    text(body.runId),
    String(body.count ?? ids.length),
    ids.join(","),
  ].join("|");
  return createHash("sha256").update(material).digest("hex").slice(0, 24);
}

/**
 * Probe supported runtime identity sources (never trust caller-supplied SHA alone).
 */
export async function observeRuntimeBuildIdentity({
  baseUrl,
  fetchImpl = globalThis.fetch,
  listHeaders = null,
} = {}) {
  const observed = {
    buildSha: null,
    source: null,
    probes: [],
  };

  if (listHeaders && typeof listHeaders.get === "function") {
    for (const key of BUILD_HEADER_KEYS) {
      const v = text(listHeaders.get(key));
      if (v) {
        observed.buildSha = v;
        observed.source = `list_header:${key}`;
        observed.probes.push({ source: observed.source, ok: true, buildSha: v });
        return observed;
      }
    }
  }

  const paths = [
    "/api/group-demand-intelligence/health",
    "/api/health",
    "/health",
  ];
  for (const p of paths) {
    const url = `${String(baseUrl).replace(/\/$/, "")}${p}`;
    try {
      const res = await fetchImpl(url, { method: "GET" });
      const raw = await res.text();
      const parsed = parseJsonBodyStrict(raw);
      const headerSha = BUILD_HEADER_KEYS.map((k) => text(res.headers?.get?.(k)))
        .find(Boolean);
      const bodySha = parsed.ok
        ? text(
            parsed.body.buildSha ||
              parsed.body.gitSha ||
              parsed.body.commitSha ||
              parsed.body.sha ||
              parsed.body.version?.gitSha ||
              parsed.body.version?.commit
          )
        : "";
      const sha = headerSha || bodySha || null;
      observed.probes.push({
        path: p,
        httpStatus: res.status,
        ok: res.ok,
        buildSha: sha,
      });
      if (sha) {
        observed.buildSha = sha;
        observed.source = headerSha
          ? `health_header:${p}`
          : `health_body:${p}`;
        return observed;
      }
    } catch (err) {
      observed.probes.push({
        path: p,
        ok: false,
        error: String(err?.message || err),
      });
    }
  }
  return observed;
}

function reconcilePublishedIds({
  listIds,
  manifest,
  requireManifest,
}) {
  if (!manifest) {
    return {
      status: requireManifest ? "blocked" : "not_provided",
      reason: requireManifest
        ? "publication_manifest_unavailable"
        : "publication_manifest_not_provided",
      missingIds: [],
      unexpectedIds: [],
      legacyPreservedIds: [],
      matchedIds: [],
    };
  }

  const published = new Set(manifest.publishedIds || []);
  const legacy = new Set(manifest.legacyPreservedIds || []);
  const served = new Set(listIds);
  const missingIds = [...published].filter((id) => !served.has(id));
  const unexpectedIds = [...served].filter(
    (id) => !published.has(id) && !legacy.has(id)
  );
  const matchedIds = [...published].filter((id) => served.has(id));
  const legacyObserved = [...legacy].filter((id) => served.has(id));

  let status = "passed";
  let reason = null;
  if (published.size === 0) {
    status = "failed";
    reason = "publication_manifest_empty";
  } else if (missingIds.length) {
    status = "failed";
    reason = "missing_expected_published_ids";
  }
  // Unexpected non-legacy IDs are recorded; fail closed when manifest is authoritative
  if (status === "passed" && unexpectedIds.length) {
    status = "failed";
    reason = "unexpected_ids_not_in_manifest_or_legacy";
  }

  return {
    status,
    reason,
    missingIds,
    unexpectedIds,
    legacyPreservedIds: legacyObserved,
    matchedIds,
    expectedCount: published.size,
    servedCount: listIds.length,
  };
}

async function verifyListDetailParity({
  baseUrl,
  hotelId,
  ids,
  fetchImpl,
  limit = 5,
}) {
  const sample = ids.slice(0, Math.max(0, limit));
  const results = [];
  for (const id of sample) {
    const url = `${String(baseUrl).replace(/\/$/, "")}/api/group-demand-intelligence/hotels/${encodeURIComponent(hotelId)}/opportunities/${encodeURIComponent(id)}`;
    try {
      const res = await fetchImpl(url, { method: "GET" });
      const raw = await res.text();
      const parsed = parseJsonBodyStrict(raw);
      if (!res.ok) {
        results.push({
          id,
          status: "failed",
          reason: `detail_http_${res.status}`,
        });
        continue;
      }
      if (!parsed.ok) {
        results.push({
          id,
          status: "failed",
          reason: parsed.reason || "detail_invalid_json",
        });
        continue;
      }
      const opp =
        parsed.body.opportunity ||
        parsed.body.data ||
        (parsed.body.id ? parsed.body : null);
      const detailId = text(opp?.id || parsed.body.opportunityId);
      const detailHotel = text(
        opp?.hotelId || parsed.body.hotelId || ""
      );
      const leak = detectProviderMetadataLeaks(parsed.body);
      if (leak.leaked) {
        results.push({
          id,
          status: "failed",
          reason: "provider_metadata_leak",
          match: leak.match,
        });
        continue;
      }
      if (detailId && detailId !== id) {
        results.push({
          id,
          status: "failed",
          reason: "detail_id_mismatch",
          detailId,
        });
        continue;
      }
      if (detailHotel && detailHotel !== hotelId) {
        results.push({
          id,
          status: "failed",
          reason: "detail_hotel_mismatch",
          detailHotel,
        });
        continue;
      }
      if (!detailId && !opp) {
        results.push({
          id,
          status: "failed",
          reason: "detail_schema_missing_opportunity",
        });
        continue;
      }
      results.push({ id, status: "passed" });
    } catch (err) {
      results.push({
        id,
        status: "blocked",
        reason: String(err?.message || err),
      });
    }
  }
  const failed = results.filter((r) => r.status === "failed");
  const blocked = results.filter((r) => r.status === "blocked");
  return {
    status: failed.length
      ? "failed"
      : blocked.length
        ? "blocked"
        : sample.length
          ? "passed"
          : "skipped",
    sampleCount: sample.length,
    results,
  };
}

function finalizeDeliveryResult({
  hotelId,
  runId,
  startedAt,
  intendedBuildSha,
  intendedDataRevision,
  status,
  notes,
  checks,
}) {
  return {
    status,
    machineStatus: status,
    hotelId,
    runId,
    verifiedAt: startedAt,
    // Intended (caller) — never treated as observed proof
    intendedBuildSha: intendedBuildSha || null,
    intendedDataRevision: intendedDataRevision || null,
    // Observed — only from supported probes / response content
    buildSha: checks?.observedBuildSha || null,
    dataRevision: checks?.observedDataRevision || null,
    notes,
    checks,
  };
}

async function runDeliveryVerification({
  surface,
  hotelId,
  runId = null,
  baseUrl,
  /** @deprecated use intendedBuildSha — caller SHA is not observation */
  buildSha = null,
  intendedBuildSha = null,
  dataRevision = null,
  intendedDataRevision = null,
  expectedPublishedIds = null,
  publicationManifest = null,
  legacyPreservedIds = null,
  requirePublicationManifest = false,
  requireBuildIdentity = false,
  detailSampleLimit = 5,
  fetchImpl = globalThis.fetch,
} = {}) {
  const startedAt = new Date().toISOString();
  const intendedSha = text(intendedBuildSha || buildSha) || null;
  const intendedRev = text(intendedDataRevision || dataRevision) || null;
  const url = `${String(baseUrl).replace(/\/$/, "")}/api/group-demand-intelligence/hotels/${encodeURIComponent(hotelId)}/opportunities`;

  let manifest = normalizePublicationManifest(publicationManifest);
  if (!manifest && expectedPublishedIds) {
    manifest = normalizePublicationManifest({
      publishedIds: expectedPublishedIds,
      legacyPreservedIds: legacyPreservedIds || [],
      hotelId,
      runId,
    });
  } else if (manifest && legacyPreservedIds?.length) {
    manifest.legacyPreservedIds = [
      ...new Set([
        ...(manifest.legacyPreservedIds || []),
        ...legacyPreservedIds.map((id) => text(id)).filter(Boolean),
      ]),
    ];
  }

  try {
    const res = await fetchImpl(url, { method: "GET" });
    const raw = await res.text();
    const parsed = parseJsonBodyStrict(raw);

    if (!res.ok) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} API status ${res.status}`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: false,
          rawLength: raw.length,
        },
      });
    }

    if (!parsed.ok) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} invalid JSON (${parsed.reason}) — empty fallback not accepted`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: false,
          parseReason: parsed.reason,
          parseError: parsed.error || null,
        },
      });
    }

    const body = parsed.body;
    const opps = Array.isArray(body.opportunities) ? body.opportunities : null;
    if (!opps) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} response missing opportunities array`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: false,
          bodyKeys: Object.keys(body),
        },
      });
    }

    const servedHotelId = text(body.hotelId);
    if (!servedHotelId) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} response missing hotelId`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: false,
          hotelIdentityOk: false,
        },
      });
    }
    if (servedHotelId !== text(hotelId)) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} wrong hotel identity: served=${servedHotelId} expected=${hotelId}`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: true,
          hotelIdentityOk: false,
          servedHotelId,
          expectedHotelId: hotelId,
        },
      });
    }

    const leak = detectProviderMetadataLeaks(body);
    if (leak.leaked) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} provider metadata leak in customer DTO: ${leak.match}`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: true,
          hotelIdentityOk: true,
          providerMetadataLeaks: 1,
          leakMatch: leak.match,
          count: opps.length,
        },
      });
    }

    const listIds = opps.map((o) => text(o?.id)).filter(Boolean);
    const pub = reconcilePublishedIds({
      listIds,
      manifest,
      requireManifest: requirePublicationManifest,
    });
    if (pub.status === "failed" || pub.status === "blocked") {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: pub.status,
        notes: `${surface} publication reconcile ${pub.status}: ${pub.reason}`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: true,
          hotelIdentityOk: true,
          providerMetadataLeaks: 0,
          count: opps.length,
          publicationReconcile: pub,
        },
      });
    }

    const detailIds =
      manifest?.publishedIds?.length > 0
        ? manifest.publishedIds.filter((id) => listIds.includes(id))
        : listIds;
    const detail = await verifyListDetailParity({
      baseUrl,
      hotelId,
      ids: detailIds,
      fetchImpl,
      limit: detailSampleLimit,
    });
    if (detail.status === "failed" || detail.status === "blocked") {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: detail.status,
        notes: `${surface} list/detail ${detail.status}`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: true,
          hotelIdentityOk: true,
          providerMetadataLeaks: 0,
          count: opps.length,
          publicationReconcile: pub,
          listDetail: detail,
        },
      });
    }

    const observedDataRevision = computeOpportunityListFingerprint(body);
    if (intendedRev && intendedRev !== observedDataRevision) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} data revision mismatch intended≠observed`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: true,
          hotelIdentityOk: true,
          providerMetadataLeaks: 0,
          count: opps.length,
          publicationReconcile: pub,
          listDetail: detail,
          observedDataRevision,
          intendedDataRevision: intendedRev,
          dataRevisionOk: false,
        },
      });
    }

    const runtime = await observeRuntimeBuildIdentity({
      baseUrl,
      fetchImpl,
      listHeaders: res.headers,
    });
    const observedBuildSha = runtime.buildSha || null;
    const mustAssertBuild = requireBuildIdentity || Boolean(intendedSha);

    if (mustAssertBuild && !observedBuildSha) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "blocked",
        notes: `${surface} observed build identity unavailable (caller-supplied SHA is not proof)`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: true,
          hotelIdentityOk: true,
          providerMetadataLeaks: 0,
          count: opps.length,
          publicationReconcile: pub,
          listDetail: detail,
          observedDataRevision,
          observedBuildSha: null,
          intendedBuildSha: intendedSha,
          buildIdentity: runtime,
          buildIdentityOk: false,
        },
      });
    }

    if (intendedSha && observedBuildSha && intendedSha !== observedBuildSha) {
      return finalizeDeliveryResult({
        hotelId,
        runId,
        startedAt,
        intendedBuildSha: intendedSha,
        intendedDataRevision: intendedRev,
        status: "failed",
        notes: `${surface} build identity mismatch intended≠observed`,
        checks: {
          surface,
          httpStatus: res.status,
          schemaOk: true,
          hotelIdentityOk: true,
          providerMetadataLeaks: 0,
          count: opps.length,
          publicationReconcile: pub,
          listDetail: detail,
          observedDataRevision,
          observedBuildSha,
          intendedBuildSha: intendedSha,
          buildIdentity: runtime,
          buildIdentityOk: false,
        },
      });
    }

    return finalizeDeliveryResult({
      hotelId,
      runId,
      startedAt,
      intendedBuildSha: intendedSha,
      intendedDataRevision: intendedRev,
      status: "passed",
      notes: `${surface} list count=${opps.length}; publication=${pub.status}; detail=${detail.status}; build=${observedBuildSha || "not_asserted"}`,
      checks: {
        surface,
        httpStatus: res.status,
        schemaOk: true,
        hotelIdentityOk: true,
        providerMetadataLeaks: 0,
        count: opps.length,
        sampleIds: listIds.slice(0, 5),
        publicationReconcile: pub,
        listDetail: detail,
        observedDataRevision,
        observedBuildSha,
        intendedBuildSha: intendedSha,
        buildIdentity: runtime,
        buildIdentityOk: mustAssertBuild ? Boolean(observedBuildSha) : true,
        dataRevisionOk: intendedRev ? intendedRev === observedDataRevision : true,
      },
    });
  } catch (err) {
    return finalizeDeliveryResult({
      hotelId,
      runId,
      startedAt,
      intendedBuildSha: intendedSha,
      intendedDataRevision: intendedRev,
      status: "blocked",
      notes: `${surface} serving unreachable: ${err?.message || err}`,
      checks: { surface, error: String(err?.message || err) },
    });
  }
}

/**
 * Probe local GDI opportunities API for a hotel.
 */
export async function runLocalDeliveryVerification(opts = {}) {
  return runDeliveryVerification({
    surface: "local",
    baseUrl: opts.baseUrl || "http://127.0.0.1:8080",
    requireBuildIdentity: opts.requireBuildIdentity === true,
    ...opts,
  });
}

/**
 * Probe production GDI opportunities API.
 */
export async function runProductionDeliveryVerification(opts = {}) {
  return runDeliveryVerification({
    surface: "production",
    baseUrl:
      opts.baseUrl ||
      process.env.GDI_PRODUCTION_BASE_URL ||
      "https://my-operators-backend-production.up.railway.app",
    // Production delivery claims a release — require observed identity when intended is set
    requireBuildIdentity:
      opts.requireBuildIdentity === true ||
      Boolean(opts.intendedBuildSha || opts.buildSha),
    ...opts,
  });
}

function readLedgerAuthorizationIdentity(doc) {
  const auth = text(doc?.authorization || doc?.authorizationId || "");
  return auth || null;
}

function readLedgerHotelScope(doc) {
  const hotelId = text(doc?.hotelId || "");
  const allowed = Array.isArray(doc?.allowedHotelIds)
    ? doc.allowedHotelIds.map((id) => text(id)).filter(Boolean)
    : [];
  return { hotelId: hotelId || null, allowedHotelIds: allowed };
}

/**
 * Authoritative budget reconciliation from hotel/product ledger JSON.
 * Ledger authorization identity is read first; requested id is compared after.
 */
export function reconcileAuthoritativeBudget({
  ledgerPath = null,
  authorizationId = null,
  hotelId = null,
  product = null,
  purpose = null,
  ledgerDoc = null,
  allowBootstrapEmpty = false,
} = {}) {
  if (allowBootstrapEmpty === true && !ledgerDoc && !ledgerPath) {
    return {
      reconciled: false,
      authoritative: false,
      reason: "bootstrap_empty_not_allowed_for_reconcile",
    };
  }

  let doc = ledgerDoc;
  let loadReason = null;
  if (!doc && ledgerPath) {
    if (!fs.existsSync(ledgerPath)) {
      return {
        reconciled: false,
        authoritative: false,
        reason: "ledger_missing",
        ledgerPath,
      };
    }
    try {
      doc = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    } catch (err) {
      return {
        reconciled: false,
        authoritative: false,
        reason: "ledger_malformed",
        ledgerPath,
        error: String(err?.message || err),
      };
    }
  }
  if (!doc || typeof doc !== "object" || Array.isArray(doc)) {
    return {
      reconciled: false,
      authoritative: false,
      reason: loadReason || "ledger_missing",
      ledgerPath: ledgerPath || null,
    };
  }

  // CRITICAL: read ledger identity independently — never prefer requested id
  const ledgerAuthorizationId = readLedgerAuthorizationIdentity(doc);
  const requestedAuthorizationId = text(authorizationId) || null;

  if (!ledgerAuthorizationId) {
    return {
      reconciled: false,
      authoritative: true,
      reason: "authorization_identity_missing",
      authorizationId: null,
      requestedAuthorizationId,
      ledgerPath: ledgerPath || null,
    };
  }

  if (
    requestedAuthorizationId &&
    requestedAuthorizationId !== ledgerAuthorizationId
  ) {
    return {
      reconciled: false,
      authoritative: true,
      reason: "authorization_mismatch",
      authorizationId: ledgerAuthorizationId,
      requestedAuthorizationId,
      ledgerPath: ledgerPath || null,
    };
  }

  const scope = readLedgerHotelScope(doc);
  const requestedHotelId = text(hotelId) || null;
  if (requestedHotelId) {
    const hotelOk =
      (scope.hotelId && scope.hotelId === requestedHotelId) ||
      scope.allowedHotelIds.includes(requestedHotelId);
    if (!scope.hotelId && scope.allowedHotelIds.length === 0) {
      return {
        reconciled: false,
        authoritative: true,
        reason: "hotel_scope_unresolved",
        authorizationId: ledgerAuthorizationId,
        requestedHotelId,
        ledgerPath: ledgerPath || null,
      };
    }
    if (!hotelOk) {
      return {
        reconciled: false,
        authoritative: true,
        reason: "hotel_mismatch",
        authorizationId: ledgerAuthorizationId,
        hotelId: scope.hotelId,
        requestedHotelId,
        ledgerPath: ledgerPath || null,
      };
    }
  }

  const ledgerProduct = text(doc.product || doc.productId || "") || null;
  const ledgerPurpose =
    text(doc.purpose || doc.authorizedPurpose || "") || null;
  const requestedProduct = text(product) || null;
  const requestedPurpose = text(purpose) || null;

  if (requestedProduct) {
    if (!ledgerProduct) {
      return {
        reconciled: false,
        authoritative: true,
        reason: "product_unresolved",
        authorizationId: ledgerAuthorizationId,
        requestedProduct,
        ledgerPath: ledgerPath || null,
      };
    }
    if (ledgerProduct !== requestedProduct) {
      return {
        reconciled: false,
        authoritative: true,
        reason: "product_mismatch",
        authorizationId: ledgerAuthorizationId,
        product: ledgerProduct,
        requestedProduct,
        ledgerPath: ledgerPath || null,
      };
    }
  }
  if (requestedPurpose) {
    if (!ledgerPurpose) {
      return {
        reconciled: false,
        authoritative: true,
        reason: "purpose_unresolved",
        authorizationId: ledgerAuthorizationId,
        requestedPurpose,
        ledgerPath: ledgerPath || null,
      };
    }
    if (ledgerPurpose !== requestedPurpose) {
      return {
        reconciled: false,
        authoritative: true,
        reason: "purpose_mismatch",
        authorizationId: ledgerAuthorizationId,
        purpose: ledgerPurpose,
        requestedPurpose,
        ledgerPath: ledgerPath || null,
      };
    }
  }

  const cap = finiteNumber(doc.budgetUsd ?? doc.capUsd, null);
  if (cap === null || cap <= 0) {
    return {
      reconciled: false,
      authoritative: true,
      reason: "cap_invalid",
      authorizationId: ledgerAuthorizationId,
      capUsd: doc.budgetUsd ?? doc.capUsd ?? null,
      ledgerPath: ledgerPath || null,
    };
  }

  const entries = Array.isArray(doc.entries) ? doc.entries : null;
  if (!entries) {
    return {
      reconciled: false,
      authoritative: true,
      reason: "entries_malformed",
      authorizationId: ledgerAuthorizationId,
      ledgerPath: ledgerPath || null,
    };
  }

  for (const e of entries) {
    if (e?.estimatedUsd != null && !Number.isFinite(Number(e.estimatedUsd))) {
      return {
        reconciled: false,
        authoritative: true,
        reason: "entry_amount_malformed",
        authorizationId: ledgerAuthorizationId,
        ledgerPath: ledgerPath || null,
      };
    }
  }

  const spentFromEntries = entries
    .filter((e) => e.status === "spent")
    .reduce((s, e) => s + Number(e.estimatedUsd || 0), 0);
  const reservedFromEntries = entries
    .filter((e) => e.status === "reserved")
    .reduce((s, e) => s + Number(e.estimatedUsd || 0), 0);

  const estimated =
    doc.estimatedSpentUsd != null
      ? finiteNumber(doc.estimatedSpentUsd, null)
      : spentFromEntries;
  const reserved =
    doc.reservedUsd != null
      ? finiteNumber(doc.reservedUsd, null)
      : reservedFromEntries;
  const hold = finiteNumber(doc.accountingHoldUsd, 0);

  if (estimated === null || reserved === null || hold === null) {
    return {
      reconciled: false,
      authoritative: true,
      reason: "amounts_malformed",
      authorizationId: ledgerAuthorizationId,
      ledgerPath: ledgerPath || null,
    };
  }

  const available = Number((cap - estimated - reserved - hold).toFixed(2));

  const keys = entries.map((e) => e.jobKey || e.reservationId).filter(Boolean);
  const dup = keys.filter((k, i) => keys.indexOf(k) !== i);

  return {
    reconciled: dup.length === 0,
    authoritative: true,
    reason: dup.length ? "duplicate_job_keys" : null,
    authorizationId: ledgerAuthorizationId,
    requestedAuthorizationId,
    hotelId: scope.hotelId,
    product: ledgerProduct,
    purpose: ledgerPurpose,
    capUsd: cap,
    estimatedIncurredUsd: estimated,
    actualBilledUsd:
      doc.actualBilledUsd === undefined ? null : doc.actualBilledUsd,
    outstandingReservationsUsd: reserved,
    accountingHoldUsd: hold,
    availableUsd: available,
    reservationsAccounted: true,
    duplicateJobKeys: [...new Set(dup)],
    exhausted: available <= 0 && reserved === 0,
    ledgerPath: ledgerPath || null,
  };
}

/**
 * Fingerprint run artifacts for resume invalidation.
 */
export function computeRunArtifactFingerprint(runDir) {
  if (!runDir || !fs.existsSync(runDir)) return null;
  const parts = [];
  const walk = (dir, depth = 0) => {
    if (depth > 3) return;
    let ents = [];
    try {
      ents = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const ent of ents) {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        if (
          [
            "full_e2e",
            "buyer_contact_enrichment",
            "paid_multisegment",
            "reuse_prior_intelligence",
          ].includes(ent.name)
        ) {
          walk(p, depth + 1);
        }
      } else if (/\.(json|csv|md)$/i.test(ent.name)) {
        try {
          const st = fs.statSync(p);
          parts.push(`${path.relative(runDir, p)}:${st.size}:${st.mtimeMs}`);
        } catch {
          /* ignore */
        }
      }
    }
  };
  walk(runDir);
  parts.sort();
  return createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 24);
}

export function verificationNeedsRerun(prevStage = {}, fingerprints = {}) {
  if (!prevStage || prevStage.status !== "completed") return true;
  const vs = String(prevStage.verificationStatus || "").toLowerCase();
  if (
    !vs ||
    vs === "not_run" ||
    vs === "blocked" ||
    vs === "failed" ||
    vs === "stale"
  ) {
    return true;
  }
  if (
    fingerprints.dataRevision &&
    prevStage.dataRevision &&
    String(fingerprints.dataRevision) !== String(prevStage.dataRevision)
  ) {
    return true;
  }
  if (
    fingerprints.buildSha &&
    prevStage.buildSha &&
    String(fingerprints.buildSha) !== String(prevStage.buildSha)
  ) {
    return true;
  }
  if (
    fingerprints.artifactFingerprint &&
    prevStage.artifactFingerprint &&
    String(fingerprints.artifactFingerprint) !==
      String(prevStage.artifactFingerprint)
  ) {
    return true;
  }
  return false;
}

export function textTrim(v) {
  return text(v);
}
