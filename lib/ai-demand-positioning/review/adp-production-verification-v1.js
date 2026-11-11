/**
 * Production delivery verification for ADP reports.
 * Local published/ filesystem is NEVER proof of production publication.
 */

export const ADP_PRODUCTION_VERIFICATION_V1 = "ADP_PRODUCTION_VERIFICATION_V1";

export const PRODUCTION_STATUS = Object.freeze({
  PRODUCTION_VERIFIED: "PRODUCTION_VERIFIED",
  PRODUCTION_NOT_VERIFIED: "PRODUCTION_NOT_VERIFIED",
  PRODUCTION_MISMATCH: "PRODUCTION_MISMATCH",
  PRODUCTION_UNREACHABLE: "PRODUCTION_UNREACHABLE",
  NO_PRODUCTION_POINTER: "NO_PRODUCTION_POINTER",
});

/**
 * Resolve configured production base URL (if any).
 * Never invent a host — missing config → PRODUCTION_NOT_VERIFIED.
 */
export function getAdpProductionBaseUrl() {
  const raw =
    process.env.ADP_PRODUCTION_BASE_URL ||
    process.env.DEALALITY_PRODUCTION_BASE_URL ||
    process.env.PRODUCTION_APP_BASE_URL ||
    "";
  const s = String(raw || "").trim().replace(/\/$/, "");
  return s || null;
}

/**
 * Probe production customer report delivery for a property.
 * Read-only; does not mutate local or remote state.
 *
 * @param {{ propertyId: string, expectedVersion?: string|null, expectedHash?: string|null }} opts
 */
export async function verifyAdpProductionDelivery(opts = {}) {
  const propertyId = String(opts.propertyId || "").trim();
  const baseUrl = getAdpProductionBaseUrl();
  const startedAt = new Date().toISOString();

  if (!propertyId) {
    return {
      ok: false,
      status: PRODUCTION_STATUS.PRODUCTION_NOT_VERIFIED,
      reason: "property_id_required",
      localPublishedFileIsNotProof: true,
      contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
      verifiedAt: startedAt,
    };
  }

  if (!baseUrl) {
    return {
      ok: false,
      status: PRODUCTION_STATUS.PRODUCTION_NOT_VERIFIED,
      reason: "ADP_PRODUCTION_BASE_URL_unset",
      message:
        "No production base URL configured. Local published/ artifacts are not production proof.",
      localPublishedFileIsNotProof: true,
      historicalPublicationRecord: opts.historicalPublicationRecord || null,
      contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
      verifiedAt: startedAt,
    };
  }

  // Avoid probing localhost as "production"
  try {
    const host = new URL(baseUrl).hostname;
    if (/^(localhost|127\.0\.0\.1)$/i.test(host)) {
      return {
        ok: false,
        status: PRODUCTION_STATUS.PRODUCTION_NOT_VERIFIED,
        reason: "production_base_is_localhost",
        message: "Configured production base resolves to localhost — not accepted as production.",
        baseUrl,
        localPublishedFileIsNotProof: true,
        contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
        verifiedAt: startedAt,
      };
    }
  } catch {
    return {
      ok: false,
      status: PRODUCTION_STATUS.PRODUCTION_NOT_VERIFIED,
      reason: "invalid_production_base_url",
      baseUrl,
      localPublishedFileIsNotProof: true,
      contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
      verifiedAt: startedAt,
    };
  }

  const url =
    `${baseUrl}/api/ai-demand-positioning/property/${encodeURIComponent(propertyId)}/report` +
    `?version=published&_cb=prod-verify-${Date.now()}`;

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), Number(opts.timeoutMs || 8000));
    const res = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
        "Cache-Control": "no-store",
        ...(opts.authHeader ? { Authorization: opts.authHeader } : {}),
      },
      signal: controller.signal,
    });
    clearTimeout(timer);

    if (res.status === 401 || res.status === 403) {
      return {
        ok: false,
        status: PRODUCTION_STATUS.PRODUCTION_NOT_VERIFIED,
        reason: "production_auth_required",
        httpStatus: res.status,
        baseUrl,
        probedUrl: url,
        localPublishedFileIsNotProof: true,
        message:
          "Production endpoint requires auth; delivery not verified in this session.",
        contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
        verifiedAt: new Date().toISOString(),
      };
    }

    if (!res.ok) {
      return {
        ok: false,
        status:
          res.status === 404
            ? PRODUCTION_STATUS.NO_PRODUCTION_POINTER
            : PRODUCTION_STATUS.PRODUCTION_UNREACHABLE,
        reason: `http_${res.status}`,
        httpStatus: res.status,
        baseUrl,
        probedUrl: url,
        localPublishedFileIsNotProof: true,
        contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
        verifiedAt: new Date().toISOString(),
      };
    }

    const body = await res.json().catch(() => null);
    const liveVersion =
      body?._adpStagedVersionId ||
      body?._adpReportVersion ||
      body?.periodId ||
      body?.reportVersion ||
      null;
    const liveHash = body?._adpArtifactHash || body?.artifactHash || null;

    if (opts.expectedVersion && liveVersion && String(liveVersion) !== String(opts.expectedVersion)) {
      return {
        ok: false,
        status: PRODUCTION_STATUS.PRODUCTION_MISMATCH,
        reason: "version_mismatch",
        expectedVersion: opts.expectedVersion,
        liveVersion,
        liveHash,
        baseUrl,
        localPublishedFileIsNotProof: true,
        contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
        verifiedAt: new Date().toISOString(),
      };
    }

    return {
      ok: true,
      status: PRODUCTION_STATUS.PRODUCTION_VERIFIED,
      liveVersion,
      liveHash,
      baseUrl,
      probedUrl: url,
      localPublishedFileIsNotProof: true,
      contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
      verifiedAt: new Date().toISOString(),
    };
  } catch (err) {
    return {
      ok: false,
      status: PRODUCTION_STATUS.PRODUCTION_UNREACHABLE,
      reason: err?.name === "AbortError" ? "timeout" : "fetch_failed",
      message: String(err?.message || err),
      baseUrl,
      localPublishedFileIsNotProof: true,
      contractVersion: ADP_PRODUCTION_VERIFICATION_V1,
      verifiedAt: new Date().toISOString(),
    };
  }
}

/**
 * Derive admin-facing production columns from registry + optional live probe.
 * Historical local publication records stay separate from live verification.
 */
export function summarizeProductionStatus({ publicationRecord, liveProbe } = {}) {
  const historicalVersion =
    publicationRecord?.publishedReportVersion ||
    publicationRecord?.productionPublishedVersion ||
    null;
  const historicalAt = publicationRecord?.publishedAt || null;
  const localFsPublished = publicationRecord?.publishedStatus === true;

  const probe = liveProbe || null;
  const productionVerificationStatus =
    probe?.status || PRODUCTION_STATUS.PRODUCTION_NOT_VERIFIED;

  return {
    localFilesystemPublishedPointer: localFsPublished,
    historicalPublicationVersion: historicalVersion,
    historicalPublishedAt: historicalAt,
    productionPublishedVersion: probe?.liveVersion || null,
    productionVerificationStatus,
    productionVerified: productionVerificationStatus === PRODUCTION_STATUS.PRODUCTION_VERIFIED,
    localPublishedFileIsNotProof: true,
    productionProbeReason: probe?.reason || null,
  };
}
