/**
 * AVA evidence-link validation V1
 *
 * Every customer-facing citation must both resolve successfully and directly
 * support the claim attached to it. A 200 status alone is not sufficient.
 *
 * Flags:
 * - HTTP_404 / HTTP_410 / HTTP_ERROR / FETCH_FAILED
 * - REDIRECT_UNRELATED (final host/path materially different; review required)
 * - HISTORICAL_AS_CURRENT (label/claim marks archived/historical as live current)
 * - CLAIM_UNSUPPORTED (caller-supplied claimSupport !== true)
 * - SOURCE_CONFLICT (caller-supplied conflict note)
 *
 * This module performs resolution checks. Claim-support scoring is supplied by
 * the caller (researcher / certification checklist) via claimSupport / notes.
 */
import https from "node:https";
import http from "node:http";
import { URL } from "node:url";

export const EVIDENCE_LINK_VALIDATION_CONTRACT =
  "AVA_EVIDENCE_LINK_VALIDATION_V1";

const BLOCKED_HOST_HINTS = [
  /bot|blocked|captcha|access denied|just a moment|cf-ray/i,
];

function collectEvidenceEntries(payload) {
  const out = [];
  const seen = new Set();

  function push(entry, context) {
    const url = String(entry?.url || entry || "").trim();
    if (!/^https?:\/\//i.test(url)) return;
    const key = `${context}|${url}|${entry?.label || ""}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      url,
      label: String(entry?.label || "").trim() || null,
      kind: entry?.kind || null,
      context,
      claimSupport:
        typeof entry?.claimSupport === "boolean" ? entry.claimSupport : null,
      claimNotes: entry?.claimNotes || entry?.notes || entry?.note || null,
      historicalAsCurrent: entry?.historicalAsCurrent === true,
      sourceConflict: entry?.sourceConflict || null,
    });
  }

  for (const f of payload?.keyFindings || []) {
    for (const e of f.evidence || []) {
      push(e, `finding:${f.relatedActionId || f.title || "unknown"}`);
    }
  }
  for (const a of payload?.actions || []) {
    for (const e of a.supportingEvidence || []) {
      push(e, `action:${a.actionId || a.headline || "unknown"}`);
    }
    for (const e of a.technicalDetails?.urls || []) {
      push(e, `action-tech-url:${a.actionId || "unknown"}`);
    }
    for (const e of a.technicalDetails?.evidence || []) {
      if (e && typeof e === "object") {
        push(e, `action-tech-evidence:${a.actionId || "unknown"}`);
      }
    }
    for (const ref of a.evidenceRefs || []) {
      push({ url: ref }, `action-ref:${a.actionId || "unknown"}`);
    }
  }
  const da = payload?.digitalAuthority;
  if (da) {
    for (const s of da.sources || []) {
      if (s?.linkingPageUrl) {
        push(
          { url: s.linkingPageUrl, label: s.referringOrganization || s.label },
          `digital-authority:source`
        );
      }
      if (s?.evidenceUrl) {
        push({ url: s.evidenceUrl, label: s.label }, `digital-authority:source-evidence`);
      }
    }
    for (const g of da.competitorGaps || []) {
      if (g?.evidenceUrl) {
        push(
          { url: g.evidenceUrl, label: g.competitorName },
          `digital-authority:competitor-gap`
        );
      }
      if (g?.competitorLinkingPageUrl) {
        push(
          { url: g.competitorLinkingPageUrl, label: g.competitorName },
          `digital-authority:competitor-page`
        );
      }
    }
    for (const o of da.opportunities || []) {
      // Outreach targets (directories/OTAs) are often bot-gated; they are not
      // claim-support evidence and must not fail customer hyperlink certification.
      // Only explicit evidenceLinks / evidenceUrl that the PDF treats as proof are registered.
      if (o?.evidenceUrl) {
        push({ url: o.evidenceUrl, label: o.title }, `digital-authority:opportunity-evidence`);
      }
      for (const e of o.evidenceLinks || []) {
        push(e, `digital-authority:opportunity-link`);
      }
    }
    for (const a of da.actions || []) {
      for (const e of a.evidenceLinks || a.supportingEvidence || []) {
        push(e, `digital-authority:action:${a.actionId || "unknown"}`);
      }
    }
  }
  const dcr = payload?.digitalContentReadiness;
  if (dcr) {
    for (const s of dcr.sourceInventory || []) {
      if (s?.url) {
        push(
          { url: s.url, label: s.platformName || s.label },
          `digital-content-readiness:source`
        );
      }
      if (s?.evidenceUrl) {
        push(
          { url: s.evidenceUrl, label: s.platformName || s.label },
          `digital-content-readiness:source-evidence`
        );
      }
    }
    for (const f of dcr.hotelFactsRegister || []) {
      if (f?.evidenceUrl) {
        push(
          { url: f.evidenceUrl, label: f.attribute || f.attributeId },
          `digital-content-readiness:fact`
        );
      }
    }
    for (const c of dcr.platformCorrections || []) {
      if (c?.url) {
        push(
          { url: c.url, label: c.platform || c.findingId },
          `digital-content-readiness:correction`
        );
      }
      for (const e of c.supportingEvidence || []) {
        push(e, `digital-content-readiness:correction-evidence`);
      }
    }
    for (const a of dcr.actions || []) {
      for (const e of a.evidenceLinks || []) {
        push(e, `digital-content-readiness:action`);
      }
      for (const e of a.supportingEvidence || []) {
        push(e, `digital-content-readiness:action-evidence`);
      }
    }
  }
  return out;
}

function requestOnce(url, { method = "HEAD", redirectCount = 0 } = {}) {
  return new Promise((resolve) => {
    let parsed;
    try {
      parsed = new URL(url);
    } catch (err) {
      resolve({
        ok: false,
        status: 0,
        finalUrl: url,
        error: `INVALID_URL:${err.message}`,
        redirected: false,
      });
      return;
    }
    const lib = parsed.protocol === "http:" ? http : https;
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const req = lib.request(
      {
        protocol: parsed.protocol,
        hostname: parsed.hostname,
        port: parsed.port || undefined,
        path: `${parsed.pathname}${parsed.search}`,
        method,
        timeout: 12000,
        headers: {
          "User-Agent":
            "DealalityAvaEvidenceValidation/1.0 (+internal; citation QA)",
          Accept: "text/html,application/pdf,*/*",
          Connection: "close",
        },
      },
      (res) => {
        const status = res.statusCode || 0;
        const location = res.headers.location;
        res.resume();
        if (
          [301, 302, 303, 307, 308].includes(status) &&
          location &&
          redirectCount < 5
        ) {
          const next = new URL(location, url).toString();
          requestOnce(next, { method, redirectCount: redirectCount + 1 }).then(
            (nested) =>
              finish({
                ...nested,
                redirected: true,
                redirectChain: [url, ...(nested.redirectChain || [])],
              })
          );
          return;
        }
        finish({
          ok: status >= 200 && status < 400,
          status,
          finalUrl: url,
          redirected: redirectCount > 0,
          redirectChain: redirectCount > 0 ? [url] : [],
          error: null,
        });
      }
    );
    req.setTimeout(12000, () => {
      req.destroy(new Error("TIMEOUT"));
    });
    req.on("timeout", () => {
      req.destroy(new Error("TIMEOUT"));
    });
    req.on("error", (err) => {
      finish({
        ok: false,
        status: 0,
        finalUrl: url,
        error: err.message || "REQUEST_ERROR",
        redirected: false,
      });
    });
    req.end();
    setTimeout(() => {
      req.destroy(new Error("HARD_TIMEOUT"));
      finish({
        ok: false,
        status: 0,
        finalUrl: url,
        error: "HARD_TIMEOUT",
        redirected: false,
      });
    }, 15000);
  });
}

async function probeUrl(url) {
  let head = await requestOnce(url, { method: "HEAD" });
  if (
    !head.ok &&
    (head.status === 403 ||
      head.status === 405 ||
      head.status === 0 ||
      head.error)
  ) {
    const get = await requestOnce(url, { method: "GET" });
    if (get.ok || get.status) return { ...get, methodUsed: "GET" };
  }
  return { ...head, methodUsed: "HEAD" };
}

function hostOf(u) {
  try {
    return new URL(u).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function pathOf(u) {
  try {
    return new URL(u).pathname.toLowerCase();
  } catch {
    return "";
  }
}

function classifyResolution(entry, probe) {
  const flags = [];
  const status = probe.status || 0;
  if (probe.error && !status) flags.push("FETCH_FAILED");
  if (status === 404) flags.push("HTTP_404");
  if (status === 410) flags.push("HTTP_410");
  if (status >= 400 && status !== 404 && status !== 410) {
    flags.push(`HTTP_${status}`);
  }
  if (probe.redirected) {
    const startHost = hostOf(entry.url);
    const endHost = hostOf(probe.finalUrl);
    const startPath = pathOf(entry.url);
    const endPath = pathOf(probe.finalUrl);
    if (startHost && endHost && startHost !== endHost) {
      flags.push("REDIRECT_UNRELATED");
    } else if (
      startPath &&
      endPath &&
      startPath !== endPath &&
      !endPath.startsWith(startPath) &&
      !startPath.startsWith(endPath)
    ) {
      flags.push("REDIRECT_UNRELATED");
    }
  }
  if (entry.historicalAsCurrent) flags.push("HISTORICAL_AS_CURRENT");
  if (entry.claimSupport === false) flags.push("CLAIM_UNSUPPORTED");
  if (entry.sourceConflict) flags.push("SOURCE_CONFLICT");
  return flags;
}

/**
 * Validate all evidence links on a payload.
 * Optional claimSupportByUrl map: { [url]: true|false|string note }
 */
export async function validateAuditEvidenceLinks(payload, options = {}) {
  const claimSupportByUrl = options.claimSupportByUrl || {};
  const probeCache = options.probeCache || null;
  const customProbe = typeof options.probeFn === "function" ? options.probeFn : null;
  const entries = collectEvidenceEntries(payload).map((e) => {
    const override = claimSupportByUrl[e.url];
    if (typeof override === "boolean") {
      return { ...e, claimSupport: override };
    }
    if (typeof override === "string") {
      return { ...e, claimSupport: false, claimNotes: override };
    }
    return e;
  });

  const results = [];
  for (const entry of entries) {
    let probe;
    if (probeCache && probeCache.has(entry.url)) {
      probe = probeCache.get(entry.url);
    } else if (customProbe) {
      const custom = await customProbe(entry.url);
      probe = {
        ok: custom.ok === true,
        status: custom.status || 0,
        finalUrl: custom.finalUrl || entry.url,
        error: custom.error || null,
        redirected: Boolean(custom.redirected),
        methodUsed: custom.methodUsed || "CUSTOM",
        flags: custom.flags || null,
      };
      if (probeCache) probeCache.set(entry.url, probe);
    } else {
      probe = await probeUrl(entry.url);
      if (probeCache) probeCache.set(entry.url, probe);
    }
    const flags =
      Array.isArray(probe.flags) && probe.flags.length && customProbe
        ? [
            ...classifyResolution(entry, probe).filter(
              (f) => !/^HTTP_/.test(f) && f !== "FETCH_FAILED"
            ),
            ...probe.flags,
          ]
        : classifyResolution(entry, probe);
    const pass =
      flags.length === 0 &&
      probe.ok &&
      entry.claimSupport !== false;
    results.push({
      ...entry,
      status: probe.status,
      finalUrl: probe.finalUrl,
      redirected: Boolean(probe.redirected),
      methodUsed: probe.methodUsed,
      error: probe.error,
      flags,
      pass,
    });
  }

  const failing = results.filter((r) => !r.pass);
  return {
    contract: EVIDENCE_LINK_VALIDATION_CONTRACT,
    propertyId: payload?.property?.propertyId || null,
    checkedAt: new Date().toISOString(),
    total: results.length,
    uniqueUrls: new Set(results.map((r) => r.url)).size,
    passCount: results.filter((r) => r.pass).length,
    failCount: failing.length,
    ok: failing.length === 0,
    results,
    failing,
  };
}

export function summarizeEvidenceValidation(report) {
  return {
    ok: report.ok,
    propertyId: report.propertyId,
    total: report.total,
    uniqueUrls: report.uniqueUrls,
    passCount: report.passCount,
    failCount: report.failCount,
    flagCounts: report.results.reduce((acc, r) => {
      for (const f of r.flags || []) acc[f] = (acc[f] || 0) + 1;
      return acc;
    }, {}),
  };
}

export { collectEvidenceEntries, BLOCKED_HOST_HINTS };
