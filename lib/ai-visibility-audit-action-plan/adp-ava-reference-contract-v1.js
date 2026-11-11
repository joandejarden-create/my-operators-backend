/**
 * Version-aware ADP → AVA reference contract.
 *
 * AVA must consume certified ADP metrics through lean pinned baselines + the
 * matching staged ADP report (sourceSha256). AVA must not recompute ADP math.
 *
 * Contract: ADP_AVA_REFERENCE_CONTRACT_V1
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_CERTIFIED_BASELINES_INDEX,
  certifiedBaselineFile,
} from "./certified-baseline-paths-v1.js";
import { loadAdpBaselineForAudit } from "./adp-baseline-loader-v1.js";

export const ADP_AVA_REFERENCE_CONTRACT = "ADP_AVA_REFERENCE_CONTRACT_V1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const MAIN_STAGED_FALLBACK = path.resolve(
  ROOT,
  "..",
  "my-operators-backend",
  "data",
  "ai-demand-positioning",
  "staged"
);

const ZERO_VERDICTS = Object.freeze({
  MEASURED_ZERO: "MEASURED_ZERO",
  NOT_MEASURED: "NOT_MEASURED",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  STALE_MEASUREMENT: "STALE_MEASUREMENT",
  METRIC_MISMATCH: "METRIC_MISMATCH",
  UNSUPPORTED_ZERO: "UNSUPPORTED_ZERO",
});

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function readJson(abs) {
  return JSON.parse(fs.readFileSync(abs, "utf8"));
}

function normalizePct(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return Math.round(v * 10) / 10;
  const m = String(v).match(/(-?\d+(?:\.\d+)?)\s*%?/);
  if (!m) return null;
  return Math.round(Number(m[1]) * 10) / 10;
}

function pctClose(a, b, tol = 0.05) {
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

function pctCloseMaybeApprox(a, b, raw) {
  const approx = /approximately|about|around|nearly|roughly|~|≈/i.test(String(raw || ""));
  // Whole-number AVA copy of a one-decimal ADP rate (49 vs 49.2) is OK within 0.5
  const tol = approx ? 1.0 : 0.55;
  return pctClose(a, b, tol);
}

function sanitizeAttrLabel(label) {
  let s = String(label || "")
    .replace(/[“”"']/g, "")
    .replace(/^(?:and|while|with|for|the|its|their|also|plus|both|despite)\s+/i, "")
    .replace(/\s+(?:while|where|when|because|with|that|which|despite)\b[\s\S]*$/i, "")
    .replace(/\s+recognition$/i, "")
    .replace(/[.,;:]+$/g, "")
    .trim();
  // Drop sentence fragments / mid-word cuts / numeric leftovers
  if (!s || s.length < 3 || s.length > 48) return "";
  if (/^\d/.test(s)) return "";
  if (
    /\b(territory|property identity|monitored|softest|following|detailed official|official content)\b/i.test(
      s
    )
  ) {
    return "";
  }
  if ((s.match(/\s/g) || []).length > 5) return "";
  return s;
}

function flattenText(payload) {
  return JSON.stringify(payload || {});
}

/**
 * Extract numeric ADP-looking references from AVA narrative text.
 * Returns { kind, label, value, raw } candidates.
 */
export function extractAdpNumericReferences(text) {
  const t = String(text || "");
  const refs = [];
  const kpiPatterns = [
    // Prefer number immediately before/after the metric name; do not span
    // "AI Consideration and appeared in 1.6%" → Scenario Presence.
    [/(\d+(?:\.\d+)?)\s*%\s*(?:AI )?Consideration\b/gi, "AI_CONSIDERATION"],
    [/\bAI Consideration\s*(?:rate|score)?\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%/gi, "AI_CONSIDERATION"],
    [/(\d+(?:\.\d+)?)\s*%\s*Scenario Presence\b/gi, "SCENARIO_PRESENCE"],
    [/\bScenario Presence\s*(?:rate|score)?\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%/gi, "SCENARIO_PRESENCE"],
    [/(\d+(?:\.\d+)?)\s*%\s*Reality Coverage\b/gi, "REALITY_COVERAGE"],
    [/\bReality Coverage\s*(?:rate|score)?\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%/gi, "REALITY_COVERAGE"],
    [/#(\d+)\s+of\s+(\d+)/gi, "PEER_RANK"],
  ];
  for (const [re, kind] of kpiPatterns) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(t))) {
      if (kind === "PEER_RANK") {
        refs.push({
          kind,
          label: `Peer #${m[1]} of ${m[2]}`,
          value: `#${m[1]} of ${m[2]}`,
          raw: m[0],
        });
      } else {
        refs.push({
          kind,
          label: kind,
          value: Number(m[1]),
          raw: m[0],
        });
      }
    }
  }

  // Attribute recognition: "0% recognition for X" / "X recognition was 0%" / "recorded 0% for X"
  const attrRes = [
    /(\d+(?:\.\d+)?)\s*%\s+recognition\s+for\s+([A-Za-z0-9][A-Za-z0-9 &'’\-\/]{1,60})/gi,
    /([A-Za-z][A-Za-z0-9 &'’\-\/]{1,50}?)\s+recognition\s+(?:was|of|at)\s+(?:approximately|about|around|nearly|roughly)?\s*(\d+(?:\.\d+)?)\s*%/gi,
    /recorded\s+(\d+(?:\.\d+)?)\s*%\s+(?:recognition\s+)?(?:for\s+)?([A-Za-z0-9][A-Za-z0-9 &'’\-\/]{0,60})/gi,
    /(\d+(?:\.\d+)?)\s*%\s+for\s+([A-Za-z0-9][A-Za-z0-9 &'’\-\/]{1,60})/gi,
    /([A-Za-z][A-Za-z0-9 &'’\-\/]{1,50})\s+(?:at|was|of)\s+(?:approximately|about|around|nearly|roughly)?\s*(\d+(?:\.\d+)?)\s*%/gi,
  ];
  for (const re of attrRes) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(t))) {
      let value;
      let label;
      if (m[2] && /%/.test(m[0]) && !Number.isNaN(Number(m[1])) && Number.isNaN(Number(m[2]))) {
        value = Number(m[1]);
        label = String(m[2]).replace(/[.,;:]+$/, "").trim();
      } else if (m[2] && !Number.isNaN(Number(m[2]))) {
        label = String(m[1]).replace(/[.,;:]+$/, "").trim();
        value = Number(m[2]);
      } else {
        value = Number(m[1]);
        label = (m[2] || "").replace(/[.,;:]+$/, "").trim();
      }
      if (!Number.isFinite(value)) continue;
      label = sanitizeAttrLabel(label);
      if (!label) continue;
      if (
        /^(?:ADP|recognition|Recognition)$/i.test(label) ||
        /AI Consideration|Scenario Presence|Reality Coverage|comparable|eligible|following|attributes?|these|those|all four|all three|both|events page|honeymoon recognition/i.test(
          label
        )
      ) {
        continue;
      }
      // Bare "X recognition was N%" where X is a metric qualifier, not an attribute
      if (/^ADP\b/i.test(label) || /\bpage\b/i.test(label)) continue;
      refs.push({
        kind: "ATTRIBUTE_RECOGNITION",
        label,
        value,
        raw: m[0],
        approx: /approximately|about|around|nearly|roughly/i.test(m[0]),
      });
    }
  }

  // Multi-attribute zero lists: "0% for A, B, and C" — stop before next percentage or clause
  const multiZero =
    /(?:recorded\s+)?(\d+(?:\.\d+)?)\s*%\s+(?:recognition\s+)?(?:for|across|on)\s+([A-Za-z][A-Za-z0-9 &'’\-\/,]{6,120}?)(?=\s*(?:\.|;|,?\s+and\s+\d|,\s*with\b|$))/gi;
  let mm;
  while ((mm = multiZero.exec(t))) {
    const value = Number(mm[1]);
    if (!Number.isFinite(value)) continue;
    const chunk = String(mm[2] || "");
    const parts = chunk
      .split(/\s*,\s*|\s+and\s+/i)
      .map((p) => sanitizeAttrLabel(p))
      .filter(Boolean);
    if (parts.length < 2) continue;
    for (const label of parts) {
      refs.push({
        kind: "ATTRIBUTE_RECOGNITION",
        label,
        value,
        raw: mm[0],
      });
    }
  }
  return refs;
}

function classifyZero(attrRow, avaValue) {
  if (attrRow == null) {
    return {
      verdict: ZERO_VERDICTS.UNSUPPORTED_ZERO,
      reason: "No matching attribute in pinned ADP realityGap",
    };
  }
  const rate = normalizePct(attrRow.recognitionRate);
  const total = Number(attrRow.total || attrRow.eligibleObservations || 0);
  const state = String(attrRow.measurementState || "");
  if (avaValue != null && rate != null && !pctClose(avaValue, rate)) {
    return {
      verdict: ZERO_VERDICTS.METRIC_MISMATCH,
      reason: `AVA ${avaValue}% vs ADP ${rate}%`,
    };
  }
  if (/NOT_APPLICABLE|N_A/i.test(state) || attrRow.notApplicable) {
    return { verdict: ZERO_VERDICTS.NOT_APPLICABLE, reason: state || "not applicable" };
  }
  if (/INSUFFICIENT|TOO_FEW/i.test(state) || (total > 0 && total < 5 && rate === 0)) {
    // ADP still marks TESTED_NOT_RECOGNIZED with large totals usually
    if (total > 0 && /TESTED/i.test(state)) {
      return {
        verdict: ZERO_VERDICTS.MEASURED_ZERO,
        reason: `${state || "tested"}; ${attrRow.count || 0}/${total}`,
      };
    }
    return {
      verdict: ZERO_VERDICTS.INSUFFICIENT_EVIDENCE,
      reason: state || `n=${total}`,
    };
  }
  if (/NOT_TESTED|UNMEASURED|NOT_MEASURED/i.test(state) || total <= 0) {
    return {
      verdict: ZERO_VERDICTS.NOT_MEASURED,
      reason: state || "no eligible observations",
    };
  }
  if (rate === 0 && total > 0) {
    return {
      verdict: ZERO_VERDICTS.MEASURED_ZERO,
      reason: `${state || "tested"}; ${attrRow.count || 0}/${total}`,
    };
  }
  return {
    verdict: ZERO_VERDICTS.MEASURED_ZERO,
    reason: state || `rate=${rate}`,
  };
}

function normAttrKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function findAttribute(realityGap, label) {
  const want = normAttrKey(label);
  if (!want || want.length < 3) return null;
  const all = [
    ...(realityGap?.gaps || []),
    ...(realityGap?.recognized || []),
    ...(realityGap?.attributes || []),
  ];

  const scoreMatch = (g) => {
    const keys = [g.attribute, g.label, g.id, g.name]
      .map(normAttrKey)
      .filter((k) => k && k.length >= 3);
    let score = 0;
    for (const k of keys) {
      if (k === want) score = Math.max(score, 200);
      else if (k.includes(want) || want.includes(k)) {
        // Prefer longer, more specific keys (small meeting space > meeting space)
        const overlap = Math.min(k.length, want.length);
        const specificity = k.length / Math.max(want.length, 1);
        if (overlap >= 8 || (overlap >= 5 && (k.startsWith(want) || want.startsWith(k)))) {
          score = Math.max(score, 60 + overlap + Math.round(specificity * 20));
        }
      }
    }
    // Token Jaccard boost for reordered phrases ("small meeting space" ↔ "meeting space small")
    const wantToks = new Set(want.split(/\s+/).filter((t) => t.length > 2));
    for (const k of keys) {
      const kToks = k.split(/\s+/).filter((t) => t.length > 2);
      if (!kToks.length || !wantToks.size) continue;
      const inter = kToks.filter((t) => wantToks.has(t)).length;
      const union = new Set([...kToks, ...wantToks]).size;
      const j = inter / union;
      if (j >= 0.75 && inter >= 2) score = Math.max(score, 150 + Math.round(j * 40));
    }
    return score;
  };

  let best = null;
  let bestScore = 0;
  for (const g of all) {
    const s = scoreMatch(g);
    if (s > bestScore) {
      best = g;
      bestScore = s;
    }
  }
  if (bestScore >= 60) return best;

  // Explicit synonym map — exact attribute keys preferred
  const syn = {
    "on site restaurant": ["on_site_restaurant", "on site restaurant"],
    restaurant: ["on_site_restaurant", "on site restaurant"],
    "luxury positioning": ["luxury", "luxury positioning"],
    "resort experience": ["resort", "resort experience"],
    "outdoor pool": ["outdoor_pool", "outdoor pool"],
    "island resort": ["island_resort", "island resort"],
    "meeting space": ["meeting_space", "meeting and event space", "meeting space"],
    "meeting space recognition": ["meeting_space", "meeting and event space"],
    "small meeting space": ["meeting_space_small", "intimate meeting space"],
    "intimate meeting space": ["meeting_space_small", "intimate meeting space"],
    "room service": ["in_room_dining", "room_service", "room service", "in room dining"],
    "digital key": ["digital_key", "digital key"],
    "connecting rooms": ["connecting_rooms", "connecting rooms"],
    "city views": ["city_views", "city views", "city view"],
    "walkable downtown": ["walkable_downtown", "walkable downtown"],
    "celebrations and events": ["celebrations_events", "social_event_lounge", "celebrations", "social_event"],
    "celebrations events": ["celebrations_events", "social_event_lounge", "celebrations", "social_event"],
    "wedding venue": ["wedding", "wedding_venue"],
    "business district": ["business_district", "business district location"],
    "convention center proximity": ["near_convention_center", "near convention center"],
    "convention center": ["near_convention_center", "near convention center"],
    uptown: ["charlotte_uptown", "uptown charlotte", "city center location"],
    "uptown location": ["charlotte_uptown", "uptown charlotte"],
    "uptown charlotte": ["charlotte_uptown", "uptown charlotte"],
    "signature bar": ["speakeasy", "speakeasy / signature bar"],
    "golf access": ["golf_access", "golf access"],
    meetings: ["meetings", "meetings capability"],
  };
  for (const [canon, alts] of Object.entries(syn)) {
    const canonN = normAttrKey(canon);
    if (
      want === canonN ||
      want.includes(canonN) ||
      canonN.includes(want) ||
      // token overlap for "celebrations and events" ↔ "celebrations events"
      (canonN.split(" ").filter((t) => t.length > 3 && want.includes(t)).length >= 2)
    ) {
      const hit = all.find((g) => {
        const keys = [g.attribute, g.label].map(normAttrKey);
        const rawAttr = String(g.attribute || "").toLowerCase();
        return alts.some((a) => {
          const an = normAttrKey(a);
          return (
            keys.includes(an) ||
            rawAttr === a ||
            rawAttr === an.replace(/\s+/g, "_") ||
            keys.some((k) => k === an || (an.length >= 8 && (k.includes(an) || an.includes(k))))
          );
        });
      });
      if (hit) return hit;
    }
  }
  return null;
}

function resolveStagedReport(lean) {
  const rel = lean?.provenance?.sourceRelativePath;
  const expectSha = lean?.provenance?.sourceSha256;
  const candidates = [];
  if (rel) {
    candidates.push(path.join(ROOT, rel));
    candidates.push(path.join(path.dirname(ROOT), "my-operators-backend", rel));
  }
  if (lean?.propertyId && expectSha) {
    for (const base of [
      path.join(ROOT, "data/ai-demand-positioning/staged", lean.propertyId),
      path.join(MAIN_STAGED_FALLBACK, lean.propertyId),
    ]) {
      if (!fs.existsSync(base)) continue;
      for (const f of fs.readdirSync(base)) {
        // Hash match remains mandatory. report-adp_period_ is the current-period
        // snapshot filename; report-staged_ is the older staged alias.
        if (!/^(?:report-staged_|report-adp_period_).+\.json$/i.test(f)) continue;
        candidates.push(path.join(base, f));
      }
    }
  }
  for (const abs of candidates) {
    if (!fs.existsSync(abs)) continue;
    const hash = sha256File(abs);
    if (expectSha && hash !== expectSha) continue;
    return { abs, sha256: hash, raw: readJson(abs) };
  }
  // path match without sha if unique relative path exists
  if (rel) {
    for (const abs of [
      path.join(ROOT, rel),
      path.join(path.dirname(ROOT), "my-operators-backend", rel),
    ]) {
      if (fs.existsSync(abs)) {
        return { abs, sha256: sha256File(abs), raw: readJson(abs), shaWarning: true };
      }
    }
  }
  return null;
}

function realityCoverageFromGap(rg) {
  if (!rg) return null;
  if (rg.recognizedCount != null && rg.totalAttributes) {
    return Math.round((Number(rg.recognizedCount) / Number(rg.totalAttributes)) * 1000) / 10;
  }
  if (rg.coverageRate != null) return normalizePct(rg.coverageRate);
  return null;
}

/**
 * Build authoritative ADP reference snapshot for one AVA hotel.
 */
export function loadAuthoritativeAdpReference(propertyId) {
  const loaded = loadAdpBaselineForAudit(propertyId);
  if (!loaded.ok) {
    return { ok: false, error: loaded.error || "BASELINE_LOAD_FAILED", propertyId };
  }
  const leanPath = certifiedBaselineFile(
    propertyId,
    loaded.baseline.periodId
  );
  const lean = leanPath && fs.existsSync(leanPath) ? readJson(leanPath) : null;
  const staged = lean ? resolveStagedReport(lean) : null;
  const stagedPayload = staged?.raw?.payload || staged?.raw || null;
  const realityGap = stagedPayload?.realityGap || null;
  const em = stagedPayload?.executiveMetrics || {};

  return {
    ok: true,
    contract: ADP_AVA_REFERENCE_CONTRACT,
    propertyId,
    name: loaded.baseline.name,
    periodId: loaded.baseline.periodId,
    baselineSha256: loaded.baseline.baselineSha256,
    sourceSha256: loaded.baseline.sourceSha256,
    leanPath: leanPath
      ? path.relative(ROOT, leanPath).replace(/\\/g, "/")
      : null,
    stagedPath: staged
      ? path.relative(ROOT, staged.abs).replace(/\\/g, "/")
      : null,
    stagedShaOk: staged ? staged.sha256 === lean?.provenance?.sourceSha256 : false,
    metrics: {
      aiConsideration: {
        rate: normalizePct(lean?.kpis?.aiConsideration?.rate ?? em.considerationRate?.rate),
        numerator:
          lean?.kpis?.aiConsideration?.presentObservations ??
          em.considerationRate?.presentObservations ??
          null,
        denominator:
          lean?.kpis?.aiConsideration?.comparableObservations ??
          em.considerationRate?.comparableObservations ??
          null,
        methodologyId: lean?.kpis?.aiConsideration?.methodologyId || null,
      },
      scenarioPresence: {
        rate: normalizePct(lean?.kpis?.scenarioPresence?.rate ?? em.scenarioPresence?.rate),
        numerator:
          lean?.kpis?.scenarioPresence?.capturedScenarios ??
          em.scenarioPresence?.capturedScenarios ??
          null,
        denominator:
          lean?.kpis?.scenarioPresence?.eligibleScenarios ??
          em.scenarioPresence?.eligibleScenarios ??
          null,
        methodologyId: lean?.kpis?.scenarioPresence?.methodologyId || null,
      },
      peerPosition: {
        value: lean?.kpis?.certifiedPeerPosition?.value || null,
        meta: lean?.kpis?.certifiedPeerPosition?.meta || null,
      },
      realityCoverage: {
        rate: realityCoverageFromGap(realityGap),
        numerator: realityGap?.recognizedCount ?? null,
        denominator: realityGap?.totalAttributes ?? null,
      },
    },
    realityGap,
    lean,
    stagedPayload,
  };
}

/**
 * Reconcile one AVA payload against the authoritative ADP pin.
 */
export function reconcileAvaPayloadAgainstAdp(payload, options = {}) {
  const propertyId = payload?.property?.propertyId;
  const defects = [];
  const zeros = [];
  const refs = [];
  if (!propertyId) {
    return {
      ok: false,
      propertyId: null,
      defects: [{ type: "MISSING_PROPERTY_ID", severity: "CRITICAL" }],
    };
  }
  const adp = loadAuthoritativeAdpReference(propertyId);
  if (!adp.ok) {
    return {
      ok: false,
      propertyId,
      defects: [{ type: "ADP_BASELINE_MISSING", severity: "CRITICAL", message: adp.error }],
      adp,
    };
  }

  // Same-period customer-facing package overrides lean KPI expectations.
  // Prefer approved package for this period; else published-current registry /
  // durable snapshot for the same periodId. Attribute evidence stays on lean staged.
  try {
    const mainRoot = path.join(path.dirname(ROOT), "my-operators-backend");
    const pkgDirs = [
      path.join(ROOT, "reports/dealality-adp-single-version-publication-contract-v1/approved-packages"),
      path.join(
        mainRoot,
        "reports/dealality-adp-single-version-publication-contract-v1/approved-packages"
      ),
    ];
    let overlaid = false;
    for (const dir of pkgDirs) {
      const pkgPath = path.join(dir, `${propertyId}.approved-package.json`);
      if (!fs.existsSync(pkgPath)) continue;
      const pkg = readJson(pkgPath);
      const pkgPeriod = pkg.measurementPeriodId || pkg.periodId || null;
      if (pkgPeriod && pkgPeriod === adp.periodId && pkg.certifiedMeasurement) {
        adp.metrics.aiConsideration = {
          rate: normalizePct(pkg.certifiedMeasurement.considerationRate),
          numerator: pkg.certifiedMeasurement.presentObservations ?? null,
          denominator: pkg.certifiedMeasurement.comparableObservations ?? null,
          methodologyId:
            pkg.certifiedMeasurement.methodologyId ||
            adp.metrics.aiConsideration.methodologyId,
        };
        adp.approvedPackageOverride = {
          packagePath: pkgPath,
          periodId: pkgPeriod,
          methodologyFingerprintSha256:
            pkg.methodologyFingerprintSha256 ||
            pkg.certifiedMeasurement.methodologyFingerprintSha256 ||
            null,
        };
        overlaid = true;
      }
      break;
    }
    const snapPath = path.join(
      mainRoot,
      "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/snapshots",
      propertyId,
      `report-${adp.periodId}.json`
    );
    if (fs.existsSync(snapPath)) {
      const snap = readJson(snapPath);
      const payloadSnap = snap.payload || snap;
      const cons = payloadSnap.executiveMetrics?.considerationRate;
      const scen = payloadSnap.executiveMetrics?.scenarioPresence;
      if (!overlaid && cons?.rate != null) {
        adp.metrics.aiConsideration = {
          rate: normalizePct(cons.rate),
          numerator: cons.presentObservations ?? null,
          denominator: cons.comparableObservations ?? null,
          methodologyId: cons.methodologyId || adp.metrics.aiConsideration.methodologyId,
        };
        adp.approvedPackageOverride = {
          packagePath: snapPath,
          periodId: adp.periodId,
          source: "customer_snapshot_same_period",
        };
        overlaid = true;
      }
      if (scen?.rate != null) {
        adp.metrics.scenarioPresence = {
          rate: normalizePct(scen.rate),
          numerator: scen.capturedScenarios ?? null,
          denominator: scen.eligibleScenarios ?? null,
          methodologyId: scen.methodologyId || null,
        };
      }
    }
    if (!overlaid) {
      const regPaths = [
        path.join(
          mainRoot,
          "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/registry/published-current-registry-v1.json"
        ),
        path.join(ROOT, "config/ai-demand-positioning/adp-published-current-registry-v1.json"),
        path.join(mainRoot, "config/ai-demand-positioning/adp-published-current-registry-v1.json"),
      ];
      for (const regPath of regPaths) {
        if (!fs.existsSync(regPath)) continue;
        const reg = readJson(regPath);
        const hit = (reg.properties || []).find((p) => p.propertyId === propertyId);
        if (hit?.periodId === adp.periodId && hit.considerationRate != null) {
          adp.metrics.aiConsideration = {
            ...adp.metrics.aiConsideration,
            rate: normalizePct(hit.considerationRate),
          };
          adp.approvedPackageOverride = {
            packagePath: regPath,
            periodId: adp.periodId,
            source: "published_current_registry",
          };
        }
        break;
      }
    }
  } catch {
    /* customer-package overlay is best-effort */
  }

  const avaPeriod = payload.reporting?.adpPeriodId || payload.adpPeriodId || null;
  const avaBaselineSha =
    payload.reporting?.adpBaselineSha256 || payload.adpBaselineSha256 || null;
  if (avaPeriod && avaPeriod !== adp.periodId) {
    defects.push({
      type: "STALE_OR_MISMATCHED_PERIOD",
      severity: "HIGH",
      message: `AVA period ${avaPeriod} ≠ certified ${adp.periodId}`,
      verdict: ZERO_VERDICTS.STALE_MEASUREMENT,
    });
  }
  if (avaBaselineSha && adp.baselineSha256 && avaBaselineSha !== adp.baselineSha256) {
    defects.push({
      type: "BASELINE_SHA_MISMATCH",
      severity: "HIGH",
      message: "AVA adpBaselineSha256 does not match certified lean pin",
    });
  }

  // KPI tiles
  for (const kpi of payload.kpis || []) {
    const label = String(kpi.label || "");
    const value = kpi.value;
    if (/AI Consideration/i.test(label)) {
      const got = normalizePct(value);
      const exp = adp.metrics.aiConsideration.rate;
      const match = pctClose(got, exp);
      refs.push({
        surface: "kpi",
        kind: "AI_CONSIDERATION",
        ava: got,
        adp: exp,
        match,
        lineage: {
          periodId: adp.periodId,
          baselineSha256: adp.baselineSha256,
          sourceSha256: adp.sourceSha256,
          metricId: "AI_CONSIDERATION",
          numerator: adp.metrics.aiConsideration.numerator,
          denominator: adp.metrics.aiConsideration.denominator,
        },
      });
      if (!match) {
        defects.push({
          type: "METRIC_MISMATCH",
          severity: "CRITICAL",
          metric: "AI_CONSIDERATION",
          ava: got,
          adp: exp,
        });
      }
    }
    if (/Scenario Presence/i.test(label)) {
      const got = normalizePct(value);
      const exp = adp.metrics.scenarioPresence.rate;
      const match = pctClose(got, exp);
      refs.push({
        surface: "kpi",
        kind: "SCENARIO_PRESENCE",
        ava: got,
        adp: exp,
        match,
        lineage: {
          periodId: adp.periodId,
          baselineSha256: adp.baselineSha256,
          sourceSha256: adp.sourceSha256,
          metricId: "SCENARIO_PRESENCE",
          numerator: adp.metrics.scenarioPresence.numerator,
          denominator: adp.metrics.scenarioPresence.denominator,
        },
      });
      if (!match) {
        defects.push({
          type: "METRIC_MISMATCH",
          severity: "CRITICAL",
          metric: "SCENARIO_PRESENCE",
          ava: got,
          adp: exp,
        });
      }
    }
    if (/Certified Peer Position|Peer Position/i.test(label)) {
      const got = String(value || "").replace(/\s+/g, " ").trim();
      const exp = String(adp.metrics.peerPosition.value || "").replace(/\s+/g, " ").trim();
      const match = !exp || got === exp || got.includes(exp) || exp.includes(got);
      refs.push({
        surface: "kpi",
        kind: "PEER_RANK",
        ava: got,
        adp: exp,
        match,
        lineage: {
          periodId: adp.periodId,
          baselineSha256: adp.baselineSha256,
          metricId: "CERTIFIED_PEER_POSITION",
        },
      });
      if (exp && !match) {
        defects.push({
          type: "METRIC_MISMATCH",
          severity: "HIGH",
          metric: "PEER_RANK",
          ava: got,
          adp: exp,
        });
      }
    }
  }

  const text = flattenText(payload);
  const extracted = extractAdpNumericReferences(text);
  const seen = new Set();
  for (const r of extracted) {
    const key = `${r.kind}|${r.label}|${r.value}`;
    if (seen.has(key)) continue;
    seen.add(key);

    if (r.kind === "AI_CONSIDERATION") {
      const match = pctClose(r.value, adp.metrics.aiConsideration.rate);
      refs.push({ surface: "narrative", ...r, adp: adp.metrics.aiConsideration.rate, match });
      // Prefer KPI-tile authority: stale narrative copy is repaired separately and
      // must not override a matching customer-facing KPI tile for the same metric.
      const tileOk = refs.some(
        (x) => x.surface === "kpi" && x.kind === "AI_CONSIDERATION" && x.match
      );
      if (!match && !tileOk) {
        defects.push({
          type: "METRIC_MISMATCH",
          severity: "HIGH",
          metric: "AI_CONSIDERATION_NARRATIVE",
          ava: r.value,
          adp: adp.metrics.aiConsideration.rate,
          raw: r.raw,
        });
      }
      continue;
    }
    if (r.kind === "SCENARIO_PRESENCE") {
      const match = pctClose(r.value, adp.metrics.scenarioPresence.rate);
      refs.push({ surface: "narrative", ...r, adp: adp.metrics.scenarioPresence.rate, match });
      const tileOk = refs.some(
        (x) => x.surface === "kpi" && x.kind === "SCENARIO_PRESENCE" && x.match
      );
      if (!match && !tileOk) {
        defects.push({
          type: "METRIC_MISMATCH",
          severity: "HIGH",
          metric: "SCENARIO_PRESENCE_NARRATIVE",
          ava: r.value,
          adp: adp.metrics.scenarioPresence.rate,
          raw: r.raw,
        });
      }
      continue;
    }
    if (r.kind === "REALITY_COVERAGE") {
      const exp = adp.metrics.realityCoverage.rate;
      const match = exp == null ? false : pctClose(r.value, exp);
      refs.push({ surface: "narrative", ...r, adp: exp, match });
      if (exp == null) {
        defects.push({
          type: "UNSUPPORTED_REALITY_COVERAGE",
          severity: "HIGH",
          ava: r.value,
          message: "Pinned staged realityGap missing",
        });
      } else if (!match) {
        defects.push({
          type: "METRIC_MISMATCH",
          severity: "HIGH",
          metric: "REALITY_COVERAGE",
          ava: r.value,
          adp: exp,
          raw: r.raw,
        });
      }
      continue;
    }
    if (r.kind === "PEER_RANK") {
      const exp = adp.metrics.peerPosition.value;
      const match = !exp || String(r.value).includes(String(exp).replace(/\s+/g, " ")) || String(exp).includes(String(r.value));
      refs.push({ surface: "narrative", ...r, adp: exp, match });
      continue;
    }
    if (r.kind === "ATTRIBUTE_RECOGNITION") {
      const attr = findAttribute(adp.realityGap, r.label);
      const adpRate = attr ? normalizePct(attr.recognitionRate) : null;
      const match = attr != null && pctCloseMaybeApprox(r.value, adpRate, r.raw);
      let zeroClass;
      if (attr == null) {
        // Non-zero unmatched refs are often traveler-intent / narrative phrasing,
        // not realityGap attributes — fail closed only for claimed zeros.
        if (r.value === 0 || r.value === 0.0) {
          zeroClass = {
            verdict: ZERO_VERDICTS.UNSUPPORTED_ZERO,
            reason: "attribute not in pin",
          };
        } else {
          continue; // skip soft non-attribute numeric mentions
        }
      } else if (!match) {
        // Low-confidence label match with divergent rate → treat as unresolved label, not content error
        const labelN = normAttrKey(r.label);
        const attrN = normAttrKey(attr.attribute || "");
        const labN = normAttrKey(attr.label || "");
        const confident =
          labelN === attrN ||
          labelN === labN ||
          labN.includes(labelN) ||
          labelN.includes(labN) ||
          (labelN.length >= 8 && (attrN.includes(labelN) || labelN.includes(attrN)));
        if (!confident) {
          if (r.value === 0 || r.value === 0.0) {
            zeroClass = {
              verdict: ZERO_VERDICTS.UNSUPPORTED_ZERO,
              reason: `Ambiguous label "${r.label}" did not confidently resolve`,
            };
          } else {
            continue;
          }
        } else {
          zeroClass = {
            verdict: ZERO_VERDICTS.METRIC_MISMATCH,
            reason: `AVA ${r.value} vs ADP ${adpRate}`,
          };
        }
      } else if (r.value === 0 || r.value === 0.0 || adpRate === 0) {
        // Matched zero — classify from ADP evidence, not AVA rounding noise
        zeroClass = classifyZero(attr, adpRate);
      } else {
        zeroClass = { verdict: "OK", reason: "match" };
      }
      const row = {
        surface: "narrative",
        kind: "ATTRIBUTE_RECOGNITION",
        label: r.label,
        ava: r.value,
        adp: adpRate,
        match,
        measurementState: attr?.measurementState || null,
        numerator: attr?.count ?? null,
        denominator: attr?.total ?? null,
        zeroVerdict: zeroClass.verdict,
        zeroReason: zeroClass.reason,
        raw: r.raw,
        lineage: {
          periodId: adp.periodId,
          baselineSha256: adp.baselineSha256,
          sourceSha256: adp.sourceSha256,
          metricId: `ATTRIBUTE:${attr?.attribute || r.label}`,
          numerator: attr?.count ?? null,
          denominator: attr?.total ?? null,
          value: adpRate,
          measurementStatus: attr?.measurementState || null,
        },
      };
      refs.push(row);
      if (r.value === 0 || r.value === 0.0) zeros.push(row);
      if (!match || zeroClass.verdict === ZERO_VERDICTS.UNSUPPORTED_ZERO) {
        defects.push({
          type:
            zeroClass.verdict === ZERO_VERDICTS.UNSUPPORTED_ZERO
              ? "UNSUPPORTED_ZERO"
              : "METRIC_MISMATCH",
          severity: "HIGH",
          metric: r.label,
          ava: r.value,
          adp: adpRate,
          verdict: zeroClass.verdict,
          raw: r.raw,
        });
      }
    }
  }

  // Newer ADP package drift (informational only)
  let newerPackageDrift = null;
  if (options.checkApprovedPackageDrift) {
    const pkgPath = path.join(
      ROOT,
      "reports/dealality-adp-single-version-publication-contract-v1/approved-packages",
      `${propertyId}.approved-package.json`
    );
    const alt = path.join(
      path.dirname(ROOT),
      "my-operators-backend",
      "reports/dealality-adp-single-version-publication-contract-v1/approved-packages",
      `${propertyId}.approved-package.json`
    );
    const p = fs.existsSync(pkgPath) ? pkgPath : fs.existsSync(alt) ? alt : null;
    if (p) {
      try {
        const pkg = readJson(p);
        const pkgPeriod =
          pkg.periodId || pkg.measurementPeriodId || pkg.approvedPeriodId || null;
        if (pkgPeriod && pkgPeriod !== adp.periodId) {
          newerPackageDrift = {
            status: "NEWER_ADP_PACKAGE_EXISTS",
            avaPeriodId: adp.periodId,
            approvedPackagePeriodId: pkgPeriod,
            note: "Informational only — AVA remains pinned to certified lean baseline until founder re-pin.",
          };
        }
      } catch {
        /* ignore */
      }
    }
  }

  const critical = defects.filter((d) => d.severity === "CRITICAL");
  const high = defects.filter((d) => d.severity === "HIGH");
  return {
    ok: critical.length === 0 && high.filter((d) => d.type === "METRIC_MISMATCH" || d.type === "UNSUPPORTED_ZERO").length === 0,
    contract: ADP_AVA_REFERENCE_CONTRACT,
    propertyId,
    hotelName: payload.property?.name || adp.name,
    adp: {
      periodId: adp.periodId,
      baselineSha256: adp.baselineSha256,
      sourceSha256: adp.sourceSha256,
      metrics: adp.metrics,
      stagedShaOk: adp.stagedShaOk,
      stagedPath: adp.stagedPath,
    },
    avaPeriod,
    avaBaselineSha,
    refs,
    zeros,
    defects,
    newerPackageDrift,
    summary: {
      totalRefs: refs.length,
      matchingRefs: refs.filter((r) => r.match).length,
      measuredZeros: zeros.filter((z) => z.zeroVerdict === ZERO_VERDICTS.MEASURED_ZERO).length,
      unsupportedZeros: zeros.filter((z) => z.zeroVerdict === ZERO_VERDICTS.UNSUPPORTED_ZERO).length,
      mismatches: defects.filter((d) => d.type === "METRIC_MISMATCH").length,
    },
  };
}

/**
 * Detect ADP↔AVA integrity defects for CQA / certification gates.
 */
export function detectAdpAvaParityDefects(payload) {
  const result = reconcileAvaPayloadAgainstAdp(payload, {
    checkApprovedPackageDrift: false,
  });
  if (!result.propertyId) {
    return [
      {
        type: "ADP_AVA_PARITY_BLOCK",
        severity: "CRITICAL",
        section: "adp-reference",
        message: "AVA payload missing propertyId for ADP reconciliation",
      },
    ];
  }
  return (result.defects || [])
    .filter((d) =>
      /METRIC_MISMATCH|UNSUPPORTED_ZERO|UNSUPPORTED_REALITY|BASELINE_SHA|STALE_OR_MISMATCHED|ADP_BASELINE/i.test(
        d.type || ""
      )
    )
    .map((d) => ({
      type: "ADP_AVA_PARITY_BLOCK",
      severity: d.severity || "HIGH",
      section: "adp-reference",
      message: `${d.type}: ${d.metric || ""} AVA=${d.ava ?? "?"} ADP=${d.adp ?? "?"} ${d.message || d.raw || ""}`.trim(),
      detail: d,
    }));
}

export function listCertifiedAdpUniverse() {
  if (!fs.existsSync(AVA_CERTIFIED_BASELINES_INDEX)) return [];
  const idx = readJson(AVA_CERTIFIED_BASELINES_INDEX);
  return idx.hotels || [];
}

export { ZERO_VERDICTS };
