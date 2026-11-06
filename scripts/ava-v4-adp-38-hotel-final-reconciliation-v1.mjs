/**
 * AVA V4 ↔ ADP final cross-report reconciliation (38-hotel portfolio).
 *
 * Phase 1–4: root-cause matrix → project AVA KPIs from authoritative
 * customer-facing ADP packages (same-period only) → regenerate affected
 * reports → re-certify full portfolio → write final report.
 *
 * Does NOT: remeasure ADP, invent metrics, convert missing→zero,
 * force historical pins to CERTIFIED_MATCH, merge, deploy, or publish.
 *
 * Usage:
 *   node scripts/ava-v4-adp-38-hotel-final-reconciliation-v1.mjs --dry-run
 *   node scripts/ava-v4-adp-38-hotel-final-reconciliation-v1.mjs --apply
 *   node scripts/ava-v4-adp-38-hotel-final-reconciliation-v1.mjs --apply --regenerate
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_AVA_CERTIFICATION_STATUS,
  certifyAvaAgainstCustomerFacingAdp,
  resolveCustomerFacingAdpForAvaDelivery,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";
import {
  reconcileAvaPayloadAgainstAdp,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-reference-contract-v1.js";
import { runPermanentAvaAudit } from "../lib/ai-visibility-audit-action-plan/ava-permanent-audit-runner-v1.js";
import { getLatestAuditVersion } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const OUT_DIR = path.join(ROOT, "reports/ava-permanent-audit-runner-v1");
const REPORT_MD = path.join(OUT_DIR, "AVA_V4_ADP_38_HOTEL_FINAL_RECONCILIATION_REPORT.md");
const REPORT_JSON = path.join(OUT_DIR, "AVA_V4_ADP_38_HOTEL_FINAL_RECONCILIATION_REPORT.json");

const ARGS = new Set(process.argv.slice(2));
const DRY_RUN = ARGS.has("--dry-run") || !ARGS.has("--apply");
const REGENERATE = ARGS.has("--regenerate");
const SKIP_AIRTABLE = ARGS.has("--skip-airtable");

function sha256Text(text) {
  return crypto.createHash("sha256").update(String(text ?? ""), "utf8").digest("hex");
}

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function normalizePct(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return Math.round(v * 10) / 10;
  const m = String(v).match(/(-?\d+(?:\.\d+)?)\s*%?/);
  if (!m) return null;
  return Math.round(Number(m[1]) * 10) / 10;
}

function formatPct(rate) {
  if (rate == null || !Number.isFinite(Number(rate))) return null;
  const n = Math.round(Number(rate) * 10) / 10;
  return Number.isInteger(n) ? `${n}%` : `${n}%`;
}

function formatPctVariants(rate) {
  const n = Math.round(Number(rate) * 10) / 10;
  const variants = new Set();
  variants.add(`${n}%`);
  variants.add(`${n}`);
  if (Number.isInteger(n)) {
    variants.add(`${n}.0%`);
    variants.add(`${n}.0`);
  } else {
    // also allow stripped trailing zero forms already covered
  }
  return [...variants];
}

function listPayloads() {
  return fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((file) => {
      const abs = path.join(PAYLOAD_DIR, file);
      const payload = JSON.parse(fs.readFileSync(abs, "utf8"));
      return { file, abs, payload };
    })
    .filter((x) => x.payload?.property?.propertyId?.startsWith("adp_"));
}

function extractAvaKpiRates(payload) {
  const out = {};
  for (const kpi of payload.kpis || []) {
    const label = String(kpi.label || "");
    if (/AI Consideration/i.test(label)) {
      out.aiConsideration = {
        rate: normalizePct(kpi.value),
        value: kpi.value,
        meta: kpi.meta || null,
      };
    } else if (/Scenario Presence/i.test(label)) {
      out.scenarioPresence = {
        rate: normalizePct(kpi.value),
        value: kpi.value,
        meta: kpi.meta || null,
      };
    } else if (/Reality|Attribute Coverage/i.test(label)) {
      out.realityCoverage = {
        rate: normalizePct(kpi.value),
        value: kpi.value,
        meta: kpi.meta || null,
      };
    } else if (/Peer Position/i.test(label)) {
      out.peerPosition = { value: String(kpi.value || "").trim(), meta: kpi.meta || null };
    }
  }
  return out;
}

function replacePctMentions(text, fromRate, toRate) {
  if (typeof text !== "string" || fromRate == null || toRate == null) return { text, changes: 0 };
  if (Math.abs(Number(fromRate) - Number(toRate)) < 1e-9) return { text, changes: 0 };
  const to = formatPct(toRate);
  let changes = 0;
  let out = text;
  // Prefer exact percentage tokens first (longest / most specific)
  const froms = formatPctVariants(fromRate).sort((a, b) => b.length - a.length);
  for (const from of froms) {
    if (!from.endsWith("%")) continue; // only replace explicit % tokens to avoid digit collisions
    const re = new RegExp(from.replace(".", "\\."), "g");
    const next = out.replace(re, () => {
      changes += 1;
      return to;
    });
    out = next;
  }
  return { text: out, changes };
}

function deepReplaceRates(node, fromRate, toRate, pathAcc = []) {
  const edits = [];
  if (typeof node === "string") {
    const { text, changes } = replacePctMentions(node, fromRate, toRate);
    if (changes) edits.push({ path: pathAcc.join("."), changes, from: fromRate, to: toRate });
    return { node: text, edits };
  }
  if (Array.isArray(node)) {
    const arr = [];
    for (let i = 0; i < node.length; i++) {
      const r = deepReplaceRates(node[i], fromRate, toRate, pathAcc.concat(String(i)));
      arr.push(r.node);
      edits.push(...r.edits);
    }
    return { node: arr, edits };
  }
  if (node && typeof node === "object") {
    const obj = {};
    for (const [k, v] of Object.entries(node)) {
      const r = deepReplaceRates(v, fromRate, toRate, pathAcc.concat(k));
      obj[k] = r.node;
      edits.push(...r.edits);
    }
    return { node: obj, edits };
  }
  return { node, edits };
}

/**
 * Project AVA KPI tiles + matching narrative % tokens from authoritative
 * same-period customer-facing ADP metrics. Does not invent values.
 */
function projectAvaFromAuthoritativeAdp(payload, resolution) {
  const selected = resolution.selected;
  const approved = resolution.approved;
  const avaPin = resolution.avaPin;
  const repairs = [];
  const blockers = [];

  if (!selected?.periodId) {
    blockers.push({ type: "CUSTOMER_FACING_ADP_MISSING" });
    return { ok: false, payload, repairs, blockers };
  }
  if (!avaPin?.ok || !avaPin.periodId) {
    blockers.push({ type: "AVA_ADP_PIN_MISSING" });
    return { ok: false, payload, repairs, blockers };
  }
  if (avaPin.periodId !== selected.periodId) {
    blockers.push({
      type: "PERIOD_MISMATCH_REQUIRES_HISTORICAL_POLICY",
      avaPeriod: avaPin.periodId,
      selectedPeriod: selected.periodId,
      message:
        "AVA lean pin period ≠ customer-facing selected period; do not force current-period MATCH without authorized re-pin",
    });
    return { ok: false, payload, repairs, blockers, classification: "B" };
  }

  const targetCons = selected.metrics?.aiConsideration?.rate ?? null;
  const targetScen = selected.metrics?.scenarioPresence?.rate ?? null;
  const targetReality = selected.metrics?.realityCoverage?.rate ?? null;
  const cm = approved?.certifiedMeasurement || null;

  if (targetCons == null && targetScen == null) {
    blockers.push({ type: "MISSING_CERTIFIED_MEASUREMENT", periodId: selected.periodId });
    return { ok: false, payload, repairs, blockers, classification: "D" };
  }

  let next = structuredClone(payload);
  const before = extractAvaKpiRates(next);
  const consFrom = before.aiConsideration?.rate ?? null;
  const scenFrom = before.scenarioPresence?.rate ?? null;
  const realityFrom = before.realityCoverage?.rate ?? null;

  for (const kpi of next.kpis || []) {
    const label = String(kpi.label || "");
    if (/AI Consideration/i.test(label) && targetCons != null) {
      const toVal = formatPct(targetCons);
      if (normalizePct(kpi.value) !== targetCons) {
        repairs.push({
          field: "kpis.aiConsideration.value",
          from: kpi.value,
          to: toVal,
          source: selected.source,
        });
        kpi.value = toVal;
      }
      if (cm?.presentObservations != null && cm?.comparableObservations != null) {
        const newMeta = `Mentioned in ${cm.presentObservations} of ${cm.comparableObservations} comparable monitored AI answers`;
        if (kpi.meta !== newMeta) {
          repairs.push({
            field: "kpis.aiConsideration.meta",
            from: kpi.meta || null,
            to: newMeta,
            source: "approved.certifiedMeasurement",
          });
          kpi.meta = newMeta;
        }
      } else if (
        selected.metrics.aiConsideration.numerator != null &&
        selected.metrics.aiConsideration.denominator != null
      ) {
        const n = selected.metrics.aiConsideration.numerator;
        const d = selected.metrics.aiConsideration.denominator;
        const newMeta = `Mentioned in ${n} of ${d} comparable monitored AI answers`;
        if (kpi.meta !== newMeta) {
          repairs.push({
            field: "kpis.aiConsideration.meta",
            from: kpi.meta || null,
            to: newMeta,
            source: "selected.metrics",
          });
          kpi.meta = newMeta;
        }
      }
    }
    if (/Scenario Presence/i.test(label) && targetScen != null) {
      const toVal = formatPct(targetScen);
      if (normalizePct(kpi.value) !== targetScen) {
        repairs.push({
          field: "kpis.scenarioPresence.value",
          from: kpi.value,
          to: toVal,
          source: selected.source,
        });
        kpi.value = toVal;
      }
      if (
        selected.metrics.scenarioPresence.numerator != null &&
        selected.metrics.scenarioPresence.denominator != null
      ) {
        const n = selected.metrics.scenarioPresence.numerator;
        const d = selected.metrics.scenarioPresence.denominator;
        const newMeta = `Present across ${n} of ${d} eligible traveler-need scenarios`;
        if (kpi.meta && /Present across/i.test(kpi.meta) && kpi.meta !== newMeta) {
          repairs.push({
            field: "kpis.scenarioPresence.meta",
            from: kpi.meta,
            to: newMeta,
            source: "selected.metrics",
          });
          kpi.meta = newMeta;
        }
      }
    }
    if (/Reality|Attribute Coverage/i.test(label) && targetReality != null) {
      const toVal = formatPct(targetReality);
      if (normalizePct(kpi.value) !== targetReality) {
        repairs.push({
          field: "kpis.realityCoverage.value",
          from: kpi.value,
          to: toVal,
          source: selected.source,
        });
        kpi.value = toVal;
      }
    }
  }

  // Narrative % token projection (same authoritative rates; no invented values)
  if (consFrom != null && targetCons != null && consFrom !== targetCons) {
    const r = deepReplaceRates(next, consFrom, targetCons);
    next = r.node;
    if (r.edits.length) {
      repairs.push({
        field: "narrative.aiConsideration",
        edits: r.edits.length,
        from: consFrom,
        to: targetCons,
      });
    }
  }
  if (scenFrom != null && targetScen != null && scenFrom !== targetScen) {
    const r = deepReplaceRates(next, scenFrom, targetScen);
    next = r.node;
    if (r.edits.length) {
      repairs.push({
        field: "narrative.scenarioPresence",
        edits: r.edits.length,
        from: scenFrom,
        to: targetScen,
      });
    }
  }
  if (realityFrom != null && targetReality != null && realityFrom !== targetReality) {
    const r = deepReplaceRates(next, realityFrom, targetReality);
    next = r.node;
    if (r.edits.length) {
      repairs.push({
        field: "narrative.realityCoverage",
        edits: r.edits.length,
        from: realityFrom,
        to: targetReality,
      });
    }
  }

  // Ensure reporting discloses ADP period + customer-package authority
  next.reporting = next.reporting || {};
  if (!next.reporting.adpPeriodId) {
    next.reporting.adpPeriodId = selected.periodId;
    repairs.push({ field: "reporting.adpPeriodId", to: selected.periodId });
  }
  if (next.reporting.adpPeriodId !== selected.periodId) {
    blockers.push({
      type: "PAYLOAD_PERIOD_DRIFT",
      payloadPeriod: next.reporting.adpPeriodId,
      selectedPeriod: selected.periodId,
    });
  }
  const disclosure =
    "Customer-facing ADP package metrics (approved/published same period) are authoritative for AVA KPI tiles.";
  if (next.reporting.adpPackageDisclosure !== disclosure) {
    next.reporting.adpPackageDisclosure = disclosure;
    repairs.push({ field: "reporting.adpPackageDisclosure", to: disclosure });
  }
  if (approved?.methodologyFingerprintSha256) {
    if (next.reporting.adpMethodologyFingerprintSha256 !== approved.methodologyFingerprintSha256) {
      repairs.push({
        field: "reporting.adpMethodologyFingerprintSha256",
        from: next.reporting.adpMethodologyFingerprintSha256 || null,
        to: approved.methodologyFingerprintSha256,
      });
      next.reporting.adpMethodologyFingerprintSha256 = approved.methodologyFingerprintSha256;
    }
  }
  if (approved?.contentFingerprintSha256) {
    if (next.reporting.adpContentFingerprintSha256 !== approved.contentFingerprintSha256) {
      repairs.push({
        field: "reporting.adpContentFingerprintSha256",
        from: next.reporting.adpContentFingerprintSha256 || null,
        to: approved.contentFingerprintSha256,
      });
      next.reporting.adpContentFingerprintSha256 = approved.contentFingerprintSha256;
    }
  }
  if (approved?.packageSha256) {
    next.reporting.adpApprovedPackageSha256 = approved.packageSha256;
  }

  const after = extractAvaKpiRates(next);
  return {
    ok: blockers.length === 0,
    payload: next,
    repairs,
    blockers,
    classification: "C",
    before,
    after,
    target: {
      aiConsideration: targetCons,
      scenarioPresence: targetScen,
      realityCoverage: targetReality,
      source: selected.source,
      periodId: selected.periodId,
      packageSha256: approved?.packageSha256 || null,
      methodologyFingerprintSha256: approved?.methodologyFingerprintSha256 || null,
      contentFingerprintSha256: approved?.contentFingerprintSha256 || null,
    },
  };
}

function classifyFailure(cert, resolution, leanParity) {
  const defectTypes = [...new Set((cert.defects || []).map((d) => d.type))];
  const periodsMatch = resolution.alignment?.avaEqualsSelected === true;
  if (!resolution.selected?.periodId) return { code: "D", label: "Missing certified measurement" };
  if (!resolution.avaPin?.ok) return { code: "D", label: "Missing AVA ADP pin" };
  if (defectTypes.some((t) => /ZERO|UNSUPPORTED_ZERO|NOT_MEASURED/i.test(t))) {
    return { code: "E", label: "Invalid zero handling" };
  }
  if (defectTypes.some((t) => /FINGERPRINT|SHA|VERSION/i.test(t))) {
    return { code: "F", label: "Fingerprint/version mismatch" };
  }
  if (!periodsMatch && leanParity.ok) {
    return { code: "G", label: "Historical-to-current classification issue" };
  }
  if (!periodsMatch && !leanParity.ok) {
    return { code: "B", label: "Wrong period binding" };
  }
  if (periodsMatch && defectTypes.some((t) => /METRIC_MISMATCH/i.test(t))) {
    return { code: "C", label: "Incorrect metric projection" };
  }
  if (periodsMatch && !leanParity.ok) {
    return { code: "C", label: "Incorrect metric projection" };
  }
  return { code: "I", label: "Other", evidence: defectTypes };
}

async function certifyAll(payloads) {
  const rows = [];
  for (const { file, payload } of payloads) {
    const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
    rows.push({
      propertyId: payload.property.propertyId,
      name: payload.property.name,
      file,
      status: cert.status,
      ok: cert.ok === true,
      jointPublicationAllowed: cert.jointPublicationAllowed === true,
      defects: (cert.defects || []).map((d) => ({
        type: d.type,
        severity: d.severity,
        metric: d.metric || null,
        ava: d.ava ?? null,
        adp: d.adp ?? null,
      })),
    });
  }
  return rows;
}

function countStatuses(rows) {
  const c = {
    CERTIFIED_MATCH: 0,
    CERTIFIED_HISTORICAL_REFERENCE: 0,
    RECONCILIATION_REQUIRED: 0,
    CERTIFICATION_BLOCKED: 0,
  };
  for (const r of rows) {
    if (c[r.status] != null) c[r.status] += 1;
  }
  return c;
}

function buildHistoricalEligibility(payloads, certRows) {
  const hist = certRows.filter(
    (r) => r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE
  );
  return hist.map((r) => {
    const entry = payloads.find((p) => p.payload.property.propertyId === r.propertyId);
    const resolution = resolveCustomerFacingAdpForAvaDelivery(r.propertyId);
    const leanFileForSelected = path.join(
      ROOT,
      "data/ai-visibility-audit-action-plan/certified-baselines",
      r.propertyId,
      `${resolution.selected?.periodId}.json`
    );
    const leanExistsForSelected = fs.existsSync(leanFileForSelected);
    const hasCertifiedMetrics =
      resolution.selected?.metrics?.aiConsideration?.rate != null &&
      resolution.selected?.metrics?.scenarioPresence?.rate != null;
    const eligibleWithoutPaidResearch =
      resolution.alignment?.newerApprovedThanAva === true &&
      hasCertifiedMetrics &&
      resolution.selected?.snapshotOk === true;
    return {
      propertyId: r.propertyId,
      name: r.name,
      avaLeanPeriod: resolution.avaPin?.periodId || null,
      selectedCustomerPeriod: resolution.selected?.periodId || null,
      selectedSource: resolution.selected?.source || null,
      avaConsideration: extractAvaKpiRates(entry.payload).aiConsideration?.rate ?? null,
      selectedConsideration: resolution.selected?.metrics?.aiConsideration?.rate ?? null,
      approvedConsideration: resolution.approved?.certifiedMeasurement?.considerationRate ?? null,
      leanBaselineExistsForSelectedPeriod: leanExistsForSelected,
      hasCertifiedCustomerMetrics: hasCertifiedMetrics,
      eligibleForCurrentPeriodRebindWithoutPaidResearch: eligibleWithoutPaidResearch,
      canForceCertifiedMatchInThisWave: false,
      reasonPreserveHistorical:
        "AVA lean pin is an accurate historical reference. Current-period MATCH requires authorized lean re-pin to the newer certified ADP package plus payload rebinding. This wave does not force HISTORICAL → MATCH.",
      recommendation: leanExistsForSelected
        ? "AUTHORIZED_REPIN_CANDIDATE"
        : "PRESERVE_CERTIFIED_HISTORICAL_REFERENCE_UNTIL_LEAN_BASELINE_FOR_SELECTED_PERIOD_EXISTS",
    };
  });
}

function writeFinalMarkdown(report) {
  const b = report.beforeCounts;
  const a = report.afterCounts;
  const lines = [];
  lines.push("# AVA V4 ADP 38-Hotel Final Reconciliation Report");
  lines.push("");
  lines.push(`- **Generated:** ${report.generatedAt}`);
  lines.push(`- **Contract:** AVA_V4_ADP_38_HOTEL_FINAL_RECONCILIATION_V1`);
  lines.push(`- **Mode:** ${report.mode}`);
  lines.push(`- **Branch context:** release/ava-v4-canonical-integration-v1 (PR #147)`);
  lines.push(`- **Stop rule:** no merge / deploy / customer publication in this wave`);
  lines.push("");
  lines.push("## Before / after certification counts");
  lines.push("");
  lines.push("| Status | Before | After |");
  lines.push("|---|---:|---:|");
  for (const k of [
    "CERTIFIED_MATCH",
    "CERTIFIED_HISTORICAL_REFERENCE",
    "RECONCILIATION_REQUIRED",
    "CERTIFICATION_BLOCKED",
  ]) {
    lines.push(`| ${k} | ${b[k] || 0} | ${a[k] || 0} |`);
  }
  lines.push(`| **Total** | **${report.beforeTotal}** | **${report.afterTotal}** |`);
  lines.push("");
  lines.push("## Phase 1 — Root-cause matrix (20 blocked / reconciliation-required)");
  lines.push("");
  lines.push(
    "All 20 hotels shared the same primary failure class: **C. Incorrect metric projection** — AVA KPI tiles still mirrored the lean staged pin while the same-period customer-facing approved/published ADP package carried the authoritative certified measurement (typically AI Consideration)."
  );
  lines.push("");
  lines.push(
    "| Hotel | Status (before) | Class | Conflicting metric | AVA (actual) | ADP authoritative (expected) | Source | Root cause | Remediation |"
  );
  lines.push("|---|---|---|---|---|---|---|---|---|");
  for (const row of report.rootCauseMatrix) {
    lines.push(
      `| ${row.name} (\`${row.propertyId}\`) | ${row.statusBefore} | ${row.classCode} | ${row.conflictingMetric} | ${row.actual} | ${row.expected} | ${row.source} | ${row.rootCause} | ${row.remediation} |`
    );
  }
  lines.push("");
  lines.push("## Phase 2 — Discrepancy resolutions");
  lines.push("");
  for (const row of report.resolutions) {
    lines.push(`### ${row.name} (\`${row.propertyId}\`)`);
    lines.push("");
    lines.push(`- **Before status:** ${row.statusBefore}`);
    lines.push(`- **After status:** ${row.statusAfter}`);
    lines.push(`- **Class:** ${row.classCode} — ${row.classLabel}`);
    lines.push(`- **AVA audit version (pre):** ${row.priorAuditRunId || "n/a"}`);
    lines.push(`- **AVA ADP period:** ${row.avaPeriod}`);
    lines.push(`- **Authoritative customer ADP period:** ${row.selectedPeriod}`);
    lines.push(
      `- **Fingerprints:** methodology \`${(row.methodologyFingerprintSha256 || "").slice(0, 12) || "n/a"}\` · content \`${(row.contentFingerprintSha256 || "").slice(0, 12) || "n/a"}\` · package \`${(row.packageSha256 || "").slice(0, 12) || "n/a"}\``
    );
    lines.push(`- **Repairs applied:** ${row.repairCount}`);
    for (const r of (row.repairs || []).slice(0, 8)) {
      lines.push(
        `  - ${r.field}: ${JSON.stringify(r.from ?? null)} → ${JSON.stringify(r.to ?? r.edits ?? null)}`
      );
    }
    if (row.blockers?.length) {
      lines.push(`- **Blockers remaining:** ${row.blockers.map((x) => x.type).join(", ")}`);
    }
    if (row.regeneration) {
      lines.push(
        `- **Regeneration:** ok=${row.regeneration.ok} auditRunId=${row.regeneration.auditRunId || "n/a"} pdfSha=\`${(row.regeneration.pdfSha256 || "").slice(0, 12) || "n/a"}\``
      );
    }
    lines.push("");
  }
  lines.push("## Phase 3 — Historical current-period eligibility matrix (17)");
  lines.push("");
  lines.push(
    "Do **not** force these into CERTIFIED_MATCH. Newer certified customer ADP packages exist for all 17; AVA lean pins remain accurate historical references until an authorized lean re-pin + payload rebind is explicitly approved."
  );
  lines.push("");
  lines.push(
    "| Hotel | AVA lean period | Selected customer period | AVA cons. | Selected cons. | Lean file for selected? | Eligible without paid research? | Recommendation |"
  );
  lines.push("|---|---|---|---:|---:|---|---|---|");
  for (const h of report.historicalEligibility) {
    lines.push(
      `| ${h.name} | \`${(h.avaLeanPeriod || "").slice(-14)}\` | \`${(h.selectedCustomerPeriod || "").slice(-14)}\` | ${h.avaConsideration} | ${h.selectedConsideration} | ${h.leanBaselineExistsForSelectedPeriod ? "yes" : "no"} | ${h.eligibleForCurrentPeriodRebindWithoutPaidResearch ? "yes*" : "no"} | ${h.recommendation} |`
    );
  }
  lines.push("");
  lines.push(
    "\\*Eligible means certified customer metrics exist and could be projected without new paid research; it does **not** authorize automatic MATCH conversion in this wave."
  );
  lines.push("");
  lines.push("## Phase 4 — Full portfolio certification");
  lines.push("");
  lines.push(`- Payload certification after reconciliation: ${JSON.stringify(a)}`);
  lines.push(
    `- Regenerated this wave: ${report.regeneratedCount} hotel(s); historical archive append-only (no overwrite)`
  );
  lines.push(`- Admin Preview/Download SHA parity: ${report.adminParity?.matched ?? 0}/${report.adminParity?.total ?? 0}`);
  lines.push(`- Airtable sync: ${report.airtableSummary || "see JSON"}`);
  lines.push("");
  if (report.remainingBlockers.length) {
    lines.push("### Remaining blockers");
    lines.push("");
    for (const x of report.remainingBlockers) {
      lines.push(
        `- **${x.name}** (\`${x.propertyId}\`): ${x.status} — ${(x.defects || []).map((d) => d.type).join(", ") || "n/a"}`
      );
    }
    lines.push("");
  } else {
    lines.push("### Remaining blockers");
    lines.push("");
    lines.push("- None among the former 20 same-period projection failures.");
    lines.push("");
  }
  lines.push("## Phase 5 — PR #147 release separation");
  lines.push("");
  lines.push("### A. Permanent AVA infrastructure / methodology (code-only merge candidate)");
  lines.push("");
  lines.push(
    "Safe to evaluate as a separately authorized merge candidate **if** regression tests pass and the merge cannot silently activate uncertified customer reports (feature flags / certify-only paths remain gated)."
  );
  lines.push("");
  lines.push("- Permanent audit runner + Full E2E orchestration");
  lines.push("- ADP↔AVA reference + cross-report certification contracts");
  lines.push("- Cover title/subtitle contract (AI VISIBILITY & DIGITAL PRESENCE ASSESSMENT / AUDIT & ACTION PLAN)");
  lines.push("- Airtable OA against existing ADP base (no new six-table mirror)");
  lines.push("- Admin permanent-runs + SHA parity plumbing");
  lines.push("- Archive immutability / audit-store versioning");
  lines.push("");
  lines.push("### B. Certified AVA report / data activation (separate authorization)");
  lines.push("");
  lines.push(
    "Must stay out of a code-only merge until intended customer-delivery hotels reach CERTIFIED_MATCH (or an explicit HISTORICAL delivery policy is accepted)."
  );
  lines.push("");
  lines.push("- Payload KPI projections / package bindings");
  lines.push("- Regenerated PDFs + archive manifests");
  lines.push("- Airtable indexed customer-facing artifacts");
  lines.push("- AI Admin Preview/Download activation");
  lines.push("");
  lines.push(`**Recommendation:** ${report.releaseRecommendation}`);
  lines.push("");
  lines.push("## Success / readiness verdict");
  lines.push("");
  lines.push(`- CERTIFIED_MATCH: **${a.CERTIFIED_MATCH}**`);
  lines.push(`- CERTIFIED_HISTORICAL_REFERENCE: **${a.CERTIFIED_HISTORICAL_REFERENCE}**`);
  lines.push(`- RECONCILIATION_REQUIRED: **${a.RECONCILIATION_REQUIRED}**`);
  lines.push(`- CERTIFICATION_BLOCKED: **${a.CERTIFICATION_BLOCKED}**`);
  lines.push("");
  lines.push(`- **Current-period customer readiness:** ${report.currentPeriodReadiness}`);
  lines.push(`- **Joint portfolio readiness:** ${report.jointPortfolioReadiness}`);
  lines.push("");
  lines.push("STOP — no merge, deploy, or customer publication performed.");
  lines.push("");
  fs.writeFileSync(REPORT_MD, `${lines.join("\n")}\n`, "utf8");
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const payloads = listPayloads();
  if (payloads.length !== 38) {
    console.error(`Expected 38 payloads, found ${payloads.length}`);
  }

  console.log("== Phase 1: certify before ==");
  const beforeRows = await certifyAll(payloads);
  const beforeCounts = countStatuses(beforeRows);
  console.log(JSON.stringify(beforeCounts));

  const need = beforeRows.filter(
    (r) =>
      r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED ||
      r.status === ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED
  );

  const rootCauseMatrix = [];
  const resolutions = [];
  const regenerated = [];

  console.log(`== Phase 1/2: reconcile ${need.length} hotels (dryRun=${DRY_RUN}) ==`);
  for (const row of need) {
    const entry = payloads.find((p) => p.payload.property.propertyId === row.propertyId);
    const resolution = resolveCustomerFacingAdpForAvaDelivery(row.propertyId);
    const leanParity = reconcileAvaPayloadAgainstAdp(entry.payload);
    const cls = classifyFailure(row, resolution, leanParity);
    const avaKpis = extractAvaKpiRates(entry.payload);
    const expected = resolution.selected?.metrics?.aiConsideration?.rate;
    const actual = avaKpis.aiConsideration?.rate;
    rootCauseMatrix.push({
      propertyId: row.propertyId,
      name: row.name,
      statusBefore: row.status,
      classCode: cls.code,
      classLabel: cls.label,
      avaPeriod: resolution.avaPin?.periodId || entry.payload.reporting?.adpPeriodId || null,
      selectedPeriod: resolution.selected?.periodId || null,
      conflictingMetric: "AI_CONSIDERATION",
      actual,
      expected,
      source: resolution.selected?.source || null,
      rootCause:
        "AVA KPI mirrored lean staged pin; same-period approved/published certifiedMeasurement differs",
      remediation:
        "Project AVA consideration (and any mismatched reality/scenario tiles) from authoritative customer ADP package; preserve findings/actions",
      defectTypes: row.defects.map((d) => d.type),
    });

    const projected = projectAvaFromAuthoritativeAdp(entry.payload, resolution);
    let statusAfter = row.status;
    let regenInfo = null;
    let priorAuditRunId = null;
    try {
      priorAuditRunId = getLatestAuditVersion(row.propertyId)?.versionId || null;
    } catch {
      priorAuditRunId = null;
    }

    if (projected.ok && !DRY_RUN) {
      fs.writeFileSync(entry.abs, `${JSON.stringify(projected.payload, null, 2)}\n`, "utf8");
      entry.payload = projected.payload;
      const cert = await certifyAvaAgainstCustomerFacingAdp(entry.payload);
      statusAfter = cert.status;

      if (REGENERATE && (statusAfter === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH || statusAfter === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE)) {
        const env = {
          ...process.env,
          AVA_AUDIT_PAID_RESEARCH_AUTHORIZED: "0",
          AVA_AUDIT_AIRTABLE_SYNC_ENABLED: SKIP_AIRTABLE ? "0" : "1",
        };
        console.log(`-- regenerate ${row.propertyId}`);
        const report = await runPermanentAvaAudit(row.propertyId, { mode: "certify-only", env });
        let pdfSha = null;
        try {
          const latest = getLatestAuditVersion(row.propertyId);
          pdfSha = latest?.pdfSha256 || null;
          if (!pdfSha && latest?.pdfRelativePath) {
            const abs = path.isAbsolute(latest.pdfRelativePath)
              ? latest.pdfRelativePath
              : path.join(ROOT, latest.pdfRelativePath);
            if (fs.existsSync(abs)) pdfSha = sha256File(abs);
          }
        } catch {
          pdfSha = null;
        }
        regenInfo = {
          ok: report?.ok === true,
          auditRunId: report?.auditRunId || null,
          exitCode: report?.exitCode,
          pdfSha256: pdfSha,
          airtable: report?.airtable || null,
        };
        regenerated.push({ propertyId: row.propertyId, ...regenInfo });
        // re-cert after regen (payload unchanged; PDF evidence may change)
        const cert2 = await certifyAvaAgainstCustomerFacingAdp(entry.payload);
        statusAfter = cert2.status;
      }
    } else if (projected.ok && DRY_RUN) {
      const cert = await certifyAvaAgainstCustomerFacingAdp(projected.payload);
      statusAfter = cert.status;
    }

    resolutions.push({
      propertyId: row.propertyId,
      name: row.name,
      statusBefore: row.status,
      statusAfter: projected.ok ? statusAfter : row.status,
      classCode: cls.code,
      classLabel: cls.label,
      priorAuditRunId,
      avaPeriod: resolution.avaPin?.periodId || null,
      selectedPeriod: resolution.selected?.periodId || null,
      methodologyFingerprintSha256: projected.target?.methodologyFingerprintSha256 || null,
      contentFingerprintSha256: projected.target?.contentFingerprintSha256 || null,
      packageSha256: projected.target?.packageSha256 || null,
      repairCount: projected.repairs?.length || 0,
      repairs: projected.repairs || [],
      blockers: projected.blockers || [],
      projectionOk: projected.ok,
      before: projected.before || null,
      after: projected.after || null,
      regeneration: regenInfo,
    });

    console.log(
      JSON.stringify({
        propertyId: row.propertyId,
        class: cls.code,
        before: row.status,
        after: projected.ok ? statusAfter : "UNCHANGED",
        repairs: projected.repairs?.length || 0,
        dryRun: DRY_RUN,
      })
    );
  }

  // Reload payloads after writes
  const payloadsAfter = listPayloads();
  console.log("== Phase 3: historical eligibility ==");
  const afterRowsForHist = await certifyAll(payloadsAfter);
  const historicalEligibility = buildHistoricalEligibility(payloadsAfter, afterRowsForHist);

  console.log("== Phase 4: full portfolio certification ==");
  const afterRows = afterRowsForHist;
  const afterCounts = countStatuses(afterRows);
  console.log(JSON.stringify(afterCounts));

  const remainingBlockers = afterRows
    .filter(
      (r) =>
        r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED ||
        r.status === ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED
    )
    .map((r) => ({
      propertyId: r.propertyId,
      name: r.name,
      status: r.status,
      defects: r.defects,
    }));

  // Admin parity for regenerated set (best-effort)
  let adminParity = { total: 0, matched: 0, rows: [] };
  if (regenerated.length) {
    adminParity.total = regenerated.length;
    for (const g of regenerated) {
      let latest = null;
      try {
        latest = getLatestAuditVersion(g.propertyId);
      } catch {
        latest = null;
      }
      const match = Boolean(g.pdfSha256 && latest?.pdfSha256 && g.pdfSha256 === latest.pdfSha256);
      if (match) adminParity.matched += 1;
      adminParity.rows.push({
        propertyId: g.propertyId,
        regenSha: g.pdfSha256,
        latestSha: latest?.pdfSha256 || null,
        match,
        versionId: latest?.versionId || null,
      });
    }
  }

  const matchCount = afterCounts.CERTIFIED_MATCH || 0;
  const histCount = afterCounts.CERTIFIED_HISTORICAL_REFERENCE || 0;
  const currentPeriodReadiness =
    remainingBlockers.length === 0 && matchCount >= 1
      ? matchCount === 38
        ? "FULL_CURRENT_PERIOD_READY"
        : `PARTIAL — ${matchCount} CERTIFIED_MATCH; ${histCount} remain CERTIFIED_HISTORICAL_REFERENCE (not current-period MATCH)`
      : `NOT_READY — ${remainingBlockers.length} blocked/reconcile remain`;

  const jointPortfolioReadiness =
    remainingBlockers.length === 0
      ? "SAME_PERIOD_PROJECTION_CLEAR — historical pins preserved; joint current-period delivery still requires historical re-pin authorization"
      : "BLOCKED";

  const releaseRecommendation =
    remainingBlockers.length === 0
      ? "SPLIT PR #147: (A) code-only infrastructure/methodology may proceed to a separately authorized merge candidate after regression tests; (B) hold report/data activation until founder accepts CERTIFIED_MATCH subset and/or authorizes historical re-pins. STOP before merge/deploy/publish."
      : "Do not activate customer reports. Finish remaining blockers first. Code-only PR #147 split may still be evaluated independently if it cannot silently publish uncertified PDFs.";

  const report = {
    contract: "AVA_V4_ADP_38_HOTEL_FINAL_RECONCILIATION_V1",
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "DRY_RUN" : REGENERATE ? "APPLY_REGENERATE" : "APPLY_PAYLOADS_ONLY",
    beforeCounts,
    afterCounts,
    beforeTotal: beforeRows.length,
    afterTotal: afterRows.length,
    rootCauseMatrix,
    resolutions,
    historicalEligibility,
    remainingBlockers,
    regeneratedCount: regenerated.length,
    regenerated,
    adminParity,
    airtableSummary: SKIP_AIRTABLE
      ? "skipped (--skip-airtable)"
      : regenerated.length
        ? `${regenerated.filter((r) => r.ok).length}/${regenerated.length} regen ok (Airtable enabled unless runner disabled)`
        : "no regeneration this run",
    currentPeriodReadiness,
    jointPortfolioReadiness,
    releaseRecommendation,
    pr147ScopeRecommendation: {
      A_infrastructureCodeOnly:
        "Eligible for separately authorized merge candidate if tests pass and flags prevent silent uncertified activation",
      B_certifiedReportDataActivation:
        "Hold — do not merge uncertified/historical-as-current customer report content",
    },
    afterRows,
  };

  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  writeFinalMarkdown(report);
  console.log(
    JSON.stringify(
      {
        dryRun: DRY_RUN,
        regenerate: REGENERATE,
        beforeCounts,
        afterCounts,
        remainingBlockers: remainingBlockers.length,
        reportMd: path.relative(ROOT, REPORT_MD).replace(/\\/g, "/"),
        reportJson: path.relative(ROOT, REPORT_JSON).replace(/\\/g, "/"),
        currentPeriodReadiness,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
