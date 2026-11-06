/**
 * Project AVA narrative attribute-recognition rates from authoritative ADP
 * realityGap for hotels blocked after current-period lean re-pin.
 *
 * Does not invent rates — only replaces AVA-cited percentages with ADP
 * measured recognition rates from the pinned current-period snapshot.
 *
 *   node scripts/ava-v4-project-attribute-rates-from-adp-v1.mjs --dry-run
 *   node scripts/ava-v4-project-attribute-rates-from-adp-v1.mjs --apply --regenerate
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  certifyAvaAgainstCustomerFacingAdp,
  ADP_AVA_CERTIFICATION_STATUS,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";
import { reconcileAvaPayloadAgainstAdp } from "../lib/ai-visibility-audit-action-plan/adp-ava-reference-contract-v1.js";
import { runPermanentAvaAudit } from "../lib/ai-visibility-audit-action-plan/ava-permanent-audit-runner-v1.js";
import { getLatestAuditVersion } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const ARGS = new Set(process.argv.slice(2));
const DRY_RUN = ARGS.has("--dry-run") || !ARGS.has("--apply");
const REGENERATE = ARGS.has("--regenerate");
const SKIP_AIRTABLE = ARGS.has("--skip-airtable");

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function formatPct(rate) {
  const n = Math.round(Number(rate) * 10) / 10;
  return `${n}%`;
}

function listPayloads() {
  return fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((file) => {
      const abs = path.join(PAYLOAD_DIR, file);
      return { file, abs, payload: JSON.parse(fs.readFileSync(abs, "utf8")) };
    })
    .filter((x) => x.payload?.property?.propertyId?.startsWith("adp_"));
}

function replaceRateInRaw(raw, fromRate, toRate) {
  if (!raw || fromRate == null || toRate == null) return null;
  const to = formatPct(toRate);
  const pairs = [
    [`about ${fromRate}%`, `about ${to}`],
    [`approximately ${fromRate}%`, `approximately ${to}`],
    [`around ${fromRate}%`, `around ${to}`],
    [`${fromRate}%`, to],
  ];
  if (Number.isInteger(Number(fromRate))) {
    pairs.push([`${fromRate}.0%`, to]);
  }
  let next = raw;
  let changed = false;
  for (const [from, replacement] of pairs.sort((a, b) => b[0].length - a[0].length)) {
    if (next.includes(from)) {
      next = next.split(from).join(replacement);
      changed = true;
      break;
    }
  }
  if (!changed && Number(fromRate) === 0) {
    const re = /(\b(?:was|were|recorded|at|of|for)\s+)0(?:\.0)?%/i;
    if (re.test(next)) {
      next = next.replace(re, `$1${to}`);
      changed = true;
    }
  }
  return changed ? next : null;
}

function deepReplaceExact(node, from, to, edits) {
  if (typeof node === "string") {
    if (!from || !node.includes(from)) return node;
    edits.push({ from, to });
    return node.split(from).join(to);
  }
  if (Array.isArray(node)) return node.map((v) => deepReplaceExact(v, from, to, edits));
  if (node && typeof node === "object") {
    const out = {};
    for (const [k, v] of Object.entries(node)) out[k] = deepReplaceExact(v, from, to, edits);
    return out;
  }
  return node;
}

function collectMismatchRepairs(lean) {
  const repairs = [];
  const seen = new Set();
  const push = (d) => {
    if (d.ava == null || d.adp == null) return;
    if (Math.abs(Number(d.ava) - Number(d.adp)) < 0.05) return;
    const raw = d.raw || d.detail?.raw || null;
    const metric = d.metric || d.label || d.detail?.metric || null;
    const key = `${metric}|${d.ava}|${d.adp}|${raw}`;
    if (seen.has(key)) return;
    seen.add(key);
    repairs.push({
      metric,
      ava: d.ava,
      adp: d.adp,
      raw,
      type: d.type,
    });
  };
  for (const d of lean.defects || []) push(d);
  for (const z of lean.zeros || []) {
    if (
      z.zeroVerdict === "METRIC_MISMATCH" ||
      z.zeroVerdict === "UNSUPPORTED_ZERO" ||
      z.match === false
    ) {
      push({
        metric: z.label,
        ava: z.ava,
        adp: z.adp,
        raw: z.raw,
        type: z.zeroVerdict,
      });
    }
  }
  return repairs;
}

function rewriteCompoundZeroList(raw, groupRepairs) {
  // "recorded 0% for A, B, and C" → "recorded A at X%, B at Y%, and C at Z%"
  if (!raw || !/0(?:\.0)?%/.test(raw) || groupRepairs.length < 2) return null;
  const parts = groupRepairs
    .filter((r) => r.adp != null && r.metric)
    .map((r) => `${r.metric} at ${formatPct(r.adp)}`);
  if (parts.length < 2) return null;
  let list;
  if (parts.length === 2) list = `${parts[0]} and ${parts[1]}`;
  else list = `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
  if (/recorded 0%\s+recognition for/i.test(raw)) {
    return raw.replace(/recorded 0%\s+recognition for[\s\S]*/i, `recorded recognition for ${list}`);
  }
  if (/recorded 0%\s+for/i.test(raw)) {
    return raw.replace(/recorded 0%\s+for[\s\S]*/i, `recorded ${list}`);
  }
  if (/0%\s+recognition for both/i.test(raw)) {
    return raw.replace(/0%\s+recognition for both[\s\S]*/i, `recognition for ${list}`);
  }
  if (/0%\s+recognition for/i.test(raw)) {
    return raw.replace(/0%\s+recognition for[\s\S]*/i, `recognition for ${list}`);
  }
  if (/0%\s+for/i.test(raw)) {
    return raw.replace(/0%\s+for[\s\S]*/i, `${list}`);
  }
  return null;
}

function projectAttributeRates(payload) {
  let next = structuredClone(payload);
  const allEdits = [];
  const applied = [];
  for (let pass = 0; pass < 12; pass++) {
    const lean = reconcileAvaPayloadAgainstAdp(next);
    if (lean.ok) break;
    const repairs = collectMismatchRepairs(lean);
    if (!repairs.length) break;
    let progress = false;

    // Group repairs that share the same compound raw (list zeros)
    const byRaw = new Map();
    for (const r of repairs) {
      if (!r.raw) continue;
      if (!byRaw.has(r.raw)) byRaw.set(r.raw, []);
      byRaw.get(r.raw).push(r);
    }
    for (const [raw, group] of byRaw.entries()) {
      if (group.length < 2) continue;
      const rewritten = rewriteCompoundZeroList(raw, group);
      if (!rewritten || rewritten === raw) continue;
      const edits = [];
      next = deepReplaceExact(next, raw, rewritten, edits);
      if (edits.length) {
        progress = true;
        applied.push({
          kind: "compound_list",
          raw,
          replacement: rewritten,
          editCount: edits.length,
          metrics: group.map((g) => g.metric),
        });
        allEdits.push(...edits);
      }
    }

    for (const r of repairs) {
      if (r.raw) {
        const updated = replaceRateInRaw(r.raw, r.ava, r.adp);
        if (updated && updated !== r.raw) {
          const edits = [];
          next = deepReplaceExact(next, r.raw, updated, edits);
          if (edits.length) {
            progress = true;
            applied.push({ ...r, replacement: updated, editCount: edits.length });
            allEdits.push(...edits);
          }
        }
      }
      if (r.metric != null && r.ava != null && r.adp != null) {
        const label = String(r.metric);
        const fromPct = formatPct(r.ava);
        const toPct = formatPct(r.adp);
        const templates = [
          [`${label} recognition was ${fromPct}`, `${label} recognition was ${toPct}`],
          [`${label} recognition about ${fromPct}`, `${label} recognition about ${toPct}`],
          [`${label} recognition was about ${fromPct}`, `${label} recognition was about ${toPct}`],
          [`0% recognition for ${label}`, `${toPct} recognition for ${label}`],
          [`recorded 0% for ${label}`, `recorded ${toPct} for ${label}`],
          [`recorded ${fromPct} for ${label}`, `recorded ${toPct} for ${label}`],
          [`recorded 0% ${label}`, `recorded ${toPct} ${label}`],
          [`recorded 0% ${label} recognition`, `recorded ${toPct} ${label} recognition`],
          [`${label} was ${fromPct}`, `${label} was ${toPct}`],
          [`${fromPct} for ${label}`, `${toPct} for ${label}`],
          [`with ${label} at ${fromPct}`, `with ${label} at ${toPct}`],
          [`${fromPct} Reality Coverage`, `${toPct} Reality Coverage`],
          [`Reality Coverage ${fromPct}`, `Reality Coverage ${toPct}`],
        ];
        for (const [from, to] of templates) {
          const edits = [];
          next = deepReplaceExact(next, from, to, edits);
          if (edits.length) {
            progress = true;
            applied.push({ metric: label, ava: r.ava, adp: r.adp, from, to, editCount: edits.length });
            allEdits.push(...edits);
          }
        }
      }
    }

    // Reality coverage KPI tile
    const realityDefect = repairs.find((r) => /REALITY_COVERAGE/i.test(String(r.metric || "")));
    if (realityDefect?.adp != null) {
      for (const kpi of next.kpis || []) {
        if (/Reality|Attribute Coverage/i.test(String(kpi.label || ""))) {
          const toVal = formatPct(realityDefect.adp);
          if (kpi.value !== toVal) {
            applied.push({
              field: "kpis.realityCoverage",
              from: kpi.value,
              to: toVal,
            });
            kpi.value = toVal;
            progress = true;
          }
        }
      }
    }

    if (!progress) break;
  }
  const leanFinal = reconcileAvaPayloadAgainstAdp(next);
  return {
    payload: next,
    applied,
    leanOk: leanFinal.ok === true,
    remainingDefects: leanFinal.defects || [],
    remainingZeros: (leanFinal.zeros || []).filter(
      (z) =>
        z.zeroVerdict === "METRIC_MISMATCH" ||
        z.zeroVerdict === "UNSUPPORTED_ZERO" ||
        z.zeroVerdict === "NOT_MEASURED"
    ),
  };
}

async function main() {
  const payloads = listPayloads();
  const targets = [];
  for (const entry of payloads) {
    const cert = await certifyAvaAgainstCustomerFacingAdp(entry.payload);
    if (
      cert.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED ||
      cert.status === ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED
    ) {
      targets.push({ ...entry, statusBefore: cert.status });
    }
  }
  console.log(JSON.stringify({ blockedCount: targets.length, dryRun: DRY_RUN }));

  const results = [];
  for (const t of targets) {
    const projected = projectAttributeRates(t.payload);
    let statusAfter = t.statusBefore;
    let regen = null;
    if (!DRY_RUN) {
      fs.writeFileSync(t.abs, `${JSON.stringify(projected.payload, null, 2)}\n`, "utf8");
      const cert = await certifyAvaAgainstCustomerFacingAdp(projected.payload);
      statusAfter = cert.status;
      if (REGENERATE && statusAfter === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH) {
        console.log(`-- regenerate ${t.payload.property.propertyId}`);
        const report = await runPermanentAvaAudit(t.payload.property.propertyId, {
          mode: "certify-only",
          env: {
            ...process.env,
            AVA_AUDIT_PAID_RESEARCH_AUTHORIZED: "0",
            AVA_AUDIT_AIRTABLE_SYNC_ENABLED: SKIP_AIRTABLE ? "0" : "1",
          },
        });
        let pdfSha = null;
        try {
          const latest = getLatestAuditVersion(t.payload.property.propertyId);
          if (latest?.pdfRelativePath) {
            const abs = path.isAbsolute(latest.pdfRelativePath)
              ? latest.pdfRelativePath
              : path.join(ROOT, latest.pdfRelativePath);
            if (fs.existsSync(abs)) pdfSha = sha256File(abs);
          }
        } catch {
          pdfSha = null;
        }
        regen = {
          ok: report?.ok === true,
          auditRunId: report?.auditRunId || null,
          pdfSha256: pdfSha,
        };
        const cert2 = await certifyAvaAgainstCustomerFacingAdp(
          JSON.parse(fs.readFileSync(t.abs, "utf8"))
        );
        statusAfter = cert2.status;
      }
    } else {
      const cert = await certifyAvaAgainstCustomerFacingAdp(projected.payload);
      statusAfter = cert.status;
    }

    results.push({
      propertyId: t.payload.property.propertyId,
      name: t.payload.property.name,
      statusBefore: t.statusBefore,
      statusAfter,
      leanOk: projected.leanOk,
      applied: projected.applied.length,
      remainingDefectTypes: [
        ...new Set(projected.remainingDefects.map((d) => d.type || d.metric)),
      ],
      remainingZeroLabels: projected.remainingZeros.map((z) => z.label),
      regeneration: regen,
    });
    console.log(
      JSON.stringify({
        propertyId: t.payload.property.propertyId,
        before: t.statusBefore,
        after: statusAfter,
        leanOk: projected.leanOk,
        applied: projected.applied.length,
        remaining: projected.remainingDefects.length,
      })
    );
  }

  // portfolio recount
  let match = 0;
  let hist = 0;
  let recon = 0;
  let blocked = 0;
  for (const { payload } of listPayloads()) {
    const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
    if (cert.status === "CERTIFIED_MATCH") match++;
    else if (cert.status === "CERTIFIED_HISTORICAL_REFERENCE") hist++;
    else if (cert.status === "RECONCILIATION_REQUIRED") recon++;
    else blocked++;
  }
  const out = {
    dryRun: DRY_RUN,
    results,
    portfolio: {
      CERTIFIED_MATCH: match,
      CERTIFIED_HISTORICAL_REFERENCE: hist,
      RECONCILIATION_REQUIRED: recon,
      CERTIFICATION_BLOCKED: blocked,
    },
  };
  fs.writeFileSync(
    path.join(
      ROOT,
      "reports/ava-permanent-audit-runner-v1/AVA_V4_ATTRIBUTE_RATE_PROJECTION_RESULTS.json"
    ),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log(JSON.stringify(out.portfolio, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
