/**
 * Gate: ADP_BPP_EVIDENCE_CLIENT_READY
 * Audits every current-published BPP-ready property for universal evidence contract.
 */
import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE,
  BPP_CUSTOMER_PUBLISHED_PACK,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import {
  auditBppEvidencePublicationGate,
  resolveBppEvidenceResponseText,
  ADP_BPP_EVIDENCE_PUBLICATION_GATE,
  ADP_BPP_EVIDENCE_RAW_RESPONSE_REQUIRED,
  ADP_BPP_EVIDENCE_COUNT_SUPPORT_EXACT_PARITY,
  ADP_BPP_CORE_EVIDENCE_LENS_ISOLATION,
  BPP_EVIDENCE_LENS,
} from "../lib/ai-demand-positioning/brand-portfolio/adp-bpp-canonical-evidence-set-v1.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_DIR = join(ROOT, "reports/ai-demand-positioning");

function readPack() {
  const candidates = [
    join(ROOT, BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE),
    join(ROOT, BPP_CUSTOMER_PUBLISHED_PACK),
  ];
  for (const p of candidates) {
    if (existsSync(p)) return { path: p, pack: JSON.parse(readFileSync(p, "utf8")) };
  }
  throw new Error("BPP customer pack not found");
}

function isReady(payload) {
  if (!payload) return false;
  if (payload.status === "EXCEPTION_SUPPRESSED") return false;
  if (payload.bppCustomerState === "BPP_EXCEPTION_SUPPRESSED") return false;
  if (payload.bppCustomerState === "BPP_NOT_APPLICABLE") return false;
  return (
    payload.status === "READY" ||
    payload.bppCustomerState === "BPP_READY_POPULATED_FULL" ||
    payload.bppCustomerState === "BPP_READY_POPULATED_RANK_ONLY"
  );
}

function main() {
  const { path: packPath, pack } = readPack();
  const uiSrc = readFileSync(
    join(ROOT, "public/js/ai-demand-positioning/ai-demand-positioning.js"),
    "utf8"
  );

  const uiGates = {
    BPP_EVIDENCE_CONTROLS_PRESENT:
      uiSrc.includes("View Positive Evidence") &&
      uiSrc.includes("View Missing Evidence") &&
      uiSrc.includes("View Portfolio Displacement") &&
      uiSrc.includes("bppReadyPopulated"),
    BPP_RAW_RESPONSE_FIELD_FALLBACK:
      uiSrc.includes("exactResponse") &&
      uiSrc.includes("resolveBppEvidenceAiResponse"),
    BPP_ZERO_EVIDENCE_STATE: uiSrc.includes("No positive portfolio evidence this period"),
  };

  const rows = [];
  let fail = 0;

  for (const [propertyId, payload] of Object.entries(pack.payloads || {}).sort()) {
    if (!isReady(payload)) {
      rows.push({
        propertyId,
        bppStatus: payload.status || payload.bppCustomerState,
        populated: false,
        pass: true,
        skip: "not_ready_or_suppressed",
      });
      continue;
    }

    const gate = auditBppEvidencePublicationGate(payload);
    const pos = payload.evidence?.positive || [];
    const miss = payload.evidence?.missing || [];
    const disp = payload.evidence?.displacement || [];

    const blankPos = pos.filter((e) => !resolveBppEvidenceResponseText(e).trim()).length;
    const blankMiss = miss.filter((e) => !resolveBppEvidenceResponseText(e).trim()).length;
    const blankDisp = disp.filter((e) => !resolveBppEvidenceResponseText(e).trim()).length;
    const lensLeak = [...pos, ...miss, ...disp].filter(
      (e) => e.lens && e.lens !== BPP_EVIDENCE_LENS && e.lens !== "bpp"
    ).length;

    const pass =
      gate.pass &&
      blankPos === 0 &&
      blankMiss === 0 &&
      blankDisp === 0 &&
      lensLeak === 0;

    if (!pass) fail += 1;

    rows.push({
      propertyId,
      bppStatus: payload.bppCustomerState || payload.status,
      populated: true,
      positive: pos.length,
      missing: miss.length,
      displacement: disp.length,
      controlsExpected: true,
      rawResponseBlank: blankPos + blankMiss + blankDisp,
      lensLeak,
      defects: gate.defects || [],
      pass,
    });
  }

  const readyRows = rows.filter((r) => r.populated);
  const report = {
    gate: "ADP_BPP_EVIDENCE_CLIENT_READY",
    packPath,
    uiGates,
    doctrine: {
      ADP_BPP_EVIDENCE_PUBLICATION_GATE,
      ADP_BPP_EVIDENCE_RAW_RESPONSE_REQUIRED,
      ADP_BPP_EVIDENCE_COUNT_SUPPORT_EXACT_PARITY,
      ADP_BPP_CORE_EVIDENCE_LENS_ISOLATION,
    },
    summary: {
      totalPayloads: rows.length,
      ready: readyRows.length,
      readyPass: readyRows.filter((r) => r.pass).length,
      readyFail: fail,
      uiPass: Object.values(uiGates).every(Boolean),
    },
    rows,
  };

  mkdirSync(OUT_DIR, { recursive: true });
  writeFileSync(join(OUT_DIR, "adp-bpp-evidence-client-ready-v1.json"), JSON.stringify(report, null, 2));

  console.log(JSON.stringify(report.summary, null, 2));
  if (fail || !report.summary.uiPass) {
    console.error("FAIL rows:", rows.filter((r) => !r.pass));
    process.exit(1);
  }
  console.log("ADP_BPP_EVIDENCE_CLIENT_READY PASS");
}

main();
