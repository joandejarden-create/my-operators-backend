/**
 * Phase 2G — sanitize reconstructed presentation for customer-facing parity.
 * Strips internal meta language, slot-key echoes, and generator padding suffixes.
 * Does NOT copy production Presentation bodies.
 * If content would be empty after sanitize → RESEARCH_REQUIRED marker (internal only).
 */
import { repairCandidatePresentation } from "./be-phase2f-presentation-repair-v1.js";
import { evaluateAntiPadding } from "./be-phase2g-anti-padding-v1.js";
import { evaluateCandidateOverlay } from "./be-phase2f-visual-quality-gates-v1.js";
import { nz, wordCount } from "./be-phase2f-content-presentation-contract-v1.js";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";

export const BE_PHASE2G_PRESENTATION_SANITIZE_V1 = "BE_PHASE2G_PRESENTATION_SANITIZE_V1";

const FORBIDDEN_BODY_RES = [
  /Evidence-grounded reconstruction[^.]*\.?/gi,
  /Focus this card on[^.]*\.?/gi,
  /timing and documentation only\.?/gi,
  /RESEARCH_REQUIRED[^.]*\.?/gi,
  /do not invent[^.]*\.?/gi,
  /diligence framing only[^.]*\.?/gi,
  /\[[a-z0-9_.]+\]\s*/gi,
  /Owner diligence note\.?/gi,
  /Internal Preview\.?/gi,
  /phase2[a-z0-9-]*/gi,
  /[A-Za-z][^.\n]{0,60}·\s*[a-z0-9_.]+\s*:\s*/gi,
];

function scrub(text, slotKey) {
  let t = nz(text);
  for (const re of FORBIDDEN_BODY_RES) t = t.replace(re, " ");
  if (slotKey) {
    t = t.replace(new RegExp(slotKey.replace(/\./g, "\\."), "gi"), "");
  }
  t = t.replace(/\s+/g, " ").replace(/\s+([,.])/g, "$1").trim();
  // Drop duplicated consecutive sentences
  const parts = t.split(/(?<=[.!?])\s+/);
  const out = [];
  const seen = new Set();
  for (const p of parts) {
    const n = p.toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
    if (n.length < 8) {
      out.push(p);
      continue;
    }
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(p);
  }
  return out.join(" ").trim();
}

function uniqueProofTitle(slotKey, title, body, brandName) {
  const t = nz(title);
  if (!/^Proof\s*\d+$/i.test(t) && t) return t.slice(0, 72);
  // Derive from first clause of body
  const first = nz(body).split(/[.!?]/)[0] || "";
  if (wordCount(first) >= 3 && wordCount(first) <= 10) return first;
  const n = (slotKey.match(/(\d+)$/) || [])[1] || "";
  return `${brandName.split(" ")[0]} proof ${n}`.trim();
}

/**
 * Sanitize overlay after Phase 2F structural repair.
 */
export function sanitizePresentationCandidate(overlay, opts = {}) {
  const name = opts.brandName || overlay.brandName || "Brand";
  const parent = opts.parentCompany || overlay.parentCompany || "";
  // Start from Phase 2F repair when evidence pack available
  let base = overlay;
  if (opts.runPhase2fRepair !== false) {
    const repaired = repairCandidatePresentation(overlay, opts);
    base = repaired.repaired;
  }

  const blocks = (base.brandExplorer?.blocks || []).map((b) => ({ ...b }));
  const researchGaps = [];

  for (const b of blocks) {
    const key = nz(b.slotKey);
    b.title = scrub(b.title, key);
    b.body = scrub(b.body, key);

    if (/^overview\.proof\.\d+$/.test(key)) {
      b.title = uniqueProofTitle(key, b.title, b.body, name);
      if (/^Proof\s*\d+$/i.test(b.title) || wordCount(b.body) < 20) {
        researchGaps.push({ slotKey: key, reason: "proof_point_needs_brand_specific_evidence" });
      }
    }
    if (/^Gallery\s*\d+$/i.test(b.title)) {
      b.title = b.imageUrl ? "Property imagery" : "Imagery unavailable";
    }

    // Negotiable / economics: if body still near-generic after scrub, mark gap instead of padding
    if (/^economics\.(negotiab|rarely_negotiable)/.test(key) || /^economics\.kpi\./.test(key)) {
      if (wordCount(b.body) < 12) {
        researchGaps.push({ slotKey: key, reason: "insufficient_distinct_economics_evidence" });
        b.body = "";
        b.title = scrub(b.title || key.split(".").pop().replace(/_/g, " "), key);
        b._internalStatus = "RESEARCH_REQUIRED";
      }
    }

    // Never leave internal status strings in customer body
    if (/RESEARCH_REQUIRED|Focus this card|Evidence-grounded/i.test(b.body)) {
      b.body = scrub(b.body, key);
    }
  }

  // Cross-profile dedupe: if two bodies still >0.9 similar, clear the later one as research gap
  for (let i = 0; i < blocks.length; i++) {
    for (let j = i + 1; j < blocks.length; j++) {
      const a = nz(blocks[i].body);
      const b = nz(blocks[j].body);
      if (wordCount(a) < 25 || wordCount(b) < 25) continue;
      const wa = new Set(a.toLowerCase().split(/\s+/).filter((w) => w.length > 3));
      const wb = new Set(b.toLowerCase().split(/\s+/).filter((w) => w.length > 3));
      let inter = 0;
      for (const w of wa) if (wb.has(w)) inter++;
      const union = new Set([...wa, ...wb]).size || 1;
      if (inter / union >= 0.9) {
        researchGaps.push({
          slotKey: nz(blocks[j].slotKey),
          reason: `near_duplicate_of_${nz(blocks[i].slotKey)}`,
        });
        blocks[j].body = "";
        blocks[j]._internalStatus = "RESEARCH_REQUIRED";
      }
    }
  }

  const sanitized = {
    ...base,
    brandName: name,
    parentCompany: parent,
    candidateVersion: "phase2g-parity",
    sanitizedAt: new Date().toISOString(),
    sanitizeContract: BE_PHASE2G_PRESENTATION_SANITIZE_V1,
    researchGaps,
    brandExplorer: { ...(base.brandExplorer || {}), blocks },
    writes: 0,
    usedPresentationBodiesAsInput: false,
  };

  // Strip internal markers from exported customer blocks
  for (const b of sanitized.brandExplorer.blocks) {
    delete b._internalStatus;
  }

  const antiPadding = evaluateAntiPadding(sanitized.brandExplorer.blocks, { brandName: name });
  const visual = evaluateCandidateOverlay(sanitized, { brandName: name });

  return {
    sanitized,
    antiPadding,
    visual,
    researchGaps,
    metrics: {
      visual: visual.metrics,
      antiPadding: antiPadding.metrics,
      researchGapCount: researchGaps.length,
      founderApprovalStatus: "NOT_FOUNDER_APPROVED",
      note: "Phase 2G automated metrics ≠ founder approval. Gaps must be filled by research, not padding.",
    },
  };
}

export function sanitizeBrandPairFromDisk() {
  const root = repoRoot();
  const load = (rel) => JSON.parse(readFileSync(join(root, rel), "utf8"));
  const curio = load("public/brand-explorer-phase2e-preview/curio-collection-quality-candidate.json");
  const ascend = load("public/brand-explorer-phase2e-preview/ascend-quality-candidate.json");
  return {
    curio: sanitizePresentationCandidate(curio, {
      brandName: "Curio Collection by Hilton",
      parentCompany: "Hilton Worldwide",
      evidencePackPath: "reports/brand-explorer/phase2e-quality-curio/01-evidence-pack.json",
    }),
    ascend: sanitizePresentationCandidate(ascend, {
      brandName: "Ascend Hotel Collection",
      parentCompany: "Choice Hotels",
      evidencePackPath: "reports/brand-explorer/phase2e-quality-ascend/01-evidence-pack.json",
    }),
  };
}

export function loadJsonIfExists(rel) {
  const p = join(repoRoot(), rel);
  return existsSync(p) ? JSON.parse(readFileSync(p, "utf8")) : null;
}
