/**
 * Brand Explorer Phase 2F — Content & Presentation Contract loader + helpers.
 * Contract: BE_CONTENT_PRESENTATION_CONTRACT_V1
 */
import { readFileSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";

export const BE_CONTENT_PRESENTATION_CONTRACT_V1 = "BE_CONTENT_PRESENTATION_CONTRACT_V1";

let _contract = null;

export function loadContentPresentationContract({ reload = false } = {}) {
  if (_contract && !reload) return _contract;
  const path = join(repoRoot(), "configs/brand-explorer-content-presentation-contract-v1.json");
  _contract = JSON.parse(readFileSync(path, "utf8"));
  return _contract;
}

export function nz(s) {
  return s == null ? "" : String(s).trim();
}

export function wordCount(s) {
  return nz(s)
    .split(/\s+/)
    .filter(Boolean).length;
}

export function ruleForSlot(slotKey) {
  const contract = loadContentPresentationContract();
  const key = nz(slotKey);
  for (const rule of contract.slotRules || []) {
    if (new RegExp(rule.match).test(key)) return rule;
  }
  return null;
}

export function semanticTypeDef(typeId) {
  const contract = loadContentPresentationContract();
  return (contract.semanticTypes || {})[typeId] || null;
}

export function titleLooksLikeParagraph(title) {
  const t = nz(title);
  if (!t) return false;
  if (t.length > 80) return true;
  if (wordCount(t) > 12) return true;
  if (/\.\s+[A-Z]/.test(t)) return true;
  if (/evidence\.|official\.|phase2|slotKey|RESEARCH_REQUIRED/i.test(t)) return true;
  return false;
}

export function isLikelyLogoUrl(url) {
  const u = nz(url);
  if (!u) return false;
  if (/logo|wordmark|brand-mark|_HR-1\.png|favicon|hotel-brand-lo|social_image|brand[-_]?lockup/i.test(u)) {
    return true;
  }
  // Brand-name-only graphics (no property identity in the filename) are logos, not hotel photos.
  try {
    const path = new URL(u).pathname;
    const base = decodeURIComponent(path.split("/").pop() || "")
      .replace(/\.(webp|png|jpe?g|gif|svg)$/i, "")
      .replace(/_/g, "-");
    if (/^(curio|ascend|autograph|tapestry|kimpton|tribute)-collection(-by-\w+)?$/i.test(base)) {
      return true;
    }
  } catch {
    /* ignore invalid URL */
  }
  return false;
}

export function isHttpImageUrl(url) {
  return /^https?:\/\/\S+\.(?:jpg|jpeg|png|webp|gif)(\?\S*)?$/i.test(nz(url)) ||
    /^https?:\/\/\S+\.(?:jpg|jpeg|png|webp)/i.test(nz(url)) ||
    (/^https?:\/\//i.test(nz(url)) && /cloudinary|scene7|airtableusercontent|stories-editor|dam\.|media\./i.test(nz(url)));
}

export function isPlaceholderImage(url, title, body) {
  const blob = `${nz(url)} ${nz(title)} ${nz(body)}`.toLowerCase();
  if (!nz(url)) return true;
  if (/placeholder|image\s*missing|no[- ]image|empty image/i.test(blob)) return true;
  return false;
}

/** Jaccard similarity on word sets (0–1). */
export function textSimilarity(a, b) {
  const wa = new Set(
    nz(a)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 2),
  );
  const wb = new Set(
    nz(b)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 2),
  );
  if (!wa.size || !wb.size) return 0;
  let inter = 0;
  for (const w of wa) if (wb.has(w)) inter += 1;
  const union = new Set([...wa, ...wb]).size;
  return union ? inter / union : 0;
}

export function collectionCardinality(slotKey) {
  const contract = loadContentPresentationContract();
  return (contract.collectionCardinalityFromBaseline || {})[nz(slotKey)] || null;
}
