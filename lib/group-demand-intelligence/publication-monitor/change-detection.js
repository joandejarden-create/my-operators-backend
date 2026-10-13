/**
 * Semantic change detection for publication monitors.
 * Reuses future-watch fingerprinting; adds artifact / list publication classification.
 * Ignores copyright year, tracking params, layout/cookie noise.
 */

import crypto from "node:crypto";
import {
  buildSourceFingerprint,
  detectSourceMaterialChange,
  materialTextSlice,
} from "../future-watch/source-fingerprint-v1.js";
import { CHANGE_CLASS, PUBLICATION_TRIGGER_TYPE } from "./constants.js";
import { detectPublicationTriggersInText } from "./multilingual-terms.js";

const IGNORE_NOISE_RE =
  /cookie|privacy policy|pol[ií]tica de cookies|©\s*20\d{2}|copyright\s*20\d{2}|utm_|fbclid|gclid|tracking/i;

const ARTIFACT_LINK_RE =
  /\.(pdf|xlsx?|docx?)(\?|$)|lista de expositores|directorio de expositores|exhibitor.?list|accepted papers|comunicaciones aceptadas|manual del expositor|programa\.pdf|programme\.pdf/i;

/**
 * Normalize HTML/text for hashing — strip scripts/styles/tags, collapse whitespace,
 * drop common noise tokens.
 */
export function normalizeMonitorContent(htmlOrText = "") {
  let t = String(htmlOrText || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&#(\d+);/g, (_, n) => {
      try {
        return String.fromCharCode(Number(n));
      } catch {
        return " ";
      }
    })
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  // Strip years that are only copyright-like noise when not near publication terms
  t = t.replace(/©\s*20\d{2}/g, " ");
  t = t.replace(/\bcookie[s]?\b.{0,80}/gi, " ");
  return t.replace(/\s+/g, " ").trim();
}

export function hashNormalizedContent(text = "") {
  const n = normalizeMonitorContent(text);
  return crypto.createHash("sha256").update(n.slice(0, 200000)).digest("hex").slice(0, 40);
}

/**
 * Extract hrefs that look like newly published artifacts (PDF/manual/list).
 */
export function extractArtifactLinks(html = "") {
  const links = [];
  const re = /href=["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(String(html || ""))) && links.length < 40) {
    const href = m[1].replace(/&amp;/g, "&");
    if (IGNORE_NOISE_RE.test(href)) continue;
    if (ARTIFACT_LINK_RE.test(href) || ARTIFACT_LINK_RE.test(href.split("/").pop() || "")) {
      links.push(href);
    }
  }
  return [...new Set(links)];
}

/**
 * Classify whether a content change is meaningful for list/programme publication.
 */
export function classifySemanticChange({
  prior = null,
  nextText = "",
  nextHtml = "",
  watchForTypes = [],
  priorArtifactLinks = [],
} = {}) {
  const plain = normalizeMonitorContent(nextText || nextHtml);
  const contentHash = hashNormalizedContent(plain);
  const fp = buildSourceFingerprint({
    url: prior?.url || null,
    text: plain.slice(0, 50000),
  });
  const materialDet = detectSourceMaterialChange(prior?.fingerprint || prior, fp);

  const triggers = detectPublicationTriggersInText(plain, { watchForTypes });
  const artifactLinks = extractArtifactLinks(nextHtml || "");
  const newArtifacts = artifactLinks.filter((u) => !(priorArtifactLinks || []).includes(u));

  // First observation — baseline only, do not fire
  if (!prior || !prior.lastContentHash) {
    return {
      changeClass: CHANGE_CLASS.NONE,
      meaningful: false,
      reason: "BASELINE_CAPTURE",
      contentHash,
      fingerprint: fp,
      triggerTypes: triggers.triggerTypes,
      triggerHits: triggers.hits,
      artifactLinks,
      newArtifacts: [],
      materialChange: false,
    };
  }

  if (prior.lastContentHash === contentHash) {
    return {
      changeClass: CHANGE_CLASS.NONE,
      meaningful: false,
      reason: "HASH_UNCHANGED",
      contentHash,
      fingerprint: fp,
      triggerTypes: triggers.triggerTypes,
      triggerHits: triggers.hits,
      artifactLinks,
      newArtifacts: [],
      materialChange: false,
    };
  }

  // Hash changed but only noise / trivial
  if (
    materialDet.reason === "TRIVIAL_HTML_CHANGE" ||
    (prior.fingerprint?.materialHash &&
      fp.materialHash === prior.fingerprint.materialHash &&
      newArtifacts.length === 0)
  ) {
    return {
      changeClass: CHANGE_CLASS.TRIVIAL,
      meaningful: false,
      reason: materialDet.reason || "TRIVIAL_CHANGE",
      contentHash,
      fingerprint: fp,
      triggerTypes: triggers.triggerTypes,
      triggerHits: triggers.hits,
      artifactLinks,
      newArtifacts: [],
      materialChange: false,
    };
  }

  const watchedHits = triggers.hits.filter((h) =>
    watchForTypes.length ? watchForTypes.includes(h.triggerType) : true
  );

  // Strong signal: new PDF/list artifact + watched publication terms
  if (newArtifacts.length > 0 && watchedHits.length > 0) {
    return {
      changeClass: CHANGE_CLASS.ARTIFACT_PUBLISHED,
      meaningful: true,
      reason: "NEW_ARTIFACT_AND_PUBLICATION_TERMS",
      contentHash,
      fingerprint: fp,
      triggerTypes: watchedHits.map((h) => h.triggerType),
      triggerHits: watchedHits,
      artifactLinks,
      newArtifacts,
      materialChange: true,
      primaryTriggerType: watchedHits[0].triggerType,
    };
  }

  if (newArtifacts.length > 0) {
    const inferred =
      /\.pdf/i.test(newArtifacts[0]) && /programa|programme|speaker|ponente/i.test(newArtifacts[0])
        ? PUBLICATION_TRIGGER_TYPE.PROGRAMME_PUBLISHED
        : /expositor|exhibitor/i.test(newArtifacts.join(" "))
          ? PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED
          : watchForTypes[0] || PUBLICATION_TRIGGER_TYPE.PROGRAMME_PUBLISHED;
    return {
      changeClass: CHANGE_CLASS.ARTIFACT_PUBLISHED,
      meaningful: true,
      reason: "NEW_DOWNLOADABLE_ARTIFACT",
      contentHash,
      fingerprint: fp,
      triggerTypes: [inferred],
      triggerHits: [{ triggerType: inferred, match: "new_artifact_link" }],
      artifactLinks,
      newArtifacts,
      materialChange: true,
      primaryTriggerType: inferred,
    };
  }

  // Newly appeared publication section text (terms present now, absent before)
  const priorPlain = String(prior.lastNormalizedSnippet || "");
  const newlyAppeared = watchedHits.filter((h) => {
    const priorDet = detectPublicationTriggersInText(priorPlain, {
      watchForTypes: [h.triggerType],
    });
    return priorDet.triggerTypes.length === 0;
  });

  if (newlyAppeared.length > 0 && materialDet.changed) {
    return {
      changeClass: CHANGE_CLASS.MEANINGFUL,
      meaningful: true,
      reason: "NEW_PUBLICATION_TERMS",
      contentHash,
      fingerprint: fp,
      triggerTypes: newlyAppeared.map((h) => h.triggerType),
      triggerHits: newlyAppeared,
      artifactLinks,
      newArtifacts: [],
      materialChange: true,
      primaryTriggerType: newlyAppeared[0].triggerType,
    };
  }

  if (materialDet.changed) {
    return {
      changeClass: CHANGE_CLASS.MEANINGFUL,
      meaningful: true,
      reason: materialDet.reason || "MATERIAL_HASH_CHANGED",
      contentHash,
      fingerprint: fp,
      triggerTypes: watchedHits.map((h) => h.triggerType),
      triggerHits: watchedHits,
      artifactLinks,
      newArtifacts: [],
      materialChange: true,
      primaryTriggerType: watchedHits[0]?.triggerType || watchForTypes[0] || null,
    };
  }

  return {
    changeClass: CHANGE_CLASS.TRIVIAL,
    meaningful: false,
    reason: "CONTENT_CHANGED_NOT_MEANINGFUL",
    contentHash,
    fingerprint: fp,
    triggerTypes: triggers.triggerTypes,
    triggerHits: triggers.hits,
    artifactLinks,
    newArtifacts: [],
    materialChange: false,
  };
}

export { materialTextSlice };
