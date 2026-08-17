/**
 * Stricter founder / external-owner readiness rules for Brand Explorer presentation rows.
 */
import {
  auditExternalOwnerPhrase,
  auditPresentationRowExternalOwner,
} from "./brand-explorer-external-owner-content-governance.js";
import { DUPLICATE_OPENING_RECORD_IDS } from "./brand-explorer-design-hotels-draft-cleanup-v35E.js";

const DUPLICATE_OPENING_SET = new Set(DUPLICATE_OPENING_RECORD_IDS);

/** Short chip / metric rows where title carries the label and body is optional. */
const OPTIONAL_BODY_SLOT_PREFIXES = [
  "overview.",
  "loyalty.hero",
  "commercial.demand",
  "footprint.portfolio_mix",
  "footprint.regions",
  "standards.metric",
  "economics.metric",
  "value.metric",
  "proof.metric",
  "materials.gallery.",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isEmptyPlaceholder(v) {
  const t = nz(v);
  return !t || t === "—" || t === "-" || t === "&nbsp;";
}

function isOptionalBodySlot(slotKey) {
  return OPTIONAL_BODY_SLOT_PREFIXES.some((p) => slotKey.startsWith(p) || slotKey === p.replace(/\.$/, ""));
}

export function evaluateExternalOwnerReadinessRule(presentationRows = []) {
  const blockers = [];
  const tabIssues = new Map();
  const visible = (presentationRows || []).filter((r) => r.visible !== false && r.active !== false);

  let urlHitCount = 0;
  let emptyCardCount = 0;
  let modalPlaceholderCount = 0;
  const emptyCardRows = [];

  for (const row of visible) {
    const slotKey = nz(row.slotKey);
    if (DUPLICATE_OPENING_SET.has(row.recordId)) continue;
    const tab = slotKey.split(".")[0] || "other";
    const audit = auditPresentationRowExternalOwner(row);
    const criticalHits = audit.hits.filter((h) => h.severity === "critical" || h.severity === "high");
    const urlHits = audit.hits.filter(
      (h) => h.patternId === "http_url" && slotKey !== "footprint.openings" && slotKey !== "footprint.momentum"
    );
    if (urlHits.length) urlHitCount += 1;
    if (criticalHits.length) {
      blockers.push(`external_copy:${slotKey}:${criticalHits.map((h) => h.patternId).join(",")}`);
      tabIssues.set(tab, (tabIssues.get(tab) || 0) + 1);
    }

    const combined = `${row.title}\n${row.body}`;
    const hits = auditExternalOwnerPhrase(combined, slotKey).filter(
      (h) => !(h.patternId === "http_url" && (slotKey === "footprint.openings" || slotKey === "footprint.momentum"))
    );
    if (hits.some((h) => ["sources_block", "source_line", "franchise_disclosure", "loi", "brand_verified"].includes(h.patternId))) {
      blockers.push(`governance_language:${slotKey}`);
    }

    if (!isOptionalBodySlot(slotKey) && nz(row.title) && isEmptyPlaceholder(row.body)) {
      emptyCardCount += 1;
      emptyCardRows.push({ recordId: row.recordId, slotKey, title: row.title });
    }

    if (slotKey === "footprint.openings" && !DUPLICATE_OPENING_SET.has(row.recordId)) {
      const modalFields = [
        row.caseSummaryOverview,
        row.caseSummaryBrandRelevance,
        row.caseSummaryOwnerObjective,
        row.caseSummaryInterpretation,
        row.caseSummaryTags,
      ];
      modalPlaceholderCount += modalFields.filter(isEmptyPlaceholder).length;
      if (modalFields.filter(isEmptyPlaceholder).length >= 2) {
        blockers.push(`modal_placeholders:${row.recordId}`);
      }
    }
  }

  if (urlHitCount > 0) blockers.push(`visible_source_urls:${urlHitCount}`);
  if (emptyCardCount > 1) blockers.push(`empty_visible_cards:${emptyCardCount}`);

  for (const [tab, count] of tabIssues.entries()) {
    if (count > 1) blockers.push(`tab_external_fail:${tab}`);
  }

  return {
    ruleId: "external_owner_readiness",
    pass: blockers.length === 0,
    blockers: [...new Set(blockers)],
    urlHitCount,
    emptyCardCount,
    emptyCardRows,
    modalPlaceholderCount,
  };
}
