/**
 * BE Cumulative Proposal Assembler V1
 *
 * RIGHT column contract for Comparison Studio (permanent URL):
 *   LEFT  = CURRENT saved profile (unchanged)
 *   RIGHT = CURRENT baseline + only complete, non-regressive staged changes
 *
 * Never silently overwrite complete existing content with a weaker factory candidate.
 * Incomplete drafts are exposed separately and do not enter the publication candidate.
 */

import { decideGallerySlotApply } from "./be-permanent-six-image-gallery-standard-v1.js";
import {
  applyEditorialTitleCaseToBlocks,
  BE_EDITORIAL_TITLE_CASE_V1,
} from "./be-editorial-title-case-v1.js";
import {
  applyPropertyImageIntegrityToBlocks,
  BE_PROPERTY_IMAGE_INTEGRITY_V1,
} from "./be-property-image-integrity-v1.js";
import {
  applyDealalityInsightAdvisoryToBlocks,
  BE_DEALALITY_INSIGHT_ADVISORY_V1,
  containsInternalCustomerLanguage,
  isCustomerFacingAdvisorySlot,
  isInternalLanguageRegression,
} from "./be-dealality-insight-advisory-v1.js";
import {
  applyNoRepeatedCardHeadingToBlocks,
  BE_NO_REPEATED_CARD_HEADING_V1,
} from "./be-no-repeated-card-heading-v1.js";
import {
  BE_FIELD_CONTENT_ROLES_V1,
  isCommercialSummarySlot,
  isInternalResearchInstructionText,
} from "./be-field-content-roles-v1.js";
import { decideLoyaltyProofCandidateApply } from "./be-factory-permanent-loyalty-region-standards-v1.js";

export const BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1 = "BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1";
export { BE_FIELD_CONTENT_ROLES_V1 };

export const PROVENANCE = Object.freeze({
  RETAINED_FROM_CURRENT: "RETAINED_FROM_CURRENT",
  MODIFIED_EXISTING: "MODIFIED_EXISTING",
  NEWLY_RESEARCHED: "NEWLY_RESEARCHED",
  NEW_COMPONENT: "NEW_COMPONENT",
  INCOMPLETE_DRAFT: "INCOMPLETE_DRAFT",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
});

const PLACEHOLDER_TITLE_RE =
  /^(loyalty proof points|proof points?|fee categories|standards diligence themes|owner diligence questions|demand themes|openings \(diligence\)|momentum \(diligence\)|growth themes|owner outcomes|owner considerations|curio proof \d+|risk|model|financials|negotiable items|rarely negotiable|\d+)$/i;

const RESEARCH_GAP_RE =
  /RESEARCH_REQUIRED|Focus this card|Evidence-grounded|slotKey|TODO_RESEARCH|\[research\]/i;

const DILIGENCE_META_RE =
  /diligence:|request current|confirm definitions and figures|do not reuse|Use disclosure\/scorecards only|from .+ development\s*[—\-]/i;

const KPI_SLOT_RE = /\.kpi\.|kpi\./i;

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function bodyOf(block) {
  return nz(block && (block.body || block.value || block.text));
}

function titleOf(block) {
  return nz(block && block.title);
}

function slotOf(block) {
  return nz(block && (block.slotKey || block.slot_key || block.fieldKey));
}

function countBullets(text) {
  const t = nz(text);
  if (!t) return 0;
  const lines = t
    .split(/\n+/)
    .map((l) => l.replace(/^[\s•\-*]+/, "").trim())
    .filter(Boolean);
  if (lines.length >= 2) return lines.length;
  // single paragraph still counts as one substantive bullet
  return t.length >= 24 ? 1 : 0;
}

function isPlaceholderBlock(block) {
  const title = titleOf(block);
  const body = bodyOf(block);
  if (!body && PLACEHOLDER_TITLE_RE.test(title)) return true;
  if (RESEARCH_GAP_RE.test(title + "\n" + body)) return true;
  if (DILIGENCE_META_RE.test(body)) return true;
  if (!body && !title) return true;
  return false;
}

function looksLikeNumericOrUnavailable(text) {
  const t = nz(text);
  if (!t) return false;
  if (/^(n\/?a|not disclosed|unavailable|—|-)$/i.test(t)) return true;
  // Compact numeric / percent / currency / short status
  if (t.length <= 48 && /[\d%~$€£+]/.test(t) && t.split(/\s+/).length <= 8) return true;
  return false;
}

/** Count insight bullets with "Lead: explanation" structure (Why Value bar). */
export function countColonInsightLeads(text) {
  const lines = nz(text)
    .split(/\n+/)
    .map((l) => l.replace(/^[\s•\-*]+/, "").trim())
    .filter(Boolean);
  let n = 0;
  for (const line of lines) {
    // Title-ish lead (3–72 chars) + colon + substantive rest
    if (/^[^:\n]{3,72}:\s+\S.{11,}/.test(line)) n += 1;
  }
  return n;
}

function isWhyValueInsightRegression(slotKey, currentBlocks, candidateBlocks) {
  if (!/^overview\.why_value$/i.test(nz(slotKey))) return false;
  const curBody = (currentBlocks || []).map(bodyOf).join("\n");
  const candBody = (candidateBlocks || []).map(bodyOf).join("\n");
  const curLeads = countColonInsightLeads(curBody);
  const candLeads = countColonInsightLeads(candBody);
  // Never replace CURRENT colon-insight structure with thin fragment dumps.
  if (curLeads >= 3 && candLeads < Math.min(curLeads, 4)) return true;
  // Fragment lines without leads (e.g. "Hilton loyalty reach") while CURRENT has leads
  if (curLeads >= 3 && candLeads === 0) return true;
  return false;
}

function isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks) {
  const cq = slotGroupQuality(currentBlocks);
  const pq = slotGroupQuality(candidateBlocks);
  const candBody = (candidateBlocks || []).map(bodyOf).join("\n");
  const curBody = (currentBlocks || []).map(bodyOf).join("\n");

  if (isWhyValueInsightRegression(slotKey, currentBlocks, candidateBlocks)) return true;

  // Bullet / list cardinality: never replace a multi-bullet list with one paragraph
  if (cq.bulletCount >= 3 && pq.bulletCount < cq.bulletCount) return true;
  if (cq.bulletCount >= 3 && pq.bulletCount <= 1 && candBody.length > 80) return true;

  // Commercial Engine summary tiles (Channels/Campaigns/B2B/Owner Lens): narrative, not numeric.
  // Never replace complete customer-facing summaries with research-instruction stubs.
  if (isCommercialSummarySlot(slotKey)) {
    if (isInternalResearchInstructionText(candBody) || DILIGENCE_META_RE.test(candBody)) return true;
    if (curBody.length >= 24 && candBody.length + 20 < curBody.length) return true;
    return false;
  }

  // Genuine numeric KPI slots: never replace compact values with diligence prose
  if (KPI_SLOT_RE.test(slotKey || "")) {
    if (DILIGENCE_META_RE.test(candBody)) return true;
    if (
      looksLikeNumericOrUnavailable(curBody) &&
      !looksLikeNumericOrUnavailable(candBody) &&
      candBody.length > 40
    ) {
      return true;
    }
  }

  // Named list slots (Often Negotiated / Usually Standard)
  if (/negotiable_items|rarely_negotiable/i.test(slotKey || "")) {
    if (DILIGENCE_META_RE.test(candBody)) return true;
    if (cq.bulletCount >= 3 && pq.bulletCount < 3) return true;
  }

  return false;
}

function hasSubstantiveContent(block) {
  if (!block) return false;
  if (isPlaceholderBlock(block)) return false;
  const body = bodyOf(block);
  const title = titleOf(block);
  if (body.length >= 24) return true;
  if (title.length >= 3 && body.length >= 12) return true;
  if (nz(block.imageUrl || block.image_url)) return true;
  return false;
}

export function slotGroupQuality(blocks) {
  const list = Array.isArray(blocks) ? blocks : [];
  const substantive = list.filter(hasSubstantiveContent);
  const placeholders = list.filter(isPlaceholderBlock);
  const emptyBodies = list.filter((b) => !bodyOf(b) && !nz(b.imageUrl || b.image_url));
  const totalBody = substantive.reduce((n, b) => n + bodyOf(b).length, 0);
  const bulletCount = substantive.reduce((n, b) => n + countBullets(bodyOf(b)), 0);
  const titles = substantive.map(titleOf).filter(Boolean);
  return {
    count: list.length,
    substantiveCount: substantive.length,
    placeholderCount: placeholders.length,
    emptyBodyCount: emptyBodies.length,
    totalBody,
    bulletCount,
    titles,
    fingerprint: substantive
      .map((b) => `${titleOf(b)}::${bodyOf(b)}`)
      .sort()
      .join("||"),
  };
}

function normalizeIncomingBlock(block, index, sourceTag) {
  const slotKey = slotOf(block);
  const body = bodyOf(block);
  const imageUrl = block.imageUrl || block.image_url || null;
  return {
    recordId: block.recordId || `${sourceTag}-${index}`,
    slotKey,
    title: titleOf(block),
    body,
    sort:
      typeof block.sort_order === "number"
        ? block.sort_order
        : typeof block.sort === "number"
          ? block.sort
          : index + 1,
    imageUrl,
    imageFilename: block.imageFilename || null,
    summaryUrl: block.summaryUrl || "",
    caseSummaryOverview: block.caseSummaryOverview || "",
    caseSummaryOwnerObjective: block.caseSummaryOwnerObjective || "",
    caseSummaryBrandRelevance: block.caseSummaryBrandRelevance || "",
    caseSummaryInterpretation: block.caseSummaryInterpretation || "",
    caseSummaryTags: block.caseSummaryTags || "",
  };
}

function groupBySlot(blocks) {
  const map = new Map();
  (blocks || []).forEach((b, i) => {
    const key = slotOf(b);
    if (!key) return;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push({ block: b, index: i });
  });
  return map;
}

/**
 * Decide whether a candidate slot group may replace the current slot group
 * in the cumulative publication proposal.
 */
export function decideSlotProvenance(slotKey, currentBlocks, candidateBlocks) {
  const cq = slotGroupQuality(currentBlocks);
  const pq = slotGroupQuality(candidateBlocks);

  // Never apply internal HPC / certification / steward workflow language as customer editorial.
  if (isCustomerFacingAdvisorySlot(slotKey)) {
    const candBlob = (candidateBlocks || [])
      .map((b) => `${titleOf(b)}\n${bodyOf(b)}`)
      .join("\n");
    const curBlob = (currentBlocks || [])
      .map((b) => `${titleOf(b)}\n${bodyOf(b)}`)
      .join("\n");
    if (isInternalLanguageRegression(candBlob, curBlob)) {
      return {
        provenance:
          cq.substantiveCount > 0 && !containsInternalCustomerLanguage(curBlob)
            ? PROVENANCE.INCOMPLETE_DRAFT
            : PROVENANCE.RETAINED_FROM_CURRENT,
        applyCandidate: false,
        reason: "candidate_internal_language_not_customer_facing",
        approvalBlocked: true,
      };
    }
  }

  // Key Benefits & Program Strengths (loyalty.proof): EXACT 6 — Factory 1.0/2.0 permanent.
  if (/^loyalty\.proof$/i.test(nz(slotKey))) {
    const d = decideLoyaltyProofCandidateApply(currentBlocks, candidateBlocks);
    if (!d.applyCandidate) {
      // Always expose as incomplete draft when a candidate was offered but blocked,
      // so Factory QA can see the rejected partial set while publication retains CURRENT.
      return {
        provenance:
          candidateBlocks.length > 0
            ? PROVENANCE.INCOMPLETE_DRAFT
            : d.retainCurrent
              ? PROVENANCE.RETAINED_FROM_CURRENT
              : PROVENANCE.INCOMPLETE_DRAFT,
        applyCandidate: false,
        reason: d.reason,
        currentQuality: cq,
        candidateQuality: pq,
      };
    }
  }

  // Permanent six-image hotel gallery: never apply logos / generic "Property imagery"
  // captions over verified CURRENT hotel photography.
  if (/^materials\.gallery\.\d+$/.test(nz(slotKey))) {
    const cur = currentBlocks[0] || null;
    const cand = candidateBlocks[0] || null;
    const g = decideGallerySlotApply(slotKey, cur, cand);
    if (!g.applyCandidate) {
      return {
        provenance:
          cq.substantiveCount > 0 ? PROVENANCE.INCOMPLETE_DRAFT : PROVENANCE.RETAINED_FROM_CURRENT,
        applyCandidate: false,
        reason: g.reason || "gallery_standard_reject",
      };
    }
    return {
      provenance: cq.substantiveCount > 0 ? PROVENANCE.MODIFIED_EXISTING : PROVENANCE.NEW_COMPONENT,
      applyCandidate: true,
      reason: g.reason || "gallery_standard_accept",
    };
  }

  if (pq.substantiveCount === 0 && cq.substantiveCount === 0) {
    if (candidateBlocks.length && RESEARCH_GAP_RE.test(JSON.stringify(candidateBlocks))) {
      return {
        provenance: PROVENANCE.RESEARCH_REQUIRED,
        applyCandidate: false,
        reason: "candidate_marks_research_required",
      };
    }
    return {
      provenance: PROVENANCE.RETAINED_FROM_CURRENT,
      applyCandidate: false,
      reason: "both_empty",
    };
  }

  if (pq.substantiveCount === 0 && cq.substantiveCount > 0) {
    return {
      provenance: PROVENANCE.INCOMPLETE_DRAFT,
      applyCandidate: false,
      reason: "candidate_empty_or_placeholder_while_current_complete",
    };
  }

  // Explicit incomplete: empty bodies / placeholders while current has content
  if (
    cq.substantiveCount > 0 &&
    (pq.placeholderCount > 0 || pq.emptyBodyCount > 0) &&
    pq.substantiveCount < cq.substantiveCount
  ) {
    return {
      provenance: PROVENANCE.INCOMPLETE_DRAFT,
      applyCandidate: false,
      reason: "candidate_incomplete_vs_complete_current",
    };
  }

  // Card-count / depth / list / KPI presentation regression
  if (
    cq.substantiveCount > 0 &&
    (pq.substantiveCount < cq.substantiveCount ||
      pq.totalBody < Math.floor(cq.totalBody * 0.7) ||
      (cq.bulletCount >= 4 && pq.bulletCount > 0 && pq.bulletCount < 4 && pq.totalBody < cq.totalBody) ||
      isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks))
  ) {
    return {
      provenance: PROVENANCE.INCOMPLETE_DRAFT,
      applyCandidate: false,
      reason: isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks)
        ? "candidate_prose_or_list_regression"
        : "candidate_regresses_depth_or_cardinality",
      currentQuality: cq,
      candidateQuality: pq,
    };
  }

  if (cq.substantiveCount === 0 && pq.substantiveCount > 0) {
    // New slot that is diligence meta / prose-in-KPI is not a publishable improvement
    if (
      isPlaceholderBlock(candidateBlocks[0]) ||
      isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks)
    ) {
      return {
        provenance: PROVENANCE.INCOMPLETE_DRAFT,
        applyCandidate: false,
        reason: "new_slot_is_diligence_or_invalid_kpi_prose",
      };
    }
    return {
      provenance: PROVENANCE.NEW_COMPONENT,
      applyCandidate: true,
      reason: "new_substantive_slot_not_in_current",
    };
  }

  if (cq.fingerprint === pq.fingerprint) {
    return {
      provenance: PROVENANCE.RETAINED_FROM_CURRENT,
      applyCandidate: false,
      reason: "identical_content",
    };
  }

  // Genuine improvement: equal-or-better cardinality, no list/KPI regression, no diligence meta
  if (
    pq.substantiveCount >= cq.substantiveCount &&
    pq.totalBody >= cq.totalBody * 0.95 &&
    pq.placeholderCount === 0 &&
    !isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks) &&
    !(cq.bulletCount >= 3 && pq.bulletCount < cq.bulletCount)
  ) {
    return {
      provenance: PROVENANCE.MODIFIED_EXISTING,
      applyCandidate: true,
      reason: "candidate_complete_non_regressive_change",
    };
  }

  // Why Value Is Strongest: prefer a candidate with more "Lead: explanation" insight
  // lines even when CURRENT prose is slightly longer (founder bold-lead bar).
  if (/^overview\.why_value$/i.test(nz(slotKey))) {
    const curLeads = countColonInsightLeads(
      (currentBlocks || []).map(bodyOf).join("\n")
    );
    const candLeads = countColonInsightLeads(
      (candidateBlocks || []).map(bodyOf).join("\n")
    );
    if (
      candLeads >= 3 &&
      candLeads > curLeads &&
      pq.placeholderCount === 0 &&
      pq.substantiveCount > 0 &&
      !isProseDiligenceRegression(slotKey, currentBlocks, candidateBlocks)
    ) {
      return {
        provenance: PROVENANCE.MODIFIED_EXISTING,
        applyCandidate: true,
        reason: "why_value_colon_insight_upgrade",
        currentQuality: cq,
        candidateQuality: pq,
      };
    }
  }

  return {
    provenance: PROVENANCE.INCOMPLETE_DRAFT,
    applyCandidate: false,
    reason: "candidate_not_safe_to_replace_current",
    currentQuality: cq,
    candidateQuality: pq,
  };
}

/**
 * Assemble cumulative publication blocks.
 * @param {object[]} currentBlocks - saved / live presentation blocks
 * @param {object[]} candidateBlocks - staged factory/candidate overlay blocks
 * @param {object} [opts]
 */
export function assembleCumulativeProposal(currentBlocks, candidateBlocks, opts = {}) {
  const current = Array.isArray(currentBlocks) ? currentBlocks : [];
  const candidate = Array.isArray(candidateBlocks) ? candidateBlocks : [];
  const currentGroups = groupBySlot(current);
  const candidateGroups = groupBySlot(candidate);

  const allSlots = new Set([...currentGroups.keys(), ...candidateGroups.keys()]);
  const provenanceBySlot = {};
  const incompleteDrafts = [];
  const assembled = [];
  let nextSort = 1;

  const sortedSlots = [...allSlots].sort((a, b) => {
    const ai = (currentGroups.get(a) || candidateGroups.get(a) || [{ index: 9999 }])[0].index;
    const bi = (currentGroups.get(b) || candidateGroups.get(b) || [{ index: 9999 }])[0].index;
    return ai - bi || a.localeCompare(b);
  });

  for (const slotKey of sortedSlots) {
    const curBlocks = (currentGroups.get(slotKey) || []).map((x) => x.block);
    const candBlocks = (candidateGroups.get(slotKey) || []).map((x) => x.block);
    const decision = decideSlotProvenance(slotKey, curBlocks, candBlocks);
    provenanceBySlot[slotKey] = {
      provenance: decision.provenance,
      applyCandidate: !!decision.applyCandidate,
      reason: decision.reason,
      currentQuality: slotGroupQuality(curBlocks),
      candidateQuality: slotGroupQuality(candBlocks),
    };

    if (decision.applyCandidate) {
      candBlocks.forEach((b) => {
        const norm = normalizeIncomingBlock(b, nextSort, "cumulative-proposed");
        norm.slotKey = slotKey;
        norm.sort = nextSort++;
        assembled.push(norm);
      });
    } else {
      // Publication column keeps CURRENT. Incomplete/research drafts stay out of the proposal.
      const publicationProvenance = PROVENANCE.RETAINED_FROM_CURRENT;
      if (
        decision.provenance === PROVENANCE.INCOMPLETE_DRAFT ||
        decision.provenance === PROVENANCE.RESEARCH_REQUIRED
      ) {
        incompleteDrafts.push({
          slotKey,
          provenance: decision.provenance,
          reason: decision.reason,
          candidateBlocks: candBlocks.map((b, i) =>
            normalizeIncomingBlock(b, i, "incomplete-draft"),
          ),
          retainedFromCurrent: true,
        });
        provenanceBySlot[slotKey] = {
          ...provenanceBySlot[slotKey],
          provenance: publicationProvenance,
          draftStatus: decision.provenance,
          publicationAction: "retain_current",
        };
      } else {
        provenanceBySlot[slotKey] = {
          ...provenanceBySlot[slotKey],
          provenance: publicationProvenance,
          publicationAction: "retain_current",
        };
      }
      curBlocks.forEach((b) => {
        const norm = normalizeIncomingBlock(b, nextSort, "retained-current");
        norm.slotKey = slotKey;
        norm.sort = nextSort++;
        assembled.push(norm);
      });
    }
  }

  const counts = {
    RETAINED_FROM_CURRENT: 0,
    MODIFIED_EXISTING: 0,
    NEWLY_RESEARCHED: 0,
    NEW_COMPONENT: 0,
    INCOMPLETE_DRAFT: 0,
    RESEARCH_REQUIRED: 0,
  };
  Object.values(provenanceBySlot).forEach((p) => {
    if (counts[p.provenance] != null) counts[p.provenance] += 1;
  });
  incompleteDrafts.forEach((d) => {
    if (d.provenance === PROVENANCE.INCOMPLETE_DRAFT) counts.INCOMPLETE_DRAFT += 1;
    if (d.provenance === PROVENANCE.RESEARCH_REQUIRED) counts.RESEARCH_REQUIRED += 1;
  });

  const regressionBlocked = incompleteDrafts.some(
    (d) =>
      d.reason === "candidate_regresses_depth_or_cardinality" ||
      d.reason === "candidate_incomplete_vs_complete_current" ||
      d.reason === "candidate_empty_or_placeholder_while_current_complete",
  );

  return {
    contract: BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
    blocks: assembled,
    provenanceBySlot,
    provenanceCounts: counts,
    incompleteDrafts,
    approvalBlockedByRegression: regressionBlocked,
    summary: {
      currentBlockCount: current.length,
      candidateBlockCount: candidate.length,
      assembledBlockCount: assembled.length,
      incompleteDraftSlotCount: incompleteDrafts.length,
      retainedSlotCount: counts.RETAINED_FROM_CURRENT,
      modifiedSlotCount: counts.MODIFIED_EXISTING,
      newComponentSlotCount: counts.NEW_COMPONENT,
      newlyResearchedSlotCount: counts.NEWLY_RESEARCHED,
    },
    note:
      opts.note ||
      "Cumulative publication proposal = CURRENT baseline + complete non-regressive staged changes only",
  };
}

/**
 * Build a renderable brand object for the RIGHT column.
 * Clones CURRENT shell (structured fields intact) and swaps Presentation blocks.
 */
export function buildCumulativeProposedBrand(currentBrand, candidateOverlay, opts = {}) {
  if (!currentBrand) throw new Error("currentBrand required");
  const currentBlocks =
    (currentBrand.brandExplorer && currentBrand.brandExplorer.blocks) ||
    currentBrand.blocks ||
    [];
  const candidateBlocks =
    (candidateOverlay &&
      candidateOverlay.brandExplorer &&
      candidateOverlay.brandExplorer.blocks) ||
    (candidateOverlay && candidateOverlay.blocks) ||
    [];

  const assembled = assembleCumulativeProposal(currentBlocks, candidateBlocks, opts);

  // RIGHT-only property image integrity: never use publication thumbnails as hotel photos.
  const imageIntegrity = applyPropertyImageIntegrityToBlocks(assembled.blocks, {
    forceVerifiedReplacements: true,
  });
  assembled.blocks = imageIntegrity.blocks;
  assembled.propertyImageIntegrity = {
    contract: BE_PROPERTY_IMAGE_INTEGRITY_V1,
    changedCount: imageIntegrity.changedCount,
    changed: imageIntegrity.changed,
  };

  // RIGHT-only Dealality Insight advisory: strip internal language; apply curated assessment.
  const brandMeta = {
    id: currentBrand.id || currentBrand.brandId || null,
    name: currentBrand.name || currentBrand.brandName || null,
  };
  const advisory = applyDealalityInsightAdvisoryToBlocks(assembled.blocks, {
    brand: brandMeta,
  });
  assembled.blocks = advisory.blocks;
  assembled.dealalityInsightAdvisory = {
    contract: BE_DEALALITY_INSIGHT_ADVISORY_V1,
    changedCount: advisory.changedCount,
    changed: advisory.changed,
    internalEvidence: advisory.internalEvidence,
    packBrandId: advisory.packBrandId,
  };
  if (advisory.internalEvidence && advisory.internalEvidence.length) {
    assembled.incompleteDrafts = [
      ...(assembled.incompleteDrafts || []),
      ...advisory.internalEvidence.map((ev) => ({
        slotKey: ev.slotKey,
        reason: "internal_language_held_out_of_customer_facing",
        rejectedBody: ev.rejectedBody,
        hits: ev.hits,
      })),
    ];
  }

  // RIGHT-only: strip leading "Title: …" echoes from card bodies.
  const headingEcho = applyNoRepeatedCardHeadingToBlocks(assembled.blocks);
  assembled.blocks = headingEcho.blocks;
  assembled.noRepeatedCardHeading = {
    contract: BE_NO_REPEATED_CARD_HEADING_V1,
    changedCount: headingEcho.changedCount,
    changed: headingEcho.changed,
  };

  // Final RIGHT-only editorial Title Case + & conjunction (after other RIGHT writers).
  // Never mutate LEFT/CURRENT saved titles.
  const titleCase = applyEditorialTitleCaseToBlocks(assembled.blocks, {
    onlySemanticSlots: true,
  });
  assembled.blocks = titleCase.blocks;
  assembled.titleCase = {
    contract: BE_EDITORIAL_TITLE_CASE_V1,
    changedCount: titleCase.changedCount,
    changedSlots: titleCase.changedSlots,
  };

  function bumpModified(key, patch) {
    if (!key) return;
    const prev = assembled.provenanceBySlot[key] || {};
    assembled.provenanceBySlot[key] = {
      ...prev,
      provenance: PROVENANCE.MODIFIED_EXISTING,
      applyCandidate: false,
      ...patch,
    };
    if (assembled.provenanceCounts && prev.provenance === PROVENANCE.RETAINED_FROM_CURRENT) {
      assembled.provenanceCounts.RETAINED_FROM_CURRENT = Math.max(
        0,
        (assembled.provenanceCounts.RETAINED_FROM_CURRENT || 0) - 1,
      );
      assembled.provenanceCounts.MODIFIED_EXISTING =
        (assembled.provenanceCounts.MODIFIED_EXISTING || 0) + 1;
    }
  }

  for (const ch of titleCase.changedSlots) {
    bumpModified(ch.slotKey, {
      publicationAction: "title_case_normalized",
      reason: "editorial_title_case_on_proposed",
      titleCaseFrom: ch.from,
      titleCaseTo: ch.to,
    });
  }
  for (const ch of imageIntegrity.changed) {
    bumpModified(ch.slotKey, {
      publicationAction: "property_image_integrity",
      reason: ch.reason,
      imageFrom: ch.from,
      imageTo: ch.to,
      imageCaption: ch.caption || null,
    });
  }
  for (const ch of advisory.changed) {
    bumpModified(ch.slotKey, {
      publicationAction: "dealality_insight_advisory",
      reason: ch.reason,
      advisoryAction: ch.action,
    });
  }
  for (const ch of headingEcho.changed) {
    bumpModified(ch.slotKey, {
      publicationAction: "no_repeated_card_heading",
      reason: ch.reason,
    });
  }

  const brand = JSON.parse(JSON.stringify(currentBrand));
  brand.brandExplorer = {
    version: 1,
    blocks: assembled.blocks,
    overlaySource: "cumulative-proposal-v1",
  };
  brand.beCandidateOverlay = true;
  brand.beCumulativeProposal = true;
  brand.beCandidateOverlayMeta = {
    phase: (candidateOverlay && candidateOverlay.phase) || null,
    candidateVersion:
      (candidateOverlay && candidateOverlay.candidateVersion) ||
      opts.candidateVersion ||
      null,
    assembly: BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
    titleCase: BE_EDITORIAL_TITLE_CASE_V1,
    titleCaseChangedCount: titleCase.changedCount,
    propertyImageIntegrity: BE_PROPERTY_IMAGE_INTEGRITY_V1,
    propertyImageIntegrityChangedCount: imageIntegrity.changedCount,
    dealalityInsightAdvisory: BE_DEALALITY_INSIGHT_ADVISORY_V1,
    dealalityInsightAdvisoryChangedCount: advisory.changedCount,
    noRepeatedCardHeading: BE_NO_REPEATED_CARD_HEADING_V1,
    noRepeatedCardHeadingChangedCount: headingEcho.changedCount,
    note: assembled.note,
    incompleteDraftSlotCount: assembled.incompleteDrafts.length,
    approvalBlockedByRegression: assembled.approvalBlockedByRegression,
    provenanceCounts: assembled.provenanceCounts,
  };
  return { brand, assembly: assembled };
}

export default {
  BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
  PROVENANCE,
  slotGroupQuality,
  decideSlotProvenance,
  assembleCumulativeProposal,
  buildCumulativeProposedBrand,
};
