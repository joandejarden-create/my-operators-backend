/**
 * Phase 2G — semantic duplication / padding detection.
 * Repeated paragraphs fail even when factually defensible.
 * Contract: BE_PHASE2G_ANTI_PADDING_V1
 */
import { loadSubsectionParityContract, nz } from "./be-phase2g-subsection-contract-v1.js";
import { textSimilarity, wordCount } from "./be-phase2f-content-presentation-contract-v1.js";

export const BE_PHASE2G_ANTI_PADDING_V1 = "BE_PHASE2G_ANTI_PADDING_V1";

export const PADDING_FAIL = Object.freeze({
  INTERNAL_META_LANGUAGE: "internal_meta_language",
  REPEATED_SENTENCE_WITHIN_FIELD: "repeated_sentence_within_field",
  NEAR_DUPLICATE_ACROSS_CARDS: "near_duplicate_across_cards",
  NEAR_DUPLICATE_ACROSS_SUBSECTIONS: "near_duplicate_across_subsections",
  GENERIC_PROOF_TITLE: "generic_proof_title",
  SLOT_KEY_ECHO: "slot_key_echo_in_customer_copy",
  PADDING_TO_MEET_MIN_WORDS: "padding_to_meet_min_words",
});

function sentences(text) {
  return nz(text)
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter((s) => wordCount(s) >= 6);
}

function normalizeSentence(s) {
  return nz(s)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function detectInternalMetaLanguage(text) {
  const contract = loadSubsectionParityContract();
  const blob = nz(text);
  const hits = [];
  for (const pat of contract.customerFacingForbiddenPatterns || []) {
    const re = new RegExp(pat, "i");
    if (re.test(blob)) hits.push(pat);
  }
  if (/\[[a-z0-9_.]+\]/.test(blob)) hits.push("bracketed_slot_token");
  if (/Focus this card on/i.test(blob)) hits.push("focus_this_card");
  return hits;
}

export function evaluateAntiPadding(blocks, { brandName = "" } = {}) {
  const list = Array.isArray(blocks) ? blocks : [];
  const failures = [];
  const sentenceIndex = new Map(); // norm -> [{slotKey}]

  for (const b of list) {
    const slotKey = nz(b.slotKey || b.slot);
    const title = nz(b.title);
    const body = nz(b.body);
    const meta = detectInternalMetaLanguage(`${title}\n${body}`);
    for (const m of meta) {
      failures.push({
        code: PADDING_FAIL.INTERNAL_META_LANGUAGE,
        slotKey,
        detail: m,
        brandName,
        founderApprovalBlocked: true,
      });
    }
    if (slotKey && new RegExp(slotKey.replace(/\./g, "\\."), "i").test(body)) {
      failures.push({
        code: PADDING_FAIL.SLOT_KEY_ECHO,
        slotKey,
        detail: "body echoes slot key",
        brandName,
        founderApprovalBlocked: true,
      });
    }
    if (/^Proof\s*\d+$/i.test(title) || /^Gallery\s*\d+$/i.test(title)) {
      failures.push({
        code: PADDING_FAIL.GENERIC_PROOF_TITLE,
        slotKey,
        detail: `generic title "${title}"`,
        brandName,
        founderApprovalBlocked: true,
      });
    }

    const sents = sentences(body);
    const seenLocal = new Set();
    for (const s of sents) {
      const n = normalizeSentence(s);
      if (seenLocal.has(n)) {
        failures.push({
          code: PADDING_FAIL.REPEATED_SENTENCE_WITHIN_FIELD,
          slotKey,
          detail: s.slice(0, 120),
          brandName,
          founderApprovalBlocked: true,
        });
      }
      seenLocal.add(n);
      if (!sentenceIndex.has(n)) sentenceIndex.set(n, []);
      sentenceIndex.get(n).push(slotKey);
    }

    // Padding heuristic: same clause repeated with leaf suffix
    if (/Focus this card on|timing and documentation only/i.test(body)) {
      failures.push({
        code: PADDING_FAIL.PADDING_TO_MEET_MIN_WORDS,
        slotKey,
        detail: "generator padding suffix detected",
        brandName,
        founderApprovalBlocked: true,
      });
    }
  }

  for (const [norm, slots] of sentenceIndex.entries()) {
    const uniq = [...new Set(slots)];
    if (uniq.length >= 2 && norm.split(" ").length >= 10) {
      failures.push({
        code: PADDING_FAIL.NEAR_DUPLICATE_ACROSS_CARDS,
        slotKey: uniq.join(" | "),
        detail: `identical sentence across ${uniq.length} slots: ${norm.slice(0, 100)}`,
        brandName,
        founderApprovalBlocked: true,
      });
    }
  }

  // Near-duplicate bodies across profile
  for (let i = 0; i < list.length; i++) {
    for (let j = i + 1; j < list.length; j++) {
      const a = nz(list[i].body);
      const b = nz(list[j].body);
      if (wordCount(a) < 20 || wordCount(b) < 20) continue;
      const sim = textSimilarity(a, b);
      if (sim >= 0.82) {
        failures.push({
          code: PADDING_FAIL.NEAR_DUPLICATE_ACROSS_SUBSECTIONS,
          slotKey: `${nz(list[i].slotKey)} ↔ ${nz(list[j].slotKey)}`,
          detail: `similarity=${sim.toFixed(2)}`,
          brandName,
          founderApprovalBlocked: true,
        });
      }
    }
  }

  const blocked = failures.filter((f) => f.founderApprovalBlocked);
  return {
    version: BE_PHASE2G_ANTI_PADDING_V1,
    brandName,
    failureRegister: failures,
    metrics: {
      fieldsTested: list.length,
      paddingFailures: failures.length,
      founderApprovalBlocked: blocked.length > 0,
      founderApprovalStatus: blocked.length ? "NOT_FOUNDER_APPROVED" : "PENDING_FOUNDER_REVIEW",
      note: "Anti-padding pass ≠ founder approval. Repeated content fails even if factual.",
    },
  };
}
