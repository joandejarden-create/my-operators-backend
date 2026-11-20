/**
 * Brand Explorer Factory — permanent loyalty Key Benefits (exact 6) +
 * regional acronym uppercase standards.
 *
 * Inherited by Factory 1.0 / 2.0 candidate assembly, cumulative retention,
 * atelier rendering, browser verification, and publication quality gates.
 *
 * Contract: BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1
 * Lessons: BE-QL-LOYALTY-SIX-001, BE-QL-REGION-ACRONYM-001
 */

export const BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1 =
  "BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1";

export const LOYALTY_PROOF_SLOT = "loyalty.proof";
export const LOYALTY_PROOF_REQUIRED_COUNT = 6;

/** Regional acronyms that must never Title-Case as proper nouns (Apac, Emea, …). */
export const REGIONAL_ACRONYMS = Object.freeze(["APAC", "EMEA", "CALA", "LATAM", "MEA"]);

export const REGIONAL_ACRONYM_RE = /\b(apac|emea|cala|latam|mea)\b/gi;

export function normalizeRegionalAcronymsInHeading(raw) {
  const s = raw == null ? "" : String(raw);
  if (!s) return "";
  return s.replace(REGIONAL_ACRONYM_RE, (m) => m.toUpperCase());
}

export function headingHasBadRegionalCasing(raw) {
  const s = raw == null ? "" : String(raw);
  // Bad: Apac / Emea / Cala / Latam as Title Case (not all-caps)
  return /\b(Apac|Emea|Cala|Latam|Mea)\b/.test(s) && !/\b(APAC|EMEA|CALA|LATAM|MEA)\b/.test(s.replace(/\b(Apac|Emea|Cala|Latam|Mea)\b/g, ""));
}

/** Detect Title-cased regional tokens that should be ALL CAPS. */
export function findBadRegionalHeadingTokens(raw) {
  const s = raw == null ? "" : String(raw);
  const bad = [];
  const re = /\b(Apac|Emea|Cala|Latam|Mea)\b/g;
  let m;
  while ((m = re.exec(s))) bad.push(m[1]);
  return bad;
}

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function isSubstantiveLoyaltyProof(block) {
  const title = nz(block && block.title);
  const body = nz(block && (block.body || block.value || block.text));
  if (!title && !body) return false;
  if (/^proof\s*points?$/i.test(title) && body.length < 40) return false;
  if (/^proof\s*point$/i.test(title) && body.length < 80) return false;
  if (body.length >= 24) return true;
  if (title.length >= 3 && body.length >= 12) return true;
  return false;
}

/**
 * Validate loyalty.proof card set for a participating brand candidate / assembly.
 * @returns {{ ok: boolean, count: number, reasons: string[], blocked: boolean }}
 */
export function validateLoyaltyProofExactSix(blocks, opts = {}) {
  const list = (Array.isArray(blocks) ? blocks : []).filter(
    (b) => nz(b.slotKey || b.slot_key) === LOYALTY_PROOF_SLOT
  );
  const substantive = list.filter(isSubstantiveLoyaltyProof);
  const titles = substantive.map((b) => nz(b.title).toLowerCase());
  const distinctTitles = new Set(titles.filter(Boolean));
  const reasons = [];

  if (opts.requireParticipating === false && substantive.length === 0) {
    return { ok: true, count: 0, reasons: ["no_loyalty_proof_optional"], blocked: false };
  }

  if (substantive.length > 0 && substantive.length < LOYALTY_PROOF_REQUIRED_COUNT) {
    reasons.push("loyalty_proof_requires_exact_six_cards");
  }
  if (substantive.length > LOYALTY_PROOF_REQUIRED_COUNT) {
    reasons.push("loyalty_proof_exceeds_six_cards");
  }
  if (
    substantive.length === LOYALTY_PROOF_REQUIRED_COUNT &&
    distinctTitles.size < LOYALTY_PROOF_REQUIRED_COUNT
  ) {
    reasons.push("loyalty_proof_titles_not_distinct");
  }
  if (substantive.some((b) => /^proof\s*point$/i.test(nz(b.title)))) {
    reasons.push("loyalty_proof_generic_proof_point_placeholder");
  }

  const ok =
    substantive.length === 0 ||
    (substantive.length === LOYALTY_PROOF_REQUIRED_COUNT &&
      distinctTitles.size === LOYALTY_PROOF_REQUIRED_COUNT &&
      !reasons.includes("loyalty_proof_generic_proof_point_placeholder"));

  return {
    ok: reasons.length === 0 && (substantive.length === 0 || ok),
    count: substantive.length,
    distinctTitleCount: distinctTitles.size,
    reasons,
    blocked: reasons.length > 0,
  };
}

/**
 * Decide whether a loyalty.proof candidate group may replace CURRENT / static six.
 * Incomplete candidates are always blocked.
 */
export function decideLoyaltyProofCandidateApply(currentBlocks, candidateBlocks) {
  const cur = validateLoyaltyProofExactSix(currentBlocks, { requireParticipating: false });
  const cand = validateLoyaltyProofExactSix(candidateBlocks, { requireParticipating: false });

  if (cand.count > 0 && cand.count < LOYALTY_PROOF_REQUIRED_COUNT) {
    return {
      applyCandidate: false,
      reason: "loyalty_proof_requires_exact_six_cards",
      retainCurrent: cur.count >= LOYALTY_PROOF_REQUIRED_COUNT,
      candidate: cand,
      current: cur,
    };
  }
  if (cand.blocked) {
    return {
      applyCandidate: false,
      reason: cand.reasons[0] || "loyalty_proof_blocked",
      retainCurrent: cur.count >= LOYALTY_PROOF_REQUIRED_COUNT,
      candidate: cand,
      current: cur,
    };
  }
  if (cur.count >= LOYALTY_PROOF_REQUIRED_COUNT && cand.count !== LOYALTY_PROOF_REQUIRED_COUNT) {
    return {
      applyCandidate: false,
      reason: "loyalty_proof_cardinality_regression",
      retainCurrent: true,
      candidate: cand,
      current: cur,
    };
  }
  if (cand.count === LOYALTY_PROOF_REQUIRED_COUNT && !cand.blocked) {
    return {
      applyCandidate: true,
      reason: "loyalty_proof_exact_six_ok",
      retainCurrent: false,
      candidate: cand,
      current: cur,
    };
  }
  return {
    applyCandidate: false,
    reason: "loyalty_proof_no_candidate",
    retainCurrent: cur.count > 0,
    candidate: cand,
    current: cur,
  };
}

/**
 * Publication / Factory 2.0 batch gate input.
 */
export function evaluateFactoryPermanentStandardsGate(input = {}) {
  const blocks = input.blocks || input.proposedBlocks || [];
  const headings = input.headings || [];
  const loyaltyParticipates = input.loyaltyParticipates !== false;

  const loyalty = validateLoyaltyProofExactSix(blocks, {
    requireParticipating: loyaltyParticipates,
  });
  if (loyaltyParticipates && loyalty.count === 0 && input.requireLoyaltyProof === true) {
    loyalty.ok = false;
    loyalty.blocked = true;
    loyalty.reasons = [...loyalty.reasons, "loyalty_proof_missing_for_participating_brand"];
  }

  const badHeadings = [];
  for (const h of headings) {
    const bad = findBadRegionalHeadingTokens(h);
    if (bad.length) badHeadings.push({ heading: h, badTokens: bad });
  }
  // Also scan loyalty/overview titles in blocks
  for (const b of blocks) {
    const title = nz(b.title);
    const bad = findBadRegionalHeadingTokens(title);
    if (bad.length) badHeadings.push({ heading: title, badTokens: bad, slotKey: b.slotKey });
  }

  const regionOk = badHeadings.length === 0;
  const loyaltyOk =
    loyalty.count === 0
      ? !loyaltyParticipates || input.requireLoyaltyProof !== true
      : loyalty.count === LOYALTY_PROOF_REQUIRED_COUNT && !loyalty.blocked;
  const ok = Boolean(loyaltyOk && regionOk);

  return {
    contract: BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1,
    lessons: ["BE-QL-LOYALTY-SIX-001", "BE-QL-REGION-ACRONYM-001"],
    factoryInheritance: ["1.0", "2.0"],
    ok,
    loyalty,
    region: { ok: regionOk, badHeadings },
    blocked: !ok,
  };
}
