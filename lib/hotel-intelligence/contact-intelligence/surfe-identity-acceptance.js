/**
 * Surfe / paid-provider identity acceptance for Contact Intelligence.
 *
 * Dealality establishes WHO the person is.
 * Surfe may only help with HOW to reach them — never replace identity.
 *
 * Evaluation and future production gates share this contract.
 * No production merges happen here.
 */

export const IDENTITY_DECISION = Object.freeze({
  ACCEPTED: "ACCEPTED",
  ACCEPTED_WITH_LIMITED_EVIDENCE: "ACCEPTED_WITH_LIMITED_EVIDENCE",
  CORROBORATION_ONLY: "CORROBORATION_ONLY",
  AMBIGUOUS: "AMBIGUOUS",
  REJECTED: "REJECTED",
  NOT_FOUND: "NOT_FOUND",
});

export const EMAIL_OUTCOME = Object.freeze({
  NEW_DIRECT_WORK_EMAIL: "NEW_DIRECT_WORK_EMAIL",
  NEW_ROLE_BASED_EMAIL: "NEW_ROLE_BASED_EMAIL",
  CORROBORATES_OFFICIAL_EMAIL: "CORROBORATES_OFFICIAL_EMAIL",
  CORROBORATES_INFERRED_EMAIL: "CORROBORATES_INFERRED_EMAIL",
  DIFFERENT_BUT_PLAUSIBLE: "DIFFERENT_BUT_PLAUSIBLE",
  CONFLICTS_WITH_OFFICIAL: "CONFLICTS_WITH_OFFICIAL",
  UNVERIFIED: "UNVERIFIED",
  NOT_FOUND: "NOT_FOUND",
  IDENTITY_REJECTED: "IDENTITY_REJECTED",
});

export const PHONE_OUTCOME = Object.freeze({
  NEW_DIRECT_PHONE: "NEW_DIRECT_PHONE",
  NEW_MOBILE_PROFESSIONAL: "NEW_MOBILE_PROFESSIONAL",
  NEW_OFFICE_PHONE: "NEW_OFFICE_PHONE",
  SAME_MAIN_LINE: "SAME_MAIN_LINE",
  CORROBORATION_ONLY: "CORROBORATION_ONLY",
  AMBIGUOUS: "AMBIGUOUS",
  OTHER_PERSON_PHONE_COLLISION: "OTHER_PERSON_PHONE_COLLISION",
  NOT_FOUND: "NOT_FOUND",
  IDENTITY_REJECTED: "IDENTITY_REJECTED",
});

/** Normalize phone digits for ownership / collision checks (reusable). */
export function normalizePhoneDigits(phone) {
  return String(phone || "").replace(/\D/g, "");
}

/**
 * True when provider phone matches another known person's published line.
 * Reusable field-ownership gate — Dealality may accept WHO but still reject HOW.
 */
export function detectOtherPersonPhoneCollision(surfePhone, knownOtherPersonPhones = []) {
  const a = normalizePhoneDigits(surfePhone);
  if (!a || a.length < 7) return null;
  const a10 = a.slice(-10);
  for (const row of knownOtherPersonPhones || []) {
    const b = normalizePhoneDigits(row?.phone);
    if (!b) continue;
    const b10 = b.slice(-10);
    if (a10 === b10 || a.endsWith(b10) || b.endsWith(a10)) {
      return {
        collision: true,
        otherPersonName: row.name || null,
        otherPersonOrg: row.org || row.organization || null,
        otherPersonPhone: row.phone,
      };
    }
  }
  return null;
}

export const MERGE_SIMULATION = Object.freeze({
  WOULD_ACCEPT: "WOULD_ACCEPT",
  WOULD_ACCEPT_AS_CORROBORATION_ONLY: "WOULD_ACCEPT_AS_CORROBORATION_ONLY",
  WOULD_HOLD_FOR_REVIEW: "WOULD_HOLD_FOR_REVIEW",
  WOULD_REJECT: "WOULD_REJECT",
});

export function normalizePersonName(s) {
  return String(s || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z\s]/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

export function nameTokens(s) {
  return normalizePersonName(s)
    .split(" ")
    .filter((t) => t.length >= 2 && !/^(jr|sr|ii|iii|iv|md|phd)$/i.test(t));
}

/**
 * Exact / strong normalized name agreement.
 */
export function evaluateNameMatch(expectedName, returnedName) {
  const a = nameTokens(expectedName);
  const b = nameTokens(returnedName);
  if (!a.length || !b.length) {
    return { ok: false, signal: false, contradiction: false, reason: "missing_name" };
  }
  const lastA = a[a.length - 1];
  const lastB = b[b.length - 1];
  if (lastA !== lastB) {
    // Contradictory surname is a hard failure when both present
    return {
      ok: false,
      signal: false,
      contradiction: true,
      reason: `surname_mismatch expected=${lastA} returned=${lastB}`,
    };
  }
  const firstOk =
    a[0] === b[0] || a[0].startsWith(b[0]) || b[0].startsWith(a[0]);
  if (!firstOk) {
    return {
      ok: false,
      signal: false,
      contradiction: true,
      reason: `first_name_mismatch expected=${a[0]} returned=${b[0]}`,
    };
  }
  const exact = normalizePersonName(expectedName) === normalizePersonName(returnedName);
  return {
    ok: true,
    signal: true,
    contradiction: false,
    reason: exact ? "exact_full_name" : "normalized_first_last_agreement",
  };
}

export function domainsRelated(a, b) {
  if (!a || !b) return false;
  const x = String(a).toLowerCase().replace(/^www\./, "");
  const y = String(b).toLowerCase().replace(/^www\./, "");
  return x === y || x.endsWith(`.${y}`) || y.endsWith(`.${x}`);
}

export function evaluateOrganizationMatch({
  expectedOrganization,
  expectedDomain,
  returnedOrganization,
  returnedDomain,
} = {}) {
  const expOrg = normalizePersonName(expectedOrganization || "");
  const retOrg = normalizePersonName(returnedOrganization || "");
  const expDom = String(expectedDomain || "").toLowerCase();
  const retDom = String(returnedDomain || "").toLowerCase();

  const domainOk = expDom && retDom && domainsRelated(expDom, retDom);
  let orgOk = false;
  if (expOrg && retOrg) {
    const expKey = expOrg.replace(/\b(inc|llc|ltd|corp|corporation|association|company|co)\b/g, "").trim();
    const retKey = retOrg.replace(/\b(inc|llc|ltd|corp|corporation|association|company|co)\b/g, "").trim();
    orgOk =
      expKey.includes(retKey.slice(0, Math.min(8, retKey.length))) ||
      retKey.includes(expKey.slice(0, Math.min(8, expKey.length)));
  }

  if (retDom && expDom && !domainsRelated(expDom, retDom) && !orgOk) {
    return {
      ok: false,
      signal: false,
      contradiction: true,
      reason: `org_domain_mismatch expected=${expDom} returned=${retDom}`,
    };
  }

  if (domainOk || orgOk) {
    return {
      ok: true,
      signal: true,
      contradiction: false,
      reason: domainOk ? "domain_match" : "organization_name_soft_match",
    };
  }

  return {
    ok: false,
    signal: false,
    contradiction: false,
    reason: "organization_not_confirmed",
  };
}

/**
 * Email local-part must support the intended person.
 * Catches Fessler vs Kessler / wrong surname locals.
 */
export function evaluateEmailLocalPartMatch(email, expectedFullName) {
  const em = String(email || "").trim().toLowerCase();
  if (!em || !em.includes("@")) {
    return { ok: false, signal: false, contradiction: false, reason: "no_email" };
  }
  const local = em.split("@")[0].replace(/[^a-z0-9]/g, "");
  const tokens = nameTokens(expectedFullName);
  if (!local || tokens.length < 2) {
    return { ok: false, signal: false, contradiction: false, reason: "insufficient_tokens" };
  }
  const first = tokens[0];
  const last = tokens[tokens.length - 1];

  // first-name-only locals (farah@, cordelia@) are identity-safe — not surname contradictions
  if (local === first || first.startsWith(local) || local.startsWith(first)) {
    return { ok: true, signal: true, contradiction: false, reason: "first_name_as_local" };
  }

  // Hard reject: local contains a different multi-letter surname-like token that is not first/last
  // e.g. jkessler vs fessler
  if (local.includes(last) || last.includes(local.replace(first[0], "").slice(0, 6))) {
    return { ok: true, signal: true, contradiction: false, reason: "last_name_in_local" };
  }
  if (local.startsWith(first[0]) && local.includes(last.slice(0, 4))) {
    return { ok: true, signal: true, contradiction: false, reason: "initial_last_pattern" };
  }
  if (local.includes(first.slice(0, Math.min(4, first.length))) && local.includes(last.slice(0, 3))) {
    return { ok: true, signal: true, contradiction: false, reason: "first_last_fragments" };
  }

  // If local looks like first+alienLast (j + kessler), flag contradiction when edit distance to last is high
  const localTail = local.replace(new RegExp(`^${first[0]}`), "").replace(first, "");
  if (localTail.length >= 4 && !last.startsWith(localTail.slice(0, 4)) && !localTail.startsWith(last.slice(0, 4))) {
    return {
      ok: false,
      signal: false,
      contradiction: true,
      reason: `local_part_contradicts_surname local=${local} last=${last}`,
    };
  }

  return {
    ok: false,
    signal: false,
    contradiction: false,
    reason: `local_part_unsupported local=${local} last=${last}`,
  };
}

/**
 * LinkedIn / profile slug token agreement.
 */
export function evaluateLinkedInTokenMatch(linkedinUrl, expectedFullName) {
  const url = String(linkedinUrl || "").trim();
  if (!url) {
    return { ok: false, signal: false, contradiction: false, reason: "no_linkedin" };
  }
  const slugMatch = url.match(/linkedin\.com\/in\/([^/?#]+)/i);
  if (!slugMatch) {
    return { ok: false, signal: false, contradiction: false, reason: "no_slug" };
  }
  const slug = decodeURIComponent(slugMatch[1])
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "");
  const tokens = nameTokens(expectedFullName);
  if (tokens.length < 2) {
    return { ok: false, signal: false, contradiction: false, reason: "insufficient_name" };
  }
  const first = tokens[0];
  const last = tokens[tokens.length - 1];

  if (slug.includes(last) && (slug.includes(first) || slug.includes(first[0]))) {
    return { ok: true, signal: true, contradiction: false, reason: "slug_first_last_agreement" };
  }

  // Contradictory surname in slug (justin-kessler vs fessler)
  const slugParts = slug.split("-").filter((p) => p.length >= 3);
  const alienLast = slugParts.find(
    (p) => p !== first && p !== last && !last.startsWith(p) && !p.startsWith(last.slice(0, 4))
  );
  if (alienLast && /[a-z]{4,}/.test(alienLast) && !first.startsWith(alienLast)) {
    // If slug has a clear other surname-like token and does not contain expected last
    if (!slug.includes(last.slice(0, 4))) {
      return {
        ok: false,
        signal: false,
        contradiction: true,
        reason: `linkedin_slug_contradicts_surname slug=${slug} last=${last}`,
      };
    }
  }

  if (!slug.includes(last.slice(0, 4))) {
    return {
      ok: false,
      signal: false,
      contradiction: true,
      reason: `linkedin_slug_missing_surname slug=${slug} last=${last}`,
    };
  }

  return {
    ok: false,
    signal: false,
    contradiction: false,
    reason: `linkedin_slug_weak slug=${slug}`,
  };
}

export function evaluateTitleConsistency(expectedTitle, returnedTitle) {
  const a = normalizePersonName(expectedTitle || "");
  const b = normalizePersonName(returnedTitle || "");
  if (!a || !b) {
    return { ok: false, signal: false, contradiction: false, reason: "title_unavailable" };
  }
  const keywords = [
    "meeting",
    "event",
    "conference",
    "tournament",
    "director",
    "vp",
    "vice",
    "manager",
    "planner",
    "program",
    "summit",
    "housing",
    "operations",
  ];
  const aHit = keywords.filter((k) => a.includes(k));
  const bHit = keywords.filter((k) => b.includes(k));
  const overlap = aHit.filter((k) => bHit.includes(k));
  if (overlap.length >= 1 || a.split(" ").some((t) => t.length > 3 && b.includes(t))) {
    return { ok: true, signal: true, contradiction: false, reason: "title_plausible_overlap" };
  }
  // Not a hard contradiction by itself — employment titles often differ from volunteer event roles
  return { ok: false, signal: false, contradiction: false, reason: "title_unrelated_not_contradiction" };
}

/**
 * Multi-signal identity acceptance.
 * Requires ≥2 positive signals and zero strong contradictions.
 * Name + company domain alone is NOT sufficient.
 */
export function acceptSurfeIdentity({
  expectedFullName,
  expectedOrganization,
  expectedDomain,
  expectedTitle = null,
  returnedFullName = null,
  returnedOrganization = null,
  returnedDomain = null,
  returnedTitle = null,
  returnedEmail = null,
  returnedLinkedInUrl = null,
} = {}) {
  if (!returnedFullName && !returnedEmail && !returnedLinkedInUrl && !returnedOrganization) {
    return {
      decision: IDENTITY_DECISION.NOT_FOUND,
      positiveSignals: [],
      contradictions: [],
      reasons: ["no_person_payload"],
      wouldAcceptForMerge: false,
    };
  }

  const name = evaluateNameMatch(expectedFullName, returnedFullName || "");
  const org = evaluateOrganizationMatch({
    expectedOrganization,
    expectedDomain,
    returnedOrganization,
    returnedDomain,
  });
  const local = returnedEmail
    ? evaluateEmailLocalPartMatch(returnedEmail, expectedFullName)
    : { ok: false, signal: false, contradiction: false, reason: "no_email" };
  const li = returnedLinkedInUrl
    ? evaluateLinkedInTokenMatch(returnedLinkedInUrl, expectedFullName)
    : { ok: false, signal: false, contradiction: false, reason: "no_linkedin" };
  const title = evaluateTitleConsistency(expectedTitle, returnedTitle);

  const contradictions = [name, org, local, li]
    .filter((x) => x.contradiction)
    .map((x) => x.reason);
  const positiveSignals = [];
  if (name.signal) positiveSignals.push(`name:${name.reason}`);
  if (org.signal) positiveSignals.push(`org:${org.reason}`);
  if (local.signal) positiveSignals.push(`email_local:${local.reason}`);
  if (li.signal) positiveSignals.push(`linkedin:${li.reason}`);
  if (title.signal) positiveSignals.push(`title:${title.reason}`);

  if (contradictions.length) {
    return {
      decision: IDENTITY_DECISION.REJECTED,
      positiveSignals,
      contradictions,
      reasons: contradictions,
      wouldAcceptForMerge: false,
      checks: { name, org, local, li, title },
    };
  }

  // Name + domain alone is insufficient (v1 false-positive lesson)
  const onlyNameAndOrg =
    positiveSignals.length === 2 &&
    positiveSignals.some((s) => s.startsWith("name:")) &&
    positiveSignals.some((s) => s.startsWith("org:")) &&
    !local.signal &&
    !li.signal;

  if (onlyNameAndOrg) {
    return {
      decision: IDENTITY_DECISION.AMBIGUOUS,
      positiveSignals,
      contradictions: [],
      reasons: ["name_plus_domain_alone_insufficient"],
      wouldAcceptForMerge: false,
      checks: { name, org, local, li, title },
    };
  }

  if (positiveSignals.length >= 3 && name.signal && (local.signal || li.signal)) {
    return {
      decision: IDENTITY_DECISION.ACCEPTED,
      positiveSignals,
      contradictions: [],
      reasons: ["multi_signal_strong"],
      wouldAcceptForMerge: true,
      checks: { name, org, local, li, title },
    };
  }

  if (positiveSignals.length >= 2 && name.signal && (local.signal || li.signal || (org.signal && title.signal))) {
    // name + local OR name + li OR (name + org + title) with ≥2 — but name+org alone already blocked
    if (local.signal || li.signal) {
      return {
        decision: IDENTITY_DECISION.ACCEPTED,
        positiveSignals,
        contradictions: [],
        reasons: ["multi_signal_accepted"],
        wouldAcceptForMerge: true,
        checks: { name, org, local, li, title },
      };
    }
    return {
      decision: IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE,
      positiveSignals,
      contradictions: [],
      reasons: ["limited_evidence_name_org_title"],
      wouldAcceptForMerge: false,
      checks: { name, org, local, li, title },
    };
  }

  if (positiveSignals.length >= 2) {
    return {
      decision: IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE,
      positiveSignals,
      contradictions: [],
      reasons: ["two_signals_but_missing_local_or_linkedin"],
      wouldAcceptForMerge: false,
      checks: { name, org, local, li, title },
    };
  }

  if (positiveSignals.length === 1) {
    return {
      decision: IDENTITY_DECISION.AMBIGUOUS,
      positiveSignals,
      contradictions: [],
      reasons: ["single_signal_only"],
      wouldAcceptForMerge: false,
      checks: { name, org, local, li, title },
    };
  }

  return {
    decision: IDENTITY_DECISION.AMBIGUOUS,
    positiveSignals,
    contradictions: [],
    reasons: ["insufficient_identity_evidence"],
    wouldAcceptForMerge: false,
    checks: { name, org, local, li, title },
  };
}

export function isGenericOrRoleLocal(email) {
  const local = String(email || "")
    .toLowerCase()
    .split("@")[0]
    .replace(/[._-]/g, "");
  return /^(info|contact|office|admin|hello|support|registrar|tournament|tournaments|gadinst|associatedirector|meetings|events|conference|housing|nice)$/i.test(
    local
  );
}

export function classifySurfeEmailOutcome({
  identityDecision,
  surfeEmail,
  baselineEmail,
  baselineEmailType,
  baselineEmailVerification,
} = {}) {
  if (
    identityDecision === IDENTITY_DECISION.REJECTED ||
    identityDecision === IDENTITY_DECISION.AMBIGUOUS ||
    identityDecision === IDENTITY_DECISION.NOT_FOUND
  ) {
    return identityDecision === IDENTITY_DECISION.NOT_FOUND
      ? EMAIL_OUTCOME.NOT_FOUND
      : EMAIL_OUTCOME.IDENTITY_REJECTED;
  }
  if (!surfeEmail) return EMAIL_OUTCOME.NOT_FOUND;

  const base = String(baselineEmail || "").trim().toLowerCase();
  const got = String(surfeEmail).trim().toLowerCase();
  if (base && got === base) {
    if (baselineEmailVerification === "OFFICIAL_SOURCE_VERIFIED") {
      return EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL;
    }
    if (baselineEmailType === "INFERRED" || baselineEmailVerification === "DOMAIN_PATTERN_INFERRED") {
      return EMAIL_OUTCOME.CORROBORATES_INFERRED_EMAIL;
    }
    return EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL;
  }

  // Official verified direct/role email wins — Surfe must not overwrite with a different generic
  if (
    baselineEmailVerification === "OFFICIAL_SOURCE_VERIFIED" &&
    base &&
    got !== base &&
    isGenericOrRoleLocal(got)
  ) {
    return EMAIL_OUTCOME.CONFLICTS_WITH_OFFICIAL;
  }

  if (isGenericOrRoleLocal(got)) return EMAIL_OUTCOME.NEW_ROLE_BASED_EMAIL;

  // Prefer local match already enforced by identity gate for ACCEPTED
  if (baselineEmailType === "ROLE_BASED" || baselineEmailType === "GENERIC_ORGANIZATION" || !baselineEmail) {
    return EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL;
  }
  if (base && got !== base) return EMAIL_OUTCOME.DIFFERENT_BUT_PLAUSIBLE;
  return EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL;
}

export function classifySurfePhoneOutcome({
  identityDecision,
  surfePhone,
  baselinePhone,
  baselinePhoneType,
  knownOtherPersonPhones = [],
  providerPhoneField = "mobilePhones",
} = {}) {
  if (
    identityDecision === IDENTITY_DECISION.REJECTED ||
    identityDecision === IDENTITY_DECISION.AMBIGUOUS ||
    identityDecision === IDENTITY_DECISION.NOT_FOUND
  ) {
    return identityDecision === IDENTITY_DECISION.NOT_FOUND
      ? PHONE_OUTCOME.NOT_FOUND
      : PHONE_OUTCOME.IDENTITY_REJECTED;
  }
  if (!surfePhone) return PHONE_OUTCOME.NOT_FOUND;

  const collision = detectOtherPersonPhoneCollision(surfePhone, knownOtherPersonPhones);
  if (collision?.collision) {
    return PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION;
  }

  const a = normalizePhoneDigits(surfePhone);
  const b = normalizePhoneDigits(baselinePhone);
  if (b && a && (a.endsWith(b.slice(-10)) || b.endsWith(a.slice(-10)))) {
    if (baselinePhoneType === "MAIN_ORGANIZATION") return PHONE_OUTCOME.SAME_MAIN_LINE;
    return PHONE_OUTCOME.CORROBORATION_ONLY;
  }

  if (providerPhoneField === "mobilePhones") return PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL;
  if (baselinePhoneType === "MAIN_ORGANIZATION" || !baselinePhone) {
    return PHONE_OUTCOME.NEW_OFFICE_PHONE;
  }
  return PHONE_OUTCOME.NEW_DIRECT_PHONE;
}

export function isMeaningfulSurfeImprovement({ emailOutcome, phoneOutcome } = {}) {
  const goodEmail = [
    EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL,
    EMAIL_OUTCOME.CORROBORATES_INFERRED_EMAIL,
  ].includes(emailOutcome);
  // Role-based new email is not meaningful
  const goodPhone = [
    PHONE_OUTCOME.NEW_DIRECT_PHONE,
    PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL,
    PHONE_OUTCOME.NEW_OFFICE_PHONE,
  ].includes(phoneOutcome);
  return goodEmail || goodPhone;
}

/**
 * Simulate future production merge decision — does not write.
 * Official source always wins over Surfe for conflicts.
 */
export function simulateProductionMergeDecision({
  identityDecision,
  emailOutcome,
  phoneOutcome,
  baselineEmailVerification,
} = {}) {
  if (
    identityDecision === IDENTITY_DECISION.REJECTED ||
    identityDecision === IDENTITY_DECISION.NOT_FOUND
  ) {
    return MERGE_SIMULATION.WOULD_REJECT;
  }
  if (identityDecision === IDENTITY_DECISION.AMBIGUOUS) {
    return MERGE_SIMULATION.WOULD_HOLD_FOR_REVIEW;
  }
  if (identityDecision === IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE) {
    return MERGE_SIMULATION.WOULD_HOLD_FOR_REVIEW;
  }

  // ACCEPTED identity
  if (
    emailOutcome === EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL ||
    phoneOutcome === PHONE_OUTCOME.CORROBORATION_ONLY ||
    phoneOutcome === PHONE_OUTCOME.SAME_MAIN_LINE
  ) {
    if (
      !isMeaningfulSurfeImprovement({ emailOutcome, phoneOutcome }) &&
      (emailOutcome === EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL ||
        phoneOutcome === PHONE_OUTCOME.CORROBORATION_ONLY ||
        phoneOutcome === PHONE_OUTCOME.SAME_MAIN_LINE)
    ) {
      return MERGE_SIMULATION.WOULD_ACCEPT_AS_CORROBORATION_ONLY;
    }
  }

  if (emailOutcome === EMAIL_OUTCOME.CONFLICTS_WITH_OFFICIAL) {
    return MERGE_SIMULATION.WOULD_REJECT;
  }

  if (isMeaningfulSurfeImprovement({ emailOutcome, phoneOutcome })) {
    // Official verified email preserved; Surfe fills missing or upgrades role→direct as additive
    if (
      baselineEmailVerification === "OFFICIAL_SOURCE_VERIFIED" &&
      emailOutcome === EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL
    ) {
      return MERGE_SIMULATION.WOULD_ACCEPT; // additive direct, does not overwrite official
    }
    return MERGE_SIMULATION.WOULD_ACCEPT;
  }

  if (
    emailOutcome === EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL ||
    phoneOutcome === PHONE_OUTCOME.CORROBORATION_ONLY
  ) {
    return MERGE_SIMULATION.WOULD_ACCEPT_AS_CORROBORATION_ONLY;
  }

  return MERGE_SIMULATION.WOULD_HOLD_FOR_REVIEW;
}
