/**
 * Packet 2.7-R6 — structured diligence items → Open Questions + Verify Next views.
 */

const TOPIC_TEMPLATES = Object.freeze({
  agreement_structure: {
    question: "What is the actual Hyatt/GSF agreement structure for this hotel?",
    why_it_matters:
      "Determines economics, control rights, termination rights, change-of-control provisions and future operating flexibility.",
    best_next_evidence:
      "Executed agreement, legal summary, or direct owner/brand confirmation of franchise, management, license, or collaboration terms.",
    verification_action:
      "Request the current executed agreement or a summary of principal commercial terms from owner or brand counsel.",
  },
  conversion_status: {
    question: "Does the Breathless conversion remain active, postponed, rescoped, or cancelled?",
    why_it_matters:
      "The opportunity thesis changes if conversion is no longer live or has been materially redefined.",
    best_next_evidence:
      "Current owner/brand confirmation, updated investor disclosure, or live brand inventory listing.",
    verification_action:
      "Confirm conversion status in writing with GSF and Hyatt Inclusive Collection contacts.",
  },
  capex_scope: {
    question: "What is the total conversion CapEx and remaining spend?",
    why_it_matters:
      "Determines remaining capital burden, disruption period, and whether current work is cosmetic or transformational.",
    best_next_evidence:
      "PIP, renovation budget, contractor scope, owner CapEx schedule, and property inspection.",
    verification_action:
      "Obtain CapEx schedule / PIP and inspect guestrooms, public areas, and MEP condition.",
  },
  operating_performance: {
    question: "How is the hotel performing during renovation?",
    why_it_matters:
      "Determines whether disruption is temporary or creating material operating stress.",
    best_next_evidence:
      "Monthly occupancy, ADR, RevPAR, EBITDA/GOP, guest satisfaction, and renovation displacement metrics.",
    verification_action:
      "Request trailing-twelve and in-renovation monthly operating statements with guest-satisfaction trend.",
  },
  physical_condition: {
    question: "What is the current physical condition of guestrooms, public areas, and MEP?",
    why_it_matters:
      "Guest maintenance patterns are diligence leads; underwriting requires inspection-grade confirmation.",
    best_next_evidence:
      "Independent property inspection, engineering report, and renovation completion certificates.",
    verification_action:
      "Schedule a condition survey focused on HVAC, plumbing, elevators, and rooms under renovation.",
  },
  post_conversion_model: {
    question: "What operating model is anticipated after any conversion?",
    why_it_matters:
      "Family-to-adults-only shifts can reprice labor, F&B, distribution, and competitive set assumptions.",
    best_next_evidence:
      "Brand operating standards, staffing plan, and owner/operator transition plan.",
    verification_action:
      "Confirm anticipated brand standards and operating concept with owner and brand development.",
  },
  timeline_status: {
    question: "What is the current revised target completion or opening date?",
    why_it_matters:
      "Sets the remaining disruption window, financing assumptions, and near-term commercial timing risk.",
    best_next_evidence:
      "Owner CapEx schedule, brand opening calendar, or current investor disclosure naming a revised date.",
    verification_action:
      "Request the current target completion/opening date and any change notices since the original announcement.",
  },
  portfolio_intent: {
    question: "Does the owner treat this property as a core hold or a potential disposition candidate?",
    why_it_matters:
      "Portfolio room-count change alone is not disposition proof; owner intent affects deal path and underwriting.",
    best_next_evidence:
      "Transaction filings, board/investor commentary on asset sales, or direct owner confirmation of hold vs sell posture.",
    verification_action:
      "Confirm hold-vs-disposition posture with IR/CFO and screen recent filings for asset-sale language tied to this hotel.",
  },
  change_of_control: {
    question: "Would the brand support or oppose a mid-conversion ownership change?",
    why_it_matters:
      "Change-of-control and consent rights can block, delay, or reprice a transfer during an active brand transition.",
    best_next_evidence:
      "Agreement change-of-control clauses, brand consent requirements, or counsel summary of transfer restrictions.",
    verification_action:
      "Obtain counsel review of transfer/consent provisions in the current Hyatt/GSF agreement.",
  },
});

function clean(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim();
}

function classifyQuestionTopic(text) {
  const t = clean(text).toLowerCase();
  // Specific lanes before broad conversion/timeline keywords
  if (/agreement|franchise|management contract|license|collaboration|partnership structure|default|remedy|renegotiation/.test(t)) {
    return "agreement_structure";
  }
  if (/capex|capital expenditure|renovation budget|remaining spend|spent to date|investment budget/.test(t)) {
    return "capex_scope";
  }
  if (/occupancy|adr|revpar|ebitda|gop|performing|performance|during renovation|property-level financial/.test(t)) {
    return "operating_performance";
  }
  if (/physical|condition|mep|hvac|inspect|guestroom|public area|deep physical|cosmetic rebrand/.test(t)) {
    return "physical_condition";
  }
  if (/operating model|adults-only|after (?:any )?conversion|staffing/.test(t)) {
    return "post_conversion_model";
  }
  if (/disposition|core strategic|hold(?:ing)?|sell(?:er)?|asset sale/.test(t)) {
    return "portfolio_intent";
  }
  if (/change in ownership|change-of-control|oppose a change|support or oppose/.test(t)) {
    return "change_of_control";
  }
  if (/target completion|revised.*date|completion date|opening date|timeline/.test(t)) {
    return "timeline_status";
  }
  if (/conversion|breathless|active|postpon|cancel|rescope/.test(t)) {
    return "conversion_status";
  }
  return null;
}

function isGenericBoilerplate(why, evidence) {
  return (
    /unresolved item from this change/i.test(why || "") ||
    /^primary-source confirmation from owner, brand, or filing\.?$/i.test(clean(evidence || ""))
  );
}

/**
 * Normalize open questions into diligence items with topic-specific why/evidence/actions.
 */
export function buildDiligenceItems(openQuestions = [], opts = {}) {
  const items = [];
  const seen = new Set();

  for (let i = 0; i < (openQuestions || []).length; i += 1) {
    const q = openQuestions[i] || {};
    const question = clean(q.question || q.title || "");
    if (!question || question.length < 12) continue;
    // Brand-family events without this hotel/owner linkage → exclude
    if (
      /breathless cancun|cancun soul|el mencho|cjng|tapalpa/i.test(question)
    ) {
      continue;
    }
    const key = question.slice(0, 100).toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);

    const topic = q.topic || classifyQuestionTopic(question) || "generic";
    const tpl = TOPIC_TEMPLATES[topic];
    let why = clean(q.why_it_matters || "");
    let evidence = clean(q.best_next_evidence || q.what_would_resolve || "");
    let action = clean(q.verification_action || "");

    if (tpl) {
      if (isGenericBoilerplate(why, evidence) || !why || why.length < 40) why = tpl.why_it_matters;
      if (isGenericBoilerplate(why, evidence) || !evidence || evidence.length < 40) {
        evidence = tpl.best_next_evidence;
      }
      // Re-check evidence after why may have been upgraded but evidence still generic
      if (isGenericBoilerplate("", evidence) || evidence.length < 40) {
        evidence = tpl.best_next_evidence;
      }
      if (!action || action.length < 40) action = tpl.verification_action;
    } else if (isGenericBoilerplate(why, evidence)) {
      why =
        "Material diligence uncertainty that could change commercial timing, risk, or underwriting for this hotel.";
      evidence =
        "Owner, brand, counsel, or current filing confirmation specific to this property and the open issue.";
      action = `Confirm with primary sources: ${question}`;
    }
    if (!why) {
      why =
        "Material diligence uncertainty that could change commercial timing, risk, or underwriting for this hotel.";
    }
    if (!evidence) {
      evidence =
        "Owner, brand, counsel, or current filing confirmation specific to this property and the open issue.";
    }
    if (!action) action = `Confirm: ${question}`;

    items.push({
      question_id: q.open_question_id || q.id || `oq_${items.length + 1}`,
      question,
      why_it_matters: why,
      best_next_evidence: evidence,
      verification_action: action,
      priority: q.priority || items.length + 1,
      topic,
    });
  }

  // Ensure core diligence lanes exist for Change & Opportunity when thin
  if (opts.ensureChangeOpportunityCore) {
    for (const topic of Object.keys(TOPIC_TEMPLATES)) {
      if (items.some((it) => it.topic === topic)) continue;
      if (items.length >= 9) break;
      const tpl = TOPIC_TEMPLATES[topic];
      items.push({
        question_id: `oq_${topic}`,
        question: tpl.question,
        why_it_matters: tpl.why_it_matters,
        best_next_evidence: tpl.best_next_evidence,
        verification_action: tpl.verification_action,
        priority: items.length + 1,
        topic,
      });
    }
  }

  return items.slice(0, opts.max || 9);
}

export function openQuestionsView(items = []) {
  return (items || []).map((it) => ({
    id: it.question_id,
    open_question_id: it.question_id,
    question: it.question,
    title: it.question,
    why_it_matters: it.why_it_matters,
    best_next_evidence: it.best_next_evidence,
    what_would_resolve: it.best_next_evidence,
    topic: it.topic,
    priority: it.priority,
  }));
}

/** Verify Next: action-oriented view of the same diligence items (not duplicated wording). */
export function verifyNextView(items = []) {
  return (items || [])
    .map(
      (it, i) =>
        `${i + 1}. ${it.verification_action}\nWhy it matters: ${it.why_it_matters}\nBest next evidence: ${it.best_next_evidence}`
    )
    .join("\n\n");
}

export { TOPIC_TEMPLATES, classifyQuestionTopic, isGenericBoilerplate };
