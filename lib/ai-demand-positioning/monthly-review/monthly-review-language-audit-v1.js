/**
 * Clear-not-clever language audit for monthly executive reviews.
 */

import {
  FORBIDDEN_CAUSATION_PATTERNS,
  FORBIDDEN_CLEVER_PATTERNS,
} from "./monthly-review-contract-v1.js";

function collectStrings(node, out, path = "") {
  if (node == null) return;
  if (typeof node === "string") {
    out.push({ path, text: node });
    return;
  }
  if (Array.isArray(node)) {
    node.forEach((v, i) => collectStrings(v, out, `${path}[${i}]`));
    return;
  }
  if (typeof node === "object") {
    for (const [k, v] of Object.entries(node)) {
      if (k === "sources" || k === "gates" || k === "schema" || k === "learningRecord") continue;
      if (k === "implementationSteps" || k === "definitionOfDone" || k === "checklist") {
        // Structured playbook lists — audited separately for completeness, not clever-copy.
        continue;
      }
      collectStrings(v, out, path ? `${path}.${k}` : k);
    }
  }
}

export function auditMonthlyReviewLanguage(review) {
  const strings = [];
  collectStrings(review, strings);
  const flags = [];

  for (const { path, text } of strings) {
    for (const re of FORBIDDEN_CAUSATION_PATTERNS) {
      if (re.test(text)) {
        flags.push({ type: "CAUSATION", path, pattern: String(re), excerpt: text.slice(0, 160) });
      }
    }
    for (const re of FORBIDDEN_CLEVER_PATTERNS) {
      if (re.test(text)) {
        flags.push({ type: "CLEVER", path, pattern: String(re), excerpt: text.slice(0, 160) });
      }
    }
    if (/\bpromptId\b/i.test(text) || /\bsystem instruction\b/i.test(text)) {
      flags.push({ type: "METHODOLOGY_LEAK", path, excerpt: text.slice(0, 160) });
    }
  }

  return {
    gate: "MONTHLY_REVIEW_CLEAR_NOT_CLEVER_LANGUAGE",
    ok: flags.length === 0,
    flagCount: flags.length,
    flags,
  };
}

export function auditActionCausationLanguage(actions) {
  const flags = [];
  for (const a of actions || []) {
    const blob = [a.recommendedAction, a.rationale, a.expectedSignal, a.observedIssue]
      .filter(Boolean)
      .join("\n");
    for (const re of FORBIDDEN_CAUSATION_PATTERNS) {
      if (re.test(blob)) {
        flags.push({
          actionId: a.actionId,
          pattern: String(re),
          excerpt: blob.slice(0, 200),
        });
      }
    }
  }
  return {
    gate: "ACTION_CAUSATION_LANGUAGE_INTEGRITY",
    ok: flags.length === 0,
    flags,
  };
}
