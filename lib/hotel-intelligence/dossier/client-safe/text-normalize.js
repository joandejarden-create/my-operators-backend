/**
 * Unicode + markdown + provider-local citation cleanup for customer dossier text.
 */

export function normalizeUnicodeText(input) {
  let s = String(input || "");
  try {
    s = s.normalize("NFC");
  } catch {
    /* ignore */
  }
  // Strip common replacement / soft-separator artifacts
  s = s.replace(/[\uFFFD\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "");
  s = s.replace(/\u00AD/g, ""); // soft hyphen
  s = s.replace(/[\u200B-\u200D\uFEFF]/g, "");
  // Repair broken word joins from PDF/MD artifacts (letter + replacement + letter)
  s = s.replace(/([A-Za-z])\uFFFD([A-Za-z])/g, "$1-$2");
  return s;
}

/**
 * Strip provider-local citation markup like [1], [4][5], [source_abc].
 * Final citations must be regenerated from normalized claim↔source links.
 */
export function stripProviderLocalCitations(input) {
  let s = String(input || "");
  s = s.replace(/\[(?:\d{1,3}|source_[a-z0-9]+)(?:\s*,\s*(?:\d{1,3}|source_[a-z0-9]+))*\]/gi, "");
  s = s.replace(/\[\d{1,3}\]/g, "");
  s = s.replace(/\(\s*(?:sources?\s*)?\d{1,3}(?:\s*,\s*\d{1,3})+\s*\)/gi, "");
  s = s.replace(/[ \t]{2,}/g, " ");
  s = s.replace(/\n{3,}/g, "\n\n");
  return s.trim();
}

/**
 * Convert residual markdown emphasis to plain text for PDF/HTML safety.
 * Does not invent HTML — renderer handles links separately when structured.
 */
export function stripRawMarkdownArtifacts(input) {
  let s = String(input || "");
  s = s.replace(/\*\*([^*]+)\*\*/g, "$1");
  s = s.replace(/__([^_]+)__/g, "$1");
  s = s.replace(/`([^`]+)`/g, "$1");
  s = s.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g, "$1 ($2)");
  s = s.replace(/^#{1,6}\s+/gm, "");
  return s;
}

/** Rewrite common research-instruction leaks into customer-safe phrasing. */
export function rewriteResearchInstructionLeaks(input) {
  let s = String(input || "");
  s = s.replace(
    /Per the user'?s? directive[^.]*\./gi,
    "Public reporting is treated carefully and is not treated as registry-grade ownership proof."
  );
  s = s.replace(/\bdo NOT auto-promote\b[^.]*\./gi, "");
  s = s.replace(/\bdo not invent\b[^.]*\./gi, "Purchase consideration was not identified in reviewed public sources.");
  s = s.replace(/\bKnown Facts(?:\s*\([^)]*\))?/gi, "established property facts");
  s = s.replace(/\bin this research (?:cycle|session)\b/gi, "in reviewed public sources");
  s = s.replace(/\bnot feasible in this research session\b/gi, "not established in reviewed public sources");
  s = s.replace(/\boriginal Known Facts\b/gi, "established property facts");
  s = s.replace(/\bresearch (?:provider|module|artifact)\b/gi, "reviewed sources");

  // Packet 2.7-R6 — Full HI / addendum internal machinery → client language
  s = s.replace(
    /reconstructs the substantive Webhound research corpus[^.]*\./gi,
    "consolidates the strongest available evidence on ownership, operation, brand status, corporate relationships, people and asset history."
  );
  s = s.replace(/\bWebhound\b/gi, "reviewed research");
  s = s.replace(
    /remain research-grade until(?: independently reviewed and)? promoted into(?: the)? canonical Hotel Intelligence[^.]*\./gi,
    "remain subject to primary legal, contractual or property-level verification where indicated."
  );
  s = s.replace(
    /not automatically promoted to canonical Hotel Intelligence[^.]*\./gi,
    "should be validated against primary legal, contractual or property-level documents where indicated."
  );
  s = s.replace(
    /(?:The )?initial research directive assumed[^.]*\./gi,
    "Early evidence suggested a third-party management structure; subsequent primary filings established that the hotel is company-owned and self-operated."
  );
  s = s.replace(/\bOwnership Chain & PropCo research module\b/gi, "ownership and property-vehicle evidence");
  s = s.replace(/\bresearch module\b/gi, "evidence review");
  s = s.replace(/\bresearch directive\b/gi, "initial working hypothesis");
  s = s.replace(/\bfounder directive\b/gi, "investigation scope");
  s = s.replace(/\buser'?s? directive\b/gi, "investigation scope");
  s = s.replace(/\bSUPERSEDED BY SUBSEQUENT EVIDENCE:\s*/gi, "Resolved by later primary evidence: ");
  s = s.replace(/\bSUPERSEDED for those two items\.\s*/gi, "Those items are resolved. ");
  s = s.replace(/\bBreak\s*#\s*\d+\b/gi, "Resolved research question");
  s = s.replace(/\bCRITICAL NOTE:?\s*/gi, "");
  s = s.replace(/\bdo not (?:present|treat) superseded[^.]*\./gi, "");
  s = s.replace(/\bclaim corpus\b/gi, "supporting evidence");
  s = s.replace(/\bauto[_ -]?promote\b/gi, "promote");
  s = s.replace(/\bcanonical Hotel Intelligence profile\b/gi, "validated Hotel Intelligence record");
  s = s.replace(/\bprimary decision-maker\b/gi, "strategically relevant executive");
  s = s.replace(/\bthe key relationship holder\b/gi, "a likely relationship holder");
  s = s.replace(/\bdirect relationship manager\b/gi, "publicly evidenced brand-development contact");
  s = s.replace(/\blikely requires warm introduction[^.]*\./gi, "introductions through IR or brand relationship channels are typically the practical path.");
  s = s.replace(/\bLinkedIn via GSF company page\b/gi, "—");
  s = s.replace(/\bAppendix A\.\s*Research Reconciliation\b/gi, "Appendix A. Evidence Reconciliation");
  s = s.replace(/\bResearch Reconciliation\b/gi, "Evidence Reconciliation");
  return s.replace(/[ \t]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").trim();
}

export function prepareCustomerProse(input) {
  return rewriteResearchInstructionLeaks(
    stripRawMarkdownArtifacts(stripProviderLocalCitations(normalizeUnicodeText(input)))
  );
}

export function prepareCustomerProseDeep(value) {
  if (value == null) return value;
  if (typeof value === "string") return prepareCustomerProse(value);
  if (Array.isArray(value)) return value.map(prepareCustomerProseDeep);
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (k.startsWith("_") || k === "claim_handoff" || k === "raw_artifact_reference") {
        out[k] = v;
        continue;
      }
      out[k] = prepareCustomerProseDeep(v);
    }
    return out;
  }
  return value;
}
