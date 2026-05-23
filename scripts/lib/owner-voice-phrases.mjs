/**
 * Phrases that read like internal editorial notes, not owner-facing copy.
 * Used by audit-brand-explorer-owner-voice.mjs
 */
export const INTERNAL_VOICE_PATTERNS = [
  { id: "narratives_should", re: /\bnarratives should\b/i, hint: "Rewrite for the owner (e.g. 'When you evaluate CALA…')" },
  { id: "proof_point", re: /\bproof point(s)?\b/i, hint: "Use 'reference hotel' or 'example'" },
  { id: "directional_proof", re: /\bdirectional proof\b/i, hint: "Use 'reference' or 'benchmark'" },
  { id: "before_external", re: /\bbefore external use\b/i, hint: "Remove; say 'confirm in your FDD'" },
  { id: "legal_comms", re: /\bLegal\/Comms\b/i, hint: "Say 'confirm in your franchise disclosure'" },
  { id: "owner_lens", re: /\bOwner lens:/i, hint: "Use owner-facing planning language" },
  { id: "deal_lens", re: /\bDeal lens\b/i, hint: "Use 'What you are buying' or similar" },
  { id: "use_when_owner", re: /\bUse when the owner story\b/i, hint: "Use 'Relevant when you are evaluating…'" },
  { id: "use_for_cala", re: /\bUse for CALA\b/i, hint: "Use 'Relevant for CALA…'" },
  { id: "owner_conversations", re: /\bowner conversations\b/i, hint: "Address the owner directly" },
  { id: "reads_as", re: /\breads as\b/i, hint: "Use 'is positioned as' or plain description" },
  { id: "the_story_is", re: /\bthe story is\b/i, hint: "Describe the hotel or market directly" },
  { id: "regional_teams", re: /\bRegional teams:/i, hint: "Remove internal org reference" },
  { id: "brand_on_page_framing", re: /\bBrand on a Page framing\b/i, hint: "Remove internal doc reference" },
  { id: "owner_operator_lens", re: /\bOwner \/ operator lens:/i, hint: "Use 'For owners and operators:'" },
  { id: "americas_narratives", re: /\bAmericas narratives\b/i, hint: "Use 'Americas franchise materials'" },
  { id: "development_materials", re: /\bdevelopment materials\b/i, hint: "Use 'franchise disclosure' or 'brand materials'" },
  { id: "illustrate_where", re: /\billustrate where\b/i, hint: "Use 'compare to' or 'shows how'" },
  { id: "this_card_summarizes", re: /\bthis card summarizes\b/i, hint: "Say 'Public information only—confirm in diligence'" },
  { id: "treat_as_performance_proof", re: /\bperformance proof\b/i, hint: "Say 'do not treat opening press as pro forma proof'" },
  { id: "owner_narrative", re: /\bowner narrative\b/i, hint: "Use 'your situation' or 'your asset'" },
  { id: "choice_colon_consumer", re: /\bChoice: a distinct\b/i, hint: "Remove franchisor pitch to internal audience" },
  { id: "verify_before_external", re: /\bverify.*before external\b/i, hint: "Owner-facing confirmation only" },
  { id: "owner_narrative_phrase", re: /\bowner narrative\b/i, hint: "Use 'your situation' or 'your asset'" },
  { id: "owner_story", re: /\bowner story\b/i, hint: "Use 'your situation' or 'your asset type'" },
  { id: "one_pager", re: /\bthe one-pager\b|\bone-pager efficiency\b|\bone-pager story\b/i, hint: "Use 'brand summary' or describe economics directly" },
  { id: "regional_decks", re: /\bregional decks\b/i, hint: "Use 'your market' or 'airport and urban sets'" },
  { id: "illustrative_in_decks", re: /\billustrative only in decks\b/i, hint: "Say 'confirm current campaign rules in your FDD'" },
  { id: "americas_narrative_phrase", re: /\bAmericas narrative\b/i, hint: "Use 'Americas positioning' or drop internal label" },
  { id: "brand_on_page_header", re: /\bBrand on a Page\b/i, hint: "Remove internal doc title; lead with owner value" },
  { id: "performance_proxy", re: /\bperformance proxy\b/i, hint: "Say what the owner should check on their deal" },
  { id: "drive_economics", re: /\bdrive economics\b/i, hint: "Explain what matters for the owner's budget in plain terms" },
  { id: "do_not_assume", re: /\bdo not assume\b/i, hint: "Say what to use instead (e.g. local comps, your contract mix)" },
  { id: "underwriting_stays", re: /\bunderwriting stays\b|\bbefore underwriting\b/i, hint: "Say 'for your deal' or 'before you sign'" },
  { id: "treat_as_positioning", re: /\bTreat as positioning\b/i, hint: "Say what to confirm for your building" },
  { id: "dealality_takeaway_heading", re: /\bDealality takeaway\b/i, hint: "Use 'What you can learn from this hotel'" },
  { id: "dealality_interpretation_heading", re: /\bDealality interpretation\b/i, hint: "Use owner-facing takeaway heading" },
];

export function scanTextForInternalVoice(text) {
  if (!text || typeof text !== "string") return [];
  const hits = [];
  for (const p of INTERNAL_VOICE_PATTERNS) {
    if (p.re.test(text)) hits.push(p);
  }
  return hits;
}
