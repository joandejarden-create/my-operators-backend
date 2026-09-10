/**
 * Structured renderer contract for Executive Read V3.
 * Prepares the interface — does not implement final visual polish.
 * Eliminates the assumption that ER is one flat paragraph.
 */

export const ADP_EXECUTIVE_READ_RENDERER_CONTRACT_V3 =
  "ADP_EXECUTIVE_READ_RENDERER_CONTRACT_V3";

export const EXECUTIVE_READ_RENDERER_CONTRACT_V3 = Object.freeze({
  contract: ADP_EXECUTIVE_READ_RENDERER_CONTRACT_V3,
  accepts: Object.freeze([
    "compositionVersion",
    "sections",
    "numericAnchors",
    "primaryIssueId",
    "insightArchetype",
    "evidenceTrace",
    "activated",
  ]),
  sectionOrder: Object.freeze([
    "headline",
    "keyInsight",
    "whyItMatters",
    "focusNow",
    "watch",
    "whatToReview",
  ]),
  rules: Object.freeze({
    flatTextContentOnly: false,
    singleParagraphShell: false,
    optionalWatch: true,
    omitNullWatch: true,
    numericAnchorsInlineBySection: true,
    productionDefaultUntilActivation: "legacy_writeup_body",
  }),
});

/**
 * Normalize a V3 payload into a renderer-ready view model.
 * Production UI must ignore this until activated === true.
 */
export function buildExecutiveReadRendererViewModelV3(composition) {
  if (!composition?.sections) {
    return { ok: false, reason: "MISSING_SECTIONS" };
  }
  const sections = EXECUTIVE_READ_RENDERER_CONTRACT_V3.sectionOrder
    .map((key) => {
      const value = composition.sections[key];
      if (key === "watch" && (value == null || value === "")) return null;
      return {
        key,
        label: labelFor(key),
        text: value,
        anchors: (composition.numericAnchors || []).filter(
          (a) => String(a.section || "KEY_INSIGHT").toLowerCase().replace(/_/g, "") ===
            key.toLowerCase().replace(/_/g, "") ||
            (key === "keyInsight" && String(a.section).toUpperCase() === "KEY_INSIGHT")
        ),
      };
    })
    .filter(Boolean);

  return {
    ok: true,
    contract: ADP_EXECUTIVE_READ_RENDERER_CONTRACT_V3,
    compositionVersion: composition.compositionVersion,
    activated: composition.activated === true,
    primaryIssueId: composition.primaryIssueId,
    insightArchetype: composition.insightArchetype,
    sections,
    // Compatibility flatten — NOT for production default rendering
    flattenedProseForLegacy: sections.map((s) => s.text).join("\n\n"),
  };
}

function labelFor(key) {
  return (
    {
      headline: "Headline",
      keyInsight: "Key Insight",
      whyItMatters: "Why It Matters",
      focusNow: "Focus Now",
      watch: "Watch",
      whatToReview: "What to Review",
    }[key] || key
  );
}
