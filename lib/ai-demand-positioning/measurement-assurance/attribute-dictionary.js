/**
 * Canonical ADP attribute dictionary V1 + lightweight consistency audit.
 * NOT OBSERVED ≠ FACTUALLY ABSENT.
 */

import { ASSURANCE_ATTRIBUTE_DICT_VERSION } from "./version.js";

export const ATTRIBUTE_DICTIONARY_V1 = Object.freeze({
  version: ASSURANCE_ATTRIBUTE_DICT_VERSION,
  inferencePolicy: "OBSERVED_ONLY_NO_NEGATIVE_FROM_SILENCE",
  attributes: Object.freeze([
    {
      id: "beachfront",
      label: "Beachfront / beach access",
      meaning: "Property has beachfront location or direct beach access.",
      aliases: ["beachfront", "on the beach", "beach access", "oceanfront"],
      positiveEvidence: ["explicit beachfront/oceanfront claim tied to subject"],
      negativeEvidence: ["explicit inland / not on beach"],
      unknown: "No beach framing in response",
      contradiction: "Prefer unknown over inventing absence",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "walkability",
      label: "Walkability",
      meaning: "Walking distance to key attractions / walkable neighborhood.",
      aliases: ["walking distance", "walk to", "steps from", "walkable"],
      positiveEvidence: ["walk / walking distance to named place"],
      negativeEvidence: ["requires car / not walkable"],
      unknown: "No walkability language",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "family_suitability",
      label: "Family suitability",
      meaning: "Suitable for families with children.",
      aliases: ["family-friendly", "kids", "children"],
      positiveEvidence: ["family-friendly / kids amenities"],
      negativeEvidence: ["adults-only"],
      unknown: "No family framing",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "connecting_rooms",
      label: "Connecting rooms",
      meaning: "Connecting / adjoining rooms available.",
      aliases: ["connecting rooms", "adjoining rooms"],
      positiveEvidence: ["connecting/adjoining rooms stated"],
      negativeEvidence: ["no connecting rooms"],
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "suites",
      label: "Suites",
      meaning: "Suite inventory available.",
      aliases: ["suite", "suites"],
      positiveEvidence: ["suite offerings"],
      negativeEvidence: null,
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "pool",
      label: "Pool",
      meaning: "Swimming pool on property.",
      aliases: ["pool", "swimming pool", "rooftop pool"],
      positiveEvidence: ["pool stated for subject"],
      negativeEvidence: ["no pool"],
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "spa",
      label: "Spa",
      meaning: "Spa / wellness treatments on site.",
      aliases: ["spa", "wellness spa"],
      positiveEvidence: ["spa stated"],
      negativeEvidence: ["no spa"],
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "meetings",
      label: "Meetings / event space",
      meaning: "Meeting rooms or event venues.",
      aliases: ["meeting space", "ballroom", "conference", "event space"],
      positiveEvidence: ["meeting/event space stated"],
      negativeEvidence: null,
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "airport_access",
      label: "Airport access",
      meaning: "Convenient airport proximity or shuttle.",
      aliases: ["airport", "airport shuttle", "near airport"],
      positiveEvidence: ["airport access stated"],
      negativeEvidence: null,
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "local_dining",
      label: "Local dining",
      meaning: "On-site or notable nearby dining.",
      aliases: ["dining", "restaurant", "fine dining"],
      positiveEvidence: ["dining/restaurant stated"],
      negativeEvidence: null,
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "wellness",
      label: "Wellness positioning",
      meaning: "Wellness / fitness / recovery positioning.",
      aliases: ["wellness", "fitness", "yoga"],
      positiveEvidence: ["wellness framing"],
      negativeEvidence: null,
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
    {
      id: "experiential",
      label: "Experiential positioning",
      meaning: "Design-forward / experiential / boutique character.",
      aliases: ["design-forward", "boutique", "experiential"],
      positiveEvidence: ["design/boutique/experiential language"],
      negativeEvidence: null,
      unknown: "Not mentioned",
      contradiction: "unknown",
      confidenceRequirement: "explicit phrase",
    },
  ]),
});

/** Bootstrap attribute gold cases (synthetic + pattern) for regression. */
export const ATTRIBUTE_GOLD_CASES_V1 = Object.freeze([
  {
    id: "attr_pool_positive",
    attributeId: "pool",
    text: "The hotel features a heated outdoor pool and sundeck.",
    expected: "POSITIVE",
  },
  {
    id: "attr_pool_unknown",
    attributeId: "pool",
    text: "Guests praise the lobby design and downtown location.",
    expected: "UNKNOWN",
  },
  {
    id: "attr_beach_positive",
    attributeId: "beachfront",
    text: "This oceanfront resort sits directly on the beach.",
    expected: "POSITIVE",
  },
  {
    id: "attr_silence_not_absent",
    attributeId: "spa",
    text: "A solid Midtown option for theatergoers.",
    expected: "UNKNOWN",
    note: "NOT OBSERVED must not become FACTUALLY ABSENT",
  },
  {
    id: "attr_meetings_positive",
    attributeId: "meetings",
    text: "Meeting space includes a ballroom for up to 200 guests.",
    expected: "POSITIVE",
  },
]);

export function classifyAttributeEvidence(attributeId, text) {
  const def = ATTRIBUTE_DICTIONARY_V1.attributes.find((a) => a.id === attributeId);
  if (!def) return { status: "UNKNOWN", reason: "unknown_attribute" };
  const lower = String(text || "").toLowerCase();
  const hit = (def.aliases || []).some((a) => lower.includes(String(a).toLowerCase()));
  if (hit) return { status: "POSITIVE", reason: "alias_hit" };
  return { status: "UNKNOWN", reason: "not_observed_not_absent" };
}

export function runAttributeGoldSet() {
  const results = ATTRIBUTE_GOLD_CASES_V1.map((c) => {
    const got = classifyAttributeEvidence(c.attributeId, c.text);
    return {
      ...c,
      got: got.status,
      pass: got.status === c.expected,
    };
  });
  return {
    version: ASSURANCE_ATTRIBUTE_DICT_VERSION,
    total: results.length,
    pass: results.filter((r) => r.pass).length,
    fail: results.filter((r) => !r.pass).length,
    results,
  };
}
