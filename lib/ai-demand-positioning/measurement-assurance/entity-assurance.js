/**
 * Entity resolution assurance + adversarial gold cases.
 */

import { ASSURANCE_ENTITY_VERSION } from "./version.js";
import { detectPropertyMention, buildNameVariants } from "../execution/response-parser.js";
import { resolveCustomerFacingEntity } from "../customer/customer-entity-resolution-v1.js";
import { isLikelyArtifactEntity } from "../metrics/entity-quality.js";

export const ENTITY_ADVERSARIAL_GOLD_V1 = Object.freeze([
  {
    id: "ent_rts_full",
    propertyId: "adp_renaissance_times_square",
    text: "Stay at the Renaissance New York Times Square Hotel for Midtown access.",
    expectSubject: true,
  },
  {
    id: "ent_rts_design_word",
    propertyId: "adp_renaissance_times_square",
    text: "Midtown Manhattan has undergone a design renaissance in recent years.",
    expectSubject: false,
  },
  {
    id: "ent_rts_nyc_short",
    propertyId: "adp_renaissance_times_square",
    text: "Also consider Renaissance NYC Times Square as a mid-range option.",
    expectSubject: true,
    ambiguityIfMiss: true,
    note: "Requires governed alias — may MANUAL_REVIEW if Path A misses",
  },
  {
    id: "ent_phillips_the",
    propertyId: "adp_hotel_phillips_kansas_city",
    text: "Book The Phillips Hotel in downtown Kansas City.",
    expectSubject: true,
  },
  {
    id: "ent_noho_known_fp",
    propertyId: "adp_now_now_noho",
    text: "The Mercer is a chic boutique hotel known for its luxurious rooms near NoHo.",
    expectSubject: false,
  },
  {
    id: "ent_noho_true",
    propertyId: "adp_now_now_noho",
    text: "Design-forward travelers should consider NOW NOW NOHO.",
    expectSubject: true,
  },
  {
    id: "ent_boca_generic",
    propertyId: "adp_waterstone_boca_raton",
    text: "Many guests also compare The Boca Raton and Eau Palm Beach Resort & Spa.",
    expectSubject: false,
    competitorExpect: ["Eau Palm Beach"],
  },
  {
    id: "ent_prose_fragment",
    propertyId: "adp_cambridge_beaches_bermuda",
    text: "This iconic hotel is presented as a cottage colony experience.",
    expectSubject: false,
    proseFragment: true,
  },
]);

export function runEntityAssurance(propertyProfile, customerEntities = []) {
  const variants = buildNameVariants(propertyProfile);
  const gold = ENTITY_ADVERSARIAL_GOLD_V1.filter(
    (g) => g.propertyId === propertyProfile.propertyId || g.propertyId === "*"
  );

  const goldResults = ENTITY_ADVERSARIAL_GOLD_V1.filter(
    (g) => g.propertyId === propertyProfile.propertyId
  ).map((g) => {
    const hit = detectPropertyMention(g.text, propertyProfile);
    let status = hit.mentioned === g.expectSubject ? "PASS" : "FAIL";
    if (status === "FAIL" && g.ambiguityIfMiss && g.expectSubject && !hit.mentioned) {
      status = "AMBIGUOUS_ALIAS_GAP";
    }
    return {
      id: g.id,
      expectSubject: g.expectSubject,
      got: hit.mentioned,
      matchedVariant: hit.matchedVariant,
      status,
      note: g.note || null,
    };
  });

  const customerAudit = [];
  for (const name of customerEntities || []) {
    const prose = isLikelyArtifactEntity(name) || /^(this|many|located)\b/i.test(String(name));
    let resolution = null;
    try {
      resolution = resolveCustomerFacingEntity(name, propertyProfile) || null;
    } catch {
      resolution = null;
    }
    customerAudit.push({
      raw: name,
      proseFragment: Boolean(prose),
      resolution,
      status: prose ? "FALSE_POSITIVE_RISK" : resolution ? "RESOLVED" : "UNRESOLVED",
    });
  }

  const fail = goldResults.filter((r) => r.status === "FAIL").length;
  const ambiguous = goldResults.filter((r) => r.status === "AMBIGUOUS_ALIAS_GAP").length;

  return {
    version: ASSURANCE_ENTITY_VERSION,
    subjectVariantsCount: variants.length,
    goldResults,
    customerAudit: customerAudit.slice(0, 50),
    counts: {
      goldPass: goldResults.filter((r) => r.status === "PASS").length,
      goldFail: fail,
      goldAmbiguous: ambiguous,
      customerUnresolved: customerAudit.filter((c) => c.status === "UNRESOLVED").length,
      customerProseRisk: customerAudit.filter((c) => c.status === "FALSE_POSITIVE_RISK").length,
    },
  };
}
