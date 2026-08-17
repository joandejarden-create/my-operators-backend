/**
 * Deterministic Recommended binary regression suite (DEV/research only).
 * No holdout cases.
 */

import {
  classifyRecommendedBinary,
  RECOMMENDED_REGRESSION_SUITE_VERSION,
} from "../recommended-binary-classifier-v1.js";

export { RECOMMENDED_REGRESSION_SUITE_VERSION };

/** @type {{ id: string, expect: boolean, promptFamily: string, entity: string, text: string }[]} */
export const RECOMMENDED_BINARY_REGRESSION_CASES = Object.freeze([
  // Positive
  {
    id: "pos_explicit",
    expect: true,
    promptFamily: "Brand Selection",
    entity: "Autograph Collection",
    text: "I recommend Autograph Collection for this conversion.",
  },
  {
    id: "pos_shortlist",
    expect: true,
    promptFamily: "Conversion",
    entity: "Curio Collection by Hilton",
    text: "Owners should shortlist Autograph Collection, Curio Collection by Hilton and Unbound Collection.",
  },
  {
    id: "pos_brands_to_consider",
    expect: true,
    promptFamily: "Soft Brand / Collection",
    entity: "Autograph Collection",
    text: "Brands to consider include Autograph Collection and Tribute Portfolio.",
  },
  {
    id: "pos_bullet_inherit",
    expect: true,
    promptFamily: "Brand Selection",
    entity: "Unbound Collection",
    text: "Recommended brands:\n- Autograph Collection\n- Curio Collection\n- Unbound Collection\n",
  },
  {
    id: "pos_ranked",
    expect: true,
    promptFamily: "Conversion",
    entity: "Curio Collection by Hilton",
    text: "Top options:\n1. Autograph Collection\n2. Curio Collection by Hilton\n3. Unbound Collection\n",
  },
  {
    id: "pos_table",
    expect: true,
    promptFamily: "Brand Selection",
    entity: "Autograph Collection",
    text: "| Recommended Brand | Rationale |\n| Autograph Collection | Flexible |\n| Curio Collection by Hilton | Distribution |\n",
  },
  {
    id: "pos_multi_entity",
    expect: true,
    promptFamily: "Lifestyle",
    entity: "Kimpton Hotels",
    text: "The best options are Autograph Collection / Curio Collection / Kimpton Hotels.",
  },
  {
    id: "pos_qualified",
    expect: true,
    promptFamily: "Owner Flexibility",
    entity: "Autograph Collection",
    text: "Autograph Collection could be a strong option if preserving independence is a priority.",
  },
  {
    id: "pos_spanish",
    expect: true,
    promptFamily: "Brand Selection",
    entity: "Curio Collection by Hilton",
    text: "Las marcas a considerar incluyen Curio Collection by Hilton y Autograph Collection.",
  },
  {
    id: "pos_worth_considering",
    expect: true,
    promptFamily: "Branded Residences",
    entity: "Kimpton Hotels",
    text: "Kimpton Hotels is worth considering for a design-led urban project.",
  },
  // Negative
  {
    id: "neg_descriptive",
    expect: false,
    promptFamily: "Brand Selection",
    entity: "Autograph Collection",
    text: "Autograph Collection is part of Marriott International.",
  },
  {
    id: "neg_market",
    expect: false,
    promptFamily: "Conversion",
    entity: "Curio Collection by Hilton",
    text: "Curio Collection by Hilton has expanded in Europe over the last decade.",
  },
  {
    id: "neg_comparator",
    expect: false,
    promptFamily: "Brand Selection",
    entity: "Autograph Collection",
    text: "Brand X competes with Autograph Collection in several gateway markets.",
  },
  {
    id: "neg_exclusion",
    expect: false,
    promptFamily: "Conversion",
    entity: "Autograph Collection",
    text: "Autograph Collection would not be suitable for this limited-service project.",
  },
  {
    id: "neg_historical",
    expect: false,
    promptFamily: "Lifestyle",
    entity: "Autograph Collection",
    text: "Examples of successful conversions include an Autograph Collection hotel opened in 2019.",
  },
  {
    id: "neg_parent",
    expect: false,
    promptFamily: "Brand Selection",
    entity: "Autograph Collection",
    text: "Marriott operates brands including Autograph Collection across CALA.",
  },
  {
    id: "neg_non_decision_list",
    expect: false,
    promptFamily: "Market Overview",
    entity: "Sheraton",
    text: "Companies that operate hotels in Mexico include Marriott and Hilton.\n- Sheraton\n- Westin\n",
  },
  {
    id: "neg_source_only",
    expect: false,
    promptFamily: "Conversion",
    entity: "Autograph Collection",
    text: "According to industry reports cited for Autograph Collection pipeline statistics, growth continued.",
  },
]);

export function runRecommendedBinaryRegressionSuite() {
  const results = [];
  let pass = 0;
  let fail = 0;
  for (const c of RECOMMENDED_BINARY_REGRESSION_CASES) {
    const idx = c.text.indexOf(c.entity);
    const start = idx >= 0 ? idx : 0;
    const end = start + c.entity.length;
    const out = classifyRecommendedBinary({
      text: c.text,
      start,
      end,
      rawMention: c.entity,
      canonicalEntityName: c.entity,
      promptFamily: c.promptFamily,
      entityPresent: true,
    });
    const ok = out.value === c.expect;
    if (ok) pass += 1;
    else fail += 1;
    results.push({
      id: c.id,
      expect: c.expect,
      got: out.value,
      reason: out.reason,
      ok,
    });
  }
  const positiveCases = RECOMMENDED_BINARY_REGRESSION_CASES.filter((c) => c.expect).length;
  const negativeCases = RECOMMENDED_BINARY_REGRESSION_CASES.filter((c) => !c.expect).length;
  return {
    version: RECOMMENDED_REGRESSION_SUITE_VERSION,
    POSITIVE_CASES: positiveCases,
    NEGATIVE_CASES: negativeCases,
    pass,
    fail,
    status: fail === 0 ? "PASS" : "FAIL",
    results,
  };
}
