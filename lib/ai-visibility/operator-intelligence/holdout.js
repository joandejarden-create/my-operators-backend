/**
 * Constructed DEV + sealed holdout for Operator Presence.
 * Holdout must not be used for tuning. Live provider responses are not in this set.
 */

import { createHash } from "crypto";
import { classifyOperatorPresence } from "./presence.js";
import { OPERATOR_AI_UNIVERSE } from "./universe.js";

export const OPERATOR_HOLDOUT_VERSION = "operator_presence_holdout_constructed_v1";

function id(founder) {
  return OPERATOR_AI_UNIVERSE.find((o) => o.founderName === founder).canonicalId;
}

const AIM = () => id("Aimbridge LATAM");
const HE = () => id("Hotel Equities CALA");
const ARBOR = () => id("Arbor Lodging");
const GHL = () => id("GHL");
const MAR = () => id("Marriott International");
const HIL = () => id("Hilton");
const IHG = () => id("IHG");
const BRIT = () => id("Brittain Resorts");
const REM = () => id("Remington CALA");

export const OPERATOR_PRESENCE_DEV_CASES = Object.freeze([
  {
    caseId: "dev_pos_aimbridge_tpm",
    expectedPresent: [AIM()],
    text: "Owners commonly consider Aimbridge Hospitality as a third-party operator for branded hotels in Mexico.",
  },
  {
    caseId: "dev_pos_he",
    expectedPresent: [HE()],
    text: "Hotel Equities is frequently discussed as a management company for owners seeking a third-party operating partner in CALA.",
  },
  {
    caseId: "dev_pos_arbor",
    expectedPresent: [ARBOR()],
    text: "Arbor Lodging is a hotel management company owners evaluate for institutional operating capability.",
  },
  {
    caseId: "dev_pos_ghl",
    expectedPresent: [GHL()],
    text: "GHL Hoteles is commonly considered for operating hotels in Colombia and the broader Latin America platform.",
  },
  {
    caseId: "dev_pos_marriott_managed",
    expectedPresent: [MAR()],
    text: "Some owners consider Marriott International (Managed) when they want a brand-managed operating path rather than a third-party manager.",
  },
  {
    caseId: "dev_neg_marriott_hotel_only",
    expectedPresent: [],
    text: "The site is a Marriott hotel with a strong loyalty following among guests.",
  },
  {
    caseId: "dev_neg_source_url_only",
    expectedPresent: [],
    text: "See https://aimbridgehospitality.com/ for corporate information.",
    citations: [{ url: "https://aimbridgehospitality.com/", domain: "aimbridgehospitality.com" }],
  },
  {
    caseId: "dev_neg_highgate_competitor",
    expectedPresent: [],
    text: "Highgate is often mentioned as a third-party operator in owner conversations.",
  },
  {
    caseId: "dev_neg_bare_he",
    expectedPresent: [],
    text: "The HE team visited the property last week.",
  },
  {
    caseId: "dev_neg_arbor_tree",
    expectedPresent: [],
    text: "Guests sat under the arbor in the courtyard garden.",
  },
  {
    caseId: "dev_pos_remington",
    expectedPresent: [REM()],
    text: "Remington Hospitality is a third-party management company owners consider for CALA resort hotels.",
  },
  {
    caseId: "dev_neg_remington_ambiguous",
    expectedPresent: [],
    text: "Remington is a well-known brand in firearms and personal-care products.",
  },
  {
    caseId: "dev_neg_remington_source_only",
    expectedPresent: [],
    text: "See https://www.remingtonhospitality.com/ for corporate information.",
    citations: [{ url: "https://www.remingtonhospitality.com/", domain: "remingtonhospitality.com" }],
  },
]);

export const OPERATOR_PRESENCE_HOLDOUT_CASES = Object.freeze([
  {
    caseId: "hold_pos_aimbridge_latam",
    expectedPresent: [AIM()],
    text: "Aimbridge LATAM is commonly considered by owners who need a Latin America third-party management company.",
  },
  {
    caseId: "hold_pos_hilton_management",
    expectedPresent: [HIL()],
    text: "Hilton Management Services is one operating option when owners ask who should operate a full-service branded hotel.",
  },
  {
    caseId: "hold_pos_ihg_operating",
    expectedPresent: [IHG()],
    text: "IHG Hotels & Resorts is considered as an operating partner when the owner wants the brand-managed path.",
  },
  {
    caseId: "hold_pos_brittain",
    expectedPresent: [BRIT()],
    text: "Brittain Resorts & Hotels is a management company commonly considered for resort hotels in the US Southeast.",
  },
  {
    caseId: "hold_pos_ghl_short",
    expectedPresent: [GHL()],
    text: "Owners in the Andes corridor often ask which operator should run the hotel, and GHL appears in those management-company discussions.",
  },
  {
    caseId: "hold_neg_hilton_honors",
    expectedPresent: [],
    text: "Guests earn Hilton Honors points at this property.",
  },
  {
    caseId: "hold_neg_footer",
    expectedPresent: [],
    text: "Follow us. Privacy policy. All rights reserved. Hotel Equities.",
  },
  {
    caseId: "hold_pos_remington",
    expectedPresent: [REM()],
    text: "Remington Hospitality is sometimes named as a third-party operator alongside other US platforms.",
  },
  {
    caseId: "hold_neg_remington_unrelated",
    expectedPresent: [],
    text: "The Remington typewriter was once a popular office product.",
  },
  {
    caseId: "hold_neg_marriott_bonvoy",
    expectedPresent: [],
    text: "Book directly to earn Marriott Bonvoy points.",
  },
  {
    caseId: "hold_pos_mxm",
    expectedPresent: [MAR()],
    text: "Managed by Marriott (MxM) is the operating path some owners choose instead of hiring a third-party manager.",
  },
]);

export function holdoutFingerprint() {
  return createHash("sha256")
    .update(JSON.stringify(OPERATOR_PRESENCE_HOLDOUT_CASES))
    .digest("hex");
}

function scoreCases(cases) {
  let tp = 0;
  let fp = 0;
  let fn = 0;
  let tn = 0;
  const falsePositives = [];
  const falseNegatives = [];
  for (const c of cases) {
    const got = new Set(classifyOperatorPresence(c).presentOperatorIds);
    const exp = new Set(c.expectedPresent || []);
    const ids = new Set([...got, ...exp]);
    if (!ids.size) {
      tn += 1;
      continue;
    }
    for (const operatorId of ids) {
      const predicted = got.has(operatorId);
      const actual = exp.has(operatorId);
      if (predicted && actual) tp += 1;
      else if (predicted && !actual) {
        fp += 1;
        falsePositives.push({ caseId: c.caseId, operatorId });
      } else if (!predicted && actual) {
        fn += 1;
        falseNegatives.push({ caseId: c.caseId, operatorId });
      } else tn += 1;
    }
  }
  const precision = tp + fp ? tp / (tp + fp) : 1;
  const recall = tp + fn ? tp / (tp + fn) : 1;
  const f1 = precision + recall ? (2 * precision * recall) / (precision + recall) : 0;
  return {
    tp,
    fp,
    fn,
    tn,
    precision,
    recall,
    f1,
    falsePositives,
    falseNegatives,
  };
}

export function scoreOperatorPresenceValidation() {
  const dev = scoreCases(OPERATOR_PRESENCE_DEV_CASES);
  const holdout = scoreCases(OPERATOR_PRESENCE_HOLDOUT_CASES);
  const productionEligible = holdout.fp === 0 && holdout.precision >= 0.99 && holdout.tp >= 3;
  return {
    version: OPERATOR_HOLDOUT_VERSION,
    classifier: "operator_signal_presence_v1",
    corpus: "CONSTRUCTED_NOT_LIVE",
    holdoutSealed: true,
    holdoutFingerprint: holdoutFingerprint(),
    devCases: OPERATOR_PRESENCE_DEV_CASES.length,
    holdoutCases: OPERATOR_PRESENCE_HOLDOUT_CASES.length,
    dev,
    holdout,
    status: productionEligible ? "PARTIAL" : "RESEARCH_ONLY",
    note: "Constructed holdout only. Live provider responses required before PRODUCTION_ELIGIBLE.",
  };
}
