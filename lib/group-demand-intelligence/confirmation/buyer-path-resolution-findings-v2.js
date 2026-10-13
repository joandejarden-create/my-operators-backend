/**
 * Buyer Path Resolution V2 — research findings packs.
 * Evidence-backed only. No invented emails. No fake lodging confirmation.
 */

import {
  BUYER_PATH_CLASS_V2,
  CONTACTABILITY,
} from "./buyer-path-resolution-v2.js";
import {
  PARTICIPATION_STATUS,
  RECURRENCE_STATUS,
  EXTENDED_COHORT_TYPE,
} from "./opportunity-confirmation-v1.js";
import { LODGING_CONTROL_HYPOTHESIS } from "../gdi-maturity-v1.js";
import {
  GDI_EVIDENCE_TYPE,
  GDI_MATURITY_CLAIM_KIND,
} from "../gdi-evidence-taxonomy-v1.js";
import { YOTEL_CONFIRMATION_FINDINGS_V1, WROME_CONFIRMATION_FINDINGS_V1 } from "./confirmation-findings-packs-v1.js";

/** @type {Record<string, object>} */
export const YOTEL_BUYER_PATH_V2 = Object.freeze({
  "CEVA Logistics": {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.RELEVANT_FUNCTION_CONTACT,
      buyerPathConfidence: "MEDIUM",
      buyerPathRationale:
        "CEVA publishes dedicated Aid & Relief / Government Aid & Relief logistics capability and an AidEx 2025 meet-us page; use Aid & Relief function (not media@) for booth-team lodging outreach.",
      buyerPathSourceUrl:
        "https://www.cevalogistics.com/en/news-and-media/newsroom/aidex-2025",
      buyerPathSourceType: "company_event_announcement",
      buyerEntity: "CEVA Logistics — Government Aid & Relief Logistics",
      buyerRole: "Aid & Relief / Humanitarian logistics events",
      namedRole: "Aid & Relief / Humanitarian logistics events",
      contactability: CONTACTABILITY.CONTACTABLE_WITH_ROLE,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.cevalogistics.com/en/news-and-media/newsroom/aidex-2025",
          excerpt:
            "CEVA AidEx 2025 page: dedicated Aid & Relief teams presenting logistics solutions at Palexpo booth",
          confidence: "HIGH",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.cevalogistics.com/en/who-we-are/case-studies/delivering-life-saving-medical-supplies-to-afghanistan",
          excerpt: "Official case study names Government Aid & Relief Logistics team",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlConfidence: "MEDIUM",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "Aid & Relief commercial booth staffing typically booked via corporate/events travel under the vertical — controller not named for AidEx 2026; YOTEL block not evidenced",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          sourceUrl:
            "https://www.cevalogistics.com/en/news-and-media/newsroom/aidex-2025",
          excerpt: "Dedicated Aid & Relief exhibition team implies account-controlled travel",
          confidence: "MEDIUM",
        },
      ],
    },
    whyNow: {
      text: "CEVA Logistics is confirmed for AidEx Geneva 2026 (21–22 Oct) and publishes a dedicated Aid & Relief exhibition function; booth-team airport-corridor lodging for Palexpo week should be decided now via that vertical — not via media contacts.",
      quality: "STRONG",
    },
    recommendedAction: {
      text: "Use the CEVA Government Aid & Relief / humanitarian logistics function (AidEx meet-us / Aid & Relief vertical — not media@) as the identified internal path to confirm who is coordinating the Geneva 2026 booth-team travel and airport-corridor accommodation — do not assume this desk books rooms.",
    },
  },

  "Maersk Logistics & Services": {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.RELEVANT_FUNCTION_CONTACT,
      buyerPathConfidence: "HIGH",
      buyerPathRationale:
        "Maersk publishes International Development and Project Logistics Aid & Relief capability pages with talk-to-expert CTAs — commercial path for humanitarian exhibition teams.",
      buyerPathSourceUrl:
        "https://www.maersk.com/supply-chain-logistics/international-development",
      buyerPathSourceType: "company_functional_page",
      buyerEntity: "Maersk — International Development / Aid & Relief",
      buyerRole: "International Development / Aid & Relief logistics",
      namedRole: "International Development / Aid & Relief logistics",
      namedPerson: {
        name: "Abiola Abodel",
        role: "Regional Center of Excellence Manager — Aid & Relief",
        sourceUrl:
          "https://www.maersk.com/news/articles/2025/12/29/breakbulk-middle-east-2026",
      },
      contactability: CONTACTABILITY.CONTACTABLE_WITH_ROLE,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.maersk.com/supply-chain-logistics/international-development",
          excerpt:
            "Official International Development page: dedicated team for aid organisations / emergency response",
          confidence: "HIGH",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.maersk.com/supply-chain-logistics/project-logistics",
          excerpt: "Project Logistics lists Aid & Relief as a named capability with contact CTA",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlConfidence: "MEDIUM",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "Aid & Relief / International Development commercial travel desk hypothesized for booth staff; TMC not named; YOTEL block not evidenced",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          sourceUrl:
            "https://www.maersk.com/supply-chain-logistics/international-development",
          excerpt: "Dedicated International Development team — account-direct lodging control inferred",
          confidence: "MEDIUM",
        },
      ],
    },
    whyNow: {
      text: "Maersk Logistics is confirmed for AidEx Geneva 2026; its International Development / Aid & Relief function is the credible commercial path for booth-team travel, and Geneva airport-corridor lodging for 21–22 Oct should be arranged now.",
      quality: "STRONG",
    },
    recommendedAction: {
      text: "Use Maersk International Development / Aid & Relief CoE (e.g. Abiola Abodel — Aid & Relief CoE) as the identified internal path to confirm who is coordinating the AidEx Geneva 2026 booth-team travel and airport-corridor accommodation — do not assume this person books rooms.",
    },
  },

  "NOMOS Glashütte": {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.PRESS_CONTACT_ONLY,
      buyerPathConfidence: "HIGH",
      buyerPathRationale:
        "Public contacts remain PR (presse.nomos-glashuette.com / pr@glashuette.com). W&W brand page is source evidence only — no events/brand-experience purchase path published.",
      buyerPathSourceUrl: "https://presse.nomos-glashuette.com/?lang=en",
      buyerPathSourceType: "press_page",
      buyerEntity: "NOMOS Glashütte — Public Relations",
      buyerRole: "Public Relations",
      namedRole: "Public Relations",
      contactability: CONTACTABILITY.NO_CREDIBLE_PATH,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://presse.nomos-glashuette.com/?lang=en",
          excerpt: "Named PR contacts only (Montag / Langenbucher) — press path",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlConfidence: "LOW",
      lodgingControlConfirmed: false,
      lodgingControlSummary: "Brand events travel desk not published",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "No public lodging controller beyond PR",
          confidence: "LOW",
        },
      ],
    },
    whyNow: {
      text: "NOMOS is confirmed for Watches & Wonders Geneva 2027; booth/support lodging planning should open early — but only PR contacts are public, not an events purchase path.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Do not pitch via PR alone. Locate NOMOS events / brand-operations travel function and ask whether the Geneva 2027 booth/support team has placed lodging.",
    },
  },

  Bremont: {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.GENERIC_CONTACT_ONLY,
      buyerPathConfidence: "MEDIUM",
      buyerPathRationale:
        "Public contact is customerservice@bremont.com / HQ contact form — retail CS, not exhibition/events purchase path. W&W brand page is source-only.",
      buyerPathSourceUrl: "https://www.bremont.com/pages/contact",
      buyerPathSourceType: "generic_contact",
      buyerEntity: "Bremont Watch Company",
      buyerRole: "Customer Service / HQ contact",
      contactability: CONTACTABILITY.NEEDS_ENRICHMENT,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://www.bremont.com/pages/contact",
          excerpt: "Published contact routes to customer service / HQ — not events desk",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlConfidence: "LOW",
      lodgingControlConfirmed: false,
      lodgingControlSummary: "Exhibition travel controller not published",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Controller unknown",
          confidence: "LOW",
        },
      ],
    },
    whyNow: {
      text: "Bremont is confirmed for W&W Geneva 2027 with multi-year salon participation; UK booth/support lodging should enter planning — purchase path still needs an events function URL.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Identify Bremont events / brand-experience / exhibition operations function (not customer service) and confirm Geneva 2027 booth-team lodging status.",
    },
  },

  "Porsche Design": {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.PRESS_CONTACT_ONLY,
      buyerPathConfidence: "HIGH",
      buyerPathRationale:
        "Named Angélique Kreichgauer / Daniel Rätz are Communications/PR. Consumer contact@ and timepieces form are retail — not exhibition lodging purchase path.",
      buyerPathSourceUrl: "https://press.porsche-design.com/en/",
      buyerPathSourceType: "press_page",
      buyerEntity: "Porsche Lifestyle Group — Communications",
      buyerRole: "Head of Communications / PR",
      namedPerson: {
        name: "Angélique Kreichgauer",
        role: "Head of Communications",
        sourceUrl:
          "https://press.porsche-design.com/en/new-porsche-design-timepieces-manufactory-in-grenchen-marks-strong-commitment-to-the-future",
      },
      contactability: CONTACTABILITY.NO_CREDIBLE_PATH,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://press.porsche-design.com/en/",
          excerpt: "Pressroom contacts only — not events/travel purchase path",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACTIVATION_AGENCY,
      lodgingControlConfidence: "LOW",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "Debut W&W activation often uses agency; controller not published. Do not assume YOTEL inventory.",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Activation agency hypothesized for debut; not confirmed",
          confidence: "LOW",
        },
      ],
    },
    whyNow: {
      text: "Porsche Design is newly confirmed for W&W Geneva 2027; debut activation lodging will be planned early — only press/retail contacts are public so far.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Ask Porsche Design / Porsche Lifestyle Group events or brand-experience function (not press) whether the Geneva 2027 exhibition/activation team has placed lodging and who controls the hotel decision.",
    },
  },

  "Sinn Spezialuhren": {
    // Forensic V3: Kimberly Kretschmer is Filialleitung/Vertrieb (retail branch sales)
    // who appears in a Messe-Team intro — GENERIC_INTERNAL_CONTACT for W&W lodging ownership.
    // Within existing Sinn scope, no events/marketing/brand-experience purchase path found.
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.GENERIC_CONTACT_ONLY,
      buyerPathConfidence: "HIGH",
      buyerPathRationale:
        "Kimberly Kretschmer (Vertrieb / Filialleitung Römerberg) appears on Sinn’s W&W Messe-Team photo, but her evidenced remit is retail/distribution sales — not events, exhibitions operations, international brand representation, or traveling-team lodging. HQ Contact Sales is the same generic commercial path. Commercially not a credible first DoS step for W&W booth lodging.",
      buyerPathSourceUrl: "https://www.sinn.de/en/contact-factory-store/",
      buyerPathSourceType: "company_sales_contact",
      buyerEntity: "Sinn Spezialuhren — Vertrieb (retail / distribution)",
      buyerRole: "Vertrieb / Filialleitung (retail branch sales)",
      namedRole: "Vertrieb / Filialleitung",
      namedPerson: {
        name: "Kimberly Kretschmer",
        role: "Vertrieb Niederlassung Römerberg — Messe-Team appearance (retail sales)",
        sourceUrl:
          "https://www.linkedin.com/posts/sinn-spezialuhren-zu-frankfurt-am-main_sinnspezialuhren-watchesandwonders2026-activity-7451314135742599168-6MF1",
      },
      contactability: CONTACTABILITY.NEEDS_ENRICHMENT,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.linkedin.com/posts/sinn-spezialuhren-zu-frankfurt-am-main_sinnspezialuhren-watchesandwonders2026-activity-7451314135742599168-6MF1",
          excerpt:
            "Official Sinn LinkedIn Messe-Team intro names Kimberly Kretschmer (Vertrieb) — booth presence, not lodging ownership",
          confidence: "HIGH",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://www.sinn.de/en/contact-factory-store/",
          excerpt: "Official Contact Sales path — general Vertrieb / factory store, not events desk",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlConfidence: "LOW",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "No events / exhibition-operations lodging controller evidenced within Sinn research scope; retail Vertrieb is not a lodging path",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          sourceUrl: "https://www.sinn.de/en/contact-factory-store/",
          excerpt: "Generic sales contact — lodging controller unknown",
          confidence: "LOW",
        },
      ],
    },
    whyNow: {
      text: "Sinn is confirmed for Watches & Wonders Geneva 2027; booth/support lodging should be planned early — but the only named path is retail Vertrieb, not an events/traveling-team owner.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Do not pitch via Filialleitung/Vertrieb alone. Locate Sinn events / brand / exhibition-operations function (within brand HQ) and ask who coordinates Geneva 2027 booth-team lodging.",
    },
  },
});

/** @type {Record<string, object>} */
export const WROME_BUYER_PATH_V2 = Object.freeze({
  "Red Bull": {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.NAMED_BUYER_PERSON,
      buyerPathConfidence: "HIGH",
      buyerPathRationale:
        "Francesco Francavilla is Head of Marketing Communications & PR for Red Bull Italy SailGP with explicit remit covering sponsor activations — commercial activation path, not press-only media desk.",
      buyerPathSourceUrl: "https://www.linkedin.com/in/francesco-francavilla-",
      buyerPathSourceType: "linkedin_professional_bio",
      buyerEntity: "Red Bull Italy SailGP Team — Marketing / Sponsor activations",
      buyerRole: "Head of Marketing Communications & PR (sponsor activations)",
      namedRole: "Head of Marketing Communications & PR",
      namedPerson: {
        name: "Francesco Francavilla",
        role: "Head of Marketing Communications & PR — sponsor activations",
        sourceUrl: "https://www.linkedin.com/in/francesco-francavilla-",
      },
      contactability: CONTACTABILITY.CONTACTABLE_WITH_ROLE,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://www.linkedin.com/in/francesco-francavilla-",
          excerpt:
            "Role spans brand narrative, media, digital, and sponsor activations for Red Bull Italy SailGP",
          confidence: "HIGH",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_TEAM,
          sourceUrl: "https://www.redbull.com/mea-en/teams/red-bull-italy-sailgp-team",
          excerpt: "Official Red Bull Italy SailGP Team page",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlConfidence: "MEDIUM",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "Team marketing/activations typically directs hospitality hotels; named lodging buyer not published; W Rome block not evidenced",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Sponsor activation remit implies account-side hospitality control — unverified",
          confidence: "MEDIUM",
        },
      ],
    },
    whyNow: {
      text: "Rome SailGP (11–12 Sep 2027) is confirmed with Red Bull Italy SailGP Team as host-side partner; Francesco Francavilla’s marketing/sponsor-activations role is the credible contact for race-week team and VIP hospitality lodging planning now.",
      quality: "STRONG",
    },
    recommendedAction: {
      text: "Contact Francesco Francavilla (Head of Marketing Communications & PR — sponsor activations, Red Bull Italy SailGP) as the identified internal path to confirm who is coordinating Rome race-week team and VIP hospitality accommodation — do not assume this person books rooms.",
    },
  },

  Azimut: {
    buyerPath: {
      // Role hypothesized (sponsorship/hospitality) but no Azimut functional contact URL — announcement pages are not purchase paths
      buyerPathClass: BUYER_PATH_CLASS_V2.SOURCE_PAGE_ONLY,
      buyerPathConfidence: "MEDIUM",
      buyerPathRationale:
        "Azimut is Global Partner with evidenced client hospitality guests, but public URLs are partnership announcements / LinkedIn posts — not an Azimut events or hospitality desk. Role hypothesized: Sponsorship / Client hospitality. Needs enrichment for a functional contact URL.",
      buyerPathSourceUrl:
        "https://pressmare.it/en/team/red-bull-ita-sailgp-team/2026-01-15/red-bull-italy-sailgp-welcomes-azimut-as-global-partner-87770",
      buyerPathSourceType: "partnership_announcement",
      buyerEntity: "Azimut Group — Sponsorship / Client hospitality (hypothesized)",
      buyerRole: "Sponsorship / Brand partnerships / Client hospitality",
      namedRole: "Sponsorship / Client hospitality",
      contactability: CONTACTABILITY.NEEDS_ENRICHMENT,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPONSOR,
          sourceUrl:
            "https://pressmare.it/en/team/red-bull-ita-sailgp-team/2026-01-15/red-bull-italy-sailgp-welcomes-azimut-as-global-partner-87770",
          excerpt: "Azimut announced as Global Partner for SailGP 2026 season",
          confidence: "HIGH",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_TRAVELING_COHORT,
          sourceUrl:
            "https://www.linkedin.com/posts/azimutgroup_azimut-azimutgroup-azimutinvestments-activity-7467581521185558528--HOn",
          excerpt: "Azimut LinkedIn: guests joined New York SailGP hospitality experience",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlConfidence: "MEDIUM",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "Sponsor client hospitality typically account-controlled; controller not named for Rome GP",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Guest hospitality posts imply Azimut-controlled guest lodging — unverified",
          confidence: "MEDIUM",
        },
      ],
    },
    whyNow: {
      text: "Azimut is Global Partner of Red Bull Italy SailGP with evidenced client hospitality at race weekends; Rome 2027 debut lodging for guests should be planned via sponsorship/hospitality — a public events desk URL is still thin.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Contact Azimut Group sponsorship / brand partnerships / client hospitality (not SailGP homepage only) to ask whether Rome GP guest hospitality lodging in centro has been placed.",
    },
  },

  "ABB S.p.A.": {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.RELEVANT_FUNCTION_CONTACT,
      buyerPathConfidence: "MEDIUM",
      buyerPathRationale:
        "ABB publishes an official fairs & events calendar (functional events path). Italy MarCom/events specialists exist (e.g. Giulia Varano — Energy Industries Italy events) — use events function, not Maker Faire organizer page alone. Note: bare org name 'ABB' previously failed entity-truth (mapping defect); canonical name is ABB S.p.A.",
      buyerPathSourceUrl: "https://new.abb.com/events/calendar",
      buyerPathSourceType: "company_events_calendar",
      buyerEntity: "ABB Italy — Marketing Communications / Events",
      buyerRole: "Events / Marketing communications",
      namedRole: "Events / Marketing communications",
      namedPerson: {
        name: "Giulia Varano",
        role: "Marketing Communications — ABB Energy Industries Italy (fairs & events)",
        sourceUrl: "https://www.linkedin.com/in/giulia-varano",
      },
      contactability: CONTACTABILITY.CONTACTABLE_WITH_ROLE,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://new.abb.com/events/calendar",
          excerpt: "Official ABB fairs and events calendar — functional events path",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlConfidence: "LOW",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "Corporate events travel hypothesized for partner booth; not verified for Maker Faire Rome",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Events function present — lodging controller unnamed",
          confidence: "LOW",
        },
      ],
    },
    whyNow: {
      text: "ABB S.p.A. is evidenced as a Maker Faire Rome 2026 partner; ABB’s published events calendar / Italy MarCom-events function is the credible path to confirm booth/demo lodging for late October centro overflow.",
      quality: "STRONG",
    },
    recommendedAction: {
      text: "Contact ABB Italy events / marketing communications via the official events calendar path to identify who is coordinating Maker Faire Rome booth-team travel and whether centro overflow lodging remains open — do not assume this desk books rooms.",
    },
  },

  Anycubic: {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.SOURCE_PAGE_ONLY,
      buyerPathConfidence: "HIGH",
      buyerPathRationale:
        "No Anycubic EMEA events/marketing function URL found. Only Maker Faire organizer sponsor@ path — source/organizer page, not Anycubic purchase path.",
      buyerPathSourceUrl: "https://makerfairerome.eu/contatti/",
      buyerPathSourceType: "event_organizer_contact",
      buyerEntity: "Anycubic",
      buyerRole: null,
      contactability: CONTACTABILITY.NO_CREDIBLE_PATH,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl: "https://makerfairerome.eu/contatti/",
          excerpt: "Organizer partner contact only — not Anycubic commercial desk",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlConfidence: "LOW",
      lodgingControlConfirmed: false,
      lodgingControlSummary: "Unknown — no company travel path published",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Controller unknown",
          confidence: "LOW",
        },
      ],
    },
    whyNow: {
      text: "Anycubic is evidenced as Maker Faire Rome 2026 partner; international demo-team lodging should be planned — Anycubic EMEA events path still missing.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Locate Anycubic EMEA / Europe marketing or trade-show events function (not Maker Faire organizer email) before lodging outreach.",
    },
  },

  "Banca Ifis": {
    buyerPath: {
      buyerPathClass: BUYER_PATH_CLASS_V2.NAMED_BUYER_PERSON,
      buyerPathConfidence: "HIGH",
      buyerPathRationale:
        "Valentina Corio is Head of Events and Institutional Relations at Banca Ifis — directly relevant to Film Fest Main Partner hospitality / events logistics. Media office is separate (press-only).",
      buyerPathSourceUrl: "https://www.linkedin.com/in/valentina-corio-b473255",
      buyerPathSourceType: "linkedin_professional_bio",
      buyerEntity: "Banca Ifis — Events & Institutional Relations",
      buyerRole: "Head of Events and Institutional Relations",
      namedRole: "Head of Events and Institutional Relations",
      namedPerson: {
        name: "Valentina Corio",
        role: "Head of Events and Institutional Relations",
        sourceUrl: "https://www.linkedin.com/in/valentina-corio-b473255",
      },
      contactability: CONTACTABILITY.CONTACTABLE_WITH_ROLE,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://www.linkedin.com/in/valentina-corio-b473255",
          excerpt: "Head of Events and Institutional Relations — events/logistics remit",
          confidence: "HIGH",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPONSOR,
          sourceUrl: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
          excerpt: "Banca Ifis Main Partner Rome Film Fest from 2026 (three-year)",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlConfidence: "MEDIUM",
      lodgingControlConfirmed: false,
      lodgingControlSummary:
        "Events & Institutional Relations typically controls VIP/client hospitality lodging; block at W Rome not evidenced",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          sourceUrl: "https://www.linkedin.com/in/valentina-corio-b473255",
          excerpt: "Events head role implies account-side hospitality lodging control — unverified",
          confidence: "MEDIUM",
        },
      ],
    },
    whyNow: {
      text: "Banca Ifis is Main Partner of Rome Film Fest from the 2026 edition; Valentina Corio (Head of Events & Institutional Relations) is the credible path for opening-week client hospitality lodging in centro — planning should be active ahead of 13–25 Oct.",
      quality: "STRONG",
    },
    recommendedAction: {
      text: "Contact Valentina Corio (Head of Events and Institutional Relations, Banca Ifis) — not ufficiostampa — as the identified events path to confirm who is coordinating Film Fest opening-week client hospitality accommodation in centro Rome — do not assume room-booking authority is confirmed.",
    },
  },
});

const CONFIRMATION_ALIASES = Object.freeze({
  ABB: "ABB S.p.A.",
  "ABB Spa": "ABB S.p.A.",
  "ABB Italy": "ABB S.p.A.",
});

export function getConfirmationBase(accountName, hotel = "yotel") {
  const resolved = CONFIRMATION_ALIASES[accountName] || accountName;
  const pack =
    hotel === "wrome"
      ? WROME_CONFIRMATION_FINDINGS_V1[resolved] || WROME_CONFIRMATION_FINDINGS_V1[accountName]
      : YOTEL_CONFIRMATION_FINDINGS_V1[resolved] || YOTEL_CONFIRMATION_FINDINGS_V1[accountName];
  return pack || {};
}

export {
  YOTEL_CONFIRMATION_FINDINGS_V1,
  WROME_CONFIRMATION_FINDINGS_V1,
  PARTICIPATION_STATUS,
  RECURRENCE_STATUS,
  EXTENDED_COHORT_TYPE,
};
