/**
 * Commercial Readiness Snapshot — deterministic analysis engine.
 * Produces hotel-specific, evidence-based interpretation from structured owner inputs.
 * Does not claim performance metrics unless owner provides them.
 */

const READINESS_LEVELS = ["Limited", "Developing", "Competitive", "Strong"];
const CONFIDENCE_LEVELS = ["Low", "Moderate", "High"];

function toText(value) {
  if (value == null) return "";
  if (Array.isArray(value)) return value.map((x) => toText(x)).filter(Boolean).join(", ");
  return String(value).trim();
}

function hasValue(value) {
  return toText(value) !== "";
}

function parsePercentLike(value) {
  const raw = toText(value);
  if (!raw) return null;
  const match = raw.replace(/,/g, "").match(/(\d+(?:\.\d+)?)/);
  if (!match) return null;
  const num = Number(match[1]);
  return Number.isFinite(num) ? num : null;
}

function normalizeBrandStatus(v) {
  return toText(v).toLowerCase();
}

function normalizeOperatorStatus(v) {
  return toText(v).toLowerCase();
}

function isBranded(inputs) {
  const s = normalizeBrandStatus(inputs.currentBrandStatus);
  return s === "branded" || s === "soft branded";
}

function isIndependent(inputs) {
  return normalizeBrandStatus(inputs.currentBrandStatus) === "independent";
}

function isBrandManaged(inputs) {
  return normalizeOperatorStatus(inputs.currentOperatorStatus) === "brand-managed";
}

function isThirdPartyOperator(inputs) {
  return normalizeOperatorStatus(inputs.currentOperatorStatus) === "third-party operator";
}

function isSelfManaged(inputs) {
  return normalizeOperatorStatus(inputs.currentOperatorStatus) === "self-managed";
}

function hasBookingEngine(inputs) {
  return hasValue(inputs.bookingEngineProvider);
}

function hasCrmCapture(inputs) {
  return toText(inputs.crmGuestEmailCapture).toLowerCase() === "yes";
}

function hasThirdPartyUrls(inputs) {
  return hasValue(inputs.bookingComUrl) || hasValue(inputs.expediaUrl);
}

function hasPerformanceNumbers(inputs) {
  return (
    parsePercentLike(inputs.actualOtaBookingShare) != null ||
    parsePercentLike(inputs.actualDirectBookingShare) != null ||
    parsePercentLike(inputs.websiteConversionRate) != null ||
    parsePercentLike(inputs.repeatGuestPercentage) != null
  );
}

function countProvidedInputs(inputs) {
  return Object.keys(inputs || {}).filter((k) => hasValue(inputs[k])).length;
}

/** Separate confidence dimensions per product spec */
function assessConfidenceDimensions(inputs) {
  const hasActualOta = parsePercentLike(inputs.actualOtaBookingShare) != null;
  const hasActualDirect = parsePercentLike(inputs.actualDirectBookingShare) != null;
  const hasConversion = parsePercentLike(inputs.websiteConversionRate) != null;
  const hasRepeat = parsePercentLike(inputs.repeatGuestPercentage) != null;
  const hasCommission = hasValue(inputs.estimatedOtaCommission);
  const hasPerformance = hasActualOta || hasActualDirect || hasConversion || hasRepeat;

  const qualitativeStrength =
    (hasValue(inputs.hotelWebsiteUrl) ? 1 : 0) +
    (hasThirdPartyUrls(inputs) ? 1 : 0) +
    (hasValue(inputs.googleBusinessProfileUrl) ? 1 : 0) +
    (hasValue(inputs.currentBrandStatus) && inputs.currentBrandStatus !== "Unknown" ? 1 : 0) +
    (hasValue(inputs.currentOperatorStatus) && inputs.currentOperatorStatus !== "Unknown" ? 1 : 0) +
    (hasValue(inputs.mainCommercialConcern) ? 1 : 0) +
    (hasValue(inputs.ownerGoal) ? 1 : 0) +
    (hasValue(inputs.topSourceMarkets) ? 1 : 0) +
    (hasValue(inputs.primaryGuestSegments) ? 1 : 0) +
    (hasValue(inputs.additionalOwnerNotes) ? 1 : 0) +
    (hasBookingEngine(inputs) ? 1 : 0) +
    (hasCrmCapture(inputs) ? 1 : 0);

  let inputCompleteness = "Low";
  if (qualitativeStrength >= 9) inputCompleteness = "High";
  else if (qualitativeStrength >= 6) inputCompleteness = "Moderate";

  let performanceDataConfidence = "Low";
  if ((hasActualOta || hasActualDirect) && (hasConversion || hasRepeat || hasCommission)) {
    performanceDataConfidence = "High";
  } else if (hasActualOta || hasActualDirect || hasConversion || hasRepeat || hasCommission) {
    performanceDataConfidence = "Moderate";
  }

  let observableEvidenceConfidence = "Low";
  if (hasThirdPartyUrls(inputs) && hasValue(inputs.hotelWebsiteUrl) && hasValue(inputs.additionalOwnerNotes)) {
    observableEvidenceConfidence = "Moderate";
  } else if (hasValue(inputs.hotelWebsiteUrl) && qualitativeStrength >= 5) {
    observableEvidenceConfidence = "Moderate";
  }

  // Overall evidence confidence — never High without actual performance shares
  let evidenceConfidence = "Low";
  if (performanceDataConfidence === "High" && (hasActualOta || hasActualDirect)) {
    evidenceConfidence = "High";
  } else if (inputCompleteness === "Moderate" || inputCompleteness === "High") {
    evidenceConfidence = "Moderate";
  }

  const missingData = [];
  if (!hasActualOta) missingData.push("Actual OTA booking share");
  if (!hasActualDirect) missingData.push("Actual direct booking share");
  if (!hasConversion) missingData.push("Website conversion rate");
  if (!hasRepeat) missingData.push("Repeat guest percentage");
  if (!hasCommission) missingData.push("OTA commission cost detail");
  missingData.push("Booking engine abandonment rate");
  missingData.push("Channel mix by source market");

  return {
    inputCompleteness,
    performanceDataConfidence,
    observableEvidenceConfidence,
    evidenceConfidence,
    confirmedPerformanceDataProvided: hasPerformance,
    missingData,
  };
}

function scoreInfrastructure(inputs) {
  let score = 0;
  if (isBranded(inputs)) score += 2;
  if (hasBookingEngine(inputs)) score += 2;
  if (hasCrmCapture(inputs)) score += 1.5;
  const estDirect = toText(inputs.estimatedDirectBookingShare).toLowerCase();
  if (estDirect === "moderate" || estDirect === "high") score += 1;
  if (hasThirdPartyUrls(inputs)) score += 0.5;
  if (hasValue(inputs.topSourceMarkets)) score += 0.5;
  if (hasValue(inputs.primaryGuestSegments)) score += 0.5;
  return score;
}

function assessReadinessLevel(inputs, directCapabilityLabel) {
  const infra = scoreInfrastructure(inputs);
  const concern = toText(inputs.mainCommercialConcern).toLowerCase();
  const estOta = toText(inputs.estimatedOtaShare).toLowerCase();

  if (isIndependent(inputs) && !hasBookingEngine(inputs) && !hasCrmCapture(inputs)) {
    return {
      label: "Limited",
      rationale:
        "As an independent property without a clearly identified booking engine or CRM/email capture, foundational direct-booking infrastructure appears limited. The hotel may need basic commercial systems before conversion optimization can meaningfully improve demand control.",
    };
  }

  if (isBranded(inputs) && hasBookingEngine(inputs) && hasCrmCapture(inputs)) {
    if (concern === "poor conversion" || concern === "weak direct bookings") {
      return {
        label: infra >= 5 ? "Developing" : "Limited",
        rationale:
          "The hotel has meaningful commercial infrastructure in place — brand affiliation, a defined booking engine, and CRM/email capture. That suggests the core question is not whether basic commercial systems exist, but whether those systems are converting enough high-intent demand relative to OTA alternatives. Conversion performance cannot be confirmed without website conversion rate and channel mix data.",
      };
    }
    if (infra >= 6 && (estOta === "low" || estOta === "moderate")) {
      return {
        label: "Competitive",
        rationale:
          "Brand affiliation, booking engine, CRM capture, and moderate estimated channel balance suggest competitive commercial readiness at the infrastructure level. Performance confirmation still depends on actual channel mix and conversion metrics.",
      };
    }
    return {
      label: "Developing",
      rationale:
        "Brand systems and direct-booking infrastructure appear present, but estimated channel mix, stated commercial concern, or missing performance data prevent a stronger readiness classification.",
    };
  }

  if (isBranded(inputs) && hasBookingEngine(inputs)) {
    return {
      label: "Developing",
      rationale:
        "Branded status and booking engine presence indicate developing commercial readiness. CRM capture, conversion metrics, and channel mix data would clarify whether the hotel is under-leveraging existing infrastructure.",
    };
  }

  if (directCapabilityLabel === "Strong" || directCapabilityLabel === "Competitive") {
    return { label: directCapabilityLabel, rationale: "Direct-booking signal strength supports this readiness level." };
  }

  if (infra >= 4) {
    return {
      label: "Developing",
      rationale:
        "Some commercial infrastructure signals are present, but performance data is insufficient to confirm stronger readiness. The hotel appears to be building capability rather than starting from zero.",
    };
  }

  return {
    label: "Limited",
    rationale:
      "Limited booking infrastructure, weak CRM capture, or high estimated OTA dependency suggest the hotel has meaningful commercial groundwork still to establish before conversion optimization alone will shift demand control.",
  };
}

function assessOtaDependencyRisk(inputs) {
  const actualOta = parsePercentLike(inputs.actualOtaBookingShare);
  const estimated = toText(inputs.estimatedOtaShare);
  const commission = parsePercentLike(inputs.estimatedOtaCommission);
  const confirmedByNumbers = actualOta != null;

  if (confirmedByNumbers) {
    let label = "Low";
    if (actualOta >= 65) label = "Critical";
    else if (actualOta >= 50) label = "High";
    else if (actualOta >= 35) label = "Moderate";

    return {
      label,
      confirmedByNumbers: true,
      rationale: `Owner-provided actual OTA booking share is approximately ${actualOta}%. This supports a ${label.toLowerCase()} OTA dependency assessment.`,
      dataNeeded: actualOta >= 35 ? ["OTA commission by channel", "Direct share trend over 12 months"] : [],
      dealImplication:
        actualOta >= 50
          ? "High OTA concentration may limit demand control and margin unless direct conversion improves or channel mix shifts."
          : "OTA dependency appears manageable, but direct channel performance should still be validated.",
    };
  }

  if (estimated === "Very high" || estimated === "High") {
    return {
      label: "Potential high risk indicated",
      confirmedByNumbers: false,
      rationale:
        "Actual OTA dependency cannot be confirmed because OTA booking share was not provided. However, the owner estimates OTA share as high, which suggests meaningful third-party channel reliance if accurate. Without channel mix data, the size of the exposure cannot be quantified.",
      dataNeeded: ["Actual OTA booking share", "OTA commission by channel", "Top OTA by volume"],
      dealImplication:
        "If estimated high OTA share is accurate, direct conversion improvement and channel mix management should be prioritized before brand or operator strategy changes.",
    };
  }

  if (estimated === "Moderate") {
    const commissionNote = commission
      ? ` Estimated OTA commission is ${toText(inputs.estimatedOtaCommission)}. If OTA share is material at that commission level, even modest direct conversion improvement may have economic relevance — but actual booking share is required to quantify exposure.`
      : "";
    return {
      label: "Potential moderate risk indicated",
      confirmedByNumbers: false,
      rationale:
        `Actual OTA dependency cannot be confirmed because OTA and direct booking shares were not provided. However, estimated OTA share is moderate.${commissionNote} ${
          isBranded(inputs)
            ? "Because brand infrastructure exists, the issue may be channel mix optimization and conversion execution rather than lack of brand reach."
            : "Channel mix validation should precede major commercial strategy decisions."
        }`,
      dataNeeded: ["Actual OTA booking share", "Actual direct booking share", "Monthly OTA commission cost"],
      dealImplication:
        "Moderate estimated OTA share combined with missing performance data means the owner should confirm channel economics before assuming brand affiliation or operator change is the primary solution.",
    };
  }

  if (estimated === "Low") {
    return {
      label: "Low",
      confirmedByNumbers: false,
      rationale:
        "Estimated OTA share is low, but actual channel mix was not provided. Low estimated dependency is a positive signal, though it should be confirmed with reporting.",
      dataNeeded: ["Actual OTA booking share", "Channel mix trend"],
      dealImplication: "OTA dependency does not appear to be the primary commercial constraint based on estimates, but confirmation is still needed.",
    };
  }

  return {
    label: "Not enough data",
    confirmedByNumbers: false,
    rationale:
      "OTA booking share was not provided and estimated OTA share is unknown. The snapshot cannot assess OTA dependency risk beyond noting this evidence gap.",
    dataNeeded: ["Estimated or actual OTA booking share", "OTA commission cost"],
    dealImplication: "Channel mix data is a prerequisite for meaningful OTA dependency analysis.",
  };
}

function assessDirectBookingCapability(inputs, urlEvidence = null) {
  const infrastructureSignals = [];
  const conversionRisks = [];
  const concern = toText(inputs.mainCommercialConcern).toLowerCase();
  const goal = toText(inputs.ownerGoal).toLowerCase();
  const estDirect = toText(inputs.estimatedDirectBookingShare).toLowerCase();

  if (isBranded(inputs)) {
    infrastructureSignals.push("Global or regional brand affiliation provides distribution and booking infrastructure");
  }
  if (hasBookingEngine(inputs)) {
    infrastructureSignals.push(`Booking engine identified: ${toText(inputs.bookingEngineProvider)}`);
  }
  if (hasCrmCapture(inputs)) {
    infrastructureSignals.push("CRM / guest email capture is in place");
  }
  if (estDirect === "moderate" || estDirect === "high") {
    infrastructureSignals.push(`Estimated direct booking share: ${toText(inputs.estimatedDirectBookingShare)}`);
  }
  if (hasValue(inputs.topSourceMarkets)) {
    infrastructureSignals.push(`Source markets identified: ${toText(inputs.topSourceMarkets)}`);
  }
  if (hasValue(inputs.primaryGuestSegments)) {
    infrastructureSignals.push(`Guest segments identified: ${toText(inputs.primaryGuestSegments)}`);
  }

  if (concern === "poor conversion") conversionRisks.push("Owner identifies poor conversion as the primary commercial concern");
  if (concern === "weak direct bookings") conversionRisks.push("Weak direct bookings may reflect offer clarity, booking path friction, or rate parity issues");
  if (!parsePercentLike(inputs.websiteConversionRate)) {
    conversionRisks.push("Website conversion rate not provided — conversion performance cannot be confirmed");
  }
  conversionRisks.push("Booking engine abandonment rate not provided");
  if (hasThirdPartyUrls(inputs)) {
    conversionRisks.push("Strong third-party visibility may divert high-intent guests unless direct value proposition is compelling");
  }
  if (urlEvidence?.ownedVsOtaComparison?.assessment === "OTA presentation appears stronger") {
    conversionRisks.push("Extracted URL evidence suggests OTA presentation may currently be stronger than owned channel in decision-support cues.");
  }

  let label = "Limited";
  const infraCount = infrastructureSignals.length;

  if (isBranded(inputs) && hasBookingEngine(inputs) && hasCrmCapture(inputs)) {
    label = concern === "poor conversion" || concern === "weak direct bookings"
      ? "Developing — infrastructure present, conversion unconfirmed"
      : "Competitive";
  } else if (infraCount >= 3) {
    label = "Developing";
  } else if (infraCount >= 1) {
    label = "Developing";
  }

  const rationale =
    infrastructureSignals.length > 0
      ? `Direct-booking infrastructure appears ${infraCount >= 3 ? "meaningfully present" : "partially present"}: ${infrastructureSignals.slice(0, 3).join("; ")}. ${
          concern === "poor conversion"
            ? "Given the stated concern of poor conversion, the issue is likely conversion performance, direct offer clarity, booking path friction, or merchandising — not absence of basic commercial systems."
            : "Conversion performance and booking path quality still require performance data to confirm."
        }`
      : "Limited direct-booking infrastructure signals were identified. Booking engine, CRM capture, and direct value proposition should be assessed.";

  return {
    label,
    infrastructureSignals,
    conversionRisks,
    rationale,
    dataNeeded: [
      "Website conversion rate",
      "Booking engine abandonment rate",
      "Direct booking value proposition clarity",
      "Mobile booking path performance",
    ],
    dealImplication:
      goal.includes("direct")
        ? "Improving direct bookings requires confirming whether the constraint is traffic, conversion, offer competitiveness, or channel mix — not assuming more brand distribution alone will solve it."
        : "Direct booking capability should be validated against owner goals before external outreach.",
  };
}

function assessBrandSystemContribution(inputs) {
  if (!isBranded(inputs)) {
    return {
      assessment: "Brand distribution may be strategically relevant",
      rationale:
        "As an independent or non-branded property, brand or soft-brand affiliation could add distribution reach, loyalty infrastructure, and conversion trust — if direct-channel and operator gaps are not the primary constraint.",
      questions: [
        "Would brand loyalty and reservation infrastructure materially improve demand capture?",
        "Is the owner willing to accept brand standards, fees, and control trade-offs?",
      ],
      dealImplication: "Brand evaluation may be appropriate, but only after confirming whether the gap is distribution reach vs. direct conversion vs. execution.",
    };
  }

  const brandName = inferBrandName(inputs);
  return {
    assessment: "Brand/system infrastructure likely contributing",
    rationale: `The hotel is ${toText(inputs.currentBrandStatus).toLowerCase()}${brandName ? ` under ${brandName}` : ""}, with ${
      hasBookingEngine(inputs) ? `${toText(inputs.bookingEngineProvider)} booking infrastructure` : "booking infrastructure to confirm"
    }. This suggests brand distribution and reservation systems are already in place. The core question is whether those systems are being fully leveraged into direct demand and loyalty contribution — not whether the hotel lacks brand reach.`,
    questions: [
      "What share of bookings comes from brand direct channels vs. OTAs?",
      "What loyalty program contribution exists (e.g., member bookings)?",
      "Are brand booking engine, rate parity, and merchandising standards being fully utilized?",
      "What commercial reporting does the brand-managed structure provide?",
    ],
    dealImplication:
      "Do not recommend brand affiliation as the primary path unless the owner explicitly seeks a brand change. Focus on conversion, channel mix, and system utilization first.",
  };
}

function inferBrandName(inputs) {
  const engine = toText(inputs.bookingEngineProvider).toLowerCase();
  const website = toText(inputs.hotelWebsiteUrl).toLowerCase();
  if (engine.includes("ihg") || website.includes("ihg") || website.includes("intercontinental")) return "IHG";
  if (engine.includes("marriott") || website.includes("marriott")) return "Marriott";
  if (engine.includes("hilton") || website.includes("hilton")) return "Hilton";
  if (engine.includes("hyatt") || website.includes("hyatt")) return "Hyatt";
  if (engine.includes("accor") || website.includes("accor")) return "Accor";
  return "";
}

function assessOperatorExecutionNeed(inputs) {
  if (isBrandManaged(inputs)) {
    return {
      assessment: "Commercial reporting and execution review — not immediate operator change",
      rationale:
        "The hotel operates under a brand-managed model. That does not mean the owner needs a new operator. Instead, the owner should assess whether the brand-managed structure is delivering adequate commercial control, transparent reporting, channel mix management, and conversion performance. Issues may need escalation through brand commercial channels before structural changes are considered.",
      capabilitiesToReview: [
        "Revenue management transparency",
        "Channel mix reporting",
        "Direct vs. OTA contribution reporting",
        "Loyalty and CRM utilization",
        "Website conversion and booking path management",
      ],
      dealImplication:
        "Prioritize commercial reporting review and brand-system utilization before evaluating operator replacement.",
    };
  }

  if (isThirdPartyOperator(inputs)) {
    return {
      assessment: "Operator commercial capability review warranted",
      rationale:
        "A third-party operator manages the property. Commercial outcomes may depend heavily on operator capabilities in revenue management, direct booking strategy, OTA optimization, and reporting transparency.",
      capabilitiesToReview: [
        "Revenue management",
        "Direct booking strategy",
        "OTA optimization",
        "CRM and repeat guest capture",
        "Website conversion",
        "Channel mix management and reporting transparency",
      ],
      dealImplication: "Operator commercial capability should be validated against the owner's stated concern and goal.",
    };
  }

  if (isSelfManaged(inputs)) {
    return {
      assessment: isIndependent(inputs) ? "Owner may need operator or advisor support" : "Self-managed execution review",
      rationale: isIndependent(inputs)
        ? "Self-managed independent hotels often lack dedicated commercial execution capacity. The owner may need operator support, advisor-led commercial review, or targeted hires to improve channel mix and conversion."
        : "Self-managed branded properties still need strong commercial execution to leverage brand systems. The gap may be internal capability rather than infrastructure.",
      capabilitiesToReview: [
        "In-house revenue management",
        "Digital marketing and conversion",
        "OTA management",
        "CRM and loyalty utilization",
      ],
      dealImplication: "Assess whether the owner has internal commercial execution capacity before external outreach.",
    };
  }

  return {
    assessment: "Operator status unclear — commercial execution unknown",
    rationale: "Operator status was not clearly identified. Commercial execution assessment is limited until management structure is confirmed.",
    capabilitiesToReview: ["Revenue management", "Direct booking strategy", "Channel mix reporting"],
    dealImplication: "Confirm operator/management structure before recommending operator-led paths.",
  };
}

function assessEconomicSensitivity(inputs) {
  const commission = parsePercentLike(inputs.estimatedOtaCommission);
  const actualOta = parsePercentLike(inputs.actualOtaBookingShare);
  const cannotCalculateBecause = [];
  if (!actualOta) cannotCalculateBecause.push("Actual OTA booking share not provided");
  cannotCalculateBecause.push("Revenue or room-night volume not provided");

  if (commission && !actualOta) {
    return {
      assessment: "Directional economic relevance — cannot quantify exposure",
      rationale: `Estimated OTA commission is ${toText(inputs.estimatedOtaCommission)}, but actual OTA booking share was not provided. The snapshot cannot calculate channel cost exposure or ROI of direct conversion improvement. However, if OTA share is material at ${toText(inputs.estimatedOtaCommission)} commission, even modest direct conversion improvement may have economic relevance. This is directional only — not a calculated savings estimate.`,
      cannotCalculateBecause,
      dataNeeded: ["Actual OTA booking share", "Monthly OTA commission cost", "Revenue or room nights by channel"],
    };
  }

  if (commission && actualOta) {
    return {
      assessment: "Economic sensitivity can be directionally assessed",
      rationale: `With approximately ${actualOta}% OTA share and ${toText(inputs.estimatedOtaCommission)} estimated commission, channel cost exposure is directionally meaningful. A modest shift toward direct bookings could reduce commission burden, though exact savings require revenue volume data.`,
      cannotCalculateBecause: ["Revenue or room-night volume not provided for exact calculation"],
      dataNeeded: ["Revenue by channel", "Room nights by channel"],
    };
  }

  return {
    assessment: "Insufficient data for economic sensitivity analysis",
    rationale: "Neither OTA commission nor actual OTA share was provided in sufficient detail to assess economic sensitivity.",
    cannotCalculateBecause,
    dataNeeded: ["Estimated or actual OTA commission", "Actual OTA booking share"],
  };
}

function assessStrategicDiagnosis(inputs, urlEvidence = null) {
  const concern = toText(inputs.mainCommercialConcern).toLowerCase();
  const goal = toText(inputs.ownerGoal).toLowerCase();
  const notYetConfirmed = [];

  let primaryDiagnosis = "Data visibility gap";
  let secondaryDiagnosis = "Mixed commercial gap";

  if (isBranded(inputs) && hasBookingEngine(inputs) && (concern === "poor conversion" || concern === "weak direct bookings")) {
    primaryDiagnosis = "Direct conversion / commercial execution gap";
    secondaryDiagnosis = isBrandManaged(inputs) ? "Brand/system utilization gap" : "Commercial execution gap";
    notYetConfirmed.push("Actual OTA vs. direct channel mix");
    notYetConfirmed.push("Website conversion rate");
    notYetConfirmed.push("Booking engine abandonment rate");
  } else if (concern === "brand distribution" && isIndependent(inputs)) {
    primaryDiagnosis = "Distribution reach gap";
    secondaryDiagnosis = "Positioning gap";
  } else if (concern === "operator capability") {
    primaryDiagnosis = "Operator capability gap";
    secondaryDiagnosis = "Commercial execution gap";
  } else if (concern === "ota dependency") {
    primaryDiagnosis = "Channel mix / OTA dependency gap";
    secondaryDiagnosis = "Direct conversion gap";
  } else if (concern === "low demand") {
    primaryDiagnosis = "Distribution reach gap";
    secondaryDiagnosis = "Positioning gap";
  } else if (concern === "repositioning") {
    primaryDiagnosis = "Positioning gap";
    secondaryDiagnosis = "Owned-channel content gap";
  } else if (!hasPerformanceNumbers(inputs)) {
    primaryDiagnosis = "Data visibility gap";
    secondaryDiagnosis = concern ? `${concern} (unconfirmed by performance data)` : "Mixed commercial gap";
    notYetConfirmed.push("Performance metrics needed to confirm diagnosis");
  }
  if (urlEvidence?.ownedVsOtaComparison?.assessment === "OTA presentation appears stronger") {
    secondaryDiagnosis = "Owned-channel content gap";
    if (!notYetConfirmed.includes("Owned vs OTA content parity")) {
      notYetConfirmed.push("Owned vs OTA content parity");
    }
  }

  const rationale = buildDiagnosisRationale(inputs, primaryDiagnosis, secondaryDiagnosis);

  return { primaryDiagnosis, secondaryDiagnosis, notYetConfirmed, rationale };
}

function buildDiagnosisRationale(inputs, primary, secondary) {
  const parts = [];
  if (isBranded(inputs)) parts.push(`branded status (${toText(inputs.currentBrandStatus)})`);
  if (hasBookingEngine(inputs)) parts.push(`booking engine (${toText(inputs.bookingEngineProvider)})`);
  if (hasCrmCapture(inputs)) parts.push("CRM/email capture");
  if (hasValue(inputs.additionalOwnerNotes)) parts.push("owner notes on third-party visibility and conversion concern");

  return `Primary diagnosis: ${primary}. Secondary: ${secondary}. ${
    parts.length
      ? `Supporting signals include ${parts.join(", ")}.`
      : ""
  } ${
    toText(inputs.mainCommercialConcern)
      ? `The stated commercial concern is "${toText(inputs.mainCommercialConcern)}" and the owner goal is "${toText(inputs.ownerGoal)}".`
      : ""
  } Performance data gaps prevent confirmation of diagnosis severity.`;
}

function buildRecommendedPath(inputs, diagnosis) {
  const concern = toText(inputs.mainCommercialConcern).toLowerCase();
  const goal = toText(inputs.ownerGoal).toLowerCase();

  if (isBranded(inputs) && isBrandManaged(inputs) && (concern === "poor conversion" || goal.includes("direct"))) {
    return {
      headline: "Direct Conversion and Brand-System Utilization Review Before Any Brand/Operator Strategy Change",
      recommendedSteps: [
        "Data Collection Before Structural Decision",
        "Direct Conversion Review",
        "Brand/System Utilization Review",
        "Commercial Reporting Review",
      ],
      rationale: `Because the hotel already has brand infrastructure${hasBookingEngine(inputs) ? ` and ${toText(inputs.bookingEngineProvider)}` : ""}, the first question is whether those systems are being fully converted into direct demand. The owner should confirm channel mix, website conversion, brand direct contribution, loyalty contribution, OTA cost exposure, and booking path friction before considering a brand or operator strategy change. ${diagnosis.primaryDiagnosis} appears more relevant than a basic brand distribution gap.`,
    };
  }

  if (isIndependent(inputs) && (goal.includes("brand") || concern === "brand distribution")) {
    return {
      headline: "Brand / Soft Brand Evaluation After Direct-Channel Assessment",
      recommendedSteps: [
        "Data Collection Before Decision",
        "Owned-Channel and Conversion Review",
        "Brand / Soft Brand Evaluation",
      ],
      rationale:
        "As an independent property, brand affiliation may add distribution and trust — but only if the primary gap is reach rather than conversion, content, or execution.",
    };
  }

  if (isThirdPartyOperator(inputs) && (concern === "operator capability" || concern === "weak direct bookings")) {
    return {
      headline: "Operator-Led Commercial Capability Review",
      recommendedSteps: [
        "Commercial Reporting and Transparency Review",
        "Operator Capability Assessment",
        "Direct Booking and Channel Mix Review",
      ],
      rationale: "Third-party operator commercial execution is likely central to outcomes. Validate operator capabilities before structural changes.",
    };
  }

  if (isSelfManaged(inputs) && isIndependent(inputs)) {
    return {
      headline: "Direct Commercial Improvement and Capability Assessment",
      recommendedSteps: [
        "Data Collection Before Decision",
        "Direct Commercial Improvement First",
        "Evaluate Operator or Advisor Support",
      ],
      rationale:
        "Self-managed independent hotels often need either stronger internal commercial capability or external operator/advisor support to shift channel mix.",
    };
  }

  return {
    headline: "Clarify Commercial Gap Before External Outreach",
    recommendedSteps: [
      "Data Collection Before Decision",
      "Direct Commercial Improvement First",
      "Advisor-Led Commercial Review",
    ],
    rationale:
      "The owner should avoid entering brand or operator conversations without a clearer commercial diagnosis. Confirm channel mix, conversion, and execution gaps first.",
  };
}

function buildExecutiveInterpretation(inputs, readiness, otaRisk, directCap, diagnosis) {
  const brandName = inferBrandName(inputs);
  const infraParts = [];
  if (isBranded(inputs)) infraParts.push(`brand affiliation${brandName ? ` (${brandName})` : ""}`);
  if (hasBookingEngine(inputs)) infraParts.push(toText(inputs.bookingEngineProvider));
  if (hasCrmCapture(inputs)) infraParts.push("CRM/email capture");
  if (hasThirdPartyUrls(inputs)) infraParts.push("recognized third-party visibility");

  const primaryQuestion = isBranded(inputs) && (toText(inputs.mainCommercialConcern).toLowerCase() === "poor conversion")
    ? "Is the current direct channel converting enough high-intent demand relative to OTA alternatives, despite existing brand infrastructure?"
    : toText(inputs.mainCommercialConcern)
    ? `Is the primary constraint "${toText(inputs.mainCommercialConcern)}" — and what evidence confirms it?`
    : "What is the primary commercial constraint before choosing brand, operator, or go-to-market path?";

  let summary;
  if (infraParts.length >= 2 && isBranded(inputs)) {
    summary = `This hotel already has several commercial infrastructure advantages: ${infraParts.join(", ")}. That suggests the core question is not whether the hotel needs basic commercial infrastructure, but whether the current direct channel is converting enough high-intent demand relative to OTA alternatives. Because actual OTA/direct booking shares and website conversion rate are missing, the snapshot cannot confirm the size of the issue. However, the stated concern of ${toText(inputs.mainCommercialConcern).toLowerCase() || "commercial performance"}, estimated OTA/direct shares, and ${
      hasValue(inputs.estimatedOtaCommission) ? `${toText(inputs.estimatedOtaCommission)} OTA commission` : "channel economics"
    } indicate that even a modest channel shift could matter economically if OTA share is material.`;
  } else if (isIndependent(inputs)) {
    summary = `As an independent property, this hotel's commercial readiness depends on owned-channel strength, booking infrastructure, and execution capability. ${readiness.label} readiness reflects ${
      hasBookingEngine(inputs) ? "some booking infrastructure" : "limited identified booking infrastructure"
    }. ${otaRisk.label !== "Not enough data" ? `OTA dependency signal: ${otaRisk.label}.` : "OTA dependency cannot be confirmed without channel data."} The owner should clarify whether the gap is distribution reach, direct conversion, or commercial execution before brand or operator outreach.`;
  } else {
    summary = `Based on structured inputs, commercial readiness is assessed as ${readiness.label} with ${directCap.label} direct-booking capability. ${otaRisk.confirmedByNumbers ? "OTA dependency is partially informed by provided figures." : "Actual OTA dependency cannot be confirmed without channel mix data."} ${diagnosis.primaryDiagnosis} is the leading diagnosis, pending performance confirmation.`;
  }

  return {
    summary,
    primaryCommercialQuestion: primaryQuestion,
    dealImplication: `For deal strategy: ${diagnosis.primaryDiagnosis} should be resolved or quantified before major brand, operator, or capital outreach. ${isBranded(inputs) ? "Brand affiliation alone is unlikely to solve conversion or execution gaps." : "Brand evaluation may be relevant only if distribution reach — not conversion — is the confirmed gap."}`,
  };
}

function buildAdaptiveQuestions(inputs) {
  const questions = [];
  const brandName = inferBrandName(inputs);
  const markets = toText(inputs.topSourceMarkets);
  const segments = toText(inputs.primaryGuestSegments);

  if (isBranded(inputs)) {
    questions.push(
      `What percentage of bookings come from ${brandName || "brand"} direct channels versus OTAs?`,
      brandName === "IHG" ? "What share of bookings comes from IHG One Rewards members?" : "What share of bookings comes from loyalty program members?",
      "What is the hotel website conversion rate?",
      "What is the booking engine abandonment rate?",
      "What is the OTA commission cost by month?",
    );
  } else {
    questions.push(
      "What percentage of bookings currently come from OTAs?",
      "What percentage of bookings are direct?",
      "What is the direct website conversion rate?",
    );
  }

  if (markets) {
    questions.push(`Do guests from ${markets} book direct or OTA at different rates?`);
    questions.push(`Are source markets (${markets}) behaving differently by channel?`);
  }

  if (segments) {
    questions.push(`Which guest segments (${segments}) convert best on direct vs. OTA?`);
  }

  questions.push(
    "Which room types are most frequently booked through OTAs?",
    "What direct-booking benefits are visible on the hotel website?",
    "Is the hotel losing guests after rate comparison on Booking.com or Expedia?",
  );

  if (isBrandManaged(inputs)) {
    questions.push("What commercial reporting does the brand-managed structure provide to the owner?");
    questions.push("Is poor conversion a website problem, offer problem, rate parity problem, or demand-segment problem?");
  }

  if (isThirdPartyOperator(inputs)) {
    questions.push("What commercial capabilities does the current operator bring — and where are gaps?");
  }

  // Dedupe and cap at 12
  const seen = new Set();
  return questions.filter((q) => {
    if (seen.has(q)) return false;
    seen.add(q);
    return true;
  }).slice(0, 12);
}

function assessOwnedChannelGap(inputs, urlEvidence = null) {
  const comparison = urlEvidence?.ownedVsOtaComparison;
  if (comparison && comparison.assessment && comparison.assessment !== "Insufficient extracted evidence") {
    return {
      assessment: comparison.assessment,
      confidence: comparison.confidence || "Low",
      rationale:
        "Based on extracted page evidence from provided sources, owned-vs-OTA comparison is now directional and should still be manually validated for blocked or partial sources.",
      itemsToCompare: [
        "Room descriptions",
        "Location story",
        "Guest reassurance",
        "Booking confidence",
        "Direct booking value",
        "Reviews/social proof",
        "FAQs/policies",
        "Offers/packages",
        "Guest-segment fit",
        "Brand/loyalty leverage",
      ],
      ownedChannelStrengths: comparison.ownedChannelStrengths || [],
      otaStrengths: comparison.otaStrengths || [],
      contentGaps: comparison.contentGaps || [],
      directBookingGaps: comparison.directBookingGaps || [],
      guestReassuranceGaps: comparison.guestReassuranceGaps || [],
      dealImplication: comparison.dealImplication || "",
    };
  }
  const hasOta = hasThirdPartyUrls(inputs);
  const hasOwned = hasValue(inputs.hotelWebsiteUrl);

  if (!hasOwned) {
    return {
      assessment: "Owned channel not provided — comparison not possible",
      rationale: "Hotel website URL was not provided.",
      itemsToCompare: [],
      dealImplication: "Provide owned-channel URL for content gap analysis.",
    };
  }

  if (!hasOta) {
    return {
      assessment: "Third-party links missing — owned vs. OTA comparison is an evidence gap",
      rationale:
        "URL-level content was provided for the hotel website but OTA listing URLs were not supplied. The snapshot uses owner-provided inputs and structured commercial logic. Future versions should add page-level extraction for room descriptions, location story, visual clarity, and booking confidence comparison.",
      itemsToCompare: ["Room descriptions", "Location story", "Guest reassurance", "Direct booking value", "Review themes", "FAQs"],
      dealImplication: "Add OTA URLs to enable owned-vs-OTA content comparison in future runs.",
    };
  }

  return {
      assessment: "Insufficient extracted evidence",
    rationale:
      "URL-level analysis was attempted, but content could not be extracted from provided sources. This snapshot therefore uses owner-provided inputs and structured commercial logic. Manual comparison is recommended.",
    itemsToCompare: [
      "Room descriptions",
      "Location story",
      "Guest reassurance",
      "Visual clarity",
      "Booking confidence",
      "Direct booking value",
      "Review themes",
      "FAQs",
    ],
    dealImplication:
      hasValue(inputs.additionalOwnerNotes) && inputs.additionalOwnerNotes.toLowerCase().includes("review")
        ? "Owner notes reference strong third-party visibility and review volume — validate whether those trust signals are reflected on the owned website."
        : "Content parity between owned and OTA channels may affect conversion — confirm with manual review or future extraction.",
  };
}

function buildSuggestedActions(inputs, recommendedPath, diagnosis) {
  const immediate = [
    "Confirm actual channel mix (OTA vs. direct vs. brand/loyalty) with current reporting.",
    "Document direct booking value proposition and booking-path friction points.",
  ];

  if (isBranded(inputs)) {
    immediate.push("Request brand direct contribution and loyalty booking reports.");
    immediate.push("Review whether rate parity, offers, and merchandising support direct conversion.");
  }

  if (parsePercentLike(inputs.estimatedOtaCommission) && !parsePercentLike(inputs.actualOtaBookingShare)) {
    immediate.push("Quantify OTA share to assess economic exposure at stated commission level.");
  }

  const dealalityNextSteps = [];
  if (!isBranded(inputs) && (toText(inputs.ownerGoal).toLowerCase().includes("brand") || toText(inputs.mainCommercialConcern).toLowerCase() === "brand distribution")) {
    dealalityNextSteps.push("Run Brand Alignment Snapshot if brand distribution appears strategically important.");
  }
  if (isThirdPartyOperator(inputs) || diagnosis.primaryDiagnosis.includes("Operator")) {
    dealalityNextSteps.push("Run Operator Alignment Snapshot if commercial execution capability is a major gap.");
  }
  dealalityNextSteps.push("Add commercial concerns to Deal Readiness Snapshot context.");
  dealalityNextSteps.push("Prepare outreach language that clearly explains the owner's commercial objective and evidence gaps.");

  return { immediateActions: immediate, dealalityNextSteps };
}

/**
 * Main export — builds full snapshot JSON + labels + narrative.
 */
export function buildCommercialReadinessSnapshot(inputs, options = {}) {
  const urlEvidence = options?.urlEvidence || null;
  const confidence = assessConfidenceDimensions(inputs);
  const directCap = assessDirectBookingCapability(inputs, urlEvidence);
  const readiness = assessReadinessLevel(inputs, directCap.label);
  const otaRisk = assessOtaDependencyRisk(inputs);
  const brandSystem = assessBrandSystemContribution(inputs);
  const operatorNeed = assessOperatorExecutionNeed(inputs);
  const economic = assessEconomicSensitivity(inputs);
  const diagnosis = assessStrategicDiagnosis(inputs, urlEvidence);
  const recommendedPath = buildRecommendedPath(inputs, diagnosis);
  const executive = buildExecutiveInterpretation(inputs, readiness, otaRisk, directCap, diagnosis);
  const ownedGap = assessOwnedChannelGap(inputs, urlEvidence);
  const questions = buildAdaptiveQuestions(inputs);
  const suggestedNextActions = buildSuggestedActions(inputs, recommendedPath, diagnosis);

  const snapshot = {
    snapshotBasis: {
      inputCompleteness: confidence.inputCompleteness,
      performanceDataConfidence: confidence.performanceDataConfidence,
      observableEvidenceConfidence: confidence.observableEvidenceConfidence,
      evidenceConfidence: confidence.evidenceConfidence,
      confirmedPerformanceDataProvided: confidence.confirmedPerformanceDataProvided,
      missingData: confidence.missingData,
      note:
        "This snapshot is based on observable commercial signals from the hotel's owned and third-party channels, plus owner-provided inputs. URL-level content was not programmatically analyzed in this MVP unless otherwise noted. Performance metrics are not confirmed unless provided.",
    },
    executiveCommercialInterpretation: executive,
    commercialReadinessLevel: {
      label: readiness.label,
      confidence: confidence.evidenceConfidence,
      rationale: readiness.rationale,
      keyDrivers: directCap.infrastructureSignals.slice(0, 5),
    },
    otaDependencyRisk: otaRisk,
    directBookingCapability: directCap,
    ownedChannelVsOtaContentGap: ownedGap,
    brandSystemContribution: brandSystem,
    operatorCommercialExecutionNeed: operatorNeed,
    economicSensitivity: economic,
    strategicDiagnosis: diagnosis,
    recommendedPath,
    urlEvidence,
    dataNeededToConfirm: confidence.missingData,
    questionsToResolve: questions,
    suggestedNextActions,
    // Legacy keys for backward compatibility
    commercialReadiness: {
      summary: executive.summary,
      level: readiness.label,
      rationale: readiness.rationale,
      keyDrivers: directCap.infrastructureSignals.slice(0, 5),
    },
    recommendedStrategicPath: {
      recommendedPath: recommendedPath.recommendedSteps,
      why: recommendedPath.rationale,
    },
  };

  const narrative = [
    executive.summary,
    readiness.rationale,
    otaRisk.rationale,
    directCap.rationale,
    recommendedPath.rationale,
    diagnosis.rationale,
  ].join("\n\n");

  return {
    labels: {
      readinessLevel: readiness.label,
      confidence: confidence.evidenceConfidence,
      inputCompleteness: confidence.inputCompleteness,
      performanceDataConfidence: confidence.performanceDataConfidence,
      observableEvidenceConfidence: confidence.observableEvidenceConfidence,
      otaRisk: otaRisk.label,
      directCapability: directCap.label,
      strategicDiagnosis: diagnosis.primaryDiagnosis,
      brandNeed: isBranded(inputs) ? "Low" : "Moderate",
      operatorNeed: operatorNeed.assessment,
      status: "Snapshot ready",
    },
    narrative,
    snapshot,
    enums: {
      readinessLevels: READINESS_LEVELS,
      confidenceLevels: CONFIDENCE_LEVELS,
    },
  };
}
