/**
 * Operator Explorer — always-on trust footnote (parallel to Brand Explorer AI-Assisted footnote).
 *
 * Every Production Operator Explorer profile must render:
 *   [Source-Informed Profile | AI-Assisted Profile | …]
 *   Last Reviewed: [MMM D, YYYY] · Source Basis: […] · Region: […]
 *
 * Prefer live Master governance when complete; otherwise enrich from posture registry
 * + Operating Model heuristics. Never invent Company Validated without checkbox + date.
 */
import {
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_EXTERNAL_DISPLAY,
  GOVERNANCE_EXTERNAL_DISPLAY_LABEL,
  GOVERNANCE_EXTERNAL_SOURCE_BASIS,
  GOVERNANCE_USAGE_PERMISSION,
} from "../profile-governance/profile-governance-fields.js";

export const OE_TRUST_FOOTNOTE_VERSION = "operator-explorer-trust-footnote-v1";
export const OE_TRUST_FOOTNOTE_EFFECTIVE_DATE = "2026-08-11";

/** @typedef {{ validationStatus: string, sourceRegion: string, lastReviewedDate?: string, sourceType?: string[], usagePermission?: string, externalDisplayStatus?: string, keepExisting?: boolean, notes?: string }} OeTrustPosture */

/**
 * Explicit postures for Production Operator Setup - Master IDs.
 * Keep Arbor / HE / GHL validation where already set; fill Region gaps.
 * @type {Record<string, OeTrustPosture>}
 */
export const OE_TRUST_FOOTNOTE_POSTURE_BY_MASTER_ID = Object.freeze({
  // Exemplars / already governed
  recF5Z87OAqFgndoq: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    lastReviewedDate: "2026-06-10",
    keepExisting: true,
    notes: "Arbor CALA exemplar — Source-Informed",
  },
  recWPKu5laVZxsvpn: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "CALA-Specific",
    lastReviewedDate: "2026-07-06",
    keepExisting: true,
    notes: "HE CALA exemplar — Company Published → AI-Assisted Profile; Region was missing",
  },
  reciI2tYQBfMoMK9G: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Regional",
    lastReviewedDate: "2026-07-06",
    keepExisting: true,
    notes: "GHL — Company Published; LatAm regional region basis",
  },

  // CALA / LatAm third-party & regional operators → Source-Informed + CALA/Regional
  rec9JSyGQjvodsPSJ: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "Regional",
    notes: "AADESA — Argentine / LatAm third-party",
  },
  recGWxIJqnYHkJZFD: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Aimbridge LATAM",
  },
  recjgHXqTJktijFUR: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "Regional",
    notes: "Álvarez Argüelles — Argentina",
  },
  receHCdI6CEsJqdG4: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Brittain Resorts — Caribbean focus",
  },
  recQ6Cf8O2z0tiqBz: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Cenote Azul — Yucatán",
  },
  reckyv9O0Y3auYpJJ: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "GHSF — Mexico",
  },
  recuEDrp6oeJIEuRX: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Grupo Marta",
  },
  recJtFkhjaO57rSDC: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Grupo Presidente — Mexico",
  },
  rectsHzacZDFTH1Ze: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "OxoHotel",
  },
  rec3TUHT9Z4AnFp5P: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "CALA-Specific",
    notes: "Playa — CALA AI owner-operator; company materials",
  },
  recOc5kpsg4Muip9Y: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "CALA-Specific",
    notes: "Royalton — Caribbean AI brand-operator",
  },
  recJ6NPSYveCTo3At: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Tafer — Mexico resorts",
  },
  recHj56wpRLUnJ5Wx: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "Regional",
    notes: "Tremun — Argentina",
  },
  reck6gjQd3wdeugmZ: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Arriva / AHG",
  },
  recfwDdU5t9h4uFnZ: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "Regional",
    notes: "Atlantica Hotels International",
  },
  recwEHUotSGpfkZEJ: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "CALA-Specific",
    notes: "Iberostar — company published Wave of Change / corporate materials; CALA leisure depth",
  },
  rec04aLAfmupWG4ZK: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Barceló — integrated Spanish group; global + CALA leisure",
  },

  // U.S. third-party with CALA expansion
  recKVILWcRLqrQlWs: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "Regional",
    notes: "Driftwood — U.S. third-party",
  },
  rec6UB6RpMKSs2tAo: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "CALA-Specific",
    notes: "Remington — CALA office expansion",
  },
  recLjxtxIIVJaGbXK: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "Regional",
    notes: "Highgate — third-party; CALA resort BU exists but posture Regional until CALA package locked",
  },

  // Global brand-managed / hybrid → Company Published → AI-Assisted Profile
  recF2WqLqNVyKGz9E: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Accor Managed",
  },
  recVtNxNeeYlngtUk: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Auberge",
  },
  rechnXKjpeiNMaqjJ: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Four Seasons",
  },
  rec3Uwxe6ovpiokuN: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Hilton Managed",
  },
  reculkMOYWDxX14Pv: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Hyatt Managed",
  },
  rec7IXYQYpKMYsrDl: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "IHG Managed",
  },
  rec5xdV2THfFjEUPk: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Mandarin Oriental",
  },
  recGmiPhRt6hiayd9: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Marriott Managed",
  },
  rec28eZ7ERwc92XWd: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Meliá",
  },
  rec8SrT3VjRkkYTxm: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Minor Managed",
  },
  recji1awMffccwox2: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Rosewood",
  },
  rec8XpNv6G0WOlMwu: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Shangri-La",
  },
  recIq0XYgt5Ghvcsz: {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
    sourceRegion: "Global Reference",
    notes: "Sonesta",
  },
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readDateIso(raw) {
  const s = nz(raw);
  if (!s) return null;
  const day = s.split("T")[0];
  if (/^\d{4}-\d{2}-\d{2}$/.test(day)) return day;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

export function formatOeFootnoteDate(isoOrDate) {
  const iso = readDateIso(isoOrDate);
  if (!iso) return null;
  const d = new Date(`${iso}T12:00:00`);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function regionOwnerFacing(sourceRegion) {
  const s = nz(sourceRegion);
  if (!s) return null;
  if (s === "CALA-Specific") return "CALA-specific";
  if (s === "Global Reference") return "Global Reference";
  if (s === "Regional") return "Regional";
  if (s === "Market-Specific") return "Market-specific";
  if (s === "Unknown") return "Not region-specific";
  return s;
}

function resolveExternalLabel(validationStatus) {
  switch (validationStatus) {
    case GOVERNANCE_VALIDATION_STATUS.companyValidated:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyValidated;
    case GOVERNANCE_VALIDATION_STATUS.companyReviewed:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyReviewed;
    case GOVERNANCE_VALIDATION_STATUS.companyPublished:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyPublished;
    case GOVERNANCE_VALIDATION_STATUS.sourceInformed:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.sourceInformed;
    case GOVERNANCE_VALIDATION_STATUS.aiAssisted:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.aiAssisted;
    default:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.sourceInformed;
  }
}

function resolveSourceBasis(validationStatus) {
  switch (validationStatus) {
    case GOVERNANCE_VALIDATION_STATUS.companyPublished:
      return GOVERNANCE_EXTERNAL_SOURCE_BASIS.companyPublished;
    case GOVERNANCE_VALIDATION_STATUS.sourceInformed:
      return GOVERNANCE_EXTERNAL_SOURCE_BASIS.sourceInformed;
    case GOVERNANCE_VALIDATION_STATUS.aiAssisted:
      return GOVERNANCE_EXTERNAL_SOURCE_BASIS.aiAssisted;
    case GOVERNANCE_VALIDATION_STATUS.companyValidated:
      return "Company Validated";
    default:
      return GOVERNANCE_EXTERNAL_SOURCE_BASIS.sourceInformed;
  }
}

export function buildOeFootnoteSubtitle({ lastReviewedFormatted, sourceBasis, regionBasis }) {
  const parts = [];
  if (lastReviewedFormatted) parts.push(`Last Reviewed: ${lastReviewedFormatted}`);
  if (sourceBasis) parts.push(`Source Basis: ${sourceBasis}`);
  if (regionBasis) parts.push(`Region: ${regionBasis}`);
  return parts.length ? parts.join(" · ") : null;
}

/**
 * Infer posture when master id not in registry.
 */
export function inferOeTrustPosture({ companyName = "", operatingModel = "" } = {}) {
  const name = nz(companyName);
  const om = nz(operatingModel);
  if (/CALA/i.test(name) || /LATAM|Mexico|Yucat|Caribbean|Riviera/i.test(name)) {
    return {
      validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
      sourceRegion: "CALA-Specific",
      notes: "Inferred CALA/LatAm from name",
    };
  }
  if (/Brand|Integrated|Managed/i.test(om) || /\(Managed\)/i.test(name)) {
    return {
      validationStatus: GOVERNANCE_VALIDATION_STATUS.companyPublished,
      sourceRegion: "Global Reference",
      notes: "Inferred brand-managed / integrated",
    };
  }
  if (/Third-Party|Hybrid/i.test(om)) {
    return {
      validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
      sourceRegion: "Regional",
      notes: "Inferred third-party / hybrid",
    };
  }
  return {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    sourceRegion: "Regional",
    notes: "Default Source-Informed",
  };
}

/**
 * Resolve always-on footnote for an operator detail payload.
 */
export function resolveOperatorExplorerTrustFootnote({
  governance = null,
  masterId = null,
  companyName = "",
  operatingModel = "",
  masterFields = null,
} = {}) {
  const prior = governance && typeof governance === "object" ? governance : {};
  const posture =
    (masterId && OE_TRUST_FOOTNOTE_POSTURE_BY_MASTER_ID[masterId]) ||
    inferOeTrustPosture({ companyName, operatingModel });

  const validationStatus =
    nz(prior.validationStatus) ||
    nz(masterFields?.["Validation Status"]) ||
    posture.validationStatus ||
    GOVERNANCE_VALIDATION_STATUS.sourceInformed;

  const sourceRegion =
    nz(prior.sourceRegion) ||
    nz(masterFields?.["Source Region"]) ||
    posture.sourceRegion ||
    "Regional";

  const lastReviewedIso =
    readDateIso(prior.lastReviewedDate) ||
    readDateIso(masterFields?.["Last Reviewed Date"]) ||
    readDateIso(posture.lastReviewedDate) ||
    OE_TRUST_FOOTNOTE_EFFECTIVE_DATE;

  const companyValidated = prior.companyValidated === true || masterFields?.["Company Validated"] === true;

  let displayLabel = nz(prior.displayLabel);
  // Always-on: if blank or incomplete subtitle, enrich (do not upgrade to Company Validated without evidence)
  if (!displayLabel) {
    if (companyValidated && validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated) {
      displayLabel = GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyValidated;
    } else {
      displayLabel = resolveExternalLabel(validationStatus);
    }
  }

  let sourceBasis = nz(prior.sourceBasis);
  if (!sourceBasis) sourceBasis = resolveSourceBasis(validationStatus);
  if (companyValidated && validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated) {
    sourceBasis = "Company Validated";
  }

  const regionBasis = regionOwnerFacing(sourceRegion);
  const lastReviewedFormatted = formatOeFootnoteDate(lastReviewedIso);
  let displaySubtitle = nz(prior.displaySubtitle);
  const needsSubtitleEnrich =
    !displaySubtitle ||
    !/Last Reviewed:/i.test(displaySubtitle) ||
    !/Source Basis:/i.test(displaySubtitle) ||
    !/Region:/i.test(displaySubtitle);

  if (needsSubtitleEnrich) {
    displaySubtitle = buildOeFootnoteSubtitle({
      lastReviewedFormatted,
      sourceBasis,
      regionBasis,
    });
  }

  return {
    version: OE_TRUST_FOOTNOTE_VERSION,
    displayLabel,
    displaySubtitle,
    sourceBasis,
    regionBasis,
    sourceRegion,
    validationStatus,
    lastReviewedIso,
    lastReviewedFormatted,
    alwaysOn: true,
    postureNotes: posture.notes || null,
  };
}

/**
 * Mutates operator.governance for Explorer trust chip.
 */
export function applyOperatorExplorerTrustFootnote(operator, options = {}) {
  if (!operator || typeof operator !== "object") return operator;
  const prior = operator.governance && typeof operator.governance === "object" ? { ...operator.governance } : {};
  const footnote = resolveOperatorExplorerTrustFootnote({
    governance: prior,
    masterId: options.masterId || operator.id || null,
    companyName:
      options.companyName ||
      operator.prefill?.company_name ||
      operator.fields?.company_name ||
      "",
    operatingModel: options.operatingModel || operator.fields?.["Operating Model"] || "",
    masterFields: options.masterFields || operator.fields || null,
  });

  operator.governance = {
    ...prior,
    validationStatus: prior.validationStatus || footnote.validationStatus,
    sourceRegion: prior.sourceRegion || footnote.sourceRegion,
    sourceBasis: footnote.sourceBasis,
    lastReviewedDate: prior.lastReviewedDate || footnote.lastReviewedIso,
    displayLabel: footnote.displayLabel,
    displaySubtitle: footnote.displaySubtitle,
    operatorExplorerFootnote: {
      version: footnote.version,
      lastReviewedIso: footnote.lastReviewedIso,
      lastReviewedFormatted: footnote.lastReviewedFormatted,
      sourceBasis: footnote.sourceBasis,
      regionBasis: footnote.regionBasis,
      alwaysOn: true,
      postureNotes: footnote.postureNotes,
    },
  };
  return operator;
}

/**
 * Airtable Master patch fields for a posture (backfill).
 */
export function buildMasterGovernancePatchFromPosture(posture, { existingFields = {} } = {}) {
  const p = posture || {};
  const fields = {};
  if (!nz(existingFields["Validation Status"]) && p.validationStatus) {
    fields["Validation Status"] = p.validationStatus;
  }
  if (!nz(existingFields["External Display Status"])) {
    fields["External Display Status"] = GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel;
  }
  if (!nz(existingFields["Usage Permission"])) {
    fields["Usage Permission"] = GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed;
  }
  if (!nz(existingFields["Source Region"]) && p.sourceRegion) {
    fields["Source Region"] = p.sourceRegion;
  }
  if (!nz(existingFields["Last Reviewed Date"])) {
    fields["Last Reviewed Date"] = p.lastReviewedDate || OE_TRUST_FOOTNOTE_EFFECTIVE_DATE;
  }
  // Always ensure Show Trust Label if currently blank/hidden for Production OE display intent
  if (
    nz(existingFields["External Display Status"]) &&
    existingFields["External Display Status"] !== GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel &&
    !p.keepExisting
  ) {
    // do not override deliberate Hide/Do Not Display without founder intent
  } else if (!nz(existingFields["External Display Status"])) {
    fields["External Display Status"] = GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel;
  }

  const st = existingFields["Source Type"];
  const hasSourceType = Array.isArray(st) ? st.length > 0 : Boolean(nz(st));
  if (!hasSourceType) {
    fields["Source Type"] =
      p.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyPublished
        ? ["Company Website", "Public website"]
        : ["Internal research", "Public website"];
  }
  return fields;
}

export function evaluateOperatorExplorerTrustFootnoteGate(operator, html = "") {
  const failures = [];
  const gov = operator?.governance || {};
  const label = nz(gov.displayLabel);
  const subtitle = nz(gov.displaySubtitle);
  if (!label) failures.push("footnote_component_not_rendered");
  if (!/Last Reviewed:\s*\S+/i.test(subtitle)) failures.push("last_reviewed_date_missing");
  if (!/Source Basis:\s*\S+/i.test(subtitle)) failures.push("source_basis_missing");
  if (!/Region:\s*\S+/i.test(subtitle)) failures.push("region_basis_missing");
  const htmlText = nz(html);
  if (htmlText) {
    if (!/Source-Informed Profile|AI-Assisted Profile|Company-Validated Profile|Company-Reviewed Profile/i.test(htmlText)) {
      failures.push("footnote_not_visible_in_rendered_html");
    }
  }
  return {
    gateId: "operator_explorer_trust_footnote_visible",
    pass: failures.length === 0,
    failures: [...new Set(failures)],
    displayLabel: label || null,
    displaySubtitle: subtitle || null,
  };
}
