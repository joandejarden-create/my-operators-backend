/**
 * Compact Scaling tile summary — customer "What is happening" only.
 *
 * Authority: tile body = CIB companyAndLocalActivity / whatIsHappening.
 * Never use Opportunity overview (opportunityOverview) or companyBackground
 * as the tile lead. Overview stays drawer section 1 only.
 */
import {
  deriveDisplayCompanyName,
  isScalingOpportunity,
  SCALING_CUSTOMER_CLASSIFICATION_LABEL,
} from "./scaling-display-identity-v1.js";

export const SCALING_TILE_CUSTOMER_SUMMARY_V1 =
  "gdi-scaling-tile-customer-summary-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Phrases that read as internal / repetitive research narration. */
const BANNED_PHRASE_RE =
  /\b(local activity|local growth activity|local expansion\/activity|mexico city-related commercial activity|documented (?:[\w-]+-related )?commercial activity|plausible hotel-demand mechanism|selection signal|dealality selected|appears in scaling inventory)\b/gi;

const OVERVIEW_BOILERPLATE_RE =
  /\b(plausible hotel-demand mechanism|mexico city-related commercial activity|documented [\w-]*related commercial activity|contracting authority, preferred-hotel status)\b/i;

function scrubResearchNarration(s, displayName) {
  let out = text(s);
  if (!out) return "";
  out = out.replace(/Selection signal:\s*/gi, "");
  out = out.replace(/Dealality selected\b[^.]*\./gi, "");
  out = out.replace(/\bScaling Corporate Account\b/g, "corporate account");
  out = out.replace(/\bappears in Scaling inventory\b/gi, "is a corporate account prospect");
  out = out.replace(/^On\s*,\s*/i, "");
  out = out.replace(/\blocal expansion\/activity\b/gi, "commercial expansion");
  out = out.replace(/\blocal commercial activity\b/gi, "commercial expansion");
  out = out.replace(/\b(local activity|local growth activity)\b/gi, "commercial expansion");
  // Drop research-narration wrappers; keep the concrete activity tip that follows.
  out = out.replace(
    /\bOn\s+\d{4}-\d{2}-\d{2},\s*public materials described\s+(?:commercial expansion\s+)?/gi,
    "",
  );
  out = out.replace(/^public materials described\s+(?:commercial expansion\s+)?/i, "");
  out = out.replace(/\bpublic materials described\s+(?:commercial expansion\s+)?/gi, "");
  out = out.replace(/\bDocumented local expansion\s*\/\s*growth activity\s*[—–-]\s*/gi, "");
  // Unwrap curly/straight quoted activity tips; keep the tip text
  out = out.replace(/[“"']([^”"']{12,200})[”"']/g, " $1 ");
  out = out.replace(/^What business:\s*/i, "");
  out = out.replace(/\(\s+/g, "(").replace(/\s+\)/g, ")");
  out = out.replace(/\(\s*([^)]{12,200}?)\s*\)/g, " $1 ");
  out = out.replace(/^commercial expansion\s+/i, "");
  // Strip industry/background lead sentences — tile must start with activity.
  out = out.replace(
    /^(?:an?\s+)?(?:technology\s*\/\s*ai|technology|fintech|software|saas|ai|retail|healthcare|biotech|payments?|enterprise)\s+company\.\s*/i,
    "",
  );
  out = out.replace(
    /^(?:this company|[A-Z][\w.&'’ -]{1,48})\s+is an?\s+(?:[\w/-]+\s+){0,6}company\.\s*/i,
    "",
  );
  // Repair empty date holes left by scrubbing ("on .", "dated .")
  out = out.replace(/\bon\s*\.\s*/gi, " ");
  out = out.replace(/\bdated\s*\.\s*/gi, " ");
  out = out.replace(/\s*Research verification date[^.]*\./gi, " ");
  out = out.replace(/\s*Original public date:\s*\d{4}-\d{2}-\d{2}\.?\s*/gi, " ");
  out = out.replace(/\s{2,}/g, " ").trim();
  if (displayName) {
    const esc = displayName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    out = out.replace(new RegExp(`^${esc}\\s*[—–-]\\s*`, "i"), "");
    out = out.replace(new RegExp(`^${esc}\\s*:\\s*`, "i"), "");
  }
  out = out.replace(BANNED_PHRASE_RE, " ").replace(/\s{2,}/g, " ").replace(/\s+([,.;])/g, "$1").trim();
  if (out && !/^[A-ZÁÉÍÓÚÑ]/.test(out)) {
    out = out.charAt(0).toUpperCase() + out.slice(1);
  }
  return out;
}

/** Ensure tile body names the company (Stage F customer contract). */
function ensureCompanyNamed(body, displayName) {
  const out = text(body);
  const name = text(displayName);
  if (!out || !name) return out;
  if (new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(out)) {
    return out;
  }
  return `${name}: ${out.charAt(0).toLowerCase()}${out.slice(1)}`;
}

function firstSentences(s, maxSentences = 4) {
  const parts = text(s).split(/(?<=\.)\s+/).filter(Boolean);
  return text(parts.slice(0, maxSentences).join(" "));
}

function truncateSoft(s, maxLen = 360) {
  const t = text(s);
  if (t.length <= maxLen) return t;
  const cut = t.slice(0, maxLen);
  const lastStop = Math.max(cut.lastIndexOf(". "), cut.lastIndexOf("; "));
  if (lastStop >= Math.floor(maxLen * 0.45)) {
    return cut.slice(0, lastStop + 1).trim();
  }
  return cut.replace(/\s+\S*$/, "").trim();
}

function looksLikeOverviewBoilerplate(s) {
  return OVERVIEW_BOILERPLATE_RE.test(String(s || ""));
}

/** Weak research stubs — prefer a stronger sibling field instead. */
function looksLikeWeakActivityStub(s) {
  const t = String(s || "");
  if (/exact original event date is unresolved/i.test(t)) return true;
  if (/public materials describe(?:\s+commercial expansion)?[.;]/i.test(t) && t.length < 160) {
    return true;
  }
  if (/^[\w.&'’ -]{1,40}\.\s*Public materials describe/i.test(t)) return true;
  return false;
}

function nearEqual(a, b) {
  const x = text(a).toLowerCase();
  const y = text(b).toLowerCase();
  if (!x || !y) return false;
  if (x === y) return true;
  const n = Math.min(120, x.length, y.length);
  return n >= 60 && x.slice(0, n) === y.slice(0, n);
}

/**
 * Resolve authoritative What-is-happening prose for the tile.
 * Explicitly excludes Opportunity overview.
 */
function resolveWhatIsHappeningSource(opportunity = {}, display = "") {
  const cib =
    opportunity.commercialIntelligenceBrief || opportunity.salesBrief || {};
  const overview = text(cib.opportunityOverview || "");

  // Prefer concrete activity narrative over weak CIB stubs / overview.
  const candidates = [
    opportunity.whatIsHappening,
    opportunity.activityHeadline,
    opportunity.growthHeadline,
    cib.companyAndLocalActivity,
    opportunity.summaryWhat,
    opportunity.datedGrowthEvidence,
    // only if already activity-derived (not overview)
    opportunity.tileCustomerSummary,
  ];

  for (const raw of candidates) {
    if (looksLikeWeakActivityStub(raw) || looksLikeOverviewBoilerplate(raw)) continue;
    const cleaned = scrubResearchNarration(raw, display);
    if (!cleaned || cleaned.length < 40) continue;
    if (overview && nearEqual(cleaned, scrubResearchNarration(overview, display))) {
      continue;
    }
    if (looksLikeOverviewBoilerplate(cleaned) || looksLikeWeakActivityStub(cleaned)) continue;
    // Reject companyBackground / presence-only “who they are” leads that used to
    // masquerade as tile copy (those belong in overview/background, not tile).
    if (
      /^(?:this company|[A-Z][\w.&'’ -]{1,40})\s+(?:is a |is an |maintains a commercial presence)\b/i.test(
        cleaned,
      ) &&
      !/\b(opened|opening|hiring|funding|office|center|launch|expansion|announced|exhibition|training|meeting)\b/i.test(
        cleaned,
      )
    ) {
      continue;
    }
    return ensureCompanyNamed(
      truncateSoft(firstSentences(cleaned, 4), 360),
      display,
    );
  }
  return "";
}

/**
 * Last-resort tile when no usable What-is-happening prose exists.
 * Still must not copy Opportunity overview.
 */
function fallbackActivityTile(opportunity = {}, display = "") {
  const cib =
    opportunity.commercialIntelligenceBrief || opportunity.salesBrief || {};
  const date =
    text(
      cib.evidenceAndUncertainty?.originalActivityDate ||
        opportunity.growthEvidenceDate ||
        opportunity.originalActivityDate ||
        "",
    ) || null;
  const tip = scrubResearchNarration(
    opportunity.activityHeadline ||
      opportunity.growthHeadline ||
      cib.companyAndLocalActivity ||
      "",
    display,
  );
  if (tip && tip.length >= 40 && !looksLikeOverviewBoilerplate(tip)) {
    return ensureCompanyNamed(truncateSoft(firstSentences(tip, 3), 360), display);
  }
  if (date) {
    return `${display} has documented commercial expansion activity dated ${date}. Exact travel patterns and lodging need still need qualification with the account.`;
  }
  return `${display} has documented commercial expansion relevant to this market. Exact travel patterns and lodging need still need qualification with the account.`;
}

/**
 * Build tile body = scrubbed “What is happening” (never Opportunity overview).
 */
export function buildScalingTileCustomerSummary(opportunity = {}) {
  const display =
    text(opportunity.displayCompanyName) ||
    deriveDisplayCompanyName(
      opportunity.organizationName || opportunity.companyName || opportunity.title || "",
    ) ||
    "This company";

  const fromWhat = resolveWhatIsHappeningSource(opportunity, display);
  if (fromWhat) return fromWhat;
  return fallbackActivityTile(opportunity, display);
}

/**
 * Normalize Scaling fields for list/tile DTO so browser tiles match drawer
 * “What is happening” — never Opportunity overview.
 */
export function normalizeScalingListPresentation(opportunity = {}) {
  // Never reclassify Local Catering / non-Scaling segments via a stale
  // "Corporate Account Relationship" type label (Nobu LMC title bug).
  const researchSeg = String(opportunity.researchSegment || "").toLowerCase();
  if (
    researchSeg === "local_meetings_catering" ||
    /^gdi_lmc_/i.test(String(opportunity.id || "")) ||
    /local (meetings|catering)/i.test(String(opportunity.segment || ""))
  ) {
    return opportunity;
  }
  if (!isScalingOpportunity(opportunity) && !/Corporate Account Relationship/i.test(
    String(opportunity.opportunityTypeLabel || ""),
  )) {
    if (!/Scaling Corporate Account|scaling_companies|SCALING_COMPANY/i.test(
      `${opportunity.title || ""} ${opportunity.segment || ""} ${opportunity.demandFamily || ""} ${opportunity.researchSegment || ""}`,
    )) {
      return opportunity;
    }
  }

  const display =
    text(opportunity.displayCompanyName) ||
    deriveDisplayCompanyName(
      opportunity.organizationName || opportunity.title || "",
    );
  const tileSummary = buildScalingTileCustomerSummary({
    ...opportunity,
    displayCompanyName: display,
  });

  return {
    ...opportunity,
    displayCompanyName: display || opportunity.displayCompanyName,
    title: display || opportunity.title,
    displayTitle: display || opportunity.displayTitle,
    segmentDisplayLabel: SCALING_CUSTOMER_CLASSIFICATION_LABEL,
    opportunityTypeLabel: SCALING_CUSTOMER_CLASSIFICATION_LABEL,
    opportunityType:
      opportunity.opportunityType || "CORPORATE_ACCOUNT_RELATIONSHIP",
    // Tile + drawer section 2 share What-is-happening — not overview
    tileCustomerSummary: tileSummary,
    whatIsHappening: tileSummary,
    summaryWhat: tileSummary,
  };
}
