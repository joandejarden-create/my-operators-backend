/**
 * Build unified Dealality customer acquisition target list from GTM + conference sources.
 */
import { readFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import {
  MAP_ACQUISITION_SCORE_WEIGHTS,
  MAP_ACQUISITION_PRIORITY_THRESHOLDS,
  VAL_ACQUISITION_SEGMENT,
  VAL_ACQUISITION_STAGE,
} from "./dealality-user-acquisition-config.js";
import { LINKEDIN_PILOT_CONTACTS } from "./pilot-target-list-linkedin-contacts.js";

/**
 * @param {string} value
 */
function normalizeKey(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeOrg(value) {
  return normalizeKey(value)
    .replace(/\b(s a de c v|s a b|s a|s r l|srl|sa|inc|llc|ltd|corp|corporation)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} name
 * @param {string} company
 * @param {string} [ownerName]
 */
function personCompanyKey(name, company, ownerName) {
  const org = normalizeOrg(ownerName || company);
  return `${normalizeKey(name)}|${org}`;
}

/**
 * @param {number} score
 */
function priorityFromScore(score) {
  if (score >= MAP_ACQUISITION_PRIORITY_THRESHOLDS.P1) return "P1";
  if (score >= MAP_ACQUISITION_PRIORITY_THRESHOLDS.P2) return "P2";
  if (score >= MAP_ACQUISITION_PRIORITY_THRESHOLDS.P3) return "P3";
  return "P4";
}

/**
 * @param {object} row
 */
function computeAcquisitionScore(row) {
  const w = MAP_ACQUISITION_SCORE_WEIGHTS;
  let score = 0;
  if (row.strikeListMember) score += w.strikeList;
  if (row.outreachReady) score += w.outreachReady;
  if (row.verificationTier === "V1R") score += w.verifiedContactV1R;
  if (row.verificationTier === "V2") score += w.verifiedContactV2;
  if (row.alisCala2026Attendee) score += w.alisCala2026Attendee;
  if (row.brandingIntent === "high") score += w.brandingIntentHigh;
  if (row.brandingIntent === "medium") score += w.brandingIntentMedium;
  if (Number(row.calaPropertyCount || 0) >= 3) score += w.calaPortfolio3Plus;
  if (row.developerPipeline) score += w.developerPipeline;
  if (row.linkedinPilotTier === "A") score += w.linkedinPilotTierA;
  if (row.acquisitionSegment === VAL_ACQUISITION_SEGMENT.capitalAdvisor) {
    score += w.capitalAdvisorCalaFocus;
  }
  return Math.min(100, score);
}

/**
 * @param {object} params
 * @param {string} params.root
 */
export function buildDealalityUserAcquisitionTargets({ root }) {
  const reports = join(root, "reports");
  const enrichmentDir = join(root, "data", "internal", "gtm-registry-enrichments");
  const prospectSeedsPath = join(
    root,
    "data",
    "internal",
    "dealality-user-acquisition-targets",
    "prospect-seeds.json"
  );

  /** @type {Map<string, object>} */
  const byKey = new Map();

  /**
   * @param {object} row
   * @param {"merge"|"replace"} mode
   */
  function upsert(row, mode = "merge") {
    const key =
      row.dedupeKey ||
      personCompanyKey(row.contactName || row.fullName, row.company, row.ownerName);
    if (!key || key === "|") return;
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, { ...row, dedupeKey: key });
      return;
    }
    if (mode === "replace") {
      byKey.set(key, { ...existing, ...row, dedupeKey: key });
      return;
    }
    byKey.set(key, {
      ...existing,
      ...row,
      sourceTracks: [...new Set([...(existing.sourceTracks || []), ...(row.sourceTracks || [])])],
      signals: [...new Set([...(existing.signals || []), ...(row.signals || [])])],
      alisCala2026Attendee: Boolean(
        row.alisCala2026Attendee || existing.alisCala2026Attendee
      ),
      strikeListMember: Boolean(row.strikeListMember || existing.strikeListMember),
      outreachReady: Boolean(row.outreachReady || existing.outreachReady),
      developerPipeline: Boolean(row.developerPipeline || existing.developerPipeline),
      prospectOnly: Boolean(row.prospectOnly || existing.prospectOnly),
      dedupeKey: key,
    });
  }

  function loadJson(path) {
    if (!existsSync(path)) return null;
    return JSON.parse(readFileSync(path, "utf8"));
  }

  // Strike list owners
  const strike = loadJson(join(reports, "gtm-owner-strike-list.json"));
  for (const item of strike?.strikeList || []) {
    upsert({
      sourceTracks: ["strike_list"],
      signals: ["strike_list_qualified"],
      ownerName: item.ownerName,
      ownerTargetId: item.id,
      company: item.ownerName,
      contactName: item.primaryContactName || "",
      contactEmail: item.primaryContactEmail || "",
      priorityTier: item.priorityTier,
      acquisitionSegment: VAL_ACQUISITION_SEGMENT.ownerOperator,
      acquisitionStage: item.hasVerifiedContact
        ? VAL_ACQUISITION_STAGE.outreachReady
        : VAL_ACQUISITION_STAGE.researched,
      strikeListMember: true,
      outreachReady: Boolean(item.hasVerifiedContact),
      calaPropertyCount: item.calaPropertyCount,
      countriesSummary: item.countriesSummary,
      pitchAngle: item.pitchAngle || "Strike-list CALA owner — platform pilot candidate.",
    });
  }

  // Branding / outreach-ready owners
  const branding = loadJson(join(reports, "gtm-branding-decision-targets.json"));
  for (const item of branding?.items || []) {
    if (!item.outreachReady && !item.hasVerifiedContact) continue;
    upsert({
      sourceTracks: ["branding_decision_targets"],
      signals: item.dealSignals || [],
      ownerName: item.ownerName,
      ownerTargetId: item.ownerTargetId,
      company: item.ownerName,
      contactName: item.contactName || "",
      contactEmail: item.contactEmail || "",
      contactLinkedIn: item.contactLinkedIn || "",
      verificationTier: item.contactVerificationTier || "",
      priorityTier: item.priorityTier,
      acquisitionSegment:
        item.icpSegment === "asset_owner"
          ? VAL_ACQUISITION_SEGMENT.assetOwner
          : VAL_ACQUISITION_SEGMENT.ownerOperator,
      acquisitionStage: item.outreachReady
        ? VAL_ACQUISITION_STAGE.outreachReady
        : VAL_ACQUISITION_STAGE.researched,
      outreachReady: Boolean(item.outreachReady),
      brandingIntent: item.confidence,
      intentScore: item.intentScore,
      primaryDealTrigger: item.primaryDealTrigger,
      calaPropertyCount: item.calaPropertyCount,
      countriesSummary: item.countriesSummary,
      pitchAngle: item.pitchAngle,
    });
  }

  // ALIS CALA 2026 delegate matches (strike / branding overlap)
  const alis = loadJson(join(reports, "gtm-alis-cala-2026-delegate-crossref.json"));
  for (const match of alis?.attendeeMatches || []) {
    const d = match.delegate || {};
    upsert({
      sourceTracks: ["alis_cala_2026_match"],
      signals: match.matchType || [],
      ownerName: match.matchedOwner,
      ownerTargetId: match.ownerTargetId,
      company: d.company || match.matchedOwner,
      contactName: d.fullName,
      contactTitle: d.title,
      contactWorkPhone: d.workPhone,
      contactMobilePhone: d.mobilePhone,
      country: d.country,
      city: d.city,
      acquisitionSegment: VAL_ACQUISITION_SEGMENT.conferenceWarm,
      acquisitionStage: match.hasVerifiedContact
        ? VAL_ACQUISITION_STAGE.outreachReady
        : VAL_ACQUISITION_STAGE.researched,
      alisCala2026Attendee: true,
      strikeListMember: match.ownerSource === "strike_list",
      outreachReady: Boolean(match.hasVerifiedContact),
      priorityTier: match.priorityTier,
      pitchAngle: `ALIS CALA 2026 attendee — matched to ${match.matchedOwner}.`,
    });
  }

  // ALIS net-new owner-like leads
  const netNewCsv = join(reports, "gtm-alis-cala-2026-net-new-owner-leads.csv");
  if (existsSync(netNewCsv)) {
    const lines = readFileSync(netNewCsv, "utf8").split(/\r?\n/).filter(Boolean);
    const header = lines[0].split(",");
    const idx = Object.fromEntries(header.map((h, i) => [h.trim(), i]));
    for (const line of lines.slice(1)) {
      const cols = line.match(/("([^"]|"")*"|[^,]*)/g)?.map((c) =>
        c.replace(/^"|"$/g, "").replace(/""/g, '"')
      ) || [];
      const fullName = cols[idx.fullName] || "";
      const company = cols[idx.company] || "";
      const country = cols[idx.country] || "";
      upsert({
        sourceTracks: ["alis_cala_2026_net_new"],
        signals: [cols[idx.leadReason] || "net_new_owner_lead"],
        ownerName: company,
        company,
        contactName: fullName,
        contactTitle: cols[idx.title] || "",
        country,
        city: cols[idx.city] || "",
        acquisitionSegment: VAL_ACQUISITION_SEGMENT.conferenceWarm,
        acquisitionStage: VAL_ACQUISITION_STAGE.identified,
        alisCala2026Attendee: true,
        pitchAngle:
          "ALIS CALA 2026 net-new owner/developer lead — research + qualify for Dealality ICP.",
      });
    }
  }

  // Registry enrichment files (verified contacts)
  if (existsSync(enrichmentDir)) {
    for (const file of readdirSync(enrichmentDir).filter((f) => f.endsWith(".json"))) {
      try {
        const enrichment = JSON.parse(readFileSync(join(enrichmentDir, file), "utf8"));
        const c = enrichment.contact || {};
        if (!c.name) continue;
        upsert({
          sourceTracks: ["registry_enrichment"],
          signals: [`enrichment:${file}`],
          ownerName: enrichment.ownerName,
          ownerTargetId: enrichment.ownerTargetId || null,
          company: enrichment.registry?.entityName || enrichment.ownerName,
          contactName: c.name,
          contactTitle: c.title,
          contactEmail: c.email || "",
          contactLinkedIn: c.linkedIn || "",
          verificationTier: c.verificationTier,
          country: enrichment.registry?.country || "",
          countriesSummary: enrichment.registry?.country || "",
          acquisitionSegment:
            enrichment.registry?.entityType === "developer_owner"
              ? VAL_ACQUISITION_SEGMENT.developer
              : enrichment.registry?.entityType === "boutique_developer"
                ? VAL_ACQUISITION_SEGMENT.boutiqueOwner
                : VAL_ACQUISITION_SEGMENT.ownerOperator,
          acquisitionStage:
            c.verificationTier === "V1R" || c.verificationTier === "V2"
              ? VAL_ACQUISITION_STAGE.outreachReady
              : VAL_ACQUISITION_STAGE.researched,
          outreachReady: c.verificationTier === "V1R" || c.verificationTier === "V2",
          pitchAngle: enrichment.registry?.lookupNotes?.split("\n")[0] || "",
        });
      } catch {
        /* skip bad file */
      }
    }
  }

  // Manual prospect seeds (non-CoStar developers, capital advisors, etc.)
  const prospects = loadJson(prospectSeedsPath);
  for (const seed of prospects?.prospects || []) {
    upsert(
      {
        sourceTracks: ["prospect_seed"],
        signals: seed.signals || [],
        ownerName: seed.ownerName || seed.company,
        ownerTargetId: seed.ownerTargetId || null,
        company: seed.company,
        contactName: seed.contactName,
        contactTitle: seed.contactTitle,
        contactEmail: seed.contactEmail || "",
        contactLinkedIn: seed.contactLinkedIn || "",
        verificationTier: seed.verificationTier || "",
        country: seed.country,
        acquisitionSegment: seed.acquisitionSegment || VAL_ACQUISITION_SEGMENT.developer,
        acquisitionStage: seed.acquisitionStage || VAL_ACQUISITION_STAGE.researched,
        alisCala2026Attendee: Boolean(seed.alisCala2026Attendee),
        developerPipeline: Boolean(seed.developerPipeline),
        outreachReady: Boolean(seed.outreachReady),
        pitchAngle: seed.pitchAngle,
        prospectOnly: Boolean(seed.prospectOnly),
      },
      "replace"
    );
  }

  // LinkedIn pilot warm network
  for (const lc of LINKEDIN_PILOT_CONTACTS) {
    if (lc.tier === "E") continue;
    upsert({
      sourceTracks: ["linkedin_pilot"],
      signals: [`linkedin_tier_${lc.tier}`],
      company: lc.company || "",
      contactName: lc.name,
      contactTitle: lc.role || "",
      acquisitionSegment:
        lc.segment?.includes("Capital")
          ? VAL_ACQUISITION_SEGMENT.capitalAdvisor
          : lc.segment?.includes("Advisor")
            ? VAL_ACQUISITION_SEGMENT.capitalAdvisor
            : VAL_ACQUISITION_SEGMENT.linkedinWarm,
      acquisitionStage: VAL_ACQUISITION_STAGE.identified,
      linkedinPilotTier: lc.tier,
      linkedinPilotPriority: lc.priority,
      pitchAngle: `LinkedIn connection — ${lc.segment || "warm intro"}.`,
    });
  }

  /** @type {object[]} */
  const items = [];
  for (const row of byKey.values()) {
    const acquisitionScore = computeAcquisitionScore(row);
    items.push({
      ...row,
      acquisitionScore,
      acquisitionPriority: priorityFromScore(acquisitionScore),
    });
  }

  items.sort((a, b) => {
    const pa = a.acquisitionPriority || "P4";
    const pb = b.acquisitionPriority || "P4";
    if (pa !== pb) return pa.localeCompare(pb);
    return (b.acquisitionScore || 0) - (a.acquisitionScore || 0);
  });

  const byPriority = {};
  for (const item of items) {
    const p = item.acquisitionPriority || "P4";
    byPriority[p] = (byPriority[p] || 0) + 1;
  }

  return {
    generatedAt: new Date().toISOString(),
    summary: {
      totalTargets: items.length,
      byPriority,
      alisAttendees: items.filter((i) => i.alisCala2026Attendee).length,
      outreachReady: items.filter((i) => i.outreachReady).length,
      strikeListOverlap: items.filter((i) => i.strikeListMember).length,
      prospectOnly: items.filter((i) => i.prospectOnly).length,
    },
    items,
  };
}
