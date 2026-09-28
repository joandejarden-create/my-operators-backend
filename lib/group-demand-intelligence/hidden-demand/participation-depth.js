/**
 * Phase 1 — Participation depth from booth/sponsor/speaker/staff signals.
 */

import { PARTICIPATION_DEPTH } from "./v3-constants.js";

/**
 * @returns {{
 *   participationConfirmed: 'YES'|'NO',
 *   participationDepth: string,
 *   signals: object,
 *   score: number
 * }}
 */
export function classifyParticipationDepth(entity = {}, deepenText = "") {
  const blob = `${entity.evidenceSnippet || ""} ${entity.category || ""} ${entity.sponsorLevel || ""} ${deepenText || ""}`;
  const role = String(entity.participationRole || entity.participationType || "").toUpperCase();

  const signals = {
    boothNumber: entity.boothNumber || extractBooth(blob),
    boothSize: extractBoothSize(blob),
    sponsorshipTier: entity.sponsorLevel || extractSponsorTier(blob),
    speaking: /\b(speaker|keynote|panelist|presenter|session)\b/i.test(blob),
    speakerCount: countSpeakers(blob),
    namedAttendees: countNamedStaff(blob),
    hostedEvent: /\b(hosted|reception|dinner|hospitality suite|activation|demo)\b/i.test(blob),
    exhibitorConfirmed:
      role === "EXHIBITOR" ||
      role === "SPONSOR" ||
      /exhibitor|booth|sponsor/i.test(blob) ||
      Boolean(entity.sourceType?.includes?.("EXHIBITOR") || /EXHIBITOR|SPONSOR/i.test(entity.sourceType || "")),
  };

  let score = 0;
  if (signals.exhibitorConfirmed) score += 20;
  if (signals.boothNumber) score += 15;
  if (signals.boothSize) {
    const sq = signals.boothSize;
    if (sq >= 400) score += 35;
    else if (sq >= 200) score += 25;
    else if (sq >= 100) score += 15;
    else score += 8;
  }
  if (signals.sponsorshipTier) {
    if (/title|presenting|platinum|diamond|anchor/i.test(signals.sponsorshipTier)) score += 40;
    else if (/gold|premier|lead/i.test(signals.sponsorshipTier)) score += 28;
    else if (/silver|bronze|supporting/i.test(signals.sponsorshipTier)) score += 12;
    else score += 10;
  }
  if (signals.speaking) score += 12;
  score += Math.min(20, (signals.speakerCount || 0) * 6);
  score += Math.min(15, (signals.namedAttendees || 0) * 3);
  if (signals.hostedEvent) score += 15;

  let depth = PARTICIPATION_DEPTH.LIGHT;
  if (score >= 70) depth = PARTICIPATION_DEPTH.ANCHOR;
  else if (score >= 45) depth = PARTICIPATION_DEPTH.HEAVY;
  else if (score >= 25) depth = PARTICIPATION_DEPTH.STANDARD;

  return {
    participationConfirmed: signals.exhibitorConfirmed ? "YES" : "NO",
    participationDepth: depth,
    signals,
    score,
  };
}

function extractBooth(blob) {
  const m = String(blob).match(/\bbooth\s*[#:]?\s*([A-Z]?\d{2,5}[A-Z]?)\b/i);
  return m ? m[1] : null;
}

function extractBoothSize(blob) {
  const m = String(blob).match(/\b(\d{2,3})\s*[x×]\s*(\d{2,3})\b/i);
  if (m) return Number(m[1]) * Number(m[2]);
  const sq = String(blob).match(/\b(\d{2,4})\s*(?:sq\.?\s*ft|square\s*feet)\b/i);
  if (sq) return Number(sq[1]);
  return null;
}

function extractSponsorTier(blob) {
  const m = String(blob).match(
    /\b(title|presenting|platinum|diamond|gold|silver|bronze|premier|lead|supporting)\s+(?:sponsor|partner|level)\b/i
  );
  return m ? m[0] : null;
}

function countSpeakers(blob) {
  const matches = String(blob).match(/\b(speaker|keynote|panelist|presenter)\b/gi);
  return matches ? Math.min(8, matches.length) : 0;
}

function countNamedStaff(blob) {
  const matches = String(blob).match(/\b[A-Z][a-z]+\s+[A-Z][a-z]+\b/g);
  return matches ? Math.min(12, matches.length) : 0;
}
