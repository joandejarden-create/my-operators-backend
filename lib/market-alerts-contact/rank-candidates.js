/**
 * Rank Surfe people-search candidates for Market Alerts contact canary.
 * Prefer commercial decision-makers; reject marketing/HR/junior/wrong-company.
 */

export const CONTACT_CANDIDATE_REJECTED_ROLE_MISMATCH = "CONTACT_CANDIDATE_REJECTED_ROLE_MISMATCH";
export const CONTACT_CANDIDATE_REJECTED_COMPANY_MISMATCH = "CONTACT_CANDIDATE_REJECTED_COMPANY_MISMATCH";
export const CONTACT_CANDIDATE_REJECTED_JUNIOR = "CONTACT_CANDIDATE_REJECTED_JUNIOR";
export const CONTACT_CANDIDATE_REJECTED_FORMER = "CONTACT_CANDIDATE_REJECTED_FORMER";

const REJECT_TITLE_RE =
  /\b(?:marketing|communications?|pr\b|public\s+relations|human\s+resources|\bhr\b|recruiter|talent|sales|revenue\s+(?:strategy|management)|business\s+development\s+rep|receptionist|coordinator|intern|assistant|housekeeping|front\s+desk|reservations|chef|spa|general\s+counsel|counsel)\b/i;

const PREFER_TITLE_RE =
  /\b(?:founder|co-?founder|owner|principal|partner|president|ceo|chief\s+executive|svp|evp|vp|vice\s+president|head\s+of|managing\s+director|cio|chief\s+investment|cfo|asset\s+manager|development|project\s+(?:director|executive|lead)|general\s+manager|pre-?opening)\b/i;

const JUNIOR_RE = /\b(?:junior|associate|coordinator|assistant|intern|analyst)\b/i;
const FORMER_RE = /\b(?:former|ex-|previously|retired)\b/i;

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function companyMatch(candidateCompany, targetCompany) {
  const a = norm(candidateCompany);
  const b = norm(targetCompany);
  if (!a || !b) return { match: false, score: 0 };
  if (a === b) return { match: true, score: 3 };
  if (a.includes(b) || b.includes(a)) return { match: true, score: 2 };
  const aTokens = new Set(a.split(" ").filter((t) => t.length > 2));
  const bTokens = b.split(" ").filter((t) => t.length > 2);
  const overlap = bTokens.filter((t) => aTokens.has(t)).length;
  if (overlap >= 2) return { match: true, score: 1 };
  if (overlap === 1 && bTokens.length <= 2) return { match: true, score: 1 };
  return { match: false, score: 0 };
}

function roleMatchScore(title, targetRoles = [], surfeTitles = []) {
  const t = norm(title);
  if (!t) return 0;
  let score = 0;
  if (PREFER_TITLE_RE.test(title)) score += 4;
  for (const role of targetRoles) {
    const r = norm(role);
    if (r && t.includes(r.split(" ")[0])) score += 2;
    if (/development/i.test(role) && /development/i.test(title)) score += 3;
    if (/principal|owner|founder/i.test(role) && /principal|owner|founder|ceo|president/i.test(title)) {
      score += 4;
    }
    if (/asset/i.test(role) && /asset/i.test(title)) score += 3;
    if (/invest/i.test(role) && /invest|cio/i.test(title)) score += 3;
  }
  for (const st of surfeTitles) {
    if (t.includes(norm(st))) score += 3;
  }
  return score;
}

/**
 * @param {object[]} people Surfe search people
 * @param {{ targetCompany?: string, targetRoles?: string[], surfeTitles?: string[] }} opts
 */
export function rankContactCandidates(people = [], opts = {}) {
  const targetCompany = opts.targetCompany || "";
  const targetRoles = opts.targetRoles || [];
  const surfeTitles = opts.surfeTitles || [];
  const accepted = [];
  const rejected = [];

  for (const p of people || []) {
    const title = p.jobTitle || p.title || "";
    const company = p.companyName || p.company || "";
    const name = p.personName || p.fullName || "";
    const cm = companyMatch(company, targetCompany);

    if (FORMER_RE.test(title) || FORMER_RE.test(String(p.employmentStatus || ""))) {
      rejected.push({
        personName: name,
        jobTitle: title || null,
        companyName: company || null,
        linkedinUrl: p.linkedinUrl || null,
        stakeholderRoleMatch: false,
        companyMatch: cm.match,
        geographyMatch: p.country || null,
        decision: "rejected",
        reason: CONTACT_CANDIDATE_REJECTED_FORMER,
      });
      continue;
    }

    if (!cm.match) {
      rejected.push({
        personName: name,
        jobTitle: title || null,
        companyName: company || null,
        linkedinUrl: p.linkedinUrl || null,
        stakeholderRoleMatch: false,
        companyMatch: false,
        geographyMatch: p.country || null,
        decision: "rejected",
        reason: CONTACT_CANDIDATE_REJECTED_COMPANY_MISMATCH,
      });
      continue;
    }

    if (
      REJECT_TITLE_RE.test(title) &&
      !/\b(?:development|investment|asset|acqui(?:sition)?|principal|founder|co-?founder|owner|ceo|chief\s+executive)\b/i.test(
        title
      ) &&
      !/(?<![Vv]ice\s)\bpresident\b/i.test(title)
    ) {
      rejected.push({
        personName: name,
        jobTitle: title || null,
        companyName: company || null,
        linkedinUrl: p.linkedinUrl || null,
        stakeholderRoleMatch: false,
        companyMatch: true,
        geographyMatch: p.country || null,
        decision: "rejected",
        reason: CONTACT_CANDIDATE_REJECTED_ROLE_MISMATCH,
      });
      continue;
    }

    if (JUNIOR_RE.test(title) && !/\b(?:ceo|president|founder|principal|svp|vp|head)\b/i.test(title)) {
      rejected.push({
        personName: name,
        jobTitle: title || null,
        companyName: company || null,
        linkedinUrl: p.linkedinUrl || null,
        stakeholderRoleMatch: false,
        companyMatch: true,
        geographyMatch: p.country || null,
        decision: "rejected",
        reason: CONTACT_CANDIDATE_REJECTED_JUNIOR,
      });
      continue;
    }

    const roleScore = roleMatchScore(title, targetRoles, surfeTitles);
    if (roleScore < 2 && REJECT_TITLE_RE.test(title)) {
      rejected.push({
        personName: name,
        jobTitle: title || null,
        companyName: company || null,
        linkedinUrl: p.linkedinUrl || null,
        stakeholderRoleMatch: false,
        companyMatch: true,
        geographyMatch: p.country || null,
        decision: "rejected",
        reason: CONTACT_CANDIDATE_REJECTED_ROLE_MISMATCH,
      });
      continue;
    }

    accepted.push({
      ...p,
      personName: name,
      jobTitle: title || null,
      companyName: company || null,
      _rankScore: roleScore + cm.score * 2,
      _companyMatch: true,
      _roleMatch: roleScore >= 2,
    });
  }

  accepted.sort((a, b) => (b._rankScore || 0) - (a._rankScore || 0));

  // If nobody preferred, still allow best company-matched non-rejected with weak role
  const selected = accepted[0] || null;
  const rankedPreview = [
    ...accepted.map((p, idx) => ({
      personName: p.personName,
      jobTitle: p.jobTitle,
      companyName: p.companyName,
      linkedinUrl: p.linkedinUrl || null,
      stakeholderRoleMatch: p._roleMatch === true,
      companyMatch: true,
      geographyMatch: p.country || null,
      decision: idx === 0 ? "accepted" : "not_selected_lower_rank",
      reason: idx === 0 ? "Best Dealality role relevance among company-matched candidates" : "Lower role relevance than selected candidate",
      rankScore: p._rankScore,
    })),
    ...rejected,
  ];

  return {
    selected,
    accepted,
    rejected,
    rankedPreview,
    falsePositivesRejected: rejected.length,
  };
}
