/**
 * Governed property rank extraction from AI responses.
 * Does NOT infer rank from ordinary prose order or first textual mention.
 */

function buildNameVariants(profile) {
  const variants = [profile.name];
  if (profile.name.includes("&")) variants.push(profile.name.replace("&", "and"));
  const words = profile.name.split(/\s+/);
  if (words.length > 2) {
    variants.push(words.slice(0, 2).join(" "));
  }
  if (words.length > 3) {
    variants.push(words.slice(0, 3).join(" "));
  }
  return [...new Set(variants.filter(Boolean))];
}

function findMentionIndex(text, variants) {
  const lower = text.toLowerCase();
  let best = null;
  for (const variant of variants) {
    const idx = lower.indexOf(variant.toLowerCase());
    if (idx !== -1 && (best === null || idx < best.idx)) {
      best = { idx, variant };
    }
  }
  return best;
}

function lineHasNumberedRank(line) {
  return /^\s*(\d+)[\.\)]\s+/.test(line);
}

function lineHasBulletedRank(line) {
  return /^\s*[-*•]\s+/.test(line);
}

function extractNumberedRankBeforeIndex(response, mentionIndex) {
  const before = response.slice(0, mentionIndex);
  const lines = before.split("\n");
  let lastNumberedRank = null;
  for (const line of lines) {
    const m = line.match(/^\s*(\d+)[\.\)]\s+/);
    if (m) lastNumberedRank = parseInt(m[1], 10);
  }
  if (lastNumberedRank !== null) return { position: lastNumberedRank, rankSource: "numbered_list", confidence: "high" };

  const inline = before.match(/(\d+)\.\s*$/);
  if (inline) return { position: parseInt(inline[1], 10), rankSource: "numbered_list", confidence: "high" };
  return null;
}

function extractBulletedRank(response, mentionIndex) {
  const before = response.slice(0, mentionIndex);
  const lines = before.split("\n");
  let bulletRank = 0;
  for (const line of lines) {
    if (lineHasBulletedRank(line)) bulletRank += 1;
  }
  const currentLineStart = before.lastIndexOf("\n") + 1;
  const currentLine = response.slice(currentLineStart, mentionIndex + 200);
  if (bulletRank > 0 && lineHasBulletedRank(currentLine.split("\n")[0] || "")) {
    return { position: bulletRank, rankSource: "bulleted_list", confidence: "medium" };
  }
  return null;
}

function extractTableRank(response, mentionIndex, variants) {
  const lines = response.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!variants.some((v) => line.toLowerCase().includes(v.toLowerCase()))) continue;
    const cells = line.split("|").map((c) => c.trim()).filter(Boolean);
    if (cells.length >= 2) {
      const rankCell = cells[0];
      const rankNum = parseInt(rankCell, 10);
      if (Number.isFinite(rankNum) && rankNum > 0 && rankNum <= 20) {
        return { position: rankNum, rankSource: "table", confidence: "high" };
      }
    }
  }
  return null;
}

function extractExplicitFirstChoice(response, variants) {
  const lower = response.toLowerCase();
  const patterns = [
    /(?:top choice|first choice|best overall|#1 pick|number one pick)[:\s]+([^\n.]{0,120})/i,
    /(?:my top recommendation)[:\s]+([^\n.]{0,120})/i,
  ];
  for (const pattern of patterns) {
    const m = response.match(pattern);
    if (!m) continue;
    const snippet = m[1].toLowerCase();
    if (variants.some((v) => snippet.includes(v.toLowerCase()))) {
      return { position: 1, rankSource: "explicit_first", confidence: "medium" };
    }
  }
  return null;
}

/**
 * @returns {{
 *   mentioned: boolean,
 *   position: number|null,
 *   rankEligible: boolean,
 *   rankSource: string|null,
 *   positionConfidence: 'high'|'medium'|'low'|null,
 *   context: string|null
 * }}
 */
export function extractPropertyRank(response, propertyProfile) {
  if (!response) {
    return { mentioned: false, position: null, rankEligible: false, rankSource: null, positionConfidence: null, context: null };
  }

  const variants = buildNameVariants(propertyProfile);
  const hit = findMentionIndex(response, variants);
  if (!hit) {
    return { mentioned: false, position: null, rankEligible: false, rankSource: null, positionConfidence: null, context: null };
  }

  const contextStart = Math.max(0, hit.idx - 50);
  const contextEnd = Math.min(response.length, hit.idx + hit.variant.length + 100);
  const context = response.slice(contextStart, contextEnd).trim();

  const ranked =
    extractNumberedRankBeforeIndex(response, hit.idx) ||
    extractTableRank(response, hit.idx, variants) ||
    extractBulletedRank(response, hit.idx) ||
    extractExplicitFirstChoice(response, variants);

  if (ranked) {
    return {
      mentioned: true,
      position: ranked.position,
      rankEligible: true,
      rankSource: ranked.rankSource,
      positionConfidence: ranked.confidence,
      context,
    };
  }

  return {
    mentioned: true,
    position: null,
    rankEligible: false,
    rankSource: null,
    positionConfidence: null,
    context,
  };
}

export function classifyPositionFormat(response) {
  if (!response) return "empty";
  if (/^\s*\d+[\.\)]\s+/m.test(response)) return "numbered_list";
  if (/^\s*[-*•]\s+/m.test(response)) return "bulleted_list";
  if (/\|.+\|/.test(response)) return "table";
  if (/top choice|first choice|best overall|#1 pick/i.test(response)) return "explicit_first";
  return "prose";
}
