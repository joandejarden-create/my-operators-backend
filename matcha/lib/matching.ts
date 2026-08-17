import type { Profile } from "@prisma/client";

interface MatchableProfile {
  userId: string;
  age: number | null;
  location: string | null;
  interests: string | null;
}

function parseInterestSet(interests: string | null | undefined): Set<string> {
  if (!interests) return new Set();
  return new Set(
    interests
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean)
  );
}

function interestOverlapScore(
  a: Set<string>,
  b: Set<string>
): number {
  if (a.size === 0 || b.size === 0) return 0;
  let overlap = 0;
  for (const item of a) {
    if (b.has(item)) overlap++;
  }
  const union = new Set([...a, ...b]).size;
  return union > 0 ? overlap / union : 0;
}

function ageCompatibilityScore(
  ageA: number | null,
  ageB: number | null
): number {
  if (ageA == null || ageB == null) return 0.5;
  const diff = Math.abs(ageA - ageB);
  if (diff <= 3) return 1;
  if (diff <= 7) return 0.75;
  if (diff <= 12) return 0.5;
  return 0.25;
}

function locationCompatibilityScore(
  locA: string | null,
  locB: string | null
): number {
  if (!locA || !locB) return 0.5;
  const normalizedA = locA.trim().toLowerCase();
  const normalizedB = locB.trim().toLowerCase();
  if (normalizedA === normalizedB) return 1;
  const cityA = normalizedA.split(",")[0]?.trim();
  const cityB = normalizedB.split(",")[0]?.trim();
  if (cityA && cityB && cityA === cityB) return 0.8;
  return 0.3;
}

export const MATCH_WEIGHTS = {
  interests: 0.5,
  age: 0.25,
  location: 0.25,
} as const;

export function calculateMatchScore(
  profileA: MatchableProfile,
  profileB: MatchableProfile
): number {
  const interestsA = parseInterestSet(profileA.interests);
  const interestsB = parseInterestSet(profileB.interests);

  const interestScore = interestOverlapScore(interestsA, interestsB);
  const ageScore = ageCompatibilityScore(profileA.age, profileB.age);
  const locationScore = locationCompatibilityScore(
    profileA.location,
    profileB.location
  );

  const total =
    interestScore * MATCH_WEIGHTS.interests +
    ageScore * MATCH_WEIGHTS.age +
    locationScore * MATCH_WEIGHTS.location;

  return Math.round(total * 100) / 100;
}

export function getMatchBreakdown(
  profileA: MatchableProfile,
  profileB: MatchableProfile
) {
  const interestsA = parseInterestSet(profileA.interests);
  const interestsB = parseInterestSet(profileB.interests);

  const interestScore = interestOverlapScore(interestsA, interestsB);
  const ageScore = ageCompatibilityScore(profileA.age, profileB.age);
  const locationScore = locationCompatibilityScore(
    profileA.location,
    profileB.location
  );

  const sharedInterests = [...interestsA].filter((i) => interestsB.has(i));

  return {
    total: calculateMatchScore(profileA, profileB),
    breakdown: {
      interests: {
        score: interestScore,
        weight: MATCH_WEIGHTS.interests,
        shared: sharedInterests,
      },
      age: { score: ageScore, weight: MATCH_WEIGHTS.age },
      location: { score: locationScore, weight: MATCH_WEIGHTS.location },
    },
    confidence:
      profileA.interests && profileB.interests && profileA.age && profileB.age
        ? "high"
        : profileA.interests || profileB.interests
          ? "medium"
          : "low",
  };
}

export async function findPotentialMatches(
  currentProfile: Profile,
  allProfiles: Profile[],
  existingMatchUserIds: Set<string>,
  limit = 20
) {
  const candidates = allProfiles
    .filter(
      (p) =>
        p.userId !== currentProfile.userId &&
        !existingMatchUserIds.has(p.userId)
    )
    .map((p) => ({
      profile: p,
      ...getMatchBreakdown(currentProfile, p),
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, limit);

  return candidates;
}
