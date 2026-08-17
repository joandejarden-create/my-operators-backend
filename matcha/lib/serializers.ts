import type { Profile } from "@prisma/client";
import type { ProfileResponse } from "@/types";
import { parseInterests } from "@/lib/utils";

export function serializeProfile(profile: Profile): ProfileResponse {
  return {
    id: profile.id,
    userId: profile.userId,
    displayName: profile.displayName,
    bio: profile.bio,
    age: profile.age,
    location: profile.location,
    interests: parseInterests(profile.interests),
    photoUrl: profile.photoUrl,
    createdAt: profile.createdAt.toISOString(),
    updatedAt: profile.updatedAt.toISOString(),
  };
}
