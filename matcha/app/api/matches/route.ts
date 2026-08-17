import { NextResponse } from "next/server";
import { db } from "@/lib/db";
import { getSession } from "@/lib/auth";
import { findPotentialMatches } from "@/lib/matching";
import { serializeProfile } from "@/lib/serializers";
import type { MatchResponse } from "@/types";

export async function GET() {
  try {
    const session = await getSession();
    if (!session) {
      return NextResponse.json(
        { success: false, error: "Not authenticated" },
        { status: 401 }
      );
    }

    const currentProfile = await db.profile.findUnique({
      where: { userId: session.id },
    });

    if (!currentProfile) {
      return NextResponse.json({
        success: true,
        data: [],
        message: "Complete your profile to see matches",
      });
    }

    const allProfiles = await db.profile.findMany({
      where: { userId: { not: session.id } },
    });

    const existingMatches = await db.match.findMany({
      where: {
        OR: [{ user1Id: session.id }, { user2Id: session.id }],
      },
    });

    const existingMatchUserIds = new Set(
      existingMatches.map((m) =>
        m.user1Id === session.id ? m.user2Id : m.user1Id
      )
    );

    const potential = await findPotentialMatches(
      currentProfile,
      allProfiles,
      existingMatchUserIds
    );

    const matchRecords: MatchResponse[] = [];

    for (const candidate of potential) {
      const [user1Id, user2Id] =
        session.id < candidate.profile.userId
          ? [session.id, candidate.profile.userId]
          : [candidate.profile.userId, session.id];

      const match = await db.match.upsert({
        where: {
          user1Id_user2Id: { user1Id, user2Id },
        },
        update: { score: candidate.total },
        create: {
          user1Id,
          user2Id,
          score: candidate.total,
          status: "pending",
        },
      });

      matchRecords.push({
        id: match.id,
        score: match.score,
        status: match.status,
        createdAt: match.createdAt.toISOString(),
        profile: serializeProfile(candidate.profile),
      });
    }

    return NextResponse.json({
      success: true,
      data: matchRecords,
    });
  } catch (error) {
    console.error("[matches/GET]", error);
    return NextResponse.json(
      { success: false, error: "Failed to load matches" },
      { status: 500 }
    );
  }
}
