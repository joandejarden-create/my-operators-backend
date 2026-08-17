import { NextResponse } from "next/server";
import { db } from "@/lib/db";
import { requireAdmin } from "@/lib/auth";
import type { AdminStatsResponse } from "@/types";

export async function GET() {
  try {
    await requireAdmin();

    const [totalUsers, totalMatches, totalMessages, usersWithProfiles] =
      await Promise.all([
        db.user.count(),
        db.match.count(),
        db.message.count(),
        db.profile.count(),
      ]);

    const data: AdminStatsResponse = {
      totalUsers,
      totalMatches,
      totalMessages,
      usersWithProfiles,
    };

    return NextResponse.json({ success: true, data });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    if (message === "Unauthorized") {
      return NextResponse.json(
        { success: false, error: "Not authenticated" },
        { status: 401 }
      );
    }
    if (message === "Forbidden") {
      return NextResponse.json(
        { success: false, error: "Admin access required" },
        { status: 403 }
      );
    }
    console.error("[admin/stats]", error);
    return NextResponse.json(
      { success: false, error: "Failed to load stats" },
      { status: 500 }
    );
  }
}
