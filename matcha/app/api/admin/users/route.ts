import { NextResponse } from "next/server";
import { db } from "@/lib/db";
import { requireAdmin } from "@/lib/auth";
import { serializeProfile } from "@/lib/serializers";
import type { AdminUserResponse } from "@/types";

export async function GET() {
  try {
    await requireAdmin();

    const users = await db.user.findMany({
      include: { profile: true },
      orderBy: { createdAt: "desc" },
    });

    const data: AdminUserResponse[] = users.map((user) => ({
      id: user.id,
      email: user.email,
      role: user.role,
      createdAt: user.createdAt.toISOString(),
      profile: user.profile ? serializeProfile(user.profile) : null,
    }));

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
    console.error("[admin/users]", error);
    return NextResponse.json(
      { success: false, error: "Failed to load users" },
      { status: 500 }
    );
  }
}
