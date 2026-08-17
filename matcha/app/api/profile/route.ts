import { NextResponse } from "next/server";
import { db } from "@/lib/db";
import { getSession } from "@/lib/auth";
import { profileSchema } from "@/lib/validations/profile";
import { serializeProfile } from "@/lib/serializers";
import { serializeInterests } from "@/lib/utils";

export async function GET() {
  try {
    const session = await getSession();
    if (!session) {
      return NextResponse.json(
        { success: false, error: "Not authenticated" },
        { status: 401 }
      );
    }

    const profile = await db.profile.findUnique({
      where: { userId: session.id },
    });

    if (!profile) {
      return NextResponse.json({
        success: true,
        data: null,
      });
    }

    return NextResponse.json({
      success: true,
      data: serializeProfile(profile),
    });
  } catch (error) {
    console.error("[profile/GET]", error);
    return NextResponse.json(
      { success: false, error: "Failed to load profile" },
      { status: 500 }
    );
  }
}

export async function PUT(request: Request) {
  try {
    const session = await getSession();
    if (!session) {
      return NextResponse.json(
        { success: false, error: "Not authenticated" },
        { status: 401 }
      );
    }

    const body = await request.json();
    const parsed = profileSchema.safeParse(body);

    if (!parsed.success) {
      return NextResponse.json(
        { success: false, error: parsed.error.errors[0]?.message ?? "Invalid input" },
        { status: 400 }
      );
    }

    const { displayName, bio, age, location, interests, photoUrl } =
      parsed.data;

    const profile = await db.profile.upsert({
      where: { userId: session.id },
      update: {
        displayName,
        bio: bio ?? null,
        age: age ?? null,
        location: location ?? null,
        interests: interests ? serializeInterests(interests) : null,
        photoUrl: photoUrl || null,
      },
      create: {
        userId: session.id,
        displayName,
        bio: bio ?? null,
        age: age ?? null,
        location: location ?? null,
        interests: interests ? serializeInterests(interests) : null,
        photoUrl: photoUrl || null,
      },
    });

    return NextResponse.json({
      success: true,
      data: serializeProfile(profile),
    });
  } catch (error) {
    console.error("[profile/PUT]", error);
    return NextResponse.json(
      { success: false, error: "Failed to update profile" },
      { status: 500 }
    );
  }
}
