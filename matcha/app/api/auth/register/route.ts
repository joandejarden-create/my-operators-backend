import { NextResponse } from "next/server";
import { db } from "@/lib/db";
import {
  hashPassword,
  createToken,
  setAuthCookie,
} from "@/lib/auth";
import { registerSchema } from "@/lib/validations/auth";
import { serializeProfile } from "@/lib/serializers";

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const parsed = registerSchema.safeParse(body);

    if (!parsed.success) {
      return NextResponse.json(
        { success: false, error: parsed.error.errors[0]?.message ?? "Invalid input" },
        { status: 400 }
      );
    }

    const { email, password, displayName } = parsed.data;

    const existing = await db.user.findUnique({ where: { email } });
    if (existing) {
      return NextResponse.json(
        { success: false, error: "Email already registered" },
        { status: 409 }
      );
    }

    const hashedPassword = await hashPassword(password);

    const user = await db.user.create({
      data: {
        email,
        password: hashedPassword,
        profile: {
          create: { displayName },
        },
      },
      include: { profile: true },
    });

    const token = await createToken({
      id: user.id,
      email: user.email,
      role: user.role,
    });
    await setAuthCookie(token);

    return NextResponse.json({
      success: true,
      data: {
        id: user.id,
        email: user.email,
        role: user.role,
        profile: user.profile ? serializeProfile(user.profile) : null,
      },
    });
  } catch (error) {
    console.error("[auth/register]", error);
    return NextResponse.json(
      { success: false, error: "Registration failed" },
      { status: 500 }
    );
  }
}
