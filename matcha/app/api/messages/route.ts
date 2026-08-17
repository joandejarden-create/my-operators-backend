import { NextResponse } from "next/server";
import { z } from "zod";
import { db } from "@/lib/db";
import { getSession } from "@/lib/auth";
import type { MessageResponse } from "@/types";

const messageSchema = z.object({
  receiverId: z.string().min(1, "Receiver ID is required"),
  content: z.string().min(1, "Message cannot be empty").max(2000),
});

export async function GET() {
  try {
    const session = await getSession();
    if (!session) {
      return NextResponse.json(
        { success: false, error: "Not authenticated" },
        { status: 401 }
      );
    }

    const messages = await db.message.findMany({
      where: {
        OR: [{ senderId: session.id }, { receiverId: session.id }],
      },
      include: {
        sender: { include: { profile: true } },
        receiver: { include: { profile: true } },
      },
      orderBy: { createdAt: "desc" },
      take: 50,
    });

    const data: MessageResponse[] = messages.map((msg) => ({
      id: msg.id,
      content: msg.content,
      read: msg.read,
      createdAt: msg.createdAt.toISOString(),
      sender: {
        id: msg.sender.id,
        displayName: msg.sender.profile?.displayName ?? msg.sender.email,
        photoUrl: msg.sender.profile?.photoUrl ?? null,
      },
      receiver: {
        id: msg.receiver.id,
        displayName: msg.receiver.profile?.displayName ?? msg.receiver.email,
        photoUrl: msg.receiver.profile?.photoUrl ?? null,
      },
    }));

    return NextResponse.json({ success: true, data });
  } catch (error) {
    console.error("[messages/GET]", error);
    return NextResponse.json(
      { success: false, error: "Failed to load messages" },
      { status: 500 }
    );
  }
}

export async function POST(request: Request) {
  try {
    const session = await getSession();
    if (!session) {
      return NextResponse.json(
        { success: false, error: "Not authenticated" },
        { status: 401 }
      );
    }

    const body = await request.json();
    const parsed = messageSchema.safeParse(body);

    if (!parsed.success) {
      return NextResponse.json(
        { success: false, error: parsed.error.errors[0]?.message ?? "Invalid input" },
        { status: 400 }
      );
    }

    const { receiverId, content } = parsed.data;

    if (receiverId === session.id) {
      return NextResponse.json(
        { success: false, error: "Cannot message yourself" },
        { status: 400 }
      );
    }

    const receiver = await db.user.findUnique({ where: { id: receiverId } });
    if (!receiver) {
      return NextResponse.json(
        { success: false, error: "Receiver not found" },
        { status: 404 }
      );
    }

    const message = await db.message.create({
      data: {
        senderId: session.id,
        receiverId,
        content,
      },
      include: {
        sender: { include: { profile: true } },
        receiver: { include: { profile: true } },
      },
    });

    const data: MessageResponse = {
      id: message.id,
      content: message.content,
      read: message.read,
      createdAt: message.createdAt.toISOString(),
      sender: {
        id: message.sender.id,
        displayName:
          message.sender.profile?.displayName ?? message.sender.email,
        photoUrl: message.sender.profile?.photoUrl ?? null,
      },
      receiver: {
        id: message.receiver.id,
        displayName:
          message.receiver.profile?.displayName ?? message.receiver.email,
        photoUrl: message.receiver.profile?.photoUrl ?? null,
      },
    };

    return NextResponse.json({ success: true, data });
  } catch (error) {
    console.error("[messages/POST]", error);
    return NextResponse.json(
      { success: false, error: "Failed to send message" },
      { status: 500 }
    );
  }
}
