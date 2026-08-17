"use client";

import { useEffect, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { formatRelativeTime } from "@/lib/utils";
import type { MessageResponse } from "@/types";

export default function MessagesPage() {
  const [messages, setMessages] = useState<MessageResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [newMessage, setNewMessage] = useState({ receiverId: "", content: "" });

  async function loadMessages() {
    try {
      const res = await fetch("/api/messages");
      if (!res.ok) {
        setError("Failed to load messages");
        return;
      }
      const { data } = await res.json();
      setMessages(data ?? []);
      setError(null);
    } catch {
      setError("Failed to load messages");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadMessages();
  }, []);

  async function handleSend(e: React.FormEvent) {
    e.preventDefault();
    if (!newMessage.receiverId || !newMessage.content.trim()) {
      toast.error("Receiver ID and message content are required");
      return;
    }
    setSending(true);
    try {
      const res = await fetch("/api/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(newMessage),
      });
      const data = await res.json();
      if (!res.ok) {
        toast.error(data.error || "Failed to send message");
        return;
      }
      toast.success("Message sent!");
      setNewMessage({ receiverId: "", content: "" });
      await loadMessages();
    } catch {
      toast.error("Something went wrong");
    } finally {
      setSending(false);
    }
  }

  if (loading) {
    return (
      <div className="mx-auto max-w-3xl space-y-4">
        <div className="h-8 w-48 animate-pulse rounded bg-muted" />
        <div className="h-64 animate-pulse rounded-lg bg-muted" />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Messages</h1>
        <p className="text-muted-foreground">
          Your conversations with matches
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Send a message</CardTitle>
          <CardDescription>
            Enter a user ID from your matches to start a conversation
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSend} className="space-y-3">
            <Input
              placeholder="Receiver user ID"
              value={newMessage.receiverId}
              onChange={(e) =>
                setNewMessage((m) => ({ ...m, receiverId: e.target.value }))
              }
            />
            <Input
              placeholder="Your message..."
              value={newMessage.content}
              onChange={(e) =>
                setNewMessage((m) => ({ ...m, content: e.target.value }))
              }
            />
            <Button type="submit" disabled={sending}>
              {sending ? "Sending..." : "Send message"}
            </Button>
          </form>
        </CardContent>
      </Card>

      {error ? (
        <Card>
          <CardContent className="pt-6">
            <p className="text-destructive">{error}</p>
          </CardContent>
        </Card>
      ) : messages.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            No messages yet. Send your first message to a match!
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {messages.map((msg) => (
            <Card key={msg.id}>
              <CardContent className="flex gap-3 pt-4">
                <Avatar className="h-8 w-8">
                  <AvatarFallback>
                    {msg.sender.displayName.charAt(0).toUpperCase()}
                  </AvatarFallback>
                </Avatar>
                <div className="flex-1">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">
                      {msg.sender.displayName}
                    </span>
                    <span className="text-xs text-muted-foreground">
                      {formatRelativeTime(msg.createdAt)}
                    </span>
                  </div>
                  <p className="mt-1 text-sm">{msg.content}</p>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
