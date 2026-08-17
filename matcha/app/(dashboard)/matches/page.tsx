"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import type { MatchResponse } from "@/types";

export default function MatchesPage() {
  const [matches, setMatches] = useState<MatchResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/matches");
        if (!res.ok) {
          setError("Failed to load matches");
          return;
        }
        const { data } = await res.json();
        setMatches(data ?? []);
      } catch {
        setError("Failed to load matches");
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  if (loading) {
    return (
      <div className="space-y-4">
        <div className="h-8 w-48 animate-pulse rounded bg-muted" />
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <div key={i} className="h-48 animate-pulse rounded-lg bg-muted" />
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <Card>
        <CardContent className="pt-6">
          <p className="text-destructive">{error}</p>
          <Button className="mt-4" onClick={() => window.location.reload()}>
            Retry
          </Button>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Your Matches</h1>
        <p className="text-muted-foreground">
          People compatible with your profile, ranked by match score
        </p>
      </div>

      {matches.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted-foreground">
              No matches found yet. Complete your profile with interests and
              location to discover compatible people.
            </p>
          </CardContent>
        </Card>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {matches.map((match) => (
            <Card key={match.id}>
              <CardHeader className="flex flex-row items-start gap-4">
                <Avatar className="h-12 w-12">
                  <AvatarFallback>
                    {match.profile.displayName.charAt(0).toUpperCase()}
                  </AvatarFallback>
                </Avatar>
                <div className="flex-1">
                  <CardTitle className="text-lg">
                    {match.profile.displayName}
                  </CardTitle>
                  <CardDescription>
                    {match.profile.location ?? "Location not set"}
                    {match.profile.age ? ` · ${match.profile.age}` : ""}
                  </CardDescription>
                </div>
                <Badge>
                  {Math.round((match.score ?? 0) * 100)}%
                </Badge>
              </CardHeader>
              <CardContent>
                {match.profile.bio && (
                  <p className="mb-3 text-sm text-muted-foreground line-clamp-2">
                    {match.profile.bio}
                  </p>
                )}
                <div className="flex flex-wrap gap-1">
                  {match.profile.interests.slice(0, 4).map((interest) => (
                    <Badge key={interest} variant="secondary">
                      {interest}
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
