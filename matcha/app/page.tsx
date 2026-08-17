import Link from "next/link";
import { Heart, Sparkles, Users } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

export default function LandingPage() {
  return (
    <div className="flex flex-col">
      <section className="container mx-auto flex flex-col items-center gap-8 px-4 py-24 text-center">
        <div className="inline-flex items-center gap-2 rounded-full border bg-secondary px-4 py-1.5 text-sm text-secondary-foreground">
          <Sparkles className="h-4 w-4" />
          Smart matching, real connections
        </div>
        <h1 className="max-w-3xl text-4xl font-bold tracking-tight sm:text-6xl">
          Find people who{" "}
          <span className="text-primary">match your vibe</span>
        </h1>
        <p className="max-w-xl text-lg text-muted-foreground">
          Matcha uses interest-based scoring to connect you with compatible
          people nearby. Create your profile, discover matches, and start
          meaningful conversations.
        </p>
        <div className="flex flex-wrap items-center justify-center gap-4">
          <Button size="lg" asChild>
            <Link href="/register">Get started free</Link>
          </Button>
          <Button size="lg" variant="outline" asChild>
            <Link href="/login">Log in</Link>
          </Button>
        </div>
      </section>

      <section className="border-t bg-muted/30 py-20">
        <div className="container mx-auto grid gap-8 px-4 md:grid-cols-3">
          <Card>
            <CardHeader>
              <Users className="mb-2 h-8 w-8 text-primary" />
              <CardTitle>Smart Matching</CardTitle>
              <CardDescription>
                Our algorithm scores compatibility based on shared interests,
                age proximity, and location.
              </CardDescription>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <Heart className="mb-2 h-8 w-8 text-primary" />
              <CardTitle>Real Profiles</CardTitle>
              <CardDescription>
                Build a rich profile with bio, interests, and location to
                attract the right connections.
              </CardDescription>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <Sparkles className="mb-2 h-8 w-8 text-primary" />
              <CardTitle>Direct Messaging</CardTitle>
              <CardDescription>
                Message your matches directly and keep conversations in one
                place.
              </CardDescription>
            </CardHeader>
          </Card>
        </div>
      </section>

      <section className="container mx-auto px-4 py-20 text-center">
        <Card className="mx-auto max-w-2xl">
          <CardHeader>
            <CardTitle className="text-2xl">Ready to meet someone new?</CardTitle>
            <CardDescription>
              Join Matcha today and start discovering compatible matches in
              your area.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button size="lg" asChild>
              <Link href="/register">Create your account</Link>
            </Button>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
