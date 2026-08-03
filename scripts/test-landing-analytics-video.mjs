/**
 * Unit checks for landing video engagement aggregation.
 * Run: node scripts/test-landing-analytics-video.mjs
 */
import assert from "assert";
import { buildVideoEngagement } from "../lib/marketing-landing-events-video.js";

const empty = buildVideoEngagement([]);
assert.strictEqual(empty.hasData, false);
assert.strictEqual(empty.opens, 0);
assert.strictEqual(empty.avgWatchSeconds, null);

const events = [
  { event: "platform_video_launcher_impression", sessionId: "s1" },
  { event: "platform_video_launcher_open", sessionId: "s1" },
  { event: "platform_video_start", sessionId: "s1" },
  { event: "platform_video_25", sessionId: "s1" },
  { event: "platform_video_50", sessionId: "s1" },
  { event: "platform_video_75", sessionId: "s1" },
  { event: "platform_video_complete", sessionId: "s1" },
  { event: "platform_video_close", sessionId: "s1", seconds: 120 },
  { event: "platform_video_launcher_open", sessionId: "s2" },
  { event: "platform_video_start", sessionId: "s2" },
  { event: "platform_video_25", sessionId: "s2" },
  { event: "platform_video_close", sessionId: "s2", seconds: 40 },
  { event: "hero_video_open", sessionId: "s3" },
  { event: "video_progress", sessionId: "s3", depth: 50 },
  { event: "hero_video_close", sessionId: "s3", seconds: 55 },
  { event: "platform_video_dismiss", sessionId: "s4" },
];

const video = buildVideoEngagement(events);
assert.strictEqual(video.hasData, true);
assert.strictEqual(video.opens, 3, "launcher opens + hero open");
assert.strictEqual(video.starts, 2);
assert.strictEqual(video.sessionsOpened, 3);
assert.strictEqual(video.sessionsCompleted, 1);
assert.strictEqual(video.completes, 1);
assert.strictEqual(video.closes, 3);
assert.strictEqual(video.impressions, 1);
assert.strictEqual(video.dismissals, 1);
assert.strictEqual(video.watchSamples, 3);
assert.strictEqual(video.avgWatchSeconds, 71.7);
assert.strictEqual(video.medianWatchSeconds, 55);
assert.strictEqual(video.maxWatchSeconds, 120);
assert.ok(video.completionRate != null);
assert.strictEqual(video.progress.find((p) => p.key === "25").count, 3);
assert.strictEqual(video.progress.find((p) => p.key === "50").count, 2);
assert.strictEqual(video.progress.find((p) => p.key === "75").count, 1);
assert.strictEqual(video.progress.find((p) => p.key === "100").count, 1);

console.log("ok: landing analytics video engagement");
