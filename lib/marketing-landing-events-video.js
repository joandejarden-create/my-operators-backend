/**
 * Aggregate landing video engagement from canonical + platform-launcher events.
 * Old-home floating launcher posts platform_video_*; iframe landing posts hero_video_*.
 */

const OPEN_EVENTS = new Set([
  "hero_video_open",
  "platform_video_launcher_open",
]);

const START_EVENTS = new Set(["platform_video_start"]);

const CLOSE_EVENTS = new Set(["hero_video_close", "platform_video_close"]);

const COMPLETE_EVENTS = new Set([
  "video_complete",
  "platform_video_complete",
]);

const PROGRESS_MARKS = [25, 50, 75, 100];

function pct(n, total) {
  if (!total) return 0;
  return Math.round((n / total) * 1000) / 10;
}

function median(nums) {
  if (!nums.length) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function mean(nums) {
  if (!nums.length) return null;
  const sum = nums.reduce((a, b) => a + b, 0);
  return Math.round((sum / nums.length) * 10) / 10;
}

function progressDepth(event) {
  if (event.event === "video_progress" && typeof event.depth === "number") {
    return event.depth;
  }
  if (event.event === "video_complete" || event.event === "platform_video_complete") {
    return 100;
  }
  if (event.event === "platform_video_25") return 25;
  if (event.event === "platform_video_50") return 50;
  if (event.event === "platform_video_75") return 75;
  return null;
}

/**
 * @param {Array<object>} events
 * @returns {object}
 */
export function buildVideoEngagement(events) {
  const openedSessions = new Set();
  const startedSessions = new Set();
  const completedSessions = new Set();
  const closedSessions = new Set();
  const progressBySession = new Map();
  const watchSeconds = [];
  let openEvents = 0;
  let startEvents = 0;
  let closeEvents = 0;
  let completeEvents = 0;
  let dismissEvents = 0;
  let impressionEvents = 0;

  for (const e of events || []) {
    if (!e || !e.event) continue;
    const sid = e.sessionId || null;

    if (e.event === "platform_video_launcher_impression") {
      impressionEvents += 1;
    }
    if (e.event === "platform_video_dismiss") {
      dismissEvents += 1;
    }

    if (OPEN_EVENTS.has(e.event)) {
      openEvents += 1;
      if (sid) openedSessions.add(sid);
    }
    if (START_EVENTS.has(e.event)) {
      startEvents += 1;
      if (sid) {
        startedSessions.add(sid);
        // Playback without a separate open still counts as an engaged open session
        openedSessions.add(sid);
      }
    }
    if (CLOSE_EVENTS.has(e.event)) {
      closeEvents += 1;
      if (sid) closedSessions.add(sid);
      if (typeof e.seconds === "number" && e.seconds >= 0) {
        watchSeconds.push(e.seconds);
      }
    }
    if (COMPLETE_EVENTS.has(e.event)) {
      completeEvents += 1;
      if (sid) completedSessions.add(sid);
    }

    const depth = progressDepth(e);
    if (depth != null && sid) {
      const prev = progressBySession.get(sid) || 0;
      if (depth > prev) progressBySession.set(sid, depth);
    }
  }

  const sessionsOpened = openedSessions.size;
  const sessionsStarted = startedSessions.size;
  const sessionsCompleted = completedSessions.size;
  const sessionsClosed = closedSessions.size;
  const openDenom = sessionsOpened || openEvents || startEvents;

  const progress = PROGRESS_MARKS.map((mark) => {
    let count = 0;
    for (const depth of progressBySession.values()) {
      if (depth >= mark) count += 1;
    }
    // Completes also imply 100 even if progress map missed them
    if (mark === 100) {
      count = Math.max(count, sessionsCompleted);
    }
    return {
      key: String(mark),
      label: mark + "% Watched",
      count,
      rate: pct(count, openDenom),
    };
  });

  return {
    opens: openEvents,
    starts: startEvents,
    closes: closeEvents,
    completes: completeEvents,
    impressions: impressionEvents,
    dismissals: dismissEvents,
    sessionsOpened,
    sessionsStarted,
    sessionsClosed,
    sessionsCompleted,
    completionRate:
      sessionsOpened > 0
        ? pct(sessionsCompleted, sessionsOpened)
        : openDenom > 0
          ? pct(completeEvents, openDenom)
          : null,
    closeRate: sessionsOpened > 0 ? pct(sessionsClosed, sessionsOpened) : null,
    avgWatchSeconds: mean(watchSeconds),
    medianWatchSeconds: median(watchSeconds),
    maxWatchSeconds: watchSeconds.length ? Math.max(...watchSeconds) : null,
    watchSamples: watchSeconds.length,
    progress,
    hasData:
      openEvents > 0 ||
      startEvents > 0 ||
      closeEvents > 0 ||
      completeEvents > 0 ||
      progressBySession.size > 0,
  };
}

export const PLATFORM_VIDEO_EVENTS = [
  "platform_video_launcher_impression",
  "platform_video_launcher_open",
  "platform_video_start",
  "platform_video_25",
  "platform_video_50",
  "platform_video_75",
  "platform_video_complete",
  "platform_video_close",
  "platform_video_minimize",
  "platform_video_dismiss",
];
