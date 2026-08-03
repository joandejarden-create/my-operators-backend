/**
 * Clarity-style dashboard aggregates for the landing analytics report.
 */

const MONTH_SHORT = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

const ACQUISITION_CHANNELS = [
  { key: "direct", label: "Direct", color: "#f5c542" }, // hero yellow
  { key: "referral", label: "Referral", color: "#5b8cff" },
  { key: "campaign", label: "Campaign", color: "#3dd6c6" },
  { key: "embed", label: "Homepage Embed", color: "#a78bfa" }, // lavender
  { key: "other", label: "Other", color: "#6b7a99" },
];

function pct(n, total) {
  if (!total) return 0;
  return Math.round((n / total) * 1000) / 10;
}

function formatDayLabel(dayKey) {
  const [, m, d] = dayKey.split("-").map(Number);
  return `${MONTH_SHORT[m - 1]} ${d}`;
}

function hostFromReferrer(referrer) {
  const raw = String(referrer || "").trim();
  if (!raw) return null;
  try {
    return new URL(raw).hostname.replace(/^www\./, "").toLowerCase();
  } catch (_e) {
    return raw.slice(0, 64).toLowerCase();
  }
}

/**
 * Classify page_land traffic source.
 * Embed (iframe on dealality.com / Webflow) is its own channel — previously lumped into "Other".
 */
function classifyAcquisitionChannel(event) {
  const utm = (event.utmSource || "").trim();
  if (utm) return "campaign";

  if (event.embed) return "embed";

  const ref = (event.referrer || "").trim().toLowerCase();
  if (!ref) return "direct";

  const host = hostFromReferrer(ref) || "";
  if (
    host === "dealality.com" ||
    host.endsWith(".dealality.com") ||
    host.includes("localhost") ||
    host.includes("webflow.io")
  ) {
    return "direct";
  }
  return "referral";
}

function buildTopReferrers(events, limit = 8) {
  /** @type {Record<string, number>} */
  const counts = {};
  for (const e of events) {
    if (e.event !== "page_land") continue;
    let label;
    if (e.utmSource) {
      label = `Campaign · ${e.utmSource}`;
    } else if (e.embed) {
      const host = hostFromReferrer(e.referrer);
      label = host
        ? `Embed on ${host}`
        : "Homepage Embed (dealality.com)";
    } else {
      const host = hostFromReferrer(e.referrer);
      label = host || "Direct / no referrer";
    }
    counts[label] = (counts[label] || 0) + 1;
  }
  return Object.entries(counts)
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
    .slice(0, limit);
}

function sessionDurationSeconds(events, sessionId) {
  let start = null;
  let end = null;
  let milestoneMax = 0;

  for (const e of events) {
    if (e.sessionId !== sessionId || !e.ts) continue;
    const t = new Date(e.ts).getTime();
    if (Number.isNaN(t)) continue;
    if (start == null || t < start) start = t;
    if (end == null || t > end) end = t;
    if (e.event === "engagement_milestone" && typeof e.seconds === "number") {
      milestoneMax = Math.max(milestoneMax, e.seconds);
    }
  }

  if (milestoneMax > 0) return milestoneMax;
  if (start != null && end != null && end >= start) {
    return Math.round((end - start) / 1000);
  }
  return 0;
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

export function buildAudienceOverview(events) {
  /** @type {Record<string, { sessions: Set<string>, durations: number[] }>} */
  const byDay = {};
  const sessionIds = new Set();
  const allDurations = [];

  for (const e of events) {
    if (e.event !== "page_land" || !e.ts || !e.sessionId) continue;
    const d = new Date(e.ts);
    if (Number.isNaN(d.getTime())) continue;
    const dayKey = d.toISOString().slice(0, 10);
    if (!byDay[dayKey]) {
      byDay[dayKey] = { sessions: new Set(), durations: [] };
    }
    byDay[dayKey].sessions.add(e.sessionId);
    sessionIds.add(e.sessionId);
  }

  for (const sid of sessionIds) {
    const dur = sessionDurationSeconds(events, sid);
    if (dur > 0) allDurations.push(dur);
    for (const bucket of Object.values(byDay)) {
      if (bucket.sessions.has(sid) && dur > 0) {
        bucket.durations.push(dur);
      }
    }
  }

  const days = Object.entries(byDay)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([key, bucket]) => ({
      key,
      label: formatDayLabel(key),
      sessions: bucket.sessions.size,
      medianDurationSeconds: median(bucket.durations),
    }));

  return {
    timezone: "UTC",
    totalSessions: sessionIds.size,
    medianDurationSeconds: median(allDurations),
    days,
  };
}

export function buildAcquisitionReport(events) {
  /** @type {Record<string, Record<string, number>>} */
  const byDay = {};

  for (const e of events) {
    if (e.event !== "page_land" || !e.ts) continue;
    const dayKey = e.ts.slice(0, 10);
    const channel = classifyAcquisitionChannel(e);
    if (!byDay[dayKey]) {
      byDay[dayKey] = {
        direct: 0,
        referral: 0,
        campaign: 0,
        embed: 0,
        other: 0,
      };
    }
    byDay[dayKey][channel] = (byDay[dayKey][channel] || 0) + 1;
  }

  const days = Object.entries(byDay)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([key, counts]) => {
      const total =
        counts.direct +
        counts.referral +
        counts.campaign +
        counts.embed +
        counts.other;
      return {
        key,
        label: formatDayLabel(key),
        total,
        channels: ACQUISITION_CHANNELS.map((ch) => ({
          key: ch.key,
          label: ch.label,
          color: ch.color,
          count: counts[ch.key] || 0,
        })).filter((ch) => ch.count > 0),
      };
    });

  const totals = {
    direct: 0,
    referral: 0,
    campaign: 0,
    embed: 0,
    other: 0,
  };
  for (const day of days) {
    for (const ch of day.channels) {
      totals[ch.key] = (totals[ch.key] || 0) + ch.count;
    }
  }

  return {
    channels: ACQUISITION_CHANNELS,
    days,
    totals: ACQUISITION_CHANNELS.map((ch) => ({
      ...ch,
      count: totals[ch.key] || 0,
    })).filter((ch) => ch.count > 0),
    topReferrers: buildTopReferrers(events),
    note:
      "Embed = iframe on dealality.com / Webflow. Campaign needs utm_source on the landing URL. Referral = external host on document.referrer.",
  };
}

export function buildPopularLandingPages(events) {
  /** @type {Record<string, number>} */
  const counts = {};

  for (const e of events) {
    if (e.event !== "page_land") continue;
    let label;
    if (e.embed) {
      label = "Homepage Embed (dealality.com)";
    } else {
      const path = (e.path || "").trim() || "/marketing/landing";
      label = path.length > 72 ? path.slice(0, 69) + "…" : path;
    }
    counts[label] = (counts[label] || 0) + 1;
  }

  return Object.entries(counts)
    .map(([label, pageviews]) => ({ label, pageviews }))
    .sort((a, b) => b.pageviews - a.pageviews || a.label.localeCompare(b.label))
    .slice(0, 10);
}

export function buildSessionsByCountry(events) {
  /** @type {Record<string, number>} */
  const countries = {};

  for (const e of events) {
    if (e.event !== "page_land" || !e.sessionId) continue;
    const country =
      e.geoCountryName || e.geoCountry || (e.geoLabel ? e.geoLabel.split(",").pop().trim() : null) || "Unknown";
    countries[country] = (countries[country] || 0) + 1;
  }

  return Object.entries(countries)
    .map(([label, sessions]) => ({ label, sessions }))
    .sort((a, b) => b.sessions - a.sessions || a.label.localeCompare(b.label))
    .slice(0, 10);
}

const DEVICE_LABELS = {
  desktop: "Desktop",
  mobile: "Mobile",
  tablet: "Tablet",
};

export function buildSessionsByDevice(events) {
  /** @type {Record<string, number>} */
  const devices = {};

  for (const e of events) {
    if (e.event !== "page_land" || !e.sessionId || !e.device) continue;
    const key = e.device;
    devices[key] = (devices[key] || 0) + 1;
  }

  const rows = Object.entries(devices)
    .map(([key, sessions]) => ({
      key,
      label: DEVICE_LABELS[key] || key,
      sessions,
    }))
    .sort((a, b) => b.sessions - a.sessions);

  const total = rows.reduce((n, r) => n + r.sessions, 0);
  return rows.map((row) => ({
    ...row,
    rate: pct(row.sessions, total),
    color:
      row.key === "mobile"
        ? "#5b8cff"
        : row.key === "desktop"
          ? "#7b82ff"
          : "#3dd6c6",
  }));
}

export function buildLandingDashboard(events) {
  return {
    audience: buildAudienceOverview(events),
    acquisition: buildAcquisitionReport(events),
    popularPages: buildPopularLandingPages(events),
    sessionsByCountry: buildSessionsByCountry(events),
    sessionsByDevice: buildSessionsByDevice(events),
  };
}
