import {
  loadLandingEvents,
  aggregateLandingEvents,
} from "../lib/marketing-landing-events-read.js";
import {
  buildReportWindow,
  loadOptionsForWindow,
} from "../lib/marketing-landing-events-window.js";
import {
  applyLandingReportFilters,
  parseReportFilters,
} from "../lib/marketing-landing-events-version.js";

function setReportNoStore(res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  res.setHeader("Pragma", "no-cache");
}

function csvEscape(value) {
  const s = value == null ? "" : String(value);
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function eventsToCsv(events) {
  const headers = [
    "ts",
    "event",
    "sessionId",
    "visitorId",
    "embed",
    "device",
    "landingVersion",
    "language",
    "section",
    "location",
    "label",
    "questionId",
    "persona",
    "depth",
    "outcome",
    "geoLabel",
    "utmSource",
    "utmMedium",
    "utmCampaign",
    "path",
  ];
  const lines = [headers.join(",")];
  for (const e of events) {
    lines.push(headers.map((h) => csvEscape(e[h])).join(","));
  }
  return lines.join("\n");
}

function funnelToCsv(aggregate) {
  const lines = ["step_key,step_label,sessions,rate_pct"];
  const steps = aggregate.funnel?.steps || [];
  for (const step of steps) {
    lines.push(
      [step.key, step.label, step.count, step.rate].map(csvEscape).join(",")
    );
  }
  return lines.join("\n");
}

/**
 * GET /api/marketing/landing-events/export?days=7&format=events|funnel&version=&cutover=&era=
 */
export async function getMarketingLandingEventsExport(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    setReportNoStore(res);
    const window = buildReportWindow(req.query?.days);
    const days = window.days;
    const filters = parseReportFilters(req.query);
    const loaded = loadLandingEvents(loadOptionsForWindow(window));
    const { events } = applyLandingReportFilters(loaded, filters);
    const format = String(req.query?.format || "events").toLowerCase();
    const versionTag =
      filters.version && filters.version !== "all" ? `-${filters.version}` : "";

    let body;
    let filename;
    if (format === "funnel") {
      const aggregate = aggregateLandingEvents(events);
      body = funnelToCsv(aggregate);
      filename = `landing-funnel-${days}d${versionTag}.csv`;
    } else {
      body = eventsToCsv(events);
      filename = `landing-events-${days}d${versionTag}.csv`;
    }

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    return res.status(200).send(body);
  } catch (err) {
    console.error("Error in marketing-landing-events-export:", err);
    return res.status(500).json({ error: "Could not export landing events" });
  }
}
