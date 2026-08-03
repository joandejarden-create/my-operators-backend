/**
 * Actionable homepage recommendations from landing analytics aggregates.
 * Categories: conversion, flow, content, locale.
 */

function stepRate(funnel, key) {
  const step = funnel?.steps?.find((s) => s.key === key);
  return step ? Number(step.rate) || 0 : 0;
}

function stepCount(funnel, key) {
  const step = funnel?.steps?.find((s) => s.key === key);
  return step ? Number(step.count) || 0 : 0;
}

function pathCount(ctaPaths, key) {
  const row = ctaPaths?.paths?.find((p) => p.key === key);
  return row ? Number(row.count) || 0 : 0;
}

function pathRate(ctaPaths, key) {
  const row = ctaPaths?.paths?.find((p) => p.key === key);
  return row ? Number(row.rate) || 0 : 0;
}

function benchmarkStatus(benchmarks, funnelKey) {
  return (benchmarks || []).find((b) => b.funnelKey === funnelKey) || null;
}

/**
 * @param {object} aggregate — output of aggregateLandingEvents
 * @param {object} [opts]
 * @param {object|null} [opts.localeCompare]
 * @param {string} [opts.localeFilter]
 */
export function buildLandingRecommendations(aggregate, opts = {}) {
  const funnel = aggregate?.funnel || {};
  const sessions = Number(funnel.sessionCount || aggregate?.totals?.sessions || 0);
  const recommendations = [];
  const localeCompare = opts.localeCompare || null;
  const localeFilter = opts.localeFilter || "all";

  function push(rec) {
    recommendations.push({
      id: rec.id,
      category: rec.category,
      severity: rec.severity,
      title: rec.title,
      body: rec.body,
      evidence: rec.evidence || null,
      locale: rec.locale || (localeFilter === "all" ? "both" : localeFilter),
      action: rec.action || null,
    });
  }

  if (sessions === 0) {
    push({
      id: "no-traffic",
      category: "flow",
      severity: "watch",
      title: "No homepage sessions in this view",
      body: "Open English (/) or Spanish (/es) on the live site, or widen the time window / clear locale filters.",
      action: "Confirm analytics is loaded on both locales, then refresh this report.",
    });
    return finalize(recommendations, sessions);
  }

  if (sessions < 8) {
    push({
      id: "small-sample",
      category: "flow",
      severity: "watch",
      title: "Treat findings as directional",
      body: `Only ${sessions} session${sessions === 1 ? "" : "s"} in this filtered window — prioritize qualitative checks until sample size grows.`,
      evidence: `${sessions} sessions`,
    });
  }

  // --- Flow ---
  const scrollRate = stepRate(funnel, "scrolled");
  const scrollBench = benchmarkStatus(aggregate.benchmarks, "scrolled");
  if (sessions >= 5 && scrollRate < (scrollBench?.goodMin || 55)) {
    push({
      id: "low-scroll",
      category: "flow",
      severity: scrollRate < 40 ? "critical" : "opportunity",
      title: "Hero is not pulling people into the page",
      body: `Only ${scrollRate}% start scrolling (target ≥${scrollBench?.targetRate || 70}%). Hero promise, load speed, or above-the-fold clutter may be blocking curiosity.`,
      evidence: `${scrollRate}% scrolled · ${stepCount(funnel, "scrolled")}/${sessions}`,
      action:
        "Test a sharper hero headline/subhead, reduce competing CTAs above the fold, and verify mobile first paint on / and /es.",
    });
  }

  const drop = funnel.biggestDropOff;
  if (drop && drop.dropRate >= 25 && sessions >= 5) {
    push({
      id: "biggest-drop",
      category: "flow",
      severity: drop.dropRate >= 40 ? "critical" : "opportunity",
      title: `Largest drop: ${drop.fromLabel} → ${drop.toLabel}`,
      body: `${drop.dropRate}% of visitors stop between these steps. That section is the highest-leverage rewrite or reorder opportunity.`,
      evidence: `${drop.drop} sessions lost (${drop.dropRate}%)`,
      action: `Inspect the content between “${drop.fromLabel}” and “${drop.toLabel}” for length, clarity, and whether the next section’s first screen earns a continue.`,
    });
  }

  const howRate = stepRate(funnel, "reached_how");
  const howBench = benchmarkStatus(aggregate.benchmarks, "reached_how");
  if (sessions >= 5 && howRate < (howBench?.goodMin || 25) && scrollRate >= 50) {
    push({
      id: "stall-before-how",
      category: "content",
      severity: "opportunity",
      title: "Visitors scroll but miss How / Platform",
      body: `${howRate}% reach the platform explanation after ${scrollRate}% start scrolling. Mid-page story may be too long or soft.`,
      evidence: `How section ${howRate}% · scroll ${scrollRate}%`,
      action:
        "Bring a concrete product proof (modules, deal compare, or video) earlier; shorten the section before How We Do It.",
    });
  }

  // --- Conversion ---
  const ctaRate = stepRate(funnel, "cta_click");
  const ctaBench = benchmarkStatus(aggregate.benchmarks, "cta_click");
  const bottomNoClick = pathCount(aggregate.ctaPaths, "reached_bottom_no_click");
  const bottomNoClickRate = pathRate(aggregate.ctaPaths, "reached_bottom_no_click");

  if (sessions >= 5 && ctaRate === 0) {
    push({
      id: "zero-cta",
      category: "conversion",
      severity: "critical",
      title: "No signup CTA clicks recorded",
      body: "Traffic is arriving but nobody is clicking Explore / Request Demo / pricing CTAs.",
      evidence: `0 CTA clicks · ${sessions} sessions`,
      action:
        "Check CTA selectors on EN and ES, sticky CTA visibility on mobile, and whether CTAs are below a long first fold.",
    });
  } else if (sessions >= 8 && ctaRate < (ctaBench?.goodMin || 4)) {
    push({
      id: "low-cta",
      category: "conversion",
      severity: "opportunity",
      title: "CTA click rate is below the floor",
      body: `${ctaRate}% of sessions click a CTA (floor ${ctaBench?.goodMin || 4}%, target ${ctaBench?.targetRate || 8}%).`,
      evidence: `${stepCount(funnel, "cta_click")} CTA sessions`,
      action:
        "Compare CTA Paths — reinforce the winning placement and rewrite the weaker one; pair primary CTA with a lower-friction secondary (demo / video).",
    });
  } else if (sessions >= 8 && ctaRate >= (ctaBench?.targetRate || 8)) {
    push({
      id: "cta-win",
      category: "conversion",
      severity: "win",
      title: "CTA click rate is on target",
      body: `${ctaRate}% of sessions click a signup CTA. Double down on the placement that leads.`,
      evidence: `${stepCount(funnel, "cta_click")} CTA sessions`,
      action: "Mirror the top CTA’s wording and placement into weaker sections and the Spanish page if EN leads.",
    });
  }

  if (sessions >= 5 && bottomNoClick >= 3 && bottomNoClickRate >= 8) {
    push({
      id: "bottom-cta-friction",
      category: "conversion",
      severity: "opportunity",
      title: "People reach the bottom CTA and leave",
      body: `${bottomNoClick} sessions (${bottomNoClickRate}%) saw the bottom CTA band without clicking. Offer or copy at the end may not close.`,
      evidence: `${bottomNoClick} reached bottom · no click`,
      action:
        "Tighten final CTA copy, add social proof next to it, or offer a softer next step (demo / overview video) beside Explore Opportunity.",
    });
  }

  const heroNoScroll = pathCount(aggregate.ctaPaths, "hero_cta_no_scroll");
  if (sessions >= 5 && heroNoScroll >= 3) {
    push({
      id: "hero-intent",
      category: "conversion",
      severity: "win",
      title: "Hero CTA converts without a read",
      body: `${heroNoScroll} sessions clicked a hero CTA before scrolling — strong above-the-fold intent.`,
      evidence: `${heroNoScroll} hero CTA · no scroll`,
      action: "Keep hero CTA prominent; A/B only supporting copy, not the primary button.",
    });
  }

  // --- Content / video / FAQ ---
  const video = aggregate.video || {};
  const videoOpens = Number(aggregate.totals?.videoOpens || video.sessionsOpened || 0);
  const completionRate = video.completionRate;
  const avgWatch = video.avgWatchSeconds;

  if (sessions >= 8 && videoOpens === 0) {
    push({
      id: "video-unused",
      category: "content",
      severity: "opportunity",
      title: "Overview video is not being opened",
      body: "No video opens in this window. The floating launcher or Watch Overview control may be easy to miss — or dismissed.",
      evidence: `0 opens · ${video.impressions || 0} launcher impressions`,
      action:
        "Make Watch Overview more explicit in the hero secondary CTA; check dismiss rate and mobile launcher collision with chat/cookies.",
    });
  } else if (videoOpens >= 3 && completionRate != null && completionRate < 20) {
    push({
      id: "video-drop",
      category: "content",
      severity: "opportunity",
      title: "Video opens but few finish",
      body: `${completionRate}% completion after ${videoOpens} open session${videoOpens === 1 ? "" : "s"}${
        avgWatch != null ? ` · avg watch ${avgWatch}s` : ""
      }. Opening works; retention does not.`,
      evidence: `${video.completes || 0}/${videoOpens} complete`,
      action:
        "Shorten the first 20s, front-load the product payoff, and check progress funnel for the steepest 25→50 or 50→75 drop.",
    });
  } else if (videoOpens >= 3 && completionRate != null && completionRate >= 35) {
    push({
      id: "video-win",
      category: "content",
      severity: "win",
      title: "Overview video is holding attention",
      body: `${completionRate}% of open sessions finish the video${
        avgWatch != null ? ` (avg watch ${avgWatch}s)` : ""
      }.`,
      evidence: `${video.completes || 0} completes`,
      action: "Surface video earlier for cold traffic and ensure /es has equivalent discoverability.",
    });
  }

  const faqs = aggregate.faqHeatmap || [];
  if (faqs.length >= 1 && sessions >= 5) {
    const top = faqs[0];
    const totalFaq = faqs.reduce((sum, row) => sum + Number(row.count || 0), 0);
    if (top && totalFaq >= 3) {
      push({
        id: "faq-top",
        category: "content",
        severity: "watch",
        title: `FAQ demand: “${top.label}”`,
        body: `“${top.label}” is the most expanded FAQ (${top.count} opens). That question belongs earlier in the page narrative or hero objection handling.`,
        evidence: `${top.count}/${totalFaq} FAQ opens`,
        action:
          "Promote the answer into How We Do It or a hero micro-line; keep FAQ wording aligned on EN and ES.",
      });
    }
  } else if (sessions >= 10 && faqs.length === 0) {
    push({
      id: "faq-unused",
      category: "content",
      severity: "watch",
      title: "FAQ section is not being used",
      body: "No FAQ expansions recorded. Either visitors never reach it, or questions are not compelling.",
      action: "Check FAQ placement vs scroll depth and rewrite question titles as real objections.",
    });
  }

  const topCta = (aggregate.ctaLocations || [])[0];
  const secondCta = (aggregate.ctaLocations || [])[1];
  if (topCta && topCta.count >= 3) {
    push({
      id: "cta-placement-win",
      category: "conversion",
      severity: "win",
      title: `Strongest CTA placement: ${topCta.label || topCta.key}`,
      body: `${topCta.count} clicks came from ${topCta.label || topCta.key}${
        secondCta
          ? ` (next: ${secondCta.label || secondCta.key} · ${secondCta.count})`
          : ""
      }.`,
      evidence: `${topCta.count} clicks`,
      action: "Reuse that placement’s wording/visual weight on weaker CTAs and the other language version.",
    });
  }

  // --- Locale gaps (when viewing both languages) ---
  if (localeFilter === "all" && localeCompare?.hasBoth) {
    for (const delta of localeCompare.deltas || []) {
      const leaderLabel = delta.leader === "en" ? "English" : "Spanish";
      const laggerLabel = delta.leader === "en" ? "Spanish" : "English";
      const leaderVal = delta.leader === "en" ? delta.en : delta.es;
      const laggerVal = delta.leader === "en" ? delta.es : delta.en;
      push({
        id: `locale-${delta.metric}`,
        category: "locale",
        severity: Math.abs(delta.gapPts) >= 8 ? "opportunity" : "watch",
        title: `${delta.label}: ${leaderLabel} leads ${laggerLabel}`,
        body: `${leaderLabel} is at ${leaderVal}% vs ${laggerVal}% on ${laggerLabel} (${Math.abs(delta.gapPts)} pt gap). Port winning copy/layout from the stronger locale.`,
        evidence: `EN ${delta.en}% · ES ${delta.es}%`,
        locale: "both",
        action: `Diff ${delta.label.toLowerCase()} treatments on / vs /es — headline, CTA label, and video launcher parity first.`,
      });
    }

    if ((localeCompare.en?.sessions || 0) >= 5 && (localeCompare.es?.sessions || 0) === 0) {
      push({
        id: "locale-es-missing",
        category: "locale",
        severity: "watch",
        title: "No Spanish homepage sessions yet",
        body: "English traffic is present but /es has no recorded sessions in this window.",
        action: "Verify /es publishes the analytics script and test a visit from an ES path.",
      });
    } else if ((localeCompare.es?.sessions || 0) >= 5 && (localeCompare.en?.sessions || 0) === 0) {
      push({
        id: "locale-en-missing",
        category: "locale",
        severity: "watch",
        title: "No English homepage sessions yet",
        body: "Spanish traffic is present but / has no recorded sessions in this window.",
        action: "Verify English homepage analytics tagging and compare acquisition sources.",
      });
    }
  }

  return finalize(recommendations, sessions);
}

function severityRank(severity) {
  if (severity === "critical") return 0;
  if (severity === "opportunity") return 1;
  if (severity === "watch") return 2;
  if (severity === "win") return 3;
  return 4;
}

function finalize(recommendations, sessions) {
  const sorted = [...recommendations].sort(
    (a, b) => severityRank(a.severity) - severityRank(b.severity)
  );
  const actionable = sorted.filter((r) => r.severity === "critical" || r.severity === "opportunity");
  const wins = sorted.filter((r) => r.severity === "win");
  return {
    items: sorted.slice(0, 12),
    summary: {
      sessions,
      actionableCount: actionable.length,
      winCount: wins.length,
      criticalCount: sorted.filter((r) => r.severity === "critical").length,
    },
  };
}
