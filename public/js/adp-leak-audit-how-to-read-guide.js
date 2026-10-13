/**
 * AI Demand Leak Audit — How to Read guide (report order).
 * Reuses AdpGuidedReportTour; 8 steps matching the report sections.
 * Hybrid step shape (same as ADP): guide + Stronger/Weaker + Look Next.
 */
(function (global) {
  "use strict";

  var ADP_BRIDGE_NOTE =
    "The AI Demand Leak Audit is a limited diagnostic. The full ADP pilot uses the same reading logic, but monitors more demand territories, more evidence, more source signals, and changes over time.";

  function singlePropertySteps() {
    return [
      {
        id: "executive-summary",
        target: '[data-adp-tour-target="executive-summary"]',
        title: "Start with the Executive Summary",
        guide:
          "The Executive Summary gives the main read: where the hotel is visible, where it may be missing, and what management should review first.",
        signal:
          "A stronger read shows clear visibility in the intended demand areas; a weaker read points to where attention is needed.",
        lookNext:
          "Note the two or three issues worth reviewing in the Executive Signal and Demand Area sections below.",
      },
      {
        id: "executive-signal",
        target: '[data-adp-tour-target="executive-signal"]',
        title: "Review the Executive Signal",
        guide:
          "The Executive Signal shows the key ADP metrics from the monitored sample, including how often the hotel appeared, how broadly it appeared across traveler needs, and how well AI reflected the hotel’s actual product attributes.",
        signal:
          "Higher consideration and scenario presence are generally stronger. Low reality coverage or a flagged primary area means the product story may not be landing clearly.",
        lookNext:
          "Open any KPI info icon for the exact definition, then compare the Primary Area to Review with the demand section below.",
      },
      {
        id: "demand-area",
        target: '[data-adp-tour-target="demand-area"]',
        title: "Check the Demand Area to Review",
        guide:
          "This section identifies the demand area that appears most important to review based on the monitored results. It is not based on a client-stated priority. The hotel should confirm whether this demand area matters commercially before taking action.",
        signal:
          "A commercially important area with weak AI recognition is a stronger management priority than a weak area that does not matter to the hotel.",
        lookNext:
          "Confirm commercial importance, then check which competitors appeared when the hotel was absent.",
      },
      {
        id: "competitors",
        target: '[data-adp-tour-target="competitors"]',
        title: "See Which Competitors Showed Up Instead",
        guide:
          "This section shows which competitors appeared most often when the subject hotel was absent. It is not a full market ranking. It is a way to see where AI may be directing attention in the monitored sample.",
        signal:
          "Repeated appearances by the same competitor matter more than a one-off mention in the monitored sample.",
        lookNext:
          "Open Supporting Evidence to see the monitored examples behind the pattern.",
      },
      {
        id: "supporting-evidence",
        target: '[data-adp-tour-target="supporting-evidence"]',
        title: "Open the Supporting Evidence",
        guide:
          "Supporting examples show why the report flagged the issue. The evidence is client-safe and does not expose full prompts, prompt IDs, or internal record IDs.",
        signal:
          "Evidence is not scored good or bad; it confirms whether the observed pattern is real and understandable.",
        lookNext:
          "Check evidence before acting on surprising, material, or competitive findings, then review the Priority AI Demand Improvements.",
      },
      {
        id: "actions",
        target: '[data-adp-tour-target="actions"]',
        title: "Review the Priority AI Demand Improvements",
        guide:
          "These improvements translate findings into targeted work Dealality can help prepare: source audits, copy drafts, competitor comparisons, and checklists to strengthen how AI presents the hotel.",
        signal:
          "A stronger next move is specific, source-backed, and tied to a monitored gap; vague or unowned tasks are weaker.",
        lookNext:
          "Read each card’s why-it-matters line, then see how publishing ownership is split in the partnership steps.",
      },
      {
        id: "who-does-work",
        target: '[data-adp-tour-target="who-does-work"]',
        title: "See How We Partner on These Improvements",
        guide:
          "Dealality readies the package, you confirm what goes live, your team publishes through existing channels, and Dealality follows through on the next monitoring cycle.",
        signal:
          "Clear ownership is stronger: Dealality prepares, you confirm, your team publishes, Dealality follows through.",
        lookNext:
          "Confirm you are comfortable with that split, then book the ADP walkthrough while findings are fresh.",
      },
      {
        id: "next-step",
        target: '[data-adp-tour-target="next-step"]',
        title: "Book Your ADP Walkthrough",
        guide:
          "Act while these findings are fresh. Book a live ADP walkthrough to review the full dashboard and evidence, then decide whether a paid pilot makes sense.",
        signal:
          "A stronger next step is a timed walkthrough with a team ready to act; delaying review weakens the value of this diagnostic.",
        lookNext:
          "If useful, schedule a live ADP walkthrough and determine whether a paid pilot is appropriate.",
        note: ADP_BRIDGE_NOTE,
      },
    ];
  }

  function portfolioSteps() {
    return [
      {
        id: "executive-summary",
        target: '[data-adp-tour-target="bottom-line"]',
        title: "Start with the Executive Summary",
        guide:
          "Begin with the portfolio summary. It tells you whether properties are broadly visible, selectively exposed, or missing in monitored AI answers.",
        signal:
          "Broad, consistent visibility across the portfolio is stronger; selective exposure or missing properties point to where attention is needed.",
        lookNext:
          "Note the hotels or demand areas that look weakest before opening the Executive Signal.",
      },
      {
        id: "executive-signal",
        target: '[data-adp-tour-target="ai-consideration"]',
        title: "Review the Executive Signal",
        guide:
          "The Executive Signal shows key portfolio metrics from the monitored sample.",
        signal:
          "Higher portfolio consideration and coverage are generally stronger; large spreads between hotels signal uneven AI demand position.",
        lookNext:
          "Identify which hotels or metrics drive the result, then check the Demand Area to Review.",
        optional: true,
      },
      {
        id: "demand-area",
        target: '[data-adp-tour-target="inferred-demand-leak"]',
        title: "Check the Demand Area to Review",
        guide:
          "This section identifies demand areas that appear most important to review from monitored results, not client-stated strategy.",
        signal:
          "Commercially important areas with weak recognition are stronger priorities than weak areas that do not matter to the portfolio.",
        lookNext:
          "Confirm commercial importance, then see which competitors appeared when subject hotels were absent.",
        optional: true,
      },
      {
        id: "competitors",
        target: '[data-adp-tour-target="competitor-displacement"]',
        title: "See Which Competitors Showed Up Instead",
        guide:
          "This section shows which competitors appeared most often when subject hotels were absent. It is not a full market ranking.",
        signal:
          "Repeated competitor appearances across hotels matter more than isolated one-off mentions.",
        lookNext:
          "Open Supporting Evidence to verify the monitored examples behind the pattern.",
        optional: true,
      },
      {
        id: "supporting-evidence",
        target: '[data-adp-tour-target="evidence"]',
        title: "Open the Supporting Evidence",
        guide:
          "Supporting examples show why the report flagged issues. Evidence stays client-safe without full prompts or internal IDs.",
        signal:
          "Evidence is not scored good or bad; it confirms whether the observed portfolio pattern is real and understandable.",
        lookNext:
          "Check evidence before acting, then review the Priority AI Demand Improvements.",
        optional: true,
      },
      {
        id: "actions",
        target: '[data-adp-tour-target="first-3-fixes"]',
        title: "Review the Priority AI Demand Improvements",
        guide:
          "Improvements translate findings into targeted work Dealality can help prepare across the portfolio.",
        signal:
          "A stronger portfolio move is specific, source-backed, and hotel-actionable; vague or unowned tasks are weaker.",
        lookNext:
          "See how publishing ownership is split, then decide the next walkthrough step.",
      },
      {
        id: "who-does-work",
        target: '[data-adp-tour-target="who-does-work"]',
        title: "See How We Partner on These Improvements",
        guide:
          "Dealality readies the package across the portfolio, hotels confirm what goes live, teams publish through existing channels, and Dealality follows through on the next monitoring cycle.",
        signal:
          "Clear ownership across properties is stronger: Dealality prepares, hotels confirm, teams publish, Dealality follows through.",
        lookNext:
          "Confirm the ownership split, then book the ADP walkthrough while findings are fresh.",
        optional: true,
      },
      {
        id: "next-step",
        target: '[data-adp-tour-target="next-step"]',
        title: "Decide the Next Step",
        guide:
          "The free audit is a limited diagnostic. The next step is a live ADP walkthrough to review the full dashboard, evidence, and whether a paid pilot makes sense.",
        signal:
          "A stronger next step is a timed walkthrough with owners ready to act; delaying review weakens the value of this diagnostic.",
        lookNext:
          "If useful, schedule a live ADP walkthrough and determine whether a paid pilot is appropriate.",
        note: ADP_BRIDGE_NOTE,
        optional: true,
      },
    ];
  }

  function init(opts) {
    opts = opts || {};
    var Tour = global.AdpGuidedReportTour;
    if (!Tour || typeof Tour.configure !== "function") {
      try {
        console.warn(
          "[AdpLeakAuditHowToReadGuide] AdpGuidedReportTour.configure unavailable"
        );
      } catch (_) {}
      return null;
    }

    var isPortfolio =
      opts.portfolio === true || global.__ALA_PORTFOLIO_MODE__ === true;
    var steps = isPortfolio ? portfolioSteps() : singlePropertySteps();

    Tour.configure({
      steps: steps,
      finishStep: false,
      buttonId: "adpHowToReadReport",
      analyticsKey: "adp_leak_audit_guided_tour_v2",
      emptyMessage:
        "Wait for the report to finish loading, then click How to read this report again.",
    });
    Tour.init();
    try {
      document.dispatchEvent(new CustomEvent("adp:report-loaded"));
    } catch (_) {}
    return Tour;
  }

  global.AdpLeakAuditHowToReadGuide = {
    init: init,
    adpBridgeNote: ADP_BRIDGE_NOTE,
    singlePropertySteps: singlePropertySteps,
    portfolioSteps: portfolioSteps,
  };
})(typeof window !== "undefined" ? window : globalThis);
