/**
 * Portfolio Leak Audit generation — aggregates Leak Audit hotel records only.
 */

import { loadLeakAuditPortfolioSampleReport } from "./sample-report-v1.js";

/**
 * @param {object} store
 * @param {object} options
 * @param {string} [options.portfolioName]
 * @param {Array<{hotelName:string,city?:string,country?:string,hotelWebsite?:string}>} [options.hotels]
 */
export function generatePortfolioLeakAuditReport(store, options = {}) {
  const sample = loadLeakAuditPortfolioSampleReport();
  const hotelsIn =
    Array.isArray(options.hotels) && options.hotels.length
      ? options.hotels
      : (sample.hotelsNeedingAttention || []).map((h) => ({
          hotelName: typeof h === "string" ? h : h.hotelName || h.name,
          city: h.city || "",
          country: h.country || "",
          hotelWebsite: h.hotelWebsite || "",
        }));

  const portfolio = store.createPortfolio({
    portfolioName: options.portfolioName || sample.portfolioName || "Portfolio Audit",
    companyName: options.companyName || sample.companyName || "",
    contactName: options.contactName || "",
    contactEmail: options.contactEmail || "",
    audienceType: options.audienceType || "portfolio_company",
    source: options.source || "manual",
    status: "report_ready",
  });

  const hotelLinks = hotelsIn.map((h) => {
    const hotel = store.createHotel({
      portfolioId: portfolio.id,
      hotelName: h.hotelName,
      hotelWebsite: h.hotelWebsite || "",
      city: h.city || "",
      country: h.country || "",
      locationLabel: [h.city, h.country].filter(Boolean).join(", "),
    });
    const link = store.createPortfolioHotel({
      portfolioId: portfolio.id,
      hotelAuditId: hotel.id,
      hotelName: hotel.hotelName,
      hotelWebsite: hotel.hotelWebsite,
      city: hotel.city,
      country: hotel.country,
      inclusionStatus: "included",
    });
    return { hotel, link };
  });

  const portfolioRun = store.createPortfolioRun({
    portfolioId: portfolio.id,
    runDate: sample.runDate || new Date().toISOString().slice(0, 10),
    hotelCount: hotelLinks.length,
    completedHotelRuns: hotelLinks.length,
    failedHotelRuns: 0,
    providersUsed: sample.providersUsed || ["openai", "gemini", "perplexity", "claude"],
    mostCommonAreaToReview:
      sample.areasToReviewMostOften?.[0] ||
      sample.mostCommonAreaToReview ||
      "Meetings & Groups",
    mostFrequentCompetitor:
      sample.competitorsBenefiting?.[0]?.competitorName ||
      sample.competitorsShowingUpInstead?.[0] ||
      "",
    hotelsWithPriorityAreasToReview: hotelLinks.length,
    status: "completed",
  });

  const portfolioReport = store.createPortfolioReport({
    portfolioId: portfolio.id,
    portfolioRunId: portfolioRun.id,
    reportStatus: "draft",
    clientReport: sample,
    portfolioSummary: sample.bottomLineSummary || sample.portfolioSummary || "",
    executiveSignal: sample.executiveSignals || sample.executiveSignal || null,
    hotelsNeedingAttention: sample.hotelsNeedingAttention || hotelsIn.map((h) => h.hotelName),
    areasToReviewMostOften: sample.areasToReviewMostOften || sample.inferredDemandSegmentsLeaking || [],
    competitorsShowingUpInstead:
      sample.competitorsShowingUpInstead || sample.competitorsBenefiting || [],
    portfolioPattern: sample.portfolioPattern || "",
    firstPortfolioAction: sample.portfolioActions?.[0] || sample.firstPortfolioAction || "",
    secondPortfolioAction: sample.portfolioActions?.[1] || "",
    thirdPortfolioAction: sample.portfolioActions?.[2] || "",
    nextStepCta: sample.recommendedNextStep || "",
  });

  return {
    ok: true,
    portfolio,
    hotelLinks,
    portfolioRun,
    portfolioReport,
    shareUrl: portfolioReport.shareUrl,
    productionAdpTouched: false,
  };
}
