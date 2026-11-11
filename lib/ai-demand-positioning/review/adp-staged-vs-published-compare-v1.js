/**
 * Meaningful staged vs published ADP report comparison for founder review.
 */

function pick(obj, path) {
  let cur = obj;
  for (const key of String(path).split(".")) {
    if (cur == null) return undefined;
    cur = cur[key];
  }
  return cur;
}

function summarizeCompetitive(payload) {
  const cs =
    payload?.competitiveSet ||
    payload?.competitiveIntelligence ||
    payload?.displacement ||
    {};
  const leaders =
    cs.leadingCompetitors ||
    cs.topCompetitors ||
    cs.competitors ||
    payload?.executiveSummary?.leadingCompetitors ||
    [];
  const names = (Array.isArray(leaders) ? leaders : [])
    .map((c) => (typeof c === "string" ? c : c?.name || c?.hotelName || c?.id))
    .filter(Boolean)
    .slice(0, 8);
  return { leaderNames: names, rawKeys: Object.keys(cs || {}) };
}

function summarizeRealityGap(payload) {
  const rg =
    payload?.realityGap ||
    payload?.metrics?.realityGap ||
    payload?.executiveSummary?.realityGap ||
    {};
  return {
    measurementStatus: rg.measurementStatus || rg.status || null,
    display: rg.display || rg.label || rg.summary || null,
    percent: rg.percent ?? rg.value ?? rg.gapPercent ?? null,
    trackedAttributeCount:
      rg.trackedAttributeCount ?? rg.attributeCount ?? rg.governedAttributeCount ?? null,
  };
}

function summarizeExecutive(payload) {
  const ex = payload?.executiveSummary || payload?.executive || {};
  return {
    headline: ex.headline || ex.title || null,
    narrative: String(ex.narrative || ex.summary || ex.body || "").slice(0, 400),
  };
}

/**
 * Compare two owner payloads (staged vs published).
 */
export function compareStagedVsPublishedReports({
  propertyId,
  stagedPayload = null,
  publishedPayload = null,
  stagedMeta = null,
  publishedMeta = null,
} = {}) {
  const differences = [];

  if (!stagedPayload && !publishedPayload) {
    return {
      ok: false,
      error: "no_versions_available",
      propertyId,
      differences: [],
    };
  }

  if (!stagedPayload) {
    differences.push({
      area: "availability",
      severity: "info",
      message: "No staged report — published only",
    });
  }
  if (!publishedPayload) {
    differences.push({
      area: "availability",
      severity: "info",
      message: "No published report — staged only (review before publish)",
    });
  }

  if (stagedPayload && publishedPayload) {
    const sComp = summarizeCompetitive(stagedPayload);
    const pComp = summarizeCompetitive(publishedPayload);
    const sLead = sComp.leaderNames.join("|");
    const pLead = pComp.leaderNames.join("|");
    if (sLead !== pLead) {
      differences.push({
        area: "competitive_set",
        severity: "high",
        staged: sComp.leaderNames,
        published: pComp.leaderNames,
        message: "Leading competitors differ between staged and published",
      });
    }

    const sRg = summarizeRealityGap(stagedPayload);
    const pRg = summarizeRealityGap(publishedPayload);
    if (
      sRg.measurementStatus !== pRg.measurementStatus ||
      String(sRg.percent) !== String(pRg.percent) ||
      sRg.display !== pRg.display
    ) {
      differences.push({
        area: "reality_gap",
        severity: "high",
        staged: sRg,
        published: pRg,
        message: "Reality Gap differs between staged and published",
      });
    }

    const sEx = summarizeExecutive(stagedPayload);
    const pEx = summarizeExecutive(publishedPayload);
    if (sEx.headline !== pEx.headline || sEx.narrative !== pEx.narrative) {
      differences.push({
        area: "executive_narrative",
        severity: "medium",
        staged: sEx,
        published: pEx,
        message: "Executive narrative differs",
      });
    }

    const metricPaths = [
      "metrics.presenceIndex",
      "presenceIndex",
      "executiveSummary.presenceIndex",
      "displacement.primaryAlternative",
      "recommendations",
    ];
    for (const path of metricPaths) {
      const s = pick(stagedPayload, path);
      const p = pick(publishedPayload, path);
      if (JSON.stringify(s) !== JSON.stringify(p) && (s != null || p != null)) {
        differences.push({
          area: "metrics",
          path,
          severity: "medium",
          staged: s,
          published: p,
          message: `Field ${path} differs`,
        });
      }
    }

    // Boca contamination probe on published only
    const pubText = JSON.stringify(publishedPayload).toLowerCase();
    const stagedText = JSON.stringify(stagedPayload).toLowerCase();
    const bocaPub =
      pubText.includes("embassy suites") && pubText.includes("boca");
    const bocaStaged =
      stagedText.includes("embassy suites") && stagedText.includes("boca");
    if (bocaPub !== bocaStaged) {
      differences.push({
        area: "displacement_findings",
        severity: "critical",
        stagedHasBocaEmbassy: bocaStaged,
        publishedHasBocaEmbassy: bocaPub,
        message: "Boca / Embassy Suites contamination status differs",
      });
    }
  }

  return {
    ok: true,
    propertyId,
    staged: stagedMeta || { available: Boolean(stagedPayload) },
    published: publishedMeta || { available: Boolean(publishedPayload) },
    differenceCount: differences.length,
    differences,
  };
}
