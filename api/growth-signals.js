/**
 * CALA submarket growth signals API — owner/brand early-entry indicators.
 */

import {
  listGrowthProfiles,
  listGrowthSignals,
  getGrowthProfile,
  getGrowthProfilesForCountry,
  buildGrowthSignalCoverageSummary,
  GROWTH_SIGNAL_TYPES,
} from "../lib/radar-buildout/growth-signals/index.js";

export async function getGrowthSignalsSummary(req, res) {
  try {
    const summary = buildGrowthSignalCoverageSummary();
    return res.json({ ok: true, summary });
  } catch (err) {
    console.error("[growth-signals] summary error", err?.message || err);
    return res.status(500).json({ ok: false, error: "growth_signals_failed", message: err?.message });
  }
}

export async function getGrowthSignalTypes(req, res) {
  return res.json({ ok: true, signalTypes: GROWTH_SIGNAL_TYPES });
}

export async function getGrowthSignalsCountries(req, res) {
  try {
    const country = req.query.country || "";
    const signalType = req.query.signalType || "";
    const profileStatus = req.query.profileStatus || "";

    const profiles = listGrowthProfiles({
      country: country || undefined,
      signalType: signalType || undefined,
      profileStatus: profileStatus || undefined,
    });

    return res.json({
      ok: true,
      totalCount: profiles.length,
      profiles,
    });
  } catch (err) {
    console.error("[growth-signals] list error", err?.message || err);
    return res.status(500).json({ ok: false, error: "growth_signals_failed", message: err?.message });
  }
}

export async function getGrowthSignalsCountryDetail(req, res) {
  try {
    const country = decodeURIComponent(req.params.country || "").trim();
    if (!country) {
      return res.status(400).json({ ok: false, error: "validation_failed", message: "country required" });
    }

    const profiles = getGrowthProfilesForCountry(country);
    if (!profiles.length) {
      return res.status(404).json({ ok: false, error: "not_found", message: `No growth profiles for ${country}` });
    }

    const signals = listGrowthSignals({ country });

    return res.json({
      ok: true,
      country,
      profileCount: profiles.length,
      signalCount: signals.length,
      profiles,
      signals,
    });
  } catch (err) {
    console.error("[growth-signals] country detail error", err?.message || err);
    return res.status(500).json({ ok: false, error: "growth_signals_failed", message: err?.message });
  }
}

export async function getGrowthSignalsSubmarket(req, res) {
  try {
    const country = decodeURIComponent(req.params.country || "").trim();
    const submarket = decodeURIComponent(req.params.submarket || "").trim();
    if (!country || !submarket) {
      return res.status(400).json({
        ok: false,
        error: "validation_failed",
        message: "country and submarket required",
      });
    }

    const profile = getGrowthProfile(country, submarket);
    if (!profile) {
      return res.status(404).json({
        ok: false,
        error: "not_found",
        message: `No growth profile for ${country} / ${submarket}`,
      });
    }

    return res.json({ ok: true, profile });
  } catch (err) {
    console.error("[growth-signals] submarket error", err?.message || err);
    return res.status(500).json({ ok: false, error: "growth_signals_failed", message: err?.message });
  }
}

export async function getGrowthSignalsFlat(req, res) {
  try {
    const country = req.query.country || "";
    const signalType = req.query.signalType || "";
    const earlyEntry = req.query.earlyEntryOpportunity || "";

    let signals = listGrowthSignals({
      country: country || undefined,
      signalType: signalType || undefined,
    });

    if (earlyEntry) {
      signals = signals.filter((s) => s.earlyEntryOpportunity === earlyEntry);
    }

    return res.json({ ok: true, totalCount: signals.length, signals });
  } catch (err) {
    console.error("[growth-signals] flat list error", err?.message || err);
    return res.status(500).json({ ok: false, error: "growth_signals_failed", message: err?.message });
  }
}
