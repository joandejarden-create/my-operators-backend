/**
 * Market Alerts stakeholder + on-demand Surfe contact API.
 *
 * GET  /contacts          — persisted stakeholder intelligence only
 * POST /contacts/find-person — user-triggered Surfe people search; identity only
 * POST /contacts/reveal      — user-triggered Surfe enrich; ephemeral details; no persist
 *
 * Legacy POST /contacts/enrich is disabled for auto Surfe (returns guidance).
 */

import {
  identifyAlertStakeholders,
  toStakeholderUiCard,
} from "../lib/market-alerts-contact/identify-stakeholders.js";
import {
  listStakeholdersForAlert,
  persistIdentificationResult,
} from "../lib/market-alerts-contact/stakeholder-airtable.js";
import {
  findDecisionMakerOnDemand,
  revealContactDetailsOnDemand,
} from "../lib/market-alerts-contact/on-demand.js";
import { isContactEnrichmentEnabled } from "../lib/market-alerts-contact/eligibility.js";
import { IDENTIFICATION_STATUS } from "../lib/market-alerts-contact/stakeholder-schema.js";
import {
  loadMarketAlertById,
} from "../lib/market-alerts-contact/alert-from-airtable.js";
import {
  ensureArticleBodyForStakeholderExtraction,
} from "../lib/market-alerts-contact/fetch-article-body.js";
import { logContactOp } from "../lib/market-alerts-contact/safe-log.js";

/**
 * Load alert + resolve article body for person extraction (no Surfe).
 */
async function loadAlertWithArticleBody(alertId) {
  const loaded = await loadMarketAlertById(alertId);
  if (!loaded) return null;
  const ensured = await ensureArticleBodyForStakeholderExtraction(loaded);
  return {
    ...ensured.alert,
    stakeholderExtractionSource: ensured.stakeholderExtractionSource,
    _articleBodyFetched: ensured.fetched,
    _articleBodyChars: ensured.bodyChars,
  };
}

function getUserId(req) {
  if (req.user && (req.user.id || req.user.email)) return req.user.id || req.user.email;
  if (req.headers["x-user-id"]) return String(req.headers["x-user-id"]);
  return null;
}

function alertFromBody(alertId, body = {}) {
  return {
    id: alertId,
    alertId,
    title: body.title || "",
    summary: body.summary || "",
    entityKey: body.entityKey || null,
    sourceUrl: body.sourceUrl || null,
    category: body.category || null,
    eventType: body.eventType || null,
    signalType: body.signalType || null,
    worthReviewing: body.worthReviewing,
    actionable: body.actionable,
    ownerDeveloper: body.ownerDeveloper || null,
    hotelProject: body.hotelProject || null,
    brandInvolved: body.brandInvolved || null,
    operatorInvolved: body.operatorInvolved || null,
    intelligence: body.intelligence || {
      eventType: body.eventType,
      signalType: body.signalType,
      worthReviewing: body.worthReviewing,
      actionable: body.actionable,
      entities: {
        ownerDeveloper: body.ownerDeveloper,
        hotelProject: body.hotelProject,
        brandInvolved: body.brandInvolved,
        operatorInvolved: body.operatorInvolved,
      },
    },
  };
}

/**
 * GET /api/market-alerts/:id/contacts
 * Stakeholder intelligence only — never Surfe contact details.
 */
export async function getMarketAlertContacts(req, res) {
  try {
    const alertId = req.params?.id;
    if (!alertId) {
      return res.status(400).json({ ok: false, error: "missing_alert_id" });
    }

    let rows = await listStakeholdersForAlert(alertId);
    let identificationStatus = IDENTIFICATION_STATUS.NOT_APPLICABLE;
    let extractionSource = null;
    let extractionStatus = null;

    if (!rows.length) {
      const alert = await loadAlertWithArticleBody(alertId);
      if (alert) {
        const identification = identifyAlertStakeholders(alert);
        identificationStatus = identification.identificationStatus;
        extractionSource =
          identification.stakeholderExtractionSource || alert.stakeholderExtractionSource || null;
        extractionStatus = identification.stakeholderExtractionStatus || null;
        if (identification.moduleVisible && identification.stakeholders.length) {
          await persistIdentificationResult(identification, { dryRun: false });
          rows = await listStakeholdersForAlert(alertId);
          if (!rows.length) rows = identification.stakeholders;
        } else if (!identification.moduleVisible) {
          return res.json({
            ok: true,
            alertId,
            enrichmentEnabled: isContactEnrichmentEnabled(),
            moduleVisible: false,
            identificationStatus,
            stakeholders: [],
            contacts: [],
            pending: false,
            surfeContactDetails: null,
            skipReason: identification.skipReason || null,
            stakeholderExtractionSource: extractionSource,
            stakeholderExtractionStatus: extractionStatus,
          });
        }
      }
    }

    const stakeholders = rows.map(toStakeholderUiCard);
    const primary = stakeholders.find((s) => s.selectedPrimary) || stakeholders[0] || null;
    const moduleVisible = stakeholders.length > 0;

    return res.json({
      ok: true,
      alertId,
      enrichmentEnabled: isContactEnrichmentEnabled(),
      moduleVisible,
      identificationStatus:
        primary?.identificationStatus || identificationStatus || IDENTIFICATION_STATUS.NOT_APPLICABLE,
      stakeholders,
      contacts: stakeholders,
      pending: primary?.contactLookupStatus === "PENDING",
      surfeContactDetails: null,
      stakeholderExtractionSource: extractionSource,
      stakeholderExtractionStatus: extractionStatus,
    });
  } catch (err) {
    logContactOp("api_get_contacts_error", { message: String(err?.message || err).slice(0, 80) });
    return res.status(500).json({ ok: false, error: "contacts_read_failed" });
  }
}

/**
 * POST /api/market-alerts/:id/contacts/find-person
 */
export async function postMarketAlertFindPerson(req, res) {
  try {
    const userId = getUserId(req);
    if (!userId && process.env.NODE_ENV === "production") {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }
    const alertId = req.params?.id;
    const body = req.body || {};
    let alert = alertFromBody(alertId, body);
    if (!body.title && !body.summary) {
      const loaded = await loadAlertWithArticleBody(alertId);
      if (loaded) alert = { ...loaded, ...alert, id: alertId };
    } else if (alert.sourceUrl && !alert.articleBody) {
      const ensured = await ensureArticleBodyForStakeholderExtraction(alert);
      alert = { ...ensured.alert, stakeholderExtractionSource: ensured.stakeholderExtractionSource };
    }
    const result = await findDecisionMakerOnDemand(alert, {
      stakeholderId: body.stakeholderId || null,
    });
    if (!result.ok && result.error === "CONTACT_ENRICHMENT_DISABLED") {
      return res.status(403).json(result);
    }
    return res.json({
      ...result,
      stakeholders: result.stakeholder ? [toStakeholderUiCard(result.stakeholder)] : [],
    });
  } catch (err) {
    logContactOp("api_find_person_error", { message: String(err?.message || err).slice(0, 80) });
    return res.status(500).json({ ok: false, error: "find_person_failed" });
  }
}

/**
 * POST /api/market-alerts/:id/contacts/reveal
 * Ephemeral Surfe contact details — never persisted.
 */
export async function postMarketAlertContactReveal(req, res) {
  try {
    const userId = getUserId(req);
    if (!userId && process.env.NODE_ENV === "production") {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }
    const alertId = req.params?.id;
    const body = req.body || {};
    let alert = alertFromBody(alertId, body);
    if (!body.title && !body.summary) {
      const loaded = await loadAlertWithArticleBody(alertId);
      if (loaded) alert = { ...loaded, ...alert, id: alertId };
    }
    const result = await revealContactDetailsOnDemand(alert, {
      stakeholderId: body.stakeholderId || null,
    });
    if (!result.ok && result.error === "CONTACT_ENRICHMENT_DISABLED") {
      return res.status(403).json(result);
    }
    // Explicit: response may include contactDetails for this request only
    return res.json({
      ok: result.ok,
      ephemeral: true,
      persisted: false,
      pending: result.pending === true,
      miss: result.miss === true,
      message: result.message || null,
      error: result.error || null,
      contactDetails: result.contactDetails || null,
      surfeCalls: result.surfeCalls || 0,
    });
  } catch (err) {
    logContactOp("api_reveal_error", { message: String(err?.message || err).slice(0, 80) });
    return res.status(500).json({ ok: false, error: "reveal_failed" });
  }
}

/**
 * Legacy enrich — identify + persist stakeholders only (zero Surfe).
 * Does not call Surfe. Use find-person / reveal for provider actions.
 */
export async function postMarketAlertContactEnrich(req, res) {
  try {
    const userId = getUserId(req);
    if (!userId && process.env.NODE_ENV === "production") {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }
    const alertId = req.params?.id;
    const body = req.body || {};
    let alert = alertFromBody(alertId, body);
    if (!body.title && !body.summary) {
      const loaded = await loadAlertWithArticleBody(alertId);
      if (loaded) alert = { ...loaded, id: alertId };
    } else if (alert.sourceUrl && !alert.articleBody) {
      const ensured = await ensureArticleBodyForStakeholderExtraction(alert);
      alert = { ...ensured.alert, stakeholderExtractionSource: ensured.stakeholderExtractionSource };
    }
    const identification = identifyAlertStakeholders(alert);
    if (identification.moduleVisible && identification.stakeholders.length) {
      await persistIdentificationResult(identification, { dryRun: false });
    }
    const rows = await listStakeholdersForAlert(alertId);
    return res.json({
      ok: true,
      alertId,
      surfeCalls: 0,
      note: "Legacy enrich maps to stakeholder identification only. Use /find-person or /reveal for Surfe.",
      identificationStatus: identification.identificationStatus,
      stakeholders: (rows.length ? rows : identification.stakeholders).map(toStakeholderUiCard),
      contacts: (rows.length ? rows : identification.stakeholders).map(toStakeholderUiCard),
    });
  } catch (err) {
    logContactOp("api_enrich_error", { message: String(err?.message || err).slice(0, 80) });
    return res.status(500).json({ ok: false, error: "enrich_failed" });
  }
}
