/**
 * Browser mirror — Dealality Insight advisory quality / internal-language ban.
 */
(function (global) {
  'use strict';

  var BE_DEALALITY_INSIGHT_ADVISORY_V1 = 'BE_DEALALITY_INSIGHT_ADVISORY_V1';

  var INTERNAL_RES = [
    /\bHPC\s+Open\s+hotels\s+matched\b/i,
    /\bRegional\s+HPC\s+counts\b/i,
    /\bHPC\s+(?:open\s+)?(?:footprint|inventory|counts?|rows?)\b/i,
    /\bkeys\s*=\s*\d+/i,
    /\bcertState\b/i,
    /\bLIVE_UNVERIFIED_INVENTORY\b/i,
    /\bsteward[- ]confirmed\b/i,
    /\bkeep\s+narrative\s+HPC[- ]qualified\b/i,
    /\bnamed\s+openings\s+need\s+steward\b/i,
    /\bno\s+fabricated\s+property\s+names\b/i,
    /\bverify\s+certState\s+before\s+customer\s+publish\b/i,
    /\bnot verified in this pack\b/i,
    /\bdo not infer brand-wide totals\b/i,
    /\bCoverage limited\b/i,
    /\bRESEARCH_REQUIRED\b/,
    /\bfield\s+mapping\b/i,
    /\bphase2[a-z]?\b/i,
    /\bslotKey\b/i,
    /\bTODO_RESEARCH\b/i,
    /\bfixture\b/i,
    /\bcandidate\s+overlay\b/i,
    /\bFocus\s+this\s+card\b/i,
    /\bEvidence-grounded\s+reconstruction\b/i,
    /\brec[A-Za-z0-9]{10,}\b/
  ];

  var CUSTOMER_FACING_RE =
    /^(?:insight\.|overview\.|valueOwners\.|commercial\.|loyalty\.|footprint\.|materials\.|economics\.|operations\.|standards\.|metric\.)/i;

  var ASSESSMENT_SECTIONS = [
    { slotKey: 'insight.assessment.overall', title: 'Overall Brand Assessment' },
    { slotKey: 'insight.assessment.opportunity', title: 'Where We See the Greatest Opportunity' },
    { slotKey: 'insight.assessment.advantages', title: 'Principal Competitive Advantages' },
    { slotKey: 'insight.assessment.fit', title: 'Ideal Owner, Asset & Operator Fit' },
    { slotKey: 'insight.assessment.commercial', title: 'Commercial & Investment Considerations' },
    { slotKey: 'insight.assessment.perspective', title: 'Dealality Perspective' }
  ];

  var PACKS = {
    curio: {
      brandId: 'receQkxgjlezsc1xg',
      matchNameRe: /curio\s+collection/i,
      blocks: [
        {
          slotKey: 'footprint.editorial',
          title: 'Dealality View on Market Presence',
          body:
            'Curio reads as a conversion-weighted upper-upscale soft collection: owners keep a distinctive property story while plugging into Hilton Honors demand and Hilton commercial systems. Treat regional presence as directional for underwriting—not as a substitute for market-level competitive-set work. The most useful footprint signal for owners is whether the asset’s positioning, F&B scope, and design narrative can sustain Curio’s collection standard in that corridor.'
        },
        {
          slotKey: 'footprint.editorial_bullets',
          title: 'Market Presence Themes',
          body:
            'Conversion and repositioning opportunities in character-rich urban and destination assets\nCulinary-forward F&B and design narrative as operating differentiators—not optional cosmetics\nHilton Honors member demand as a distribution advantage when the property product earns the rate\nFlexible design story relative to hard brands—still with QA, brand standards, and commercial discipline'
        },
        {
          slotKey: 'insight.assessment.overall',
          title: 'Overall Brand Assessment',
          body:
            'Curio sits in Hilton’s upper-upscale soft-collection lane: conversion-friendly, design-flexible, and commercially tied to Hilton Honors. For owners, attractiveness comes from combining a distinctive asset story with system distribution—not from standardizing the product into a hard-brand template. The brand is worth shortlisting when the property already has (or can earn) a credible lifestyle narrative and the market will pay for that positioning.'
        },
        {
          slotKey: 'insight.assessment.opportunity',
          title: 'Where We See the Greatest Opportunity',
          body:
            'Greatest owner value tends to appear in character-rich conversions, premium urban repositioning, destination and experiential resorts, and heritage or adaptive-reuse assets where individuality is the commercial thesis. Opportunity is strongest when distribution lift and design credibility both matter—and weaker when the asset lacks a story the collection can amplify.'
        },
        {
          slotKey: 'insight.assessment.advantages',
          title: 'Principal Competitive Advantages',
          body:
            'Relative to peer soft collections, Curio’s practical advantages are Hilton Honors demand interoperability, Hilton commercial and reservations infrastructure, and conversion latitude that preserves property identity. Those advantages matter most to owners who need member-driven demand and system reliability without surrendering the asset’s distinctive positioning.'
        },
        {
          slotKey: 'insight.assessment.fit',
          title: 'Ideal Owner, Asset & Operator Fit',
          body:
            'Best fit: sponsors underwriting upper-upscale lifestyle economics; assets with authentic public spaces, design narrative, and F&B complexity; operators with branded or lifestyle experience and reporting discipline. Poor fit: assets without identity, markets that will not reward premium positioning, operators expecting total design freedom without QA guardrails, or structures that treat Curio like select-service economics.'
        },
        {
          slotKey: 'insight.assessment.commercial',
          title: 'Commercial & Investment Considerations',
          body:
            'Owners should evaluate Hilton Honors contribution versus retail/OTA mix, affiliation and program costs in current disclosure, design and F&B PIP capital, labor intensity of culinary-forward operations, and competitive set versus other Hilton soft brands in-market. Do not invent fee schedules or ROI claims—confirm commercial terms and ramp assumptions in the deal file before capital allocation.'
        },
        {
          slotKey: 'insight.assessment.perspective',
          title: 'Dealality Perspective',
          body:
            'Curio is a compelling choice when individuality plus Hilton system access is the investment thesis, and when the owner can fund and operate the product standard that thesis requires. The conditions that matter most are market rate support, honest conversion capital, operator capability, and clear commercial agreement economics—not brochure geography alone.'
        }
      ]
    },
    ascend: {
      brandId: 'reclkgOzvAcBheUSo',
      matchNameRe: /ascend\s+hotel\s+collection/i,
      blocks: [
        {
          slotKey: 'footprint.editorial',
          title: 'Dealality View on Market Presence',
          body:
            'Ascend Hotel Collection is best read as Choice’s upscale soft-collection path for distinctive conversions and independent-minded assets. Market presence should inform corridor diligence, not replace property-level competitive-set analysis. Owners should judge whether the asset’s identity and service model can earn Ascend’s collection positioning in that market.'
        },
        {
          slotKey: 'footprint.editorial_bullets',
          title: 'Market Presence Themes',
          body:
            'Soft-collection conversions that preserve local character\nChoice Privileges member demand as a distribution lever when product quality holds\nFlexible design narrative versus hard-brand prototypes\nOwner diligence still required on fees, standards, and competitive set in-market'
        },
        {
          slotKey: 'insight.assessment.overall',
          title: 'Overall Brand Assessment',
          body:
            'Ascend offers an upscale soft-collection affiliation for properties that need retained character with Choice Hotels distribution and Choice Privileges participation. Attractiveness depends on whether the asset’s story and operating model can sustain collection positioning—and whether Choice system access materially improves the owner’s demand and channel mix.'
        },
        {
          slotKey: 'insight.assessment.opportunity',
          title: 'Where We See the Greatest Opportunity',
          body:
            'Opportunity concentrates in conversions and repositionings where local character is the commercial edge: boutique urban assets, adaptive reuse, and distinctive regional hotels that benefit from loyalty and reservations reach without a hard-brand prototype. Value is weaker when the property lacks a differentiating story.'
        },
        {
          slotKey: 'insight.assessment.advantages',
          title: 'Principal Competitive Advantages',
          body:
            'Practical advantages include soft-collection design latitude, Choice distribution infrastructure, and Choice Privileges member demand. These matter most to owners who need system reach while keeping a property-specific guest narrative—and who can meet collection standards without over-capitalizing relative to market ADR.'
        },
        {
          slotKey: 'insight.assessment.fit',
          title: 'Ideal Owner, Asset & Operator Fit',
          body:
            'Best fit: owners seeking soft-brand flexibility; assets with authentic identity; operators comfortable with upscale service expectations and brand QA. Poor fit: commodity boxes without story, markets that will not support soft-collection pricing, or operators expecting hard-brand plug-and-play simplicity.'
        },
        {
          slotKey: 'insight.assessment.commercial',
          title: 'Commercial & Investment Considerations',
          body:
            'Evaluate loyalty contribution, affiliation economics in current disclosure, conversion PIP scope, operating complexity versus ADR support, and competitive alternatives in the soft-collection set. Confirm fees and standards in deal materials—do not rely on generic marketing claims or invented performance lifts.'
        },
        {
          slotKey: 'insight.assessment.perspective',
          title: 'Dealality Perspective',
          body:
            'Ascend is most compelling when local character plus Choice system access is the thesis, and when capital and operating plans match that promise. The decisive conditions are product authenticity, market rate support, operator capability, and clear commercial terms.'
        }
      ]
    }
  };

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function containsInternal(text) {
    var t = nz(text);
    if (!t) return false;
    for (var i = 0; i < INTERNAL_RES.length; i++) {
      if (INTERNAL_RES[i].test(t)) return true;
    }
    return false;
  }

  function sanitizeCustomerFacingText(text) {
    var t = nz(text);
    if (!t) return '';
    if (containsInternal(t)) return '';
    return t;
  }

  function resolvePack(brand) {
    var id = nz(brand && (brand.id || brand.brandId));
    var name = nz(brand && (brand.name || brand.brandName));
    var keys = Object.keys(PACKS);
    for (var i = 0; i < keys.length; i++) {
      var pack = PACKS[keys[i]];
      if (id && pack.brandId === id) return pack;
      if (name && pack.matchNameRe.test(name)) return pack;
    }
    return null;
  }

  function applyDealalityInsightAdvisoryToBlocks(blocks, opts) {
    opts = opts || {};
    var list = Array.isArray(blocks)
      ? blocks.map(function (b) {
          var next = {};
          Object.keys(b).forEach(function (k) {
            next[k] = b[k];
          });
          return next;
        })
      : [];
    var changed = [];
    var internalEvidence = [];
    list.forEach(function (b) {
      var slotKey = nz(b.slotKey || b.slot);
      if (!CUSTOMER_FACING_RE.test(slotKey)) return;
      if (!containsInternal((b.title || '') + '\n' + (b.body || ''))) return;
      internalEvidence.push({ slotKey: slotKey, rejectedBody: nz(b.body) });
      b.body = '';
      b.__dealalityInsightAdvisory = { action: 'stripped_internal_language' };
      changed.push({ slotKey: slotKey, reason: 'internal_technical_language_leakage' });
    });
    var pack = resolvePack(opts.brand || {});
    if (pack) {
      pack.blocks.forEach(function (row) {
        var idx = -1;
        for (var i = 0; i < list.length; i++) {
          if (nz(list[i].slotKey) === row.slotKey) {
            idx = i;
            break;
          }
        }
        var prevBody = idx >= 0 ? nz(list[idx].body) : '';
        var isAssessment = /^insight\.assessment\./.test(row.slotKey);
        var prevWeak =
          !prevBody ||
          containsInternal(prevBody) ||
          /Brand Footprint-scoped|Absence in one region is not a global zero|HPC Open hotels matched/i.test(
            prevBody
          ) ||
          prevBody.length < 80;
        if (!isAssessment && !prevWeak) return;
        if (prevBody === row.body) return;
        var next = idx >= 0 ? list[idx] : { sort: list.length + 1 };
        next.slotKey = row.slotKey;
        next.title = row.title;
        next.body = row.body;
        next.__dealalityInsightAdvisory = { action: 'applied_advisory_pack' };
        if (idx >= 0) list[idx] = next;
        else list.push(next);
        changed.push({
          slotKey: row.slotKey,
          reason: 'advisory_pack_applied',
          from: prevBody || null,
          to: row.body
        });
      });
    }
    return {
      contract: BE_DEALALITY_INSIGHT_ADVISORY_V1,
      blocks: list,
      changed: changed,
      changedCount: changed.length,
      internalEvidence: internalEvidence,
      packBrandId: pack ? pack.brandId : null
    };
  }

  global.BeDealalityInsightAdvisory = {
    BE_DEALALITY_INSIGHT_ADVISORY_V1: BE_DEALALITY_INSIGHT_ADVISORY_V1,
    ASSESSMENT_SECTIONS: ASSESSMENT_SECTIONS,
    containsInternalCustomerLanguage: containsInternal,
    sanitizeCustomerFacingText: sanitizeCustomerFacingText,
    applyDealalityInsightAdvisoryToBlocks: applyDealalityInsightAdvisoryToBlocks,
    isCustomerFacingAdvisorySlot: function (slotKey) {
      return CUSTOMER_FACING_RE.test(nz(slotKey));
    }
  };
})(typeof globalThis !== 'undefined' ? globalThis : window);
