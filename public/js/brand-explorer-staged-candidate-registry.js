/**
 * Canonical staged Brand Explorer candidates for founder product preview
 * and Comparison Studio RIGHT. Latest cumulative Phase 2H only — no silent
 * Phase 2F / 2G / 2D fallback.
 */
(function (global) {
  'use strict';

  var CONTRACT = 'BE_STAGED_CANDIDATE_REGISTRY_V1';

  var CANDIDATES = [
    {
      id: 'receQkxgjlezsc1xg',
      slug: 'curio-collection',
      name: 'Curio Collection by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json',
      scorecardUrl: '/brand-explorer-phase2e-preview/curio-collection-phase2h-scorecard.json',
      baselineUrl: '/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json',
      manifestUrl: '/brand-explorer-phase2e-preview/curio-collection-staged-candidate-manifest-v1.json',
      candidateVersion: 'phase2h-component-parity',
      source: 'phase2h-cumulative-staged',
      // Presentation writeback applied 2026-11-20 — Current Saved is authoritative.
      founderApprovalStatus: 'FOUNDER_APPROVED_WRITEBACK'
    },
    {
      id: 'reclkgOzvAcBheUSo',
      slug: 'ascend',
      name: 'Ascend Hotel Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/ascend-phase2h-candidate.json',
      scorecardUrl: '/brand-explorer-phase2e-preview/ascend-phase2h-scorecard.json',
      baselineUrl: null,
      manifestUrl: '/brand-explorer-phase2e-preview/ascend-staged-candidate-manifest-v1.json',
      candidateVersion: 'phase2h-component-parity',
      source: 'phase2h-cumulative-staged',
      founderApprovalStatus: 'FOUNDER_APPROVED_WRITEBACK'
    },
    {
      id: 'recEJCTDj1zrsjPM6',
      slug: 'autograph-collection',
      name: 'Autograph Collection',
      // Immutable assembled approval payload (CURRENT+overlay freeze) — not the gap-fill overlay alone.
      overlayUrl:
        '/brand-explorer-phase2e-preview/autograph-collection-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: '/brand-explorer-phase2e-preview/autograph-collection-staged-candidate-manifest-v1.json',
      candidateVersion: 'phase2h-component-parity',
      source: 'phase2h-factory-1-0-frozen-approval',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash:
        '43c473e65fa26df531ec4886a8f7373992fff0a335f2cc94c3205898cf9ba7e6'
    }
  ];

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function normSlug(s) {
    return nz(s)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
  }

  function list() {
    return CANDIDATES.slice();
  }

  function byId(id) {
    var want = nz(id);
    if (!want) return null;
    for (var i = 0; i < CANDIDATES.length; i++) {
      if (CANDIDATES[i].id === want) return Object.assign({}, CANDIDATES[i]);
    }
    return null;
  }

  function bySlug(slug) {
    var want = normSlug(slug);
    if (!want) return null;
    for (var i = 0; i < CANDIDATES.length; i++) {
      if (normSlug(CANDIDATES[i].slug) === want) return Object.assign({}, CANDIDATES[i]);
    }
    return null;
  }

  function resolveForBrand(brand) {
    if (!brand) return null;
    var id =
      nz(brand.id) ||
      nz(brand.recordId) ||
      nz(brand.brandBasicsRecordId) ||
      nz(brand.airtableRecordId);
    var hit = byId(id);
    if (hit) return hit;
    var slug = nz(brand.slug) || nz(brand.brandSlug) || nz(brand.brand_slug);
    hit = bySlug(slug);
    if (hit) return hit;
    var name = nz(brand.name) || nz(brand.brandName);
    if (/curio/i.test(name)) return bySlug('curio-collection');
    if (/ascend/i.test(name)) return bySlug('ascend');
    return null;
  }

  global.BrandExplorerStagedCandidateRegistry = {
    CONTRACT: CONTRACT,
    list: list,
    byId: byId,
    bySlug: bySlug,
    resolveForBrand: resolveForBrand
  };
})(typeof window !== 'undefined' ? window : globalThis);
