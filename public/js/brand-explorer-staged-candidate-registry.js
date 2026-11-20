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
    },
    {
      id: 'rec9aZp7GHtzUEg0c',
      slug: 'ac-hotels-by-marriott',
      name: 'AC Hotels by Marriott',
      overlayUrl:
        '/brand-explorer-phase2e-preview/ac-hotels-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth-batch-five-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '551af8631699b8853647fd05a20795695580b43c1b3baec717cfbdd699b8d167'
    },
    {
      id: 'recahVIW4aCx0Ao84',
      slug: 'moxy-hotels',
      name: 'Moxy Hotels',
      overlayUrl:
        '/brand-explorer-phase2e-preview/moxy-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth-batch-five-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'a9426eee651806470f243444eae774d8bc3cccf60adc27136da7f0167a4a5978'
    },
    {
      id: 'recDwzv86TWnz2gGB',
      slug: 'vignette-collection',
      name: 'Vignette Collection',
      overlayUrl:
        '/brand-explorer-phase2e-preview/vignette-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth-batch-five-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '238e00056b023ccc591b83074570185516ad5718b6c34c38d2c48630cfb862eb'
    },
    {
      id: 'recsggfbKlJbjeRP9',
      slug: 'canopy-by-hilton',
      name: 'Canopy by Hilton',
      overlayUrl:
        '/brand-explorer-phase2e-preview/canopy-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth-batch-five-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '31b18d4e445e7e833153328cef1ad046894b867e205293e951166f68002f8794'
    },
    {
      id: 'recvvmiyReHhiKdoK',
      slug: 'even-hotels',
      name: 'EVEN Hotels',
      overlayUrl:
        '/brand-explorer-phase2e-preview/even-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth-batch-five-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'ec158fb22512e63bcadef73c243cd8817019613d755e66d0e3cd44320106ad6a'
    },
    {
      id: 'recwONQTqGU1jHCsM',
      slug: 'voco-hotels',
      name: 'voco Hotels',
      overlayUrl:
        '/brand-explorer-phase2e-preview/voco-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-3-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '89af22fb2f82f4ec630a2321010f416ee957c885568e4165f7dde58f929e8dcb'
    },
    {
      id: 'reclt44apoi8co0e6',
      slug: 'motto-by-hilton',
      name: 'Motto by Hilton',
      overlayUrl:
        '/brand-explorer-phase2e-preview/motto-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-3-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'eef91e2ed6949fe5f6210760ca57c42be4b87331c023381e66dc6670814441db'
    },
    {
      id: 'recoEarnE8T6sDjZq',
      slug: 'avid-hotels',
      name: 'avid hotels',
      overlayUrl:
        '/brand-explorer-phase2e-preview/avid-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-3-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '6220bbeacc1e6edc7c1ab81d24c45f2bc94d0b6efdba72b53405ee2e25a2352d'
    },
    {
      id: 'recCvV0PuZOi8c3hC',
      slug: 'tribute-portfolio',
      name: 'Tribute Portfolio',
      overlayUrl:
        '/brand-explorer-phase2e-preview/tribute-portfolio-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-3-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '75a9a65d7bb129327924a0b5c965a97345f4920dde67364ea049bf61449222d0'
    },
    {
      id: 'recegXrqaPiSLGCIe',
      slug: 'hotel-indigo',
      name: 'Hotel Indigo',
      overlayUrl:
        '/brand-explorer-phase2e-preview/hotel-indigo-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-3-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '018c4c0143741ed0817bb28b5ff91d72b6763d20a06ffd2abc15c11acac007c4'
    },
    {
      id: 'reccXxMHEh7NNRhIE',
      slug: 'tapestry-collection-by-hilton',
      name: 'Tapestry Collection by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/tapestry-collection-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e2f595a6c2d35da1333c838d0e840389e2624346b9f3c854a9f4bb198b7c4f1a'
    },
    {
      id: 'recqiHq3GHKMj8Meo',
      slug: 'tempo-by-hilton',
      name: 'Tempo by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/tempo-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9025151cd2e09b96b54ac75c5f034194cc553188d3bc8687f61ae12d87af6590'
    },
    {
      id: 'rec02zPClpWUTCyXM',
      slug: 'design-hotels',
      name: 'Design Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/design-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'be8a61c49129a293822275f496a5f2e34ddcdeff3a4daeedcb6db0a9244b5418'
    },
    {
      id: 'recJ1GZQpttX7qHgw',
      slug: 'aloft-hotels',
      name: 'Aloft Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/aloft-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3b6218b8cd7844493a8fe5bac4852fdc23a8ca2053de8557ac8ba2303c3e92a4'
    },
    {
      id: 'recCKuXCmGvxHPfb3',
      slug: 'kimpton-hotels',
      name: 'Kimpton Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/kimpton-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '49e624b0a59143dc6f051fe14e7c99287b6acc92c54e458877f1ff6c90dd7fc9'
    },
    {
      id: 'recGv268Wda31PlSZ',
      slug: 'bunkhouse-hotels',
      name: 'Bunkhouse Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/bunkhouse-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'ef4a952c2cb82a89f40637429666b9bdf2ca57d70c04672a0ea7733a50656c4d'
    },
    {
      id: 'recNy2efMm4N1JtgC',
      slug: 'hyatt-centric',
      name: 'Hyatt Centric',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-centric-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'a117a06519b56319f54557aa72f8c028b3a168c3020c2112b43bc52588e67e33'
    },
    {
      id: 'rec6hye5H8zJmAGv3',
      slug: 'courtyard-by-marriott',
      name: 'Courtyard by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/courtyard-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3500dc7289a8808fd5f1f0d482550ae6568173fdb984f2b0adf8c54ca2936daa'
    },
    {
      id: 'recIPuBC50fv13zRR',
      slug: 'westin',
      name: 'Westin',
      overlayUrl: '/brand-explorer-phase2e-preview/westin-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '6adeb132751e08f8ac6d59d6f6fead08c570946f74eddbe1fe5c99c8b1ed49e9'
    },
    {
      id: 'recmGmiIqDtAsm01f',
      slug: 'holiday-inn-express',
      name: 'Holiday Inn Express',
      overlayUrl: '/brand-explorer-phase2e-preview/holiday-inn-express-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '207bed048829d783095eb4415d38b82121acfb1e8ad0bf1b133e15d47708d9d4'
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
    if (/autograph/i.test(name)) return bySlug('autograph-collection');
    if (/^ac hotels/i.test(name) || /\bac hotels by marriott\b/i.test(name))
      return bySlug('ac-hotels-by-marriott');
    if (/^moxy/i.test(name)) return bySlug('moxy-hotels');
    if (/vignette/i.test(name)) return bySlug('vignette-collection');
    if (/canopy/i.test(name)) return bySlug('canopy-by-hilton');
    if (/^even\b/i.test(name)) return bySlug('even-hotels');
    if (/^voco/i.test(name)) return bySlug('voco-hotels');
    if (/motto/i.test(name)) return bySlug('motto-by-hilton');
    if (/^avid\b/i.test(name)) return bySlug('avid-hotels');
    if (/tribute/i.test(name)) return bySlug('tribute-portfolio');
    if (/hotel indigo/i.test(name)) return bySlug('hotel-indigo');
    if (/tapestry/i.test(name)) return bySlug('tapestry-collection-by-hilton');
    if (/^tempo\b/i.test(name)) return bySlug('tempo-by-hilton');
    if (/design hotels/i.test(name)) return bySlug('design-hotels');
    if (/^aloft\b/i.test(name)) return bySlug('aloft-hotels');
    if (/kimpton/i.test(name)) return bySlug('kimpton-hotels');
    if (/bunkhouse/i.test(name)) return bySlug('bunkhouse-hotels');
    if (/hyatt centric/i.test(name)) return bySlug('hyatt-centric');
    if (/courtyard/i.test(name)) return bySlug('courtyard-by-marriott');
    if (/^westin\b/i.test(name)) return bySlug('westin');
    if (/holiday inn express/i.test(name)) return bySlug('holiday-inn-express');
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
