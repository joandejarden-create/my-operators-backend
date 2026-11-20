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
      approvalContentHash: 'a6a3d188f4ec7937e6a7a81ac5bd9b525e9058db65c59cc403dc1bdae1c5a72a'
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
      approvalContentHash: '56ddf490d08f4f43d3d0b144b1e3f1dce93876577e8d361051f8abe5c6836b34'
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
      approvalContentHash: '720302e62e22fd935fc2ff45becb472b2456180c3891ff29c00a13b8698c21ec'
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
      approvalContentHash: 'dea23c4dc37ed74289897ee94e5b21d8112d8055202a5a81a92f03c6a14ba06b'
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
      approvalContentHash: 'aac8348e1e6eee92ed4ce2fd798be63c7f06c235903c204e35ddbc5bbaca79f1'
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
      approvalContentHash: '44f02c5480205fd1828f40c34ef462ee5c7addaf59e5a771df029f31bb41b82d'
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
      approvalContentHash: '94d407e5c5cd9ebc8606213fde7a5393179483ac10aedf84f7d0b9f1fd8938c0'
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
      approvalContentHash: 'aa7e867e7e8634be2ee2473336fce02412884e391a264a81204e502e449161a3'
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
      approvalContentHash: 'e8dbc4e3db9666f2de4911228d0436b6b5298b2e478645d2a979704b72ec82dd'
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
      approvalContentHash: 'f675515f19d26b7027d2376cbe8398835e3fb07200a0a7d2064549ceb6c18653'
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
      approvalContentHash: '7e530a42515a669c69f8a8bca85775e188cfcaa42d8672ea8da4b1df18b4b93b'
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
      approvalContentHash: 'ee5a527a1cbc7b36e2f6165edf89b90cf328911a36b20d7c837516e7eab557c1'
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
      approvalContentHash: 'ed46cc0d0a8a410bd3260e61ef0f873c0ee27aad5886b203e36cf5427d37678b'
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
      approvalContentHash: '97a4f7d3b31c1c592d52461d9a2658ae0f60075bf2a24aae42e4d1277c9c6f91'
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
      approvalContentHash: '00cf67072b29b9a355ddce281bf7994ba3fe75f29f95cf2d5b6c9d81f5e79645'
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
      approvalContentHash: '5b814817561437d2acbf2041f0ad15af921b4f263f782deaf356269cb9b2745c'
    },
    {
      id: 'recj5BCSpWqMEEBDg',
      slug: 'jw-marriott',
      name: 'JW Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/jw-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'f661c492b3fd06498d512763c7840159e4ce7b02b0c71bc0fd5e45e45f4194f3'
    },
    {
      id: 'recqBiokz5ePxbFTE',
      slug: 'w-hotels',
      name: 'W Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/w-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9fc1dad57cfe92947127d898343a618588d33ec169e9991d5f1c15d9d7d11497'
    },
    {
      id: 'recN6sas0Km17O2Yn',
      slug: 'le-meridien',
      name: 'Le Méridien',
      overlayUrl: '/brand-explorer-phase2e-preview/le-meridien-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e1ef42247fef9496631e376d2fd40cffb10222a80128a1fbcedf6efd1d1e6df7'
    },
    {
      id: 'recXopdFodrD3IyIR',
      slug: 'waldorf-astoria',
      name: 'Waldorf Astoria Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/waldorf-astoria-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'be361c9f0a2dabe2ba0506bc67e840d47cbbbd4a99d05e811a2864a3ae740131'
    },
    {
      id: 'rec90n1dSVVyoEOto',
      slug: 'conrad-hotels-resorts',
      name: 'Conrad Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/conrad-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c4b0735f5b7c63e879bcf8b3e52eb76db43e3f9bf7db9fd9d83503f8f7d0e3d8'
    },
    {
      id: 'recUsZOxtqIpeA5ZW',
      slug: 'lxr-hotels-resorts',
      name: 'LXR Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/lxr-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '45f541912ae65665ea561c3eec20a8ae459213b4387cf7fadc95948f028b7c32'
    },
    {
      id: 'recswdE1mhwDvXtBi',
      slug: 'park-hyatt',
      name: 'Park Hyatt',
      overlayUrl: '/brand-explorer-phase2e-preview/park-hyatt-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'dda50f5c4fca84922a02b942ead9f1c2780de2a18f01701158914486ae213cd6'
    },
    {
      id: 'recGCWhkeEvsQxuez',
      slug: 'grand-hyatt',
      name: 'Grand Hyatt',
      overlayUrl: '/brand-explorer-phase2e-preview/grand-hyatt-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '88932bdc1db6b93c9150f7467503399662f371f6c698cd27636be7eddeb5a568'
    },
    {
      id: 'recbtJoMub5wcankK',
      slug: 'andaz',
      name: 'Andaz',
      overlayUrl: '/brand-explorer-phase2e-preview/andaz-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '89839023d24fe7539d8e3f7a97c5f7ce5140a1b6bad9b6093205e638772ff703'
    },
    {
      id: 'recgwxli5IsoPKCOY',
      slug: 'hyatt-place',
      name: 'Hyatt Place',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-place-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-5-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '95233f851eb16b556d2491edb0d39ef18ed5f3f57d58ebbe78d63cb1cc7737cd'
    },
    {
      id: 'recX9RJcmafZxWdXx',
      slug: 'ritz-carlton',
      name: 'The Ritz-Carlton',
      overlayUrl: '/brand-explorer-phase2e-preview/ritz-carlton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '96779d8244170bd1b4204e6d580a27a7ae6c5d397b4984b8f984a7d8184450d6'
    },
    {
      id: 'recYsUrz4Po7Tg48T',
      slug: 'st-regis',
      name: 'St. Regis Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/st-regis-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e1390999f610353ca7c2209a169ff6eb73762cf022fd51df20e5ccdb6f5d5705'
    },
    {
      id: 'rec66ikTdLlT49Vkh',
      slug: 'edition-hotels',
      name: 'EDITION Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/edition-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e9352aa5025634d4bad6f2109f199e5cfe025b99b4fea6e6b73e29697309f39e'
    },
    {
      id: 'recpKmYbyFr8ScGTi',
      slug: 'luxury-collection',
      name: 'The Luxury Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/luxury-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '73a9eaee4efe8a936d29ec2d62ae5d79322fc25e8a72af55fc01367041f80e3c'
    },
    {
      id: 'recmj7cNIUbYPKVJl',
      slug: 'renaissance-hotels',
      name: 'Renaissance Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/renaissance-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5765b91b9828abc0d2ebcb6c5c8667763e3b2ec8d168a7b69dc33034a6d96df4'
    },
    {
      id: 'recJUualJaqReVHLO',
      slug: 'signia-by-hilton',
      name: 'Signia by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/signia-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4a1bb0ca2be181938b70a92fc9aca72d106b7c6e414500705ed3367097ceb913'
    },
    {
      id: 'reczor3qV3qAXFydD',
      slug: 'alila',
      name: 'Alila',
      overlayUrl: '/brand-explorer-phase2e-preview/alila-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '64d488c7c7b69e44dccdcbc2f4dcf1f4127971a81cc7f683b9760bb88637f018'
    },
    {
      id: 'recumPNYZZkNdbTXF',
      slug: 'hyatt-house',
      name: 'Hyatt House',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-house-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'de73a1989bc0a295a0c445ee7d3f61b1dfb41cb0e087d1c40b76dc2109ad4aae'
    },
    {
      id: 'recC3w95Hgi9SzXzD',
      slug: 'intercontinental',
      name: 'InterContinental Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/intercontinental-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '61b4b001b12875fdcf18f8cd14615ce4a956fd1bef2780a7490ac6b0c91a0c82'
    },
    {
      id: 'rec4ckn09CLLmppCe',
      slug: 'six-senses',
      name: 'Six Senses Hotels Resorts Spas',
      overlayUrl: '/brand-explorer-phase2e-preview/six-senses-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-6-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '81a40796fcc7c1ffe056466aea65a05885e0987ca42147d3fc84712daaba08d2'
    },
    {
      id: 'rec4y33IpMYdmWAqS',
      slug: 'garner-hotels',
      name: 'Garner Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/garner-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '8cfc65d9ea033a68d246478aa8ae3d2c5e4a6886fe8bd3868f162f127769bda1'
    },
    {
      id: 'recXWQe2uMx69bQDL',
      slug: 'sleep-inn',
      name: 'Sleep Inn',
      overlayUrl: '/brand-explorer-phase2e-preview/sleep-inn-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0d56499f0b255859dae8d31fbd36f9989fbcdebd890b19116fb6e258bf947c8f'
    },
    {
      id: 'recagQusDvkOUf2XN',
      slug: 'econo-lodge',
      name: 'Econo Lodge',
      overlayUrl: '/brand-explorer-phase2e-preview/econo-lodge-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2428f59e73416c3ccd7f063a8a4c9606df93efbabc01b222523a53c814ebfcb7'
    },
    {
      id: 'recjkV8i8Px2jckSn',
      slug: 'rodeway-inn',
      name: 'Rodeway Inn',
      overlayUrl: '/brand-explorer-phase2e-preview/rodeway-inn-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '8320d44386e280201a3ac1c686c2c14c542806b8f5cfd8bce5007ee6e0bb18f3'
    },
    {
      id: 'recy9lvcO8iBf7q47',
      slug: 'mainstay-suites',
      name: 'MainStay Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/mainstay-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5dbab7a42bd6571f701ab1c5f37dfb6d49c673953586a25777d4157be3f17d5b'
    },
    {
      id: 'recLenDbM22RP7PsB',
      slug: 'days-inn-by-wyndham',
      name: 'Days Inn by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/days-inn-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '8c7b2f760e9ed443980c3710fdcc3442cbb399233a2cea3b7e108fd8faf937b7'
    },
    {
      id: 'recBmtjThn1HgNX9d',
      slug: 'super-8-by-wyndham',
      name: 'Super 8 by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/super-8-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'cce8b547f8d35a58c17ab625dc8cfee9e88d2b952d0f637d6402f83f2fa43474'
    },
    {
      id: 'recIjv1h6tQPQTRXI',
      slug: 'microtel-by-wyndham',
      name: 'Microtel by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/microtel-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '64385ca01594aa80bcbc2b24481c361c5e98cda423d84a19b89b66563dd3a60b'
    },
    {
      id: 'recaORoZYBbFXyuCs',
      slug: 'baymont-by-wyndham',
      name: 'Baymont by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/baymont-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5be388088c8e090d5468848a562e133824acbf9c3f02b76599f6d49935f7d4a4'
    },
    {
      id: 'recgrfnefkcrkZtEa',
      slug: 'travelodge-by-wyndham',
      name: 'Travelodge by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/travelodge-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-7-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '24a40d5d7d74233b22e22fefd383b60362bb26f9aa052a89c57f93475a8762be'
    },
    {
      id: 'recRHZlHTXBkVySUC',
      slug: 'la-quinta-by-wyndham',
      name: 'La Quinta by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/la-quinta-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'bd03a8914b4d2c3bc5b6886694dd8204a0348a721659e962067ee8698e3e0e9b'
    },
    {
      id: 'recvCgmyT7vBpJmcE',
      slug: 'americinn-by-wyndham',
      name: 'AmericInn by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/americinn-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b5a22da077387ac407bedcf97b19a0682b9b421c26de91d4cf939fdc431c3c1e'
    },
    {
      id: 'recrMPYs4pOsA6Nys',
      slug: 'howard-johnson-by-wyndham',
      name: 'Howard Johnson by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/howard-johnson-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '66dee91996eab541d49be0f25574286f4738e9d6c939d847de8b9c750bc49335'
    },
    {
      id: 'reczvWzz6ugk5IzFc',
      slug: 'wingate-by-wyndham',
      name: 'Wingate by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/wingate-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'ff441e24011489d8b3bb9a3423851a093fb931f148b489ae1c3d51d38484ebf1'
    },
    {
      id: 'rec0wIg8ie5ZfPY1f',
      slug: 'ramada-by-wyndham',
      name: 'Ramada by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/ramada-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '975b19b17d776db58e2780246dbd765430bdc5d79899bb508446e17d3b9804d8'
    },
    {
      id: 'recyggm1Bif6Mk0a2',
      slug: 'hawthorn-extended-stay-by-wyndham',
      name: 'Hawthorn Extended Stay by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/hawthorn-extended-stay-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5b1be6930fb26ec8a7e14501ab3ffcf58e826ae501dd4da545e0bb5c2f4c1952'
    },
    {
      id: 'recTDqjhN6rXWoC07',
      slug: 'wyndham-garden',
      name: 'Wyndham Garden',
      overlayUrl: '/brand-explorer-phase2e-preview/wyndham-garden-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '12062e60a7b545bee40918e9d357c04caf024846d83b53ed6634a48ec9e86c70'
    },
    {
      id: 'rec5RhdBK4iNCQF4u',
      slug: 'best-western',
      name: 'Best Western',
      overlayUrl: '/brand-explorer-phase2e-preview/best-western-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '02a6c1215cc8da8d5fec9afce64fce5cae5cb63d0a3b4029863d051af1148164'
    },
    {
      id: 'rec5KPgalPPAFl7UZ',
      slug: 'best-western-plus',
      name: 'Best Western Plus',
      overlayUrl: '/brand-explorer-phase2e-preview/best-western-plus-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'db149aee5f8722fb44e2d8d04fa0c16c1490a946c34ceb1992d60874756ee064'
    },
    {
      id: 'rec9ViCcmQBA55vGA',
      slug: 'surestay-by-best-western',
      name: 'SureStay by Best Western',
      overlayUrl: '/brand-explorer-phase2e-preview/surestay-by-best-western-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5bb1768e90b7a2e77bf03967f03072bf619e01316e7fba3f2a3b4649ddd18cde'
    },
    {
      id: 'recpmv6vrM9ztGedL',
      slug: 'surestay-plus-by-best-western',
      name: 'SureStay Plus by Best Western',
      overlayUrl: '/brand-explorer-phase2e-preview/surestay-plus-by-best-western-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0955b3f619640ff0e6b1dc714812f51ec3b1f47fec371338c3ffd8b8afafc2f5'
    },
    {
      id: 'recTsfe9nlEMqLE3j',
      slug: 'surestay-studio-by-best-western',
      name: 'SureStay Studio by Best Western',
      overlayUrl: '/brand-explorer-phase2e-preview/surestay-studio-by-best-western-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'cf4dd32651db42b81789f7005668d615b4da56e313cedcefc3b0ca4d87845385'
    },
    {
      id: 'recLOEXjnKYZdxuEb',
      slug: 'glo-by-best-western',
      name: 'GLō by Best Western',
      overlayUrl: '/brand-explorer-phase2e-preview/glo-by-best-western-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '40116dd66722e21275c6145ce81357b8f45db3e1f461c23fe1b7826ea4992603'
    },
    {
      id: 'rectk6HIVra7oyoDP',
      slug: 'executive-residency-by-best-western',
      name: 'Executive Residency by Best Western',
      overlayUrl: '/brand-explorer-phase2e-preview/executive-residency-by-best-western-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4c8f75c233dea6ffc9b2aa1d75147ef531afaf807e0d861432fd4736840ce254'
    },
    {
      id: 'recPpLc3gOsPOwtCJ',
      slug: 'aiden-by-best-western',
      name: 'Aiden by Best Western',
      overlayUrl: '/brand-explorer-phase2e-preview/aiden-by-best-western-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '35a65a2b9f01b42e81b890c9db46b509bb49b449a883378bd4ce61e2cf76409b'
    },
    {
      id: 'rec4ztolqpD1uYrsG',
      slug: 'sonesta-select',
      name: 'Sonesta Select',
      overlayUrl: '/brand-explorer-phase2e-preview/sonesta-select-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3a136ab28a68d4c8d4eeb110f2b90953bba626b4b322e20d1f7f4fe8521d015a'
    },
    {
      id: 'recEaNmmaNdbWvipS',
      slug: 'sonesta-es-suites',
      name: 'Sonesta ES Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/sonesta-es-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'a82b7b28dc8cc06606e026b6618fd03e67fa72556c2aa3a180bfda89a74ec612'
    },
    {
      id: 'rechMdsQxPPhet6rV',
      slug: 'red-lion-hotels',
      name: 'Red Lion Hotels, Inns & Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/red-lion-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '60d99b1646cf4cff2b0bc1ecacdb2fa6ef71fd0e91eadf16ca6da4f033a72b16'
    },
    {
      id: 'recgYprNavH0rgb7z',
      slug: 'signature-inn',
      name: 'Signature Inn',
      overlayUrl: '/brand-explorer-phase2e-preview/signature-inn-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'cd6559a86b62a3403497e241826fcb757ccf1829623449a1d0f5d6d1ca767bd4'
    },
    {
      id: 'recgMqPkLsfaMU9vA',
      slug: 'americas-best-value-inn',
      name: 'Americas Best Value Inn',
      overlayUrl: '/brand-explorer-phase2e-preview/americas-best-value-inn-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-8-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'cb339edab1e0ae417f461200c1ba7c8858d82b7021afd58e9dbe3ef307eb4323'
    },
    {
      id: 'recucEzAS6724tOYA',
      slug: 'city-express-by-marriott',
      name: 'City Express by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/city-express-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3fbe065def7a14950dab3892c82f0aa16c729614e6c6fedd7dc60caec5175570'
    },
    {
      id: 'recBzdGfkMUN9fYsv',
      slug: 'springhill-suites-by-marriott',
      name: 'SpringHill Suites by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/springhill-suites-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0f9bea84077b231407b675de9a86d408e2bca100020817dc27fd23223b91b9a0'
    },
    {
      id: 'recDM0LAD8jVRA2x3',
      slug: 'studiores',
      name: 'StudioRes',
      overlayUrl: '/brand-explorer-phase2e-preview/studiores-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0a182f0674dca4e4fcd0261d5a98057ccc82e9fd5c0027937ff57fa21cce5ccd'
    },
    {
      id: 'recUPiPDivkhNUogr',
      slug: 'towneplace-suites-by-marriott',
      name: 'TownePlace Suites by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/towneplace-suites-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '57f476dd12644182622549452ce02b2a413eeb5513600bb9b892a8e9c32ae35c'
    },
    {
      id: 'recpUTDtwt1wPMDPj',
      slug: 'fairfield-by-marriott',
      name: 'Fairfield by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/fairfield-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4cd60d64a0ac3996eb45b71d95ce07e78dd8375985452bebe444156e8c58947d'
    },
    {
      id: 'recH5ZF9V6ivz9p5h',
      slug: 'four-points-by-sheraton',
      name: 'Four Points by Sheraton',
      overlayUrl: '/brand-explorer-phase2e-preview/four-points-by-sheraton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9bb2504d73e8e5cbeb4dc4034323c9ad7a3256018d94085f7edd9eab572f23b6'
    },
    {
      id: 'rec9Ufbpa0GxJGzt8',
      slug: 'residence-inn-by-marriott',
      name: 'Residence Inn by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/residence-inn-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'd1cbcc2439f3529b89aadbfef1495725ace242d47e3161e2c0d98d0b7da5e57c'
    },
    {
      id: 'recn59UtkyyoYwzSz',
      slug: 'marriott-hotels',
      name: 'Marriott Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/marriott-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c024aeeb4e9c9e1204d03968cfa49085fc7684da17785cb2a1a3bf3e2407573b'
    },
    {
      id: 'recg8HjT5Bky7NXeV',
      slug: 'sheraton',
      name: 'Sheraton',
      overlayUrl: '/brand-explorer-phase2e-preview/sheraton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9925c4a62e537b5ca7536aa743a6798b73b9a6ebcd7bc78fd7ec3a93022894f0'
    },
    {
      id: 'recWubG3rhiS1BaWi',
      slug: 'hilton-hotels-resorts',
      name: 'Hilton Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/hilton-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9c6959ca94a6eb88cc953a2d570266924ec307aa814a010d860a420623265a85'
    },
    {
      id: 'rechVYWQ5ikRnr99B',
      slug: 'doubletree-by-hilton',
      name: 'DoubleTree by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/doubletree-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '6d46a6ef122a5fdb44f3a033f5241e59c2058dd1e7636ff96e82f0092189ff8c'
    },
    {
      id: 'rectRvOWQPaL6FkzZ',
      slug: 'hampton-by-hilton',
      name: 'Hampton by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/hampton-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '14e39e18509538fc897771a00d7cd6c33ae39f5c0f0036b4a9df70d0acb37c98'
    },
    {
      id: 'recrvdAjRlXxPvPPF',
      slug: 'hilton-garden-inn',
      name: 'Hilton Garden Inn',
      overlayUrl: '/brand-explorer-phase2e-preview/hilton-garden-inn-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4cede539294d1e18323a3e84a2a08666ace6f0d14b45b0ddf2000b595bbd8c9b'
    },
    {
      id: 'reccZ4zV6wMav7a2i',
      slug: 'home2-suites-by-hilton',
      name: 'Home2 Suites by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/home2-suites-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'ced3800663ae6a562f432eaabd4056737dbdf0e37b915fc93740b8151e7d8f69'
    },
    {
      id: 'recZjYI4nYflGHFNR',
      slug: 'homewood-suites-by-hilton',
      name: 'Homewood Suites by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/homewood-suites-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '7d33599abba5e271ef586d226ad98e0359497a3cddf61244782299afee924175'
    },
    {
      id: 'recfv66er4Ch2vJDO',
      slug: 'spark-by-hilton',
      name: 'Spark by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/spark-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b2ea4172170145401031e9f328f03500a78d85a12d3ea71d847125db982abdc7'
    },
    {
      id: 'recJLiMTv4W8VgO9L',
      slug: 'tru-by-hilton',
      name: 'Tru by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/tru-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2054712d1b99cef8f8507077209838f7bca0aee84d3ae782891e34150e8943b6'
    },
    {
      id: 'recP9SqDootMrzaU1',
      slug: 'hyatt-regency',
      name: 'Hyatt Regency',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-regency-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '625dba4f2c6a1f59f9722903128d39294493f30e5dae0d8ebfa37b12282502b6'
    },
    {
      id: 'rec4Mga6ejz3L1M3P',
      slug: 'thompson-hotels',
      name: 'Thompson Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/thompson-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '89d89a5c59eb697e937c65596cf890dd764cf4c0c0ca346d9fe16e384513f53d'
    },
    {
      id: 'rec50qkCj6yt9fMPg',
      slug: 'delta-hotels-by-marriott',
      name: 'Delta Hotels by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/delta-hotels-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-9-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3c94517f446301534c93c7eb36d25d88f23fe5ba131ac3a9f8a2f06bd7b25065'
    },
    {
      id: 'rec7hTXwMRC81EPqz',
      slug: 'handwritten-collection',
      name: 'Handwritten Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/handwritten-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3ba271410ac9b260d7643132a8b713987d51b914300163adac5f5c4f363c1fb1'
    },
    {
      id: 'recrWCD1LMqu864oU',
      slug: 'mgallery-collection',
      name: 'MGallery Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/mgallery-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b80187075947731ab0bb1748c8b47725355c6dbe8483780eb14b4872721d3b7e'
    },
    {
      id: 'recFW9kfqKfOjv7Z1',
      slug: 'pullman',
      name: 'Pullman',
      overlayUrl: '/brand-explorer-phase2e-preview/pullman-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'bd22de81186e42d046f52b5f67bd1b512dce4a05ceea2be61d860cd7dc75b6d1'
    },
    {
      id: 'recJhPaDVU3YUDQUt',
      slug: 'fairmont',
      name: 'Fairmont',
      overlayUrl: '/brand-explorer-phase2e-preview/fairmont-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '15623d5b7b63256cf4f33e1e706a2ca4c2b4445e80671e492cd6f6226cd1b522'
    },
    {
      id: 'recevrLJ3m6rIug3S',
      slug: 'mercure',
      name: 'Mercure',
      overlayUrl: '/brand-explorer-phase2e-preview/mercure-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'cda817bc43c29aa330d035749f13c15d372296de93437165380b512d07677c8c'
    },
    {
      id: 'reclFXbpZ5XzLWbGP',
      slug: 'ibis',
      name: 'ibis',
      overlayUrl: '/brand-explorer-phase2e-preview/ibis-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '23af2f6d8aae15a2f57494d3c5b681c2d73a34a1a854e0b23ae5fa37089fef34'
    },
    {
      id: 'recXCZCK05XXYX7Q8',
      slug: 'mama-shelter',
      name: 'Mama Shelter',
      overlayUrl: '/brand-explorer-phase2e-preview/mama-shelter-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'eeac143348438a13e14f257ed82e608ca6014731fb9a993e708b729ed172d3f8'
    },
    {
      id: 'recQE2lSSSSyuUrMQ',
      slug: 'novotel',
      name: 'Novotel',
      overlayUrl: '/brand-explorer-phase2e-preview/novotel-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'ef45ffffbd22f54237b0439d6421effed0c7ffff4867283b516ca09df499f4af'
    },
    {
      id: 'recTJdPlr4mDs9app',
      slug: 'so',
      name: 'SO/',
      overlayUrl: '/brand-explorer-phase2e-preview/so-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '11e522dc570c4ac2764eb496ac222764189d2fa05f5bbc6a57d49795846bdbc1'
    },
    {
      id: 'recwXZ5gVZ8ZH8ekA',
      slug: 'bw-premier-collection',
      name: 'BW Premier Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/bw-premier-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '7f8101ec6eb21bf198a669564e7ecdbcf4163d53d3ff106a1f71bfdab3721e93'
    },
    {
      id: 'recdeh1NsP4gjrv80',
      slug: 'bw-signature-collection',
      name: 'BW Signature Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/bw-signature-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'eb27adcc693ce59b0571fcbc575df7a3e9f6d74aae743c8ac4fe52dc52071fea'
    },
    {
      id: 'recob7tgHRryRSbeO',
      slug: 'trademark-collection-by-wyndham',
      name: 'Trademark Collection by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/trademark-collection-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'cddc95bfa42e39e87111382c7433259ead11ba0d39ef48c1089f5349c4e13911'
    },
    {
      id: 'rec5CNMM4ZUD7ZHlM',
      slug: 'dazzler-by-wyndham',
      name: 'Dazzler by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/dazzler-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'd410f1a37b11fd83fb25b9437900be3cebae604a954c4b92b2d3c40788fc8514'
    },
    {
      id: 'reclcjg5Foa9Vs5TC',
      slug: 'suburban-studios',
      name: 'Suburban Studios',
      overlayUrl: '/brand-explorer-phase2e-preview/suburban-studios-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '607d120fcb05fa3d8263835ea9eea610cd59bd4a6efbd1b5ee2af4c34a044374'
    },
    {
      id: 'recqkkrsevi4r9ibj',
      slug: 'everhome-suites',
      name: 'Everhome Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/everhome-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'bc4fbfe3bdc6ef052d3053ee5959cabc987bb23aacb25e38fd13b5bb1e23e997'
    },
    {
      id: 'recd8o4k1JddhkRWW',
      slug: 'quality-inn',
      name: 'Quality Inn',
      overlayUrl: '/brand-explorer-phase2e-preview/quality-inn-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9fa8bcd422c2390b8540bf7316d018196bb735fb5b5e7fca59e49a3558b77e02'
    },
    {
      id: 'recmKqo7M7mLZgRqQ',
      slug: 'radisson-red-by-choice',
      name: 'Radisson RED by Choice',
      overlayUrl: '/brand-explorer-phase2e-preview/radisson-red-by-choice-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '733e1771d41cb192d8a2445a98c4fb44e135b3f3625bb8d67719e14c10914ade'
    },
    {
      id: 'recywbx1YQSTCPqW1',
      slug: 'radisson-by-choice',
      name: 'Radisson by Choice',
      overlayUrl: '/brand-explorer-phase2e-preview/radisson-by-choice-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2d9822eaa32c2f469ee3e2bfc77bb6bbd74d44492ac36297a22b6ae59a2da05e'
    },
    {
      id: 'recwl5JOYxlChuCAr',
      slug: 'preferred-hotels-and-resorts',
      name: 'Preferred Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/preferred-hotels-and-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9750ab381b302d416f6c3eeb6f920b81cae1044851c4c12473894b298e66deef'
    },
    {
      id: 'recjjSnY2opb8P4DG',
      slug: 'small-luxury-hotels-of-the-world',
      name: 'Small Luxury Hotels of the World',
      overlayUrl: '/brand-explorer-phase2e-preview/small-luxury-hotels-of-the-world-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-10-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b958d77c754d387bd46a59d380d8b3ab5950e7aa782811e32b10c08ff5db6116'
    },
    {
      id: 'recWPEvxBQxVVzSq3',
      slug: 'radisson-blu-by-choice',
      name: 'Radisson Blu by Choice',
      overlayUrl: '/brand-explorer-phase2e-preview/radisson-blu-by-choice-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '1182042be1a582b1adbea618efb7481859a09259bada315a196c576a8d669629'
    },
    {
      id: 'recOzH5iAE1xEjyD0',
      slug: 'comfort-inn-suites',
      name: 'Comfort Inn & Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/comfort-inn-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'd25fe766858d72644e8ee71fbecaa8ed79916f251e2a2ce7dc2c0bf0ff8725c6'
    },
    {
      id: 'recRyvM8OmLlDj9G7',
      slug: 'radisson-individuals-by-choice',
      name: 'Radisson Individuals by Choice',
      overlayUrl: '/brand-explorer-phase2e-preview/radisson-individuals-by-choice-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '419fb582613e17b89923f77a2ff0167ec338a88d93f48027acfbf1f9d3d50a4a'
    },
    {
      id: 'recaayt9u7YYg8h7Y',
      slug: 'country-inn-suites-by-choice',
      name: 'Country Inn & Suites by Choice',
      overlayUrl: '/brand-explorer-phase2e-preview/country-inn-suites-by-choice-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '54cb14fa0851613f4f2adb0d8e539f577e51fc16c5afdafa9bbd39e27d7b1eb6'
    },
    {
      id: 'recsOd51NzRPYsMko',
      slug: 'woodspring-suites',
      name: 'WoodSpring Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/woodspring-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'f42e9aac95a71f73c6ca5b1222525d5f448d099f53a321b157a176ef9db5af7c'
    },
    {
      id: 'recAT3p3d7LAZZNkb',
      slug: 'cambria-hotels',
      name: 'Cambria Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/cambria-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0390460ac69eb1648b5236c9b51c38804fa4ad4f6a9a4f13bd6da46c4dae7ace'
    },
    {
      id: 'recq3vCeOn8lQdY4P',
      slug: 'clarion-pointe',
      name: 'Clarion Pointe',
      overlayUrl: '/brand-explorer-phase2e-preview/clarion-pointe-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4b4dc671d65f808522d2f5f409c0e734a128c52e88258d4021821d6f6322edbd'
    },
    {
      id: 'recsbxr7pMGMN0N6R',
      slug: 'clarion',
      name: 'Clarion',
      overlayUrl: '/brand-explorer-phase2e-preview/clarion-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'eab822c22ad98c044ce595af4a6be473c77508d14700ec3c0ec8c0a4f12b05bc'
    },
    {
      id: 'recWpSnsKxoUTcWs2',
      slug: 'wyndham-grand',
      name: 'Wyndham Grand',
      overlayUrl: '/brand-explorer-phase2e-preview/wyndham-grand-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'dafb737a880ac28c1d38bf14bb401070ec8711a02ed91df319b3d8a6bf12a9eb'
    },
    {
      id: 'rec4w2XsL3yDUsv2C',
      slug: 'dolce-hotels-resorts-by-wyndham',
      name: 'Dolce Hotels & Resorts by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/dolce-hotels-resorts-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'a04525d964062ea8c995153ce911f5cc79a6e7a3a378189e8bd753c34b405747'
    },
    {
      id: 'recXIwHYrvJ89a1yK',
      slug: 'tryp-by-wyndham',
      name: 'TRYP by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/tryp-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2e06959753d4aaf1620b9e297f798218610238c4fbe091c3ad300f840d8fddb7'
    },
    {
      id: 'recE5LoJH1EaszkqG',
      slug: 'esplendor-by-wyndham',
      name: 'Esplendor by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/esplendor-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'f8b2fbf69b101fc830a4e3cf69e4c1a109d47cf8cedb443a1785395b7e3c31d1'
    },
    {
      id: 'rec72kWHpqp5awuZY',
      slug: 'registry-collection-hotels',
      name: 'Registry Collection Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/registry-collection-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '619832252827baeccbfe17b58c7cd9b578db376c1bed44709ae5414c666697e0'
    },
    {
      id: 'recSgIQ0bhRpPXZbU',
      slug: 'caption-by-hyatt',
      name: 'Caption by Hyatt',
      overlayUrl: '/brand-explorer-phase2e-preview/caption-by-hyatt-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2dd02364b832bd9255caab8b19c0a86be6f281251a00d525c0ff5040b4145079'
    },
    {
      id: 'rec4oSoJsqHtGfFYL',
      slug: 'jdv-by-hyatt',
      name: 'JdV by Hyatt',
      overlayUrl: '/brand-explorer-phase2e-preview/jdv-by-hyatt-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'f19e3be88a1eb52f4c0249461c79190497e69c6d869f343e6cfc60b7a7711f10'
    },
    {
      id: 'recDQwcMFontK2CSP',
      slug: 'unbound-collection-by-hyatt',
      name: 'The Unbound Collection by Hyatt',
      overlayUrl: '/brand-explorer-phase2e-preview/unbound-collection-by-hyatt-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '49b45e2fe0f368e2642077ab4fde322486bff65baed9ef09b0660a62ba9eecac'
    },
    {
      id: 'recmK2PWDTHW6Y6tx',
      slug: 'sonesta-simply-suites',
      name: 'Sonesta Simply Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/sonesta-simply-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-11-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '7351813f6738995da03bf9d5b66843189b52c8b532b43512759ac3c3a8b42965'
    },
    {
      id: 'recQxUKKgH0Pq1VgR',
      slug: 'graduate-by-hilton',
      name: 'Graduate by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/graduate-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5cd155ca150af047df7e2704a7a1e32d55719cdfccc70bf15bddf2d12f36048e'
    },
    {
      id: 'recVAnsCsz4ifaAw2',
      slug: 'livsmart-studios-by-hilton',
      name: 'LivSmart Studios by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/livsmart-studios-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2bd9c12eac509d596a1ab8d079b11b4c994b1fcd7ea6cba253efaf6c525b366d'
    },
    {
      id: 'recjvESTY8tYZjlAJ',
      slug: 'embassy-suites-by-hilton',
      name: 'Embassy Suites by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/embassy-suites-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '30fdd048da63d750ca3e8d0a1dfec12afae491d421edc065d9c8c5ff27a81418'
    },
    {
      id: 'recqsodgJLUzsdCED',
      slug: 'nomad-hotels',
      name: 'NoMad Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/nomad-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '28b89e9faabe423c9e08f7348bc89f6241f8b8d260f0ad2d61b27cad30778678'
    },
    {
      id: 'recF9TQzTd3wwEiKa',
      slug: 'ibis-styles',
      name: 'ibis Styles',
      overlayUrl: '/brand-explorer-phase2e-preview/ibis-styles-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '089c795f05fc12ecb63a76a3b6e46c01581b5bc0bbcecab79c0df4babb0af50b'
    },
    {
      id: 'rec0lQ4Imf614F8jo',
      slug: 'ibis-budget',
      name: 'ibis budget',
      overlayUrl: '/brand-explorer-phase2e-preview/ibis-budget-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '7acd35a74644de5f40c490c5a90d02a954360aed00cdfb3ba7b4b090bc85cb7c'
    },
    {
      id: 'recGJctYTmJiwwXKG',
      slug: 'greet',
      name: 'greet',
      overlayUrl: '/brand-explorer-phase2e-preview/greet-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '66f5b0a874abbcfc89e66d9691d8f8bb508ce76a0b9eaa3e06910239c3a1c49c'
    },
    {
      id: 'rec9Q89yzK4QZKnal',
      slug: 'hotelf1',
      name: 'hotelF1',
      overlayUrl: '/brand-explorer-phase2e-preview/hotelf1-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5432c106e2c2587eafeefe84517521f98952cf85fadeed4f59c3e6c8ba096c8e'
    },
    {
      id: 'rechyrQmHmeR1ovme',
      slug: 'tribe',
      name: 'TRIBE',
      overlayUrl: '/brand-explorer-phase2e-preview/tribe-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '00cab43b2b07a9557276259c439d96b25bb0e14265cfff85eb49f7ecf437e6eb'
    },
    {
      id: 'reck9Gq60El13HG5Z',
      slug: 'adagio',
      name: 'Adagio',
      overlayUrl: '/brand-explorer-phase2e-preview/adagio-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5bf268cd9d306bfa5a1821c93689011042f481c6ef1bc9d3f25e68b04584b8bb'
    },
    {
      id: 'recSAVQWVAnn0NKYg',
      slug: 'adagio-access',
      name: 'Adagio Access',
      overlayUrl: '/brand-explorer-phase2e-preview/adagio-access-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e5e2ab7c10aa5cade702ae43661ba324cbcfd19e7d2ab50be7103c22af86986d'
    },
    {
      id: 'recKPpUCq9EZlSSYx',
      slug: '25hours-hotels',
      name: '25hours Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/25hours-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4a21315f8c9d8d0e6abe7a69413340ccdf84bf9d052306953ec2bf50c37f41f9'
    },
    {
      id: 'recXmadOxgzU6Ug5l',
      slug: 'the-hoxton',
      name: 'The Hoxton',
      overlayUrl: '/brand-explorer-phase2e-preview/the-hoxton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e1f34e0d3cfcb26e844cd36f9ee50e503f258adc65163ba9f95b39011835819c'
    },
    {
      id: 'recPF5NM9kDZwkNmS',
      slug: 'mondrian',
      name: 'Mondrian',
      overlayUrl: '/brand-explorer-phase2e-preview/mondrian-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5cd801162b0cf0ba121360606a36f53b5e175a5c49f8e5d3c653147481f76b2f'
    },
    {
      id: 'recharEz6EaOg9AiP',
      slug: 'hyde-hotels',
      name: 'Hyde Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/hyde-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5e06fd49e6fe8131ab55bf233b146ced0a4e7e1a643bf3ca13ac9c73eb91e90e'
    },
    {
      id: 'recrJtboQyjYGsZZm',
      slug: 'sls-hotels',
      name: 'SLS Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/sls-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'a758d8143a2d43d2554a0dfe273ca6151125b0b76be90bcf56afcff76a45e2f2'
    },
    {
      id: 'rect7iv1y3mGFadqL',
      slug: 'swissotel',
      name: 'Swissôtel',
      overlayUrl: '/brand-explorer-phase2e-preview/swissotel-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '01ae79394a115b1c1a4cff41e7dbf8087ebabf39d0e9bfce56abfbb33153c444'
    },
    {
      id: 'reclu1Fgv5Leq25qn',
      slug: 'park-plaza',
      name: 'Park Plaza',
      overlayUrl: '/brand-explorer-phase2e-preview/park-plaza-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9f634546b940cb758a360f8e7d5bdd594141f3750a745698fc9ba42d4bcd397a'
    },
    {
      id: 'recyI1O61Or7LXwXl',
      slug: 'park-inn-by-radisson',
      name: 'Park Inn by Radisson',
      overlayUrl: '/brand-explorer-phase2e-preview/park-inn-by-radisson-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-12-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '7eda81323762a7e6ddf086350bc42f170f4bf6c7fb2609904f19171128478a28'
    },
    {
      id: 'reckR3Txp5OzwVl5q',
      slug: 'regent-hotels-resorts',
      name: 'Regent Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/regent-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '430dece30445e06837c0598331b68e39e39793e7ac9df7061827bbe89b66232b'
    },
    {
      id: 'recaPiDEZ7K7IXhcZ',
      slug: 'crowne-plaza',
      name: 'Crowne Plaza Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/crowne-plaza-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2d6e4ea9d86b6c4a9e271b86e0c3676ad7334e305c316c8815c86a34cb7203cc'
    },
    {
      id: 'reccQERSltr3CcDgs',
      slug: 'hualuxe-hotels-resorts',
      name: 'HUALUXE Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/hualuxe-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2c35973445995004456d785a9dc0b96599dd7d1c1fdc0fee505107237738124c'
    },
    {
      id: 'rechOYMLJ3tZI0y1P',
      slug: 'staybridge-suites',
      name: 'Staybridge Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/staybridge-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'fb9d6d1160df301463fa4f04e793261b7f8bba7dc4c67ede9a9f7a29e700a09a'
    },
    {
      id: 'recTGrNzTA6DRe0LL',
      slug: 'candlewood-suites',
      name: 'Candlewood Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/candlewood-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '1ce468416dbc32b66b61826dcd1b5e99390aa513b21c4e3e36fe635ca4b646f9'
    },
    {
      id: 'rec2lJCTSYIwsQYMc',
      slug: 'atwell-suites',
      name: 'Atwell Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/atwell-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9df1e20cc4238af73e840f9e28e4c982e13e8aef75e2bede6c7dca48d7ffec2d'
    },
    {
      id: 'recokU1aiTKb0bhgE',
      slug: 'holiday-inn',
      name: 'Holiday Inn',
      overlayUrl: '/brand-explorer-phase2e-preview/holiday-inn-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c0ce3f7af0e1382ee6a27b1924934d1b2568d85b830521a8906df5d8bcdda737'
    },
    {
      id: 'rec0sfR7SrVZUBm18',
      slug: 'hyatt-ziva',
      name: 'Hyatt Ziva',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-ziva-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'd9f091bc397b6b90b4cc997bf90631eceb0bf2a14a43bb325383140eb8029349'
    },
    {
      id: 'rechIjRoVmm4bo8g1',
      slug: 'hyatt-zilara',
      name: 'Hyatt Zilara',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-zilara-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'da059f62b7b452fbf149ffcabe78c2f07d6e116823c936f1d037dab9dddf7490'
    },
    {
      id: 'recNMxUhbOaIq3so5',
      slug: 'secrets-resorts-spas',
      name: 'Secrets Resorts & Spas',
      overlayUrl: '/brand-explorer-phase2e-preview/secrets-resorts-spas-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '127f6667f17da13da3d44025cffb09e16d53fc6e6ae1cc1ec160c203bddce07d'
    },
    {
      id: 'rec6iaZERtyk8Zxen',
      slug: 'dreams-resorts-spas',
      name: 'Dreams Resorts & Spas',
      overlayUrl: '/brand-explorer-phase2e-preview/dreams-resorts-spas-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '295a206b977e8f2cf4d9cbb5357a5c2be878699275c9b61185551bf0d4932837'
    },
    {
      id: 'recY7MP8WBRiLyddc',
      slug: 'breathless-resorts-spas',
      name: 'Breathless Resorts & Spas',
      overlayUrl: '/brand-explorer-phase2e-preview/breathless-resorts-spas-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '888877da98b8a3b2d8bce5a8184e0fb08a50a557758bb4556aef1081702d673e'
    },
    {
      id: 'recI2FAEkY46LSx3m',
      slug: 'sunscape-resorts-spas',
      name: 'Sunscape Resorts & Spas',
      overlayUrl: '/brand-explorer-phase2e-preview/sunscape-resorts-spas-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5fa8b2c159780f727ba6253200532a46de374ee4fe692658fe9a1938cd1ad824'
    },
    {
      id: 'recpl5pAKwA6buOat',
      slug: 'hyatt-vivid',
      name: 'Hyatt Vivid Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-vivid-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'dddae1c21a1bd3f7f2a70cf6ea2989df079ddc7427832aac51a16a574d1ee5d8'
    },
    {
      id: 'recdgXCQU6GIc0l5Z',
      slug: 'miraval-resorts-spas',
      name: 'Miraval Resorts & Spas',
      overlayUrl: '/brand-explorer-phase2e-preview/miraval-resorts-spas-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-13-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '9b27c7833f73495b8750839b052a7c11083752f0ef0af47172944b6773d7d526'
    },
    {
      id: 'recJgviX5z4tLcMom',
      slug: 'sofitel',
      name: 'Sofitel',
      overlayUrl: '/brand-explorer-phase2e-preview/sofitel-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '387d863bcc7a2fa6bcf184448b360cd92e960c0b4f7dda6b68b77fb7d8d68252'
    },
    {
      id: 'recxAGLjrBt8PF3IT',
      slug: 'sofitel-legend',
      name: 'Sofitel Legend',
      overlayUrl: '/brand-explorer-phase2e-preview/sofitel-legend-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c1a44ff1db350d477c8f5ef65fd307008b33221d1b7671f432a03f0945d37a06'
    },
    {
      id: 'recjYuYOLjL4zusI6',
      slug: 'raffles-hotels-resorts',
      name: 'Raffles Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/raffles-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '095316ed37ba6a2bce990fee78279c7768f7785bd32d7f9db12c578145c7c494'
    },
    {
      id: 'recfG3hikZOMqDEb0',
      slug: 'movenpick-hotels-resorts',
      name: 'Mövenpick Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/movenpick-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'fd45d3e853fe1bf16fd2ee68094e858e531b2de70c4668fbe8d9db3627bdcc67'
    },
    {
      id: 'recnRlPoQjNlDFVJn',
      slug: 'grand-mercure',
      name: 'Grand Mercure',
      overlayUrl: '/brand-explorer-phase2e-preview/grand-mercure-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '10bf67c7c13b8a56a6722129e1b14fb5aec35d7492b8848bafcca6a0ff89db85'
    },
    {
      id: 'rec7F80S01oyTADN2',
      slug: 'peppers',
      name: 'Peppers',
      overlayUrl: '/brand-explorer-phase2e-preview/peppers-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b88f6aa7813f6867712235d74979a51a31d9e178f5122e2626b51dfc003e9705'
    },
    {
      id: 'receSUBBbaxgbenTd',
      slug: 'mantra',
      name: 'Mantra',
      overlayUrl: '/brand-explorer-phase2e-preview/mantra-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e50f66fe84445639cb4b4817eea131a87845f3d69bf1b22813de681bf17e737d'
    },
    {
      id: 'recjRHpf0a0ylJbXQ',
      slug: 'breakfree',
      name: 'BreakFree',
      overlayUrl: '/brand-explorer-phase2e-preview/breakfree-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c37536788a2accd7793c34f33fce1ac67eabdc65b993e355db90725486fc1b5d'
    },
    {
      id: 'recilV5oDDP3wtKJn',
      slug: 'the-sebel',
      name: 'The Sebel',
      overlayUrl: '/brand-explorer-phase2e-preview/the-sebel-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '7bf3761dc79ac6d182f338e1f862326312cae35c2aee28475e7844f7b400f04b'
    },
    {
      id: 'recNabpDL7arkX5mi',
      slug: 'banyan-tree',
      name: 'Banyan Tree',
      overlayUrl: '/brand-explorer-phase2e-preview/banyan-tree-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '802977e22a84241648fcaca7b2903a875867da541c2e817c7811d06176f0fd70'
    },
    {
      id: 'recR1DWI8QIecWQDl',
      slug: 'angsana',
      name: 'Angsana',
      overlayUrl: '/brand-explorer-phase2e-preview/angsana-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e255745eacb06cd9f01d10fbd02fad24e6ca1efc375e56a53115ea1312aa491d'
    },
    {
      id: 'recmkvmV84vr5LNwi',
      slug: 'art-series-hotels',
      name: 'Art Series Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/art-series-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b56941cc5fea9a7233aa5fd771d3d7a41036c1ae3c3d5f08c1e1941cd3887a6b'
    },
    {
      id: 'recpJ13Fo4nxXmEzD',
      slug: 'citizenm',
      name: 'citizenM',
      overlayUrl: '/brand-explorer-phase2e-preview/citizenm-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'aacd2cd11085ca60023eec857f309180d8e5c1a4c58e15550ff9940b6f8b9dec'
    },
    {
      id: 'recCtMPwNFDoE3e9m',
      slug: 'element-hotels',
      name: 'Element Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/element-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '675537c6fca036eba03310ab95e8d61273378f0728187b034e06a1e21478620c'
    },
    {
      id: 'rec5SgAtIKEAC9Jgr',
      slug: 'protea-hotels-by-marriott',
      name: 'Protea Hotels by Marriott',
      overlayUrl: '/brand-explorer-phase2e-preview/protea-hotels-by-marriott-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e07d54ea97265143811bcce4ddb85c0878bcee454b0b5ee74e7f17b3e403ead2'
    },
    {
      id: 'recgaMzDn2GKkpUsi',
      slug: 'four-points-flex-by-sheraton',
      name: 'Four Points Flex by Sheraton',
      overlayUrl: '/brand-explorer-phase2e-preview/four-points-flex-by-sheraton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-14-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '118976dbef15ac16afc5dfb9bff8e2195dbe222a0515c7e54fb682974de79d0d'
    },
    {
      id: 'recJPlQbmat5KZDPe',
      slug: 'anantara-hotels-resorts',
      name: 'Anantara Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/anantara-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '1330e5551320ab6991e8c52727d85abd6dba411e8a75cd9ecdc34db40b7a08ed'
    },
    {
      id: 'recXVvsweIigv064m',
      slug: 'tivoli-hotels-resorts',
      name: 'Tivoli Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/tivoli-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'ed0db3caca45a17a2de5f26ec3f80a7541194d9af5d8ad8cc41b9df510e42a59'
    },
    {
      id: 'recZZlLvzB7NDs8yB',
      slug: 'nh-hotels-resorts',
      name: 'NH Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/nh-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '87e554d1cd3d96f0cb9f49ea9d68856c681e408bd1f2c04ea9d6d27e3c981cda'
    },
    {
      id: 'recSAUYLDhZPmEita',
      slug: 'oaks-hotels-resorts-suites',
      name: 'Oaks Hotels, Resorts & Suites',
      overlayUrl: '/brand-explorer-phase2e-preview/oaks-hotels-resorts-suites-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c81f419a7b06d8f0c42fd0eb20e11450fd0519a4bd34aa122ac19081d18eed2d'
    },
    {
      id: 'recm6hadfIlOVdszS',
      slug: 'gran-melia-hotels-resorts',
      name: 'Gran Meliá Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/gran-melia-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e0f52b2fe1b75e62370179e75967653a306728cee7c770998b066fc9cf53776f'
    },
    {
      id: 'recoMzqIJuI2du3VS',
      slug: 'me-by-melia',
      name: 'ME by Meliá',
      overlayUrl: '/brand-explorer-phase2e-preview/me-by-melia-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'f9e590f5df40372841903e8e9924b52f2d3c38d32a77fef1632664cc01b96ca4'
    },
    {
      id: 'recR7im3gU3xoddBM',
      slug: 'melia-hotels-resorts',
      name: 'Meliá Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/melia-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2ecd6f8401c2fe995959afba953fed63f759ed75a96a51f40e5d49899096e285'
    },
    {
      id: 'rec5dHX0hHSdA8nbd',
      slug: 'barcelo-hotels-resorts',
      name: 'Barceló Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/barcelo-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-15-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '74b790cf59748b86eaef5b212107930e81e37800573a529094c0ea6d5f2445cf'
    },
    {
      id: 'recRqHJW2SbKY0N2v',
      slug: 'shangri-la-hotels-resorts',
      name: 'Shangri-La Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/shangri-la-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-16-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0a26c0f405306228eaaa223785229befdfb78d31d84d75ce872025ef6fabf9c4'
    },
    {
      id: 'NEW_allegro',
      slug: 'allegro-hotels',
      name: 'Allegro Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/allegro-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'afedc242beb11f5a6bd538d98959007c87dffd18eec5859ac80d9a33bbd30bd5',
      parentCompany: 'Barceló Hotel Group',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_alua',
      slug: 'alua-hotels-resorts',
      name: 'Alua Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/alua-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'bf7a9aadd98ad2024ef327141c5dd9ea60d8970e99b06a7ff9cefa22519885cf',
      parentCompany: 'Hyatt Hotels Corporation',
      batchTag: 'batch-13',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_arjaan',
      slug: 'arjaan-hotel-apartments',
      name: 'Arjaan Hotel Apartments',
      overlayUrl: '/brand-explorer-phase2e-preview/arjaan-hotel-apartments-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '828454bb80ad00189265226e8c038fb24caaf2e9d658021e1e364df65ac2b85f',
      parentCompany: 'Rotana',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_asai',
      slug: 'asai-hotels',
      name: 'ASAI Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/asai-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '871e585a087d333b00eed79e3beccaffeaae9f7a64c084a2a74dbf5eaa71251b',
      parentCompany: 'Dusit International',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_avani',
      slug: 'avani-hotels-resorts',
      name: 'Avani Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/avani-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e917047ccfcc1b82bc8b79c7f80a049b2204011bdb04ad3a3290f08af62680a9',
      parentCompany: 'Minor Hotels',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_centro',
      slug: 'centro-hotels',
      name: 'Centro Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/centro-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3a414fda55a7f36f4273f80079b86ca13b00b00cb1a900e90b1f68860b621f6c',
      parentCompany: 'Rotana',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_citadines',
      slug: 'citadines',
      name: 'Citadines',
      overlayUrl: '/brand-explorer-phase2e-preview/citadines-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c97b792f76d8aadbb5014dbd8e2ea3e9a9aacd25dbd86f2d0ab16aa5393dabd6',
      parentCompany: 'The Ascott Limited',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_dusit-collection',
      slug: 'dusit-collection',
      name: 'Dusit Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/dusit-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '89b6cf1ed4c3936fadf3bab91f0f693dac2f07e8a18c0aa95689c915c80e6604',
      parentCompany: 'Dusit International',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_dusit-princess',
      slug: 'dusit-princess',
      name: 'Dusit Princess',
      overlayUrl: '/brand-explorer-phase2e-preview/dusit-princess-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5ad04c4ac11d32dad231cffe0ae991f149a98408609b4bde52a23533982bd874',
      parentCompany: 'Dusit International',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_dusit-thani',
      slug: 'dusit-thani',
      name: 'Dusit Thani',
      overlayUrl: '/brand-explorer-phase2e-preview/dusit-thani-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'a41360c2fcae2beb227a5ca9ed1a329c99695a45894b960f284b96cdf0a88363',
      parentCompany: 'Dusit International',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_dusitd2',
      slug: 'dusitd2',
      name: 'dusitD2',
      overlayUrl: '/brand-explorer-phase2e-preview/dusitd2-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'db6a2fbdda5a3e913c5fa97aecde71edc8edfddab7e572beab9c20719c4dea74',
      parentCompany: 'Dusit International',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_echo-suites',
      slug: 'echo-suites-extended-stay-by-wyndham',
      name: 'ECHO Suites Extended Stay by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/echo-suites-extended-stay-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4b167cf4fc1fa9bd6c90200d5e53b88ded719b4cc1369b91764f5468471d4a02',
      parentCompany: 'Wyndham Hotels & Resorts',
      batchTag: 'batch-14',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_elewana',
      slug: 'elewana-collection',
      name: 'Elewana Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/elewana-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'f0bfdd4ea0ad9555ec54cbfb51b3e611e8bbec68bd986f6c08fe239c92dc3013',
      parentCompany: 'Minor Hotels',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_holiday-inn-club-vacations',
      slug: 'holiday-inn-club-vacations',
      name: 'Holiday Inn Club Vacations',
      overlayUrl: '/brand-explorer-phase2e-preview/holiday-inn-club-vacations-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'de31b091f152903a5da4fc0b466fdc191c79fb429d2c1fe6f8572782c1db1fe3',
      parentCompany: 'InterContinental Hotels Group',
      batchTag: 'batch-13',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_hyatt-select',
      slug: 'hyatt-select',
      name: 'Hyatt Select',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-select-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '94c4b9ff4afd876c007195738a9eec6c8f9166613a136bc214b1d2e1f38f7f7d',
      parentCompany: 'Hyatt Hotels Corporation',
      batchTag: 'batch-11',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_hyatt-studios',
      slug: 'hyatt-studios',
      name: 'Hyatt Studios',
      overlayUrl: '/brand-explorer-phase2e-preview/hyatt-studios-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '250fcfe7ba4ac2e449cb8b01d2434e478c313629f91d298f9c6c08cb39f073a8',
      parentCompany: 'Hyatt Hotels Corporation',
      batchTag: 'batch-11',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_innside',
      slug: 'innside-by-melia',
      name: 'INNSiDE by Meliá',
      overlayUrl: '/brand-explorer-phase2e-preview/innside-by-melia-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '97e5216532d5c1bf97aafc4dab432d64846852827729188b97409e04edb0f696',
      parentCompany: 'Meliá Hotels International',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_jen',
      slug: 'jen-by-shangri-la',
      name: 'JEN by Shangri-La',
      overlayUrl: '/brand-explorer-phase2e-preview/jen-by-shangri-la-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '725b696bdcc2dc839636b157849347373a3a2fad0506113b1d53a557a7a5bc98',
      parentCompany: 'Shangri-La Group',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_kerry-hotels',
      slug: 'kerry-hotels',
      name: 'Kerry Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/kerry-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b1af0f1957b440fdd37127ef1dfa3709044d9be944c712d4ba311a6ae377c4aa',
      parentCompany: 'Shangri-La Group',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_lyf',
      slug: 'lyf',
      name: 'lyf',
      overlayUrl: '/brand-explorer-phase2e-preview/lyf-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '637cfedeb1d04891ac7888132e14744bbe786a0263466c755ae6fecadefc644d',
      parentCompany: 'The Ascott Limited',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_marriott-executive-apartments',
      slug: 'marriott-executive-apartments',
      name: 'Marriott Executive Apartments',
      overlayUrl: '/brand-explorer-phase2e-preview/marriott-executive-apartments-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0202aa1b0d8b468f5f15dd3f33451be344969608a782712f856a55673daa5bd7',
      parentCompany: 'Marriott International',
      batchTag: 'batch-14',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_nh-collection',
      slug: 'nh-collection-hotels-resorts',
      name: 'NH Collection Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/nh-collection-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4a2a6569810f6b95f04542fe6b82200fd00dead8534546a129fc48c687fc651f',
      parentCompany: 'Minor Hotels',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_nhow',
      slug: 'nhow-hotels-resorts',
      name: 'nhow Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/nhow-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'e41d705aeefcbe7eb19d581cc2b345ad4fe9a97d11af8dfce75408723bfe6193',
      parentCompany: 'Minor Hotels',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_noted-collection',
      slug: 'noted-collection',
      name: 'Noted Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/noted-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '8a49dbe7ae461764841e3de7cf245aaec03c9df8d64e9b4b3b213b1bbfefe426',
      parentCompany: 'InterContinental Hotels Group',
      batchTag: 'batch-13',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_oakwood',
      slug: 'oakwood',
      name: 'Oakwood',
      overlayUrl: '/brand-explorer-phase2e-preview/oakwood-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'fc7ae1dbb321d8f9552d2f81b4a0202f211a0cc936cfb142f3a875577ca024dc',
      parentCompany: 'The Ascott Limited',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_occidental',
      slug: 'occidental-hotels-resorts',
      name: 'Occidental Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/occidental-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '34289d59d777150fe8ec3c399503d405d5e09a178ec05cc7fb1fbce384f669fe',
      parentCompany: 'Barceló Hotel Group',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_outset-collection',
      slug: 'outset-collection-by-hilton',
      name: 'Outset Collection by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/outset-collection-by-hilton-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b7b6ff193a3399c4af06589e45899f954e4d01001776cf02e4fee3d9a8cdfa7b',
      parentCompany: 'Hilton Worldwide',
      batchTag: 'batch-12',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_pan-pacific',
      slug: 'pan-pacific-hotels-resorts',
      name: 'Pan Pacific Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/pan-pacific-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '0d0beee2ac5b7801012c5f9abc08052f2edf02242d8a6a9951ea6f659f7d0dd0',
      parentCompany: 'Pan Pacific Hotels Group',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_paradisus',
      slug: 'paradisus-by-melia',
      name: 'Paradisus by Meliá',
      overlayUrl: '/brand-explorer-phase2e-preview/paradisus-by-melia-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '2faf4655de639aa666be02a5b0d45f3044f8a0144f5aa784de850b002f6a65ed',
      parentCompany: 'Meliá Hotels International',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_parkroyal',
      slug: 'parkroyal-hotels-resorts',
      name: 'PARKROYAL Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/parkroyal-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '983a80ab74f06a4a7113032064d73ee2df9cce7dc565457c0b3aa53eb0d34873',
      parentCompany: 'Pan Pacific Hotels Group',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_parkroyal-collection',
      slug: 'parkroyal-collection',
      name: 'PARKROYAL COLLECTION',
      overlayUrl: '/brand-explorer-phase2e-preview/parkroyal-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '95c8236e11f94d8241954dab59358e78bdd6c20490894bb06f387bc6a4f873e4',
      parentCompany: 'Pan Pacific Hotels Group',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_rayhaan',
      slug: 'rayhaan-hotels-resorts',
      name: 'Rayhaan Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/rayhaan-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '3c142668cdf6de28ed18e2aa299a0d79abb1dd3f1768904503b742a306e429a0',
      parentCompany: 'Rotana',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_rotana',
      slug: 'rotana-hotels-resorts',
      name: 'Rotana Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/rotana-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'bef7ae4c2a4155c123259e7ce23a57e23ccd66ac37bcc71382e8568afdfc6467',
      parentCompany: 'Rotana',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_royal-hideaway',
      slug: 'royal-hideaway-hotels-resorts',
      name: 'Royal Hideaway Hotels & Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/royal-hideaway-hotels-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '6c0ee0cfdb7e610d1fdd667d02eeb07b2ed97fcbdd7add1a640f1a9d19d8332f',
      parentCompany: 'Barceló Hotel Group',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_ruby-hotels',
      slug: 'ruby-hotels',
      name: 'Ruby Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/ruby-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '34932e629b13fc921ede3672d9351315a18c6c2cdcee1161bb07235d0c7ba262',
      parentCompany: 'InterContinental Hotels Group',
      batchTag: 'batch-13',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_sol-by-melia',
      slug: 'sol-by-melia',
      name: 'Sol by Meliá',
      overlayUrl: '/brand-explorer-phase2e-preview/sol-by-melia-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5d99c1fa119641dee3a18e69352d4492b58cd603f32032802a7e1976c495fe8e',
      parentCompany: 'Meliá Hotels International',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_somerset',
      slug: 'somerset',
      name: 'Somerset',
      overlayUrl: '/brand-explorer-phase2e-preview/somerset-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '183400539ef0a74e2e6e31a65ed96ccbf6b11e35022ff6be4162e5400c457568',
      parentCompany: 'The Ascott Limited',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_sonesta-essential',
      slug: 'sonesta-essential-hotels',
      name: 'Sonesta Essential Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/sonesta-essential-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '57ef523900692961bf38b115017cddd84b1d087e66a129c1a65621069ce73291',
      parentCompany: 'Sonesta International Hotels Corporation',
      batchTag: 'batch-11',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_traders',
      slug: 'traders-hotels',
      name: 'Traders Hotels',
      overlayUrl: '/brand-explorer-phase2e-preview/traders-hotels-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'c9830e273968d253b941d0d28124a45464d843ad29fec836f3102aef873b7241',
      parentCompany: 'Shangri-La Group',
      batchTag: 'batch-16',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_waterwalk',
      slug: 'waterwalk-extended-stay-by-wyndham',
      name: 'WaterWalk Extended Stay by Wyndham',
      overlayUrl: '/brand-explorer-phase2e-preview/waterwalk-extended-stay-by-wyndham-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '4a73cbb0bed2c77c970b32e337092e2b6321c4f56c1317483d1312b38f939fa3',
      parentCompany: 'Wyndham Hotels & Resorts',
      batchTag: 'batch-14',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_wyndham-alltra',
      slug: 'wyndham-alltra',
      name: 'Wyndham Alltra',
      overlayUrl: '/brand-explorer-phase2e-preview/wyndham-alltra-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '5522ac5919e33c54117a5997e8b9aea9a77f7b81b83b274102efe6539cfd34dd',
      parentCompany: 'Wyndham Hotels & Resorts',
      batchTag: 'batch-14',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_zel',
      slug: 'zel',
      name: 'ZEL',
      overlayUrl: '/brand-explorer-phase2e-preview/zel-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'da9782f79fc90b8b1cd2312297cc611e00337f4627423834e9c17ee5881e5263',
      parentCompany: 'Meliá Hotels International',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_zoetry',
      slug: 'zoetry-wellness-spa-resorts',
      name: 'Zoëtry Wellness & Spa Resorts',
      overlayUrl: '/brand-explorer-phase2e-preview/zoetry-wellness-spa-resorts-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b4d17411fe3e7b3b814d1c8a5939511919e10143414b53826aac67594a5fcc78',
      parentCompany: 'Hyatt Hotels Corporation',
      batchTag: 'batch-13',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
    },
    {
      id: 'NEW_melia-collection',
      slug: 'the-melia-collection',
      name: 'The Meliá Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/the-melia-collection-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: 'b60a67d44956657c0a97a52dd15d29ef5ae5f5e560b32a084928f28b02839af2',
      parentCompany: 'Meliá Hotels International',
      batchTag: 'batch-15',
      factoryStatus: 'NEEDS_REMEDIATION',
      awaitingBrandBasicsCreate: true
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
    if (/^jw marriott\b/i.test(name)) return bySlug('jw-marriott');
    if (/^w hotels\b/i.test(name)) return bySlug('w-hotels');
    if (/le m[eé]ridien/i.test(name)) return bySlug('le-meridien');
    if (/waldorf/i.test(name)) return bySlug('waldorf-astoria');
    if (/^conrad\b/i.test(name)) return bySlug('conrad-hotels-resorts');
    if (/\blxr\b/i.test(name)) return bySlug('lxr-hotels-resorts');
    if (/park hyatt/i.test(name)) return bySlug('park-hyatt');
    if (/grand hyatt/i.test(name)) return bySlug('grand-hyatt');
    if (/^andaz\b/i.test(name)) return bySlug('andaz');
    if (/ritz[- ]?carlton/i.test(name)) return bySlug('ritz-carlton');
    if (/st\.?\s*regis/i.test(name)) return bySlug('st-regis');
    if (/\bedition\b/i.test(name)) return bySlug('edition-hotels');
    if (/luxury collection/i.test(name)) return bySlug('luxury-collection');
    if (/renaissance/i.test(name)) return bySlug('renaissance-hotels');
    if (/signia/i.test(name)) return bySlug('signia-by-hilton');
    if (/^alila\b/i.test(name)) return bySlug('alila');
    if (/hyatt house/i.test(name)) return bySlug('hyatt-house');
    if (/intercontinental/i.test(name)) return bySlug('intercontinental');
    if (/^garner\b/i.test(name)) return bySlug('garner-hotels');
    if (/sleep inn/i.test(name)) return bySlug('sleep-inn');
    if (/econo\s*lodge/i.test(name)) return bySlug('econo-lodge');
    if (/rodeway/i.test(name)) return bySlug('rodeway-inn');
    if (/mainstay/i.test(name)) return bySlug('mainstay-suites');
    if (/days inn/i.test(name)) return bySlug('days-inn-by-wyndham');
    if (/super\s*8/i.test(name)) return bySlug('super-8-by-wyndham');
    if (/microtel/i.test(name)) return bySlug('microtel-by-wyndham');
    if (/baymont/i.test(name)) return bySlug('baymont-by-wyndham');
    if (/travelodge/i.test(name)) return bySlug('travelodge-by-wyndham');
    if (/la\s*quinta/i.test(name)) return bySlug('la-quinta-by-wyndham');
    if (/americinn/i.test(name)) return bySlug('americinn-by-wyndham');
    if (/howard\s*johnson|\bhojo\b/i.test(name)) return bySlug('howard-johnson-by-wyndham');
    if (/^wingate\b/i.test(name)) return bySlug('wingate-by-wyndham');
    if (/^ramada\b/i.test(name)) return bySlug('ramada-by-wyndham');
    if (/hawthorn/i.test(name)) return bySlug('hawthorn-extended-stay-by-wyndham');
    if (/wyndham\s*garden/i.test(name)) return bySlug('wyndham-garden');
    if (/best\s*western\s*plus/i.test(name)) return bySlug('best-western-plus');
    if (/surestay\s*studio/i.test(name)) return bySlug('surestay-studio-by-best-western');
    if (/surestay\s*plus/i.test(name)) return bySlug('surestay-plus-by-best-western');
    if (/surestay/i.test(name)) return bySlug('surestay-by-best-western');
    if (/\bgl[oō]\b|glo by best western/i.test(name)) return bySlug('glo-by-best-western');
    if (/executive\s*residency/i.test(name)) return bySlug('executive-residency-by-best-western');
    if (/^aiden\b/i.test(name)) return bySlug('aiden-by-best-western');
    if (/^best\s*western\b/i.test(name) && !/plus|premier|surestay|aiden|gl/i.test(name)) return bySlug('best-western');
    if (/sonesta\s*select/i.test(name)) return bySlug('sonesta-select');
    if (/sonesta\s*es|\bes\s*suites\b/i.test(name)) return bySlug('sonesta-es-suites');
    if (/red\s*lion/i.test(name)) return bySlug('red-lion-hotels');
    if (/signature\s*inn/i.test(name)) return bySlug('signature-inn');
    if (/americas?\s*best\s*value|\babvi\b/i.test(name)) return bySlug('americas-best-value-inn');
    if (/city\s*express/i.test(name)) return bySlug('city-express-by-marriott');
    if (/spring\s*hill|springhill/i.test(name)) return bySlug('springhill-suites-by-marriott');
    if (/studio\s*res|studiores/i.test(name)) return bySlug('studiores');
    if (/towne\s*place|towneplace/i.test(name)) return bySlug('towneplace-suites-by-marriott');
    if (/fairfield/i.test(name)) return bySlug('fairfield-by-marriott');
    if (/four\s*points/i.test(name)) return bySlug('four-points-by-sheraton');
    if (/residence\s*inn/i.test(name)) return bySlug('residence-inn-by-marriott');
    if (/^marriott\s*hotels?\b/i.test(name) && !/bonvoy|city express|fairfield|spring|towne|residence|delta|autograph|tribute|aloft|moxy|ac hotels|courtyard|westin|sheraton|renaissance|design|bunkhouse|edition|ritz|st\.?\s*regis|jw|w hotels|le m/i.test(name)) return bySlug('marriott-hotels');
    if (/^sheraton\b/i.test(name) && !/four\s*points/i.test(name)) return bySlug('sheraton');
    if (/hilton\s*hotels|hilton\s*&\s*resorts/i.test(name)) return bySlug('hilton-hotels-resorts');
    if (/double\s*tree|doubletree/i.test(name)) return bySlug('doubletree-by-hilton');
    if (/^hampton\b/i.test(name)) return bySlug('hampton-by-hilton');
    if (/hilton\s*garden/i.test(name)) return bySlug('hilton-garden-inn');
    if (/home\s*2|home2/i.test(name)) return bySlug('home2-suites-by-hilton');
    if (/homewood/i.test(name)) return bySlug('homewood-suites-by-hilton');
    if (/\bspark\b/i.test(name) && /hilton/i.test(name)) return bySlug('spark-by-hilton');
    if (/^tru\b|tru by hilton/i.test(name)) return bySlug('tru-by-hilton');
    if (/hyatt\s*regency/i.test(name)) return bySlug('hyatt-regency');
    if (/thompson/i.test(name)) return bySlug('thompson-hotels');
    if (/delta\s*hotels/i.test(name)) return bySlug('delta-hotels-by-marriott');
    if (/handwritten/i.test(name)) return bySlug('handwritten-collection');
    if (/mgallery|m gallery/i.test(name)) return bySlug('mgallery-collection');
    if (/^pullman\b/i.test(name)) return bySlug('pullman');
    if (/^fairmont\b/i.test(name)) return bySlug('fairmont');
    if (/^mercure\b/i.test(name) && !/grand\s*mercure/i.test(name)) return bySlug('mercure');
    if (/^ibis\b/i.test(name) && !/styles|budget/i.test(name)) return bySlug('ibis');
    if (/mama\s*shelter/i.test(name)) return bySlug('mama-shelter');
    if (/^novotel\b/i.test(name)) return bySlug('novotel');
    if (/^so\/?$/i.test(name) || /^so\/\b/i.test(name) || /\bso\/\b/i.test(name)) return bySlug('so');
    if (/bw\s*premier|premier\s*collection/i.test(name) && /best\s*western|bw\b/i.test(name)) return bySlug('bw-premier-collection');
    if (/bw\s*signature|signature\s*collection/i.test(name) && /best\s*western|bw\b/i.test(name)) return bySlug('bw-signature-collection');
    if (/trademark/i.test(name) && /wyndham/i.test(name)) return bySlug('trademark-collection-by-wyndham');
    if (/dazzler/i.test(name)) return bySlug('dazzler-by-wyndham');
    if (/suburban\s*studios|^suburban\b/i.test(name)) return bySlug('suburban-studios');
    if (/everhome/i.test(name)) return bySlug('everhome-suites');
    if (/^quality\s*inn\b/i.test(name)) return bySlug('quality-inn');
    if (/radisson\s*red/i.test(name)) return bySlug('radisson-red-by-choice');
    if (/^radisson\b/i.test(name) && !/red|blu|individuals|collection/i.test(name)) return bySlug('radisson-by-choice');
    if (/preferred\s*hotels/i.test(name)) return bySlug('preferred-hotels-and-resorts');
    if (/small\s*luxury\s*hotels|\bslh\b/i.test(name)) return bySlug('small-luxury-hotels-of-the-world');
    if (/radisson\s*blu/i.test(name)) return bySlug('radisson-blu-by-choice');
    if (/comfort\s*inn\s*&\s*suites|comfort\s*inn\s*and\s*suites/i.test(name)) return bySlug('comfort-inn-suites');
    if (/radisson\s*individuals/i.test(name)) return bySlug('radisson-individuals-by-choice');
    if (/country\s*inn\s*&\s*suites|country\s*inn\s*and\s*suites/i.test(name)) return bySlug('country-inn-suites-by-choice');
    if (/woodspring/i.test(name)) return bySlug('woodspring-suites');
    if (/^cambria\b/i.test(name)) return bySlug('cambria-hotels');
    if (/clarion\s*pointe/i.test(name)) return bySlug('clarion-pointe');
    if (/^clarion\b/i.test(name) && !/pointe/i.test(name)) return bySlug('clarion');
    if (/wyndham\s*grand/i.test(name)) return bySlug('wyndham-grand');
    if (/\bdolce\b/i.test(name)) return bySlug('dolce-hotels-resorts-by-wyndham');
    if (/\btryp\b/i.test(name)) return bySlug('tryp-by-wyndham');
    if (/esplendor/i.test(name)) return bySlug('esplendor-by-wyndham');
    if (/registry\s*collection/i.test(name)) return bySlug('registry-collection-hotels');
    if (/caption\s*by\s*hyatt|^caption\b/i.test(name)) return bySlug('caption-by-hyatt');
    if (/joie\s*de\s*vivre|\bjdv\b/i.test(name)) return bySlug('jdv-by-hyatt');
    if (/unbound\s*collection/i.test(name)) return bySlug('unbound-collection-by-hyatt');
    if (/sonesta\s*simply\s*suites/i.test(name)) return bySlug('sonesta-simply-suites');
    if (/graduate/i.test(name) && /hilton/i.test(name)) return bySlug('graduate-by-hilton');
    if (/livsmart/i.test(name)) return bySlug('livsmart-studios-by-hilton');
    if (/embassy\s*suites/i.test(name)) return bySlug('embassy-suites-by-hilton');
    if (/\bnomad\b/i.test(name)) return bySlug('nomad-hotels');
    if (/ibis\s*styles/i.test(name)) return bySlug('ibis-styles');
    if (/ibis\s*budget/i.test(name)) return bySlug('ibis-budget');
    if (/^greet\b/i.test(name)) return bySlug('greet');
    if (/hotelf1|hotel\s*f1|formule\s*1/i.test(name)) return bySlug('hotelf1');
    if (/^tribe\b/i.test(name)) return bySlug('tribe');
    if (/adagio\s*access/i.test(name)) return bySlug('adagio-access');
    if (/\badagio\b/i.test(name) && !/access/i.test(name)) return bySlug('adagio');
    if (/25\s*hours|25hours/i.test(name)) return bySlug('25hours-hotels');
    if (/hoxton/i.test(name)) return bySlug('the-hoxton');
    if (/mondrian/i.test(name)) return bySlug('mondrian');
    if (/^hyde\b/i.test(name)) return bySlug('hyde-hotels');
    if (/\bsls\b/i.test(name)) return bySlug('sls-hotels');
    if (/swissotel|swissôtel/i.test(name)) return bySlug('swissotel');
    if (/park\s*plaza/i.test(name)) return bySlug('park-plaza');
    if (/park\s*inn/i.test(name)) return bySlug('park-inn-by-radisson');
    if (/^regent\b/i.test(name)) return bySlug('regent-hotels-resorts');
    if (/crowne\s*plaza/i.test(name)) return bySlug('crowne-plaza');
    if (/hualuxe/i.test(name)) return bySlug('hualuxe-hotels-resorts');
    if (/staybridge/i.test(name)) return bySlug('staybridge-suites');
    if (/candlewood/i.test(name)) return bySlug('candlewood-suites');
    if (/atwell/i.test(name)) return bySlug('atwell-suites');
    if (/^holiday\s*inn\b/i.test(name) && !/express|club/i.test(name)) return bySlug('holiday-inn');
    if (/hyatt\s*ziva|^ziva\b/i.test(name)) return bySlug('hyatt-ziva');
    if (/hyatt\s*zilara|^zilara\b/i.test(name)) return bySlug('hyatt-zilara');
    if (/secrets\s*resorts|\bsecrets\b/i.test(name)) return bySlug('secrets-resorts-spas');
    if (/dreams\s*resorts/i.test(name) || (/\bdreams\b/i.test(name) && /resort|spa|hyatt/i.test(name))) return bySlug('dreams-resorts-spas');
    if (/breathless/i.test(name)) return bySlug('breathless-resorts-spas');
    if (/sunscape/i.test(name)) return bySlug('sunscape-resorts-spas');
    if (/hyatt\s*vivid|^vivid\b/i.test(name)) return bySlug('hyatt-vivid');
    if (/miraval/i.test(name)) return bySlug('miraval-resorts-spas');
    if (/sofitel\s*legend/i.test(name)) return bySlug('sofitel-legend');
    if (/^sofitel\b/i.test(name) && !/legend/i.test(name)) return bySlug('sofitel');
    if (/\braffles\b/i.test(name)) return bySlug('raffles-hotels-resorts');
    if (/m[oö]venpick|movenpick/i.test(name)) return bySlug('movenpick-hotels-resorts');
    if (/grand\s*mercure/i.test(name)) return bySlug('grand-mercure');
    if (/\bpeppers\b/i.test(name)) return bySlug('peppers');
    if (/\bmantra\b/i.test(name)) return bySlug('mantra');
    if (/break\s*free|breakfree/i.test(name)) return bySlug('breakfree');
    if (/\bsebel\b/i.test(name)) return bySlug('the-sebel');
    if (/banyan\s*tree/i.test(name)) return bySlug('banyan-tree');
    if (/\bangsana\b/i.test(name)) return bySlug('angsana');
    if (/art\s*series/i.test(name)) return bySlug('art-series-hotels');
    if (/citizen\s*m|citizenm/i.test(name)) return bySlug('citizenm');
    if (/\belement\b/i.test(name) && /westin|hotel/i.test(name)) return bySlug('element-hotels');
    if (/\bprotea\b/i.test(name)) return bySlug('protea-hotels-by-marriott');
    if (/four\s*points\s*flex/i.test(name)) return bySlug('four-points-flex-by-sheraton');
    if (/\banantara\b/i.test(name)) return bySlug('anantara-hotels-resorts');
    if (/\btivoli\b/i.test(name)) return bySlug('tivoli-hotels-resorts');
    if (/nh\s*collection/i.test(name)) return bySlug('nh-collection-hotels-resorts');
    if (/^nh\b/i.test(name) && !/collection|nhow/i.test(name)) return bySlug('nh-hotels-resorts');
    if (/\bnhow\b/i.test(name)) return bySlug('nhow-hotels-resorts');
    if (/\bavani\b/i.test(name)) return bySlug('avani-hotels-resorts');
    if (/\boaks\b/i.test(name)) return bySlug('oaks-hotels-resorts-suites');
    if (/\belewana\b/i.test(name)) return bySlug('elewana-collection');
    if (/gran\s*meli/i.test(name)) return bySlug('gran-melia-hotels-resorts');
    if (/\bme\b.*meli|meli.*\bme\b/i.test(name)) return bySlug('me-by-melia');
    if (/meli[aá]\s*collection|the\s*meli[aá]\s*collection/i.test(name)) return bySlug('the-melia-collection');
    if (/\bparadisus\b/i.test(name)) return bySlug('paradisus-by-melia');
    if (/^meli[aá]\b/i.test(name) && !/gran|collection|paradisus|innside|\bme\b|\bzel\b|\bsol\b/i.test(name)) return bySlug('melia-hotels-resorts');
    if (/innside/i.test(name)) return bySlug('innside-by-melia');
    if (/\bzel\b/i.test(name)) return bySlug('zel');
    if (/\bsol\b.*meli|meli.*\bsol\b/i.test(name)) return bySlug('sol-by-melia');
    if (/royal\s*hideaway/i.test(name)) return bySlug('royal-hideaway-hotels-resorts');
    if (/barcel/i.test(name) && !/royal\s*hideaway|occidental|allegro/i.test(name)) return bySlug('barcelo-hotels-resorts');
    if (/\boccidental\b/i.test(name)) return bySlug('occidental-hotels-resorts');
    if (/\ballegro\b/i.test(name)) return bySlug('allegro-hotels');
    if (/pan\s*pacific/i.test(name)) return bySlug('pan-pacific-hotels-resorts');
    if (/park\s*royal\s*collection/i.test(name)) return bySlug('parkroyal-collection');
    if (/park\s*royal/i.test(name) && !/collection/i.test(name)) return bySlug('parkroyal-hotels-resorts');
    if (/shangri/i.test(name) && !/kerry|\bjen\b|traders/i.test(name)) return bySlug('shangri-la-hotels-resorts');
    if (/\bkerry\b/i.test(name)) return bySlug('kerry-hotels');
    if (/\bjen\b.*shangri|shangri.*\bjen\b/i.test(name)) return bySlug('jen-by-shangri-la');
    if (/\btraders\b/i.test(name)) return bySlug('traders-hotels');
    if (/dusit\s*thani/i.test(name)) return bySlug('dusit-thani');
    if (/dusit\s*d2|dusitd2/i.test(name)) return bySlug('dusitd2');
    if (/dusit\s*princess/i.test(name)) return bySlug('dusit-princess');
    if (/\basai\b/i.test(name)) return bySlug('asai-hotels');
    if (/dusit\s*collection/i.test(name)) return bySlug('dusit-collection');
    if (/^rotana\b/i.test(name) && !/rayhaan|arjaan|centro/i.test(name)) return bySlug('rotana-hotels-resorts');
    if (/\brayhaan\b/i.test(name)) return bySlug('rayhaan-hotels-resorts');
    if (/\barjaan\b/i.test(name)) return bySlug('arjaan-hotel-apartments');
    if (/\bcentro\b/i.test(name) && /rotana|hotel/i.test(name)) return bySlug('centro-hotels');
    if (/\bcitadines\b/i.test(name)) return bySlug('citadines');
    if (/\bsomerset\b/i.test(name)) return bySlug('somerset');
    if (/\blyf\b/i.test(name)) return bySlug('lyf');
    if (/\boakwood\b/i.test(name)) return bySlug('oakwood');
    if (/six senses/i.test(name)) return bySlug('six-senses');
    if (/hyatt place/i.test(name)) return bySlug('hyatt-place');
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
