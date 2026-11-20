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
      approvalContentHash: '60d56158cef771d0c57afaba9c75c87f4ce4225fc21ab4c7ab570884d1fc8568'
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
      approvalContentHash: '3367f7905d0448feb3f490ee0311d42814a8571f162eb01dd0db657dc7b90082'
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
      approvalContentHash: '79c2683a7deab241e088e949e82ea03bf4dc9d690383fa2e89ced7bde03e07c8'
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
      approvalContentHash: '17f7a2a46d180b4fd442f7c788f30b446835ca78c8977fed635b0c5f62ad6f35'
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
      approvalContentHash: '99a941f5fb3bd80d587fd90b45c1d33e5f373e3e88599b3024ae835fb975f5bd'
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
      approvalContentHash: '49d39d29274d54bea47704756fe4f07bae36a4583c9dde551b95aa21ae224fb1'
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
      approvalContentHash: '4ac19f56b3b47aef1618e254d345a32ba31e33b46622ca32045b5f6852571380'
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
      approvalContentHash: 'ec44f44dc10ee710a10f50a57f21026b6f7d0cf698ba1a645ac96886edbe521b'
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
      approvalContentHash: 'c540832f80afa187bbdc48c20abebba1ed13e9f0e0f5d28c45dc0ecfb2d22515'
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
      approvalContentHash: 'b317d06e90df190fb5804078e9f9b93e71b69cf60c6429631be4f64150b0e728'
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
      approvalContentHash: '659b46483a83bfe8777d28df45b5e269cdbc3d1fc612c1ebf97d4c52bc9e8107'
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
      approvalContentHash: '75ffb9606fd3fd01281256da3d2f895287590771bb6dfbbc9ca0af8697e750c9'
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
      approvalContentHash: '5e0c1a86e39447c2a22f5a9b0be2583ab785d35e3d96df38361e8b15a4a6c7bb'
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
      approvalContentHash: '7385b4995f5c508e86ac0bac5663d845c022ee1d00422064793a10625edfb4b8'
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
      approvalContentHash: '7847806e0566639e125fcf15b59973399531886768030e06012e1a748fdf6c86'
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
      approvalContentHash: '07cabdbd707656155a25992bcfc7ff836f52ed5c49c3c8faee1c88e090a0fa3f'
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
      approvalContentHash: '22026b685e8f3ab78b673a3a7f452f285dd28e2d31aa86e85473466075bf72ce'
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
      approvalContentHash: '3b60eebb1ad9fd84a62a5f40d53367099c6e9f3b19cf6a57b81a29a233cd9b62'
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
      approvalContentHash: '3bdae3d0e9cd0027d08ca93d5001a70adcf678d7337741b7cefb854853364760'
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
      approvalContentHash: 'd62a547d53c5d46ab73fb173d075d6cc04800432d6704ca4e3da214b9064f0ee'
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
      approvalContentHash: '230326afb02f7ce4c4819abcbac3d2e4005a4eb688bdbbf668515cd3f3de2cfd'
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
      approvalContentHash: '7e324e145ba689ebfebb81647d26b5a3c377d9b49960071e46f98ae0cb1ff03b'
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
      approvalContentHash: 'df5f7d90d4a1a91b05f674a2404d409eb7880af563a61053e39790cba9599115'
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
      approvalContentHash: '76150adfe76c6936895933463364e82b66defe9b25af7b0ecf539b22285ff7c7'
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
      approvalContentHash: 'dc1986c8d2c90b636d328fc4b879ca559c89890a83735d0a87438e005d05b7f5'
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
      approvalContentHash: '64222be812c82a0bcea562dd8e7bd2383933cd7d46b8941b190df82932cdcc35'
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
      approvalContentHash: 'ff2377da71815a04a2a336cd8f5bcdabe1d11ca22dde979bc791c0f56b376500'
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
      approvalContentHash: '64ddf795ee3e0de8c35b0fc377ab0fd2898c9ce7d0543b73edd476be7df6de60'
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
      approvalContentHash: '20d96efee66ab8f93dd800294ee0cd827a485991ea8465961d13e62bee37bcc4'
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
      approvalContentHash: 'b97a53fbb4aa3f6c2f21c8960d851b759b5448ba3a541c720431da00913a72b6'
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
      approvalContentHash: '7cc8153cbfca2ac0fb8e54b76d6c5208c9c5151d078c9e999dc275e7ee267a4e'
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
      approvalContentHash: '1ae5225bd9ba07d92e7edf460d98745f0a83ddae1410f20ae777b046d82f3373'
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
      approvalContentHash: 'a927f239601e38726b980f97e4fe563f3ed71126d5f4d5f5f39a9c91ee29a4b8'
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
      approvalContentHash: '5c2db72da1462bb0ed2f6104f4b4d650c540054b0de2de72f09a97027f81d252'
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
      approvalContentHash: 'c7a836176402937088694c835bd82b3a1e2c4e5557d5d8c00d10a7654bf03a59'
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
      approvalContentHash: '8a421d2c83bdddcb198dbc357e482841046199f2c85213ec734f1496b0002a98'
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
      approvalContentHash: '7da6e8c430e00a93830f0d36cb317d20b5856b9494d2ed679af3e64a81661c60'
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
      approvalContentHash: '27fcacfeab9670c540b17785bef85594a903c68cfe1ed4c63ca751e7f5d321d5'
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
      approvalContentHash: '7768cfe2040a1f075586c3d1435ec711004c70d549cc70c137cf33d9fdb7a519'
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
      approvalContentHash: '69b0a472d9f222fc55f8d55f18e513fb80ac9cd1da73e9aa25a95dee74705f49'
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
      approvalContentHash: '745f05c37e260fdb87fbb43c5fcde99ded66611c9b4343d214ccafe2b897cb75'
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
      approvalContentHash: '4f9bace008fdc471afcdc272dfd2fb60ec28b4dc49d5d8827e1d6530f9d7d179'
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
      approvalContentHash: 'a2b1e91e1384eef7335c59d833c1f812e6866f6bfa68b3f2badc10d6cd6864ec'
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
