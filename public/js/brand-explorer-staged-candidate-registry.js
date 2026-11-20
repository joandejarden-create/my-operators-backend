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
      approvalContentHash: '5c4bb5361d03960995d1e9351eff47603fcd909dcdff71bd329594ec346e03ef'
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
      approvalContentHash: '07c07d9211cffd0339e34b64a50ffa3a59d290c847ad709ca43328050db9945f'
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
      approvalContentHash: 'ae4840c2a1784c0bd5668445565c2ae62277646e1e66e4f7ee2d48678f40c027'
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
      approvalContentHash: '687bb8738b6b79f667ca00601594e4c7973e26be96daa42baef7baced570b185'
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
      approvalContentHash: '102f95762fff0b7df5a789c2367021400834b331f3256ad4d9c99b2ca3143d63'
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
      approvalContentHash: '8869ce818d410d306938baeed43410c3bbe9f30d6d950c5522b9e157f698348d'
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
      approvalContentHash: '82b84135f62aea1622087bf8ca59d76938bfdce48b9827f7f0df38f1dcb3ebad'
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
      approvalContentHash: '67eb43b3257c7a8068a2f2b930ca4e05e57b99ac1ec2ffa6a30530c4683f73fd'
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
      approvalContentHash: '3699732d438bb1ce1978250effb131886f1362272b033112922b0accf50f1f60'
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
      approvalContentHash: 'f5d9a67602ad0bdeed8d565a13c26d9dd2b77cb41ce7b5c27399ead8a9ce09ca'
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
      approvalContentHash: '698bbdc1afe054e50139d5dd9c707bbdd37103c5ce3cdb73a6b2b8e3513ec161'
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
      approvalContentHash: 'c8d4a1358d3fb451dd734c4d55441317f80e90a85fbda9c81aa12b62cacc18bd'
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
      approvalContentHash: 'b296767d2a5182d4ad2224e2819dffd55b4ca5db65623ae86ab3f78887bc1815'
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
      approvalContentHash: '1938eec7d202e207a9fc61ba51a68eac7e1ef64443dc2b431866e265b8ad82d3'
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
      approvalContentHash: '27a9c7a05a3ef26dee31e10fc47dcf4abf67e07f1edb671654c1257dd0cc0260'
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
      approvalContentHash: '79d952fe46e7337ca6def78c54e401b8ab2431b0f4970e66dc5ff9770fd94cb6'
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
      approvalContentHash: '9c8ff695fc77d7178728e1ebeffeb77cc8dd0c1f75725d589a19491706c559da'
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
      approvalContentHash: '20590601a99f526140cbfa29b60348d3bf64158851f3f6e0ff7fa866e9c7ee71'
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
      approvalContentHash: '62896a83160d4f478851642a5595e3cbb17a3286df8f9cfeee936561d7ddfe37'
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
      approvalContentHash: 'bac738c22553cbb77d2fddc01975c9d29ff93b0fd7a460214fe8f214ad54e112'
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
      approvalContentHash: 'e4567d792819d2b23cd696fe423fe0a3a1855ebe3c317bedbb1bbfbd616f119e'
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
      approvalContentHash: '9a11ee34928dbf76d13ca8241b8f6eeb14d2d880d90762b9ca712e40bd220398'
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
      approvalContentHash: '5daab738784b53eaf24bbbc0b562d9ed87bdebbb0610ce59734f4fe776d7b97d'
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
      approvalContentHash: '8b4a7c97c44e12d68e47128d099626f3b2598603a965a4914e6d1653ef0b4ec5'
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
      approvalContentHash: 'b46271e0aa30b66ce83464a2050c8b29e6717749bb1207c9b1c1b23f7b02c0ba'
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
      approvalContentHash: 'e02686b934b85375d801b356d1ae0d44cf12d1d9d77d8fb7d4ffce028bdd3bd3'
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
      approvalContentHash: '01766f0898319d0620eaf4faff4b2b7e8b7c738bfb548e492807447cea0bc33b'
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
      approvalContentHash: 'ce30b2ed3395db805b41c37212c82dc11d7f21e03e3a56f23fbd834ad90a9ad1'
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
      approvalContentHash: '01d32caeee56330d635a8431fcd203b29c74b3d9a7340054980cc897f930cf4f'
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
      approvalContentHash: '486556b7dd1408687f3362ff1e9de71b984729b4a1d0f14b6d386bb10933046e'
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
      approvalContentHash: 'e3d0b349cd4ed3b9e4b45f443e3005a6bbbcbd46713c0490817ab0908e66e317'
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
      approvalContentHash: 'aebecf79e5f93eb430ffeaa5216fc9ee6c541efdf27603b638c792a87a495cc5'
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
      approvalContentHash: '7d7da73d38a3ea21416adc76e427192ad644fdf63caf1a5a69e2c29d936de40f'
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
      approvalContentHash: '6a58c46c20c0239f0230ed3d27c9f3b2feaa9311a6a4cf19a3c35e7ad2bf6671'
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
      approvalContentHash: '1c9c1f6787a4acef4fb450cd623613c557dfd4a07a29f9348cb2e99e11459a02'
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
      approvalContentHash: 'f0387fe800b490b3faa9e78f25e15db7037141a8fa07c83e7995f286c211641a'
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
      approvalContentHash: 'ea369b418b88f64c13ac317bc5fd255f87c8c48c5b1f3465c2000bea6abbf581'
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
      approvalContentHash: '74f148ed76e2fbb51cd3f201e9c3bc917fa90e633c37b21c3912150097d9555e'
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
      approvalContentHash: '292c1faa9d79921a49c48383c1be120ce5fc54db62f9f413181d031555b830af'
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
      approvalContentHash: '0c804342a6e729c3299a88c7729ebddd5c3ee9af8e8c71693b4f37b789f4698c'
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
      approvalContentHash: 'd42447f9e8ffcfbf957703e7bc029ea6c951d4f356ec9ebcaf0af1f848b08926'
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
      approvalContentHash: 'a5e4b93dc475ec5bafe5f4388afcf3cd87bee25200ac986d712e6f253c1a90e6'
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
      approvalContentHash: '075cdc9c6e01f8ab15461be39d7b161aedb1d9f35944761e29715d3ad3e5b05c'
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
      approvalContentHash: '3f5ae3531ed46da4fde6d53d0cadfd557b029f5a9cc75a328fb176805666a419'
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
      approvalContentHash: 'ca3b413aa8a9e5466350ab070f0a9fc847611d4d565e9e27133ed0c04622860c'
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
      approvalContentHash: '08593e2f64e6ee6a61b311d8f1a50c00aff386b7fb3ab66ba69e32d1c352d185'
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
      approvalContentHash: '83befe6c225aff37efd9d146e19b5278e4944e733c143fc6d47df9f9d23182a4'
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
      approvalContentHash: '9a053839df37d204849d324ecb204d59f30d7940548c8d1f4bf19b72d1e3d619'
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
      approvalContentHash: '1378530bfed1b104d3affafcc1497afbdf392cff43ea2c72411b4342c181f4ae'
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
      approvalContentHash: '49a34d5ccf2f0d9deed5c7ba0f88aaee464b0af0465eceefecdeafe72fd03ee4'
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
      approvalContentHash: 'dc185200d683b65b5fb4175ffa08a4d7ecd1f156fd6aa8944083d6739cfcb279'
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
      approvalContentHash: '125092f97ed5ff6f77d5a311886bd756cc872dadfbe817b953d689dab1b1089b'
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
      approvalContentHash: '8b6309d823f49f4e25adc38f70c3f6dc6acfa68d69c46c2bbd31c64417761825'
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
      approvalContentHash: '8255c27be2f2f3e8cb9e037072a64daaba4977c313f51cec8772590905d6b646'
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
      approvalContentHash: '4b9c3f9dca1d0a6a860e8c4e9546cc5672fe75a930e3a4430b6a504dc4ac4abc'
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
      approvalContentHash: '928fcd5629ab35a86f55d95bed191c4931c8ad33e58a8b7324fae8a476fa02e1'
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
      approvalContentHash: '8c36e63136d4590f0f297ac1e1817d3a16c9fb9140a0f1c90e84a281150f09eb'
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
      approvalContentHash: 'e7557bb8af809b6bf956104f44526d1bc645f7e67018be1085b5ed453af7567a'
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
      approvalContentHash: 'f51458585a9a5e5342940b78f412e15e2632f61e3becca6cb932910fb2a30b31'
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
      approvalContentHash: 'd4fe39bb6e5bccf8f361bf5feeabe7e6dc1e0bc8c867d62ee2bd8784ab0c0d16'
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
      approvalContentHash: 'aa0728e28f37c9455942b8bf6065972cd3f487c89fd081d07148e2a86c021ee0'
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
      approvalContentHash: '805016825d68d3f1fc872f75f961f1c72903899afac44ac98a97d956872d3916'
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
      approvalContentHash: '1564a69b61d4fa89191f3923a0b48ce47ad18454617b52dff022677758502409'
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
      approvalContentHash: 'bd962f847d444374328fe9d3349ef721e02ad325a07387a9a5005ff2e68b7420'
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
      approvalContentHash: '5a33260345730879662a51a24c7252753ad5b168900ce38944f790e4d76e7577'
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
      approvalContentHash: 'b414ebb1bb044c3064f9e110d7aab9f416fc1ed2d472a50933cd1dd1948de07a'
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
      approvalContentHash: '260793492dfaadd660f3f2506188cf6d33ac62f8cecac386ab4b9b7fd7e98a71'
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
      approvalContentHash: 'e41f2ce49c43cbaf891db12b2aef4bbfeac01a91c55668cffbeb7096abefd6e2'
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
      approvalContentHash: '09ec819c0b17d0a0aed7daecd8fd11927404e74e88d8e56e4264ae66b5340138'
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
      approvalContentHash: 'd377fc0ac6316196be8badf70ad55c6a4737972edb4d7fdd02f24659992c9c72'
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
      approvalContentHash: '477c454fba593f409b710bf95452c931aff2e8cff371d942f0919b22943c41da'
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
      approvalContentHash: 'addcfe32df6e2c3ffc2f2cc3957f02a4fea3d9986ddfe82f96bf49b498d39d31'
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
      approvalContentHash: 'c41a1ae547bbf3de8d8db8a19e5c203ec30373548e313a7de769cc7b2b47022f'
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
      approvalContentHash: 'f651733e99a803e8a23cdd8bfc94e9ec40a2f89b3ceb925c943104612647900a'
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
      approvalContentHash: 'bb2616eda9a63c45e8c8f3246955fff497741b720d5ceefe7fc77316caf9de12'
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
      approvalContentHash: '740dd29b4ae8ac8620d573aa31136dd8a920de9f6e933f4848b21b9b28df6639'
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
      approvalContentHash: 'f471355f3ba8f8258969d062744d8c41f703799e9ecc307f13688a971a2a933a'
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
      approvalContentHash: 'c4dc34e1f36ffd78e0d69f826be29a799e2c0fcd1f4e814dba853250155042f9'
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
      approvalContentHash: '416c3646aa04f0a6b59c7ca4c730a9556fb0cb2b95f624153f7f619466f70cd6'
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
      approvalContentHash: '3c784c26215a8658c82d0ae95f1f13d73d5b3c6ac5d472ee7dd2b34f68c506e0'
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
      approvalContentHash: 'e2cb77438d91164dca025ba81c9ee58bc5cff62843ac0565a63236d62948ecf1'
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
      approvalContentHash: 'd1129126d6f31d9dbc283258e065c37d2ffa080473d0cae7082edf9ec8580253'
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
      approvalContentHash: '9ce888e7509a7d2ed7dfe49a0c6ae2688b42a3b0ba5a67aad49f9beac264a4cb'
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
      approvalContentHash: 'a300faaab1a53afe40d2670f71d5c68bb888bbbc378f60d8965df853bf1e7b79'
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
      approvalContentHash: '8ce8dc9eb8141deaae943f512e40aa28155bd5ceac1d5f1d593191e1b41e367b'
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
      approvalContentHash: '3f2cf7a7c8e15e7a89a5b5c8d6e7e22c61d991774a17154f35e42503767318a7'
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
      approvalContentHash: '9e67a79d8d397c3dc01d2e6bdc4f02413518b800953f5f9e417a08bdfadf993d'
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
      approvalContentHash: '080555a38747d745d1d5a292456735623b1fdba17a299c58d3710167b835fcfb'
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
      approvalContentHash: 'c0fae091bec80bed82b1a84811216a42ec9b7fd674844695d962f8cf79a6f890'
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
      approvalContentHash: '7437a08db245f18eda09e880ff06dbd8a36947e90b9565978bff66224bace720'
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
      approvalContentHash: 'dad516456e24a1e4059caf9a062582947936db300d96b5089f04eb649320bd54'
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
      approvalContentHash: '1535c62bcad19dd0a994669afb5112585ebf379c730fc76767a2043c93cbf32c'
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
      approvalContentHash: 'f164d7d881ec0db58b65402eec0538f586c0a1b7f7e25876d88cbee783ed4e5f'
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
