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
        '82312419db2f4118c45ff71306e533fcf34ae216c7c3360bb0c6233e0e239c98'
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
      approvalContentHash: '5bad6e765dc37aae40a196f866a2c805a255cd22188eacb6f7de5dad51229779'
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
      approvalContentHash: '773141cf77faa6c51c4b1c4bd0995dffff037ec4c9e9affe8c8b3ea442e297cc'
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
      approvalContentHash: 'f75be4d7abf35b1b9cddf4d9cf3a9e1edd2c51644913c922f366f89351bd364f'
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
      approvalContentHash: '4f0823f6b8df5751098934c6bf9451ce9e7b04369fcf1129d0930766d8b3c77d'
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
      approvalContentHash: '45dc42d8f7b3f84a5c007e42e50bafc6b19fc6bf2b0d4dcd854f380fdc997d25'
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
      approvalContentHash: 'febddbc0cda0abbec8fdd937139c78751148936bb29ae3d7991abf2292a8b875'
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
      approvalContentHash: '4d57a4d340a24d9e48c315974b4f5cacd848bb8e930dce51d6e0e1820c8a3e1e'
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
      approvalContentHash: '4a9aa6d267645655c73c5abab46c24fef49780823c35e648deba65f354962100'
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
      approvalContentHash: 'b5e7cc0256ff63a7a5236edd292649460f944004b68566d3d303ece5ebb02f6c'
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
      approvalContentHash: '09bec1594c443d219261b1b2088a293a712e464ab21244139145c520d2f47124'
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
      approvalContentHash: 'efb23b1856ee3b435dc0803667b1ac7eabf47075740e08605d77254c56e0d7c9'
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
      approvalContentHash: 'd4351ba88c9cfa708676534a82b9641b225c178abf1f56a3508c05a8b4f6cf49'
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
      approvalContentHash: 'a4e817dcc8daa88ab3743e5f1c39721800918148b4f58e69f3f9e4b6af106d07'
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
      approvalContentHash: 'fb99fa8fadc15fc5734a81cefd2509a00fd28a0361f17d4cafaf5218bd834ed7'
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
      approvalContentHash: '0bc54855e4d5a34823766edee8d2c919405d1e48e830ba0ff7f53bffe00ccc67'
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
      approvalContentHash: 'c979898dfb028fda194f605e7cbbcfd80ee90a37c2852b66eee9dc8cd2e3720f'
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
      approvalContentHash: '1e59c573f69a60c443cea7d2a245d0d072b1941458579580531e2dcdf67ec64a'
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
      approvalContentHash: '9b21c1fc0dd3513f30f93f22f9faacbddac6faea3abfbb59b9d8edcc20736d82'
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
      approvalContentHash: '883130e19accbf8d7915882ce3b02ae0ce53a87b69bff4bdc11ccd066dcca0a7'
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
      approvalContentHash: 'ff3f1ec79153cef5e1d7d1cf6299fd29319c07df04475d0f08c6924b57aa0704'
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
      approvalContentHash: 'faf624ee285b4532b193fbf8a7e1ea282a7712bc52cc6c8f7595a810801d356f'
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
      approvalContentHash: '80f0b8c18350926785fc6d4e898de328685a0c57f397e56cbe2df8569495b199'
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
      approvalContentHash: 'fc736165f329f790ab9977784ba41d3edd8524d3de04a54aa05db97fedb90090'
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
      approvalContentHash: '131e3ed94d5e589d7b1b0892bb406310ef15555fe8df34e886fe3991067aa878'
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
      approvalContentHash: '2e44365f16a6abfb91123696ca4332fe57f105be41a3f9655107e5557f341d9e'
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
      approvalContentHash: '601e392a6ec33261ed3607d584b6159b7d56ac0a841fa12e0f28ec5fb48c4e5a'
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
      approvalContentHash: 'e6fe7a461aafc8ca5820ab815e4b24dcd840cf05f4f8ccac8b043d147f3a65bd'
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
      approvalContentHash: '34c709ffb100e1c4a12f7765d262d7d1d13c02bacbab3696fc28f6466400f5d9'
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
      approvalContentHash: 'fc3a61865deec1668b2d92a73f0cb830688dd7d09fec7b4e06eaf165e72d0f16'
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
      approvalContentHash: 'd1e103556f05fa706d5c4f7cce4d1a25c083caa2b1e4e71ec1df2c1d59e545ae'
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
      approvalContentHash: '052d17a63e6dca097403fa2957b016e9879ea6e40101876d6a65a430544d8790'
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
      approvalContentHash: '6c90d4aaf2b20bcdefb09d359e15c18e5853baf23511418ea84809f4c948e560'
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
      approvalContentHash: '4ebbd62b9a5c4c30932b49525e6c7462245f59d968626a04de08efc33113aca8'
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
      approvalContentHash: '350b2abd1af496af7f3045cc573639cc846127774dc8b68dc48c2fdaafd61ef6'
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
      approvalContentHash: 'b9c4b76f6cba3458397fb778e8d96eb3ec1c530b907d25621ff00355b027d95d'
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
      approvalContentHash: 'bf3001b3b2c63fc161f6c259a87d31d13f608f46a919e07762627a37cc659c7f'
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
      approvalContentHash: '9c898bd4d8e5b4c3d447a80eeeab862bcfc39fb93d34537dd5b12d22d91bd833'
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
      approvalContentHash: '9a22f4d63e13709328e819420a083087165fec47515f2b9398eec0cdc257aed7'
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
      approvalContentHash: '077a9aadc071a28f24edd9858f45426352316cec0e3843459c01dad26b5184ea'
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
      approvalContentHash: '23126375476b5184fdee4c2895af4ceaafc5873bcb52ce364ade74d34437efef'
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
      approvalContentHash: '578b139b6e0a6bc99fe29d83aac99409f767fde0266580a0b4903f39ed28081b'
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
      approvalContentHash: '43aa60f3ddc5ea5b850cc8aa8a0e2a6fe8bb6a8ad163ecf357d572548c4b26f2'
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
      approvalContentHash: 'f4648cb592e9b5a3520822e9662e9760f49b89cac2ad4f426edac67d889267ff'
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
      approvalContentHash: '1a6d90fcad323cbf50edfc1135c7221196e5cb7e1a3a3e03817f18f8af6524ed'
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
      approvalContentHash: 'ff774fd7a409d7a6154aca2ab86c0ea38e4bdc1daeed452f44ecc2c3dfb267c3'
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
      approvalContentHash: '5641900b77448614b7a90e9ee9dc83899ba343a850621325c96da8260079753e'
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
      approvalContentHash: 'f3b1415fb90e27d071f64c5e4a82faca79695a51e9b2dbf8c09e4927ba17b6a4'
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
      approvalContentHash: '6d91d6dd5a11e8595135a654473c66b6507c07f02e1ebdc7b1fe24b717bb738d'
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
      approvalContentHash: '0dc758392a58866742f5c38283e62dfc716eebf3e59796a9d9dd0f2373d0580b'
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
      approvalContentHash: '459c43d1ae5d071cf730656db5ef59b02b6e9f4a68fb191071329304316b5f9f'
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
      approvalContentHash: '5e87f316bdafca76d1a52ab30b11f5411b58604e4700670b39aae563c8bf296d'
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
      approvalContentHash: '0461232a64e591f9fa1a9cc0256e85acff85a37e0d4d65637a9e3fdc9fe32ed7'
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
      approvalContentHash: '60eb80e0a1713070fdd8906a3dcaaccb62dbb8440ecd889dbb7837088b0d750b'
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
      approvalContentHash: '8b83112258fe97538d38c04544ac51529def5c5b2f33676c014e41cb1378357b'
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
      approvalContentHash: 'ad469eebc8e2a0e544f2c44265096dd97aaa5651554461fae201f5f4a4ae38e7'
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
      approvalContentHash: 'e4037f55980be05ad45020ca532a156562533c1c8eb8cb3914ab9760e8fcd507'
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
      approvalContentHash: '72b0a3a5e113b56fe2812f73a2d470ffd7eabe0bc07af69ccc0289e95bc4927a'
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
      approvalContentHash: 'af482a6cb67a1fe47cfbb924f31a5ac315a61f36528c1d566d7d41d913b1f722'
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
      approvalContentHash: 'ed0566136df67528a20ad6ec2cfcef49f6c08c3d7fc58a9fb0732a420daa4430'
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
      approvalContentHash: 'f8bd391be00df7b49a22a71085ea762e0d82229da22f837ed19d7ac1bdc1e929'
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
      approvalContentHash: '8c5d58f4b0925a26925277ffa2f4eedc9b5e982291f7b592defb60e31bce6e68'
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
      approvalContentHash: 'c05ddb938bfcf594b4b0e775adb45aafeda0fa07ec716e3dee112b4c9d62edbb'
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
      approvalContentHash: '04c71d0191100e705814578dfb0ce06c996b6a5701ba01fab1b4f50dd1b89775'
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
      approvalContentHash: '0e83cb67ce09e3c63e30742a5b230bd30821a695990645c6faa014a99b6462fc'
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
      approvalContentHash: '7d7fc1f759d934f136dae0eda85ce9d9d5b426823101e4ec7d552185e5c0c8b4'
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
      approvalContentHash: 'c4f7453e33cfd3979bcab5fae14d8272af38c40cf47eb68a7082cda6bd785325'
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
      approvalContentHash: '6d1a363005b02028f7a2a025e8dd97299d35972b6b77d67d88b2ac2e6bf950ff'
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
      approvalContentHash: 'ebf1367ae1e22d6e556305b2b0e3fc245f4457303e01d736ccbd1cde4ebf3bb1'
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
      approvalContentHash: 'affa46804cfec0964cc0dbe9a9493533f0c5b349f945728a47179e0676d4210d'
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
      approvalContentHash: '02ffbf8ace1071ae74166a22a45aae5843d3b883db7be935af33f34049ffabd8'
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
      approvalContentHash: '723081e65e445be48c3469b29c758c8055c0fd5130b96ed27334b97864b6f95f'
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
      approvalContentHash: '8fd80110c4211bda9b6f700bc4c813997cb56d09ba0d3500d28d2317777ab605'
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
      approvalContentHash: '767e3bd8c2686b07ef052419a7f21a18b95b7198f4af1681af864c31020d6230'
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
      approvalContentHash: 'a104692cbefd23d4c7005ccf72b4c3232d29ee8177a268098aa533b9f7a7f7d5'
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
      approvalContentHash: 'ca7360dd7004bdd5162296e61cdc2a0339a697c99dd5701d8ddeca810e5eafc8'
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
      approvalContentHash: '974f8377ebbaaaf09e1777edd682837fc13fcc4c68cfafe345f5c2a64e423ef0'
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
      approvalContentHash: '2126f886c850175ef9671d7913b48b9bbc3c0bce01a613add4de9bce57a48f02'
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
      approvalContentHash: 'c5c3cf19c7f694fe6b93bc158b2f7db20326ffb53667556719ec5a9f0ed0004d'
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
      approvalContentHash: 'a90794f5ad290e100118c5749fa78799a07de8d9df392fd60936bb6a7a22b3c5'
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
      approvalContentHash: '09b1978bc925d6451bd12b7841747df5a433c0fc0672898456adb2a4a38cc700'
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
      approvalContentHash: '4afeb5bbeb6da9308640bcf1b54dedfd119f0238d765a9b1541948d43553b826'
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
      approvalContentHash: '3b40ccf8900ad41ad46f9e58f9331553624c4a0be38ad3dbfdb124b96977ca11'
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
      approvalContentHash: 'd5b603f632f12d21764cd6b5f62fa05680aac422ce3564f0f3c101f0e357b7ea'
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
      approvalContentHash: '66e621bae07b26fc2dee8089d95d34238bca05e15caf1b36b5a9d335bfffeb20'
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
      approvalContentHash: '922736bd1a517008bee6fa5a2af4d024e1e98362106bcc6f2f7cc857991135bc'
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
      approvalContentHash: 'd31c04d3c1bba0af029b5faa5c0f3a31e2cecd04f39b07ca1d3a29d5834f9ad1'
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
      approvalContentHash: 'cf8ddc696be3dd4a77001a786a12f44234a1f05e4ea73d2021fe6e0d5f4e94f2'
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
      approvalContentHash: '0cb0e128e83ef5072ce634322311f069a70990cc8ae116e0e0ecbdd22a009d84'
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
      approvalContentHash: '5ff32c96147f8369bf462168dcb128be554e4bde36d9c84c9823d4adc00f191a'
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
      approvalContentHash: '2b6b4f2d791c71b435435b7829e2d7fc5cb72151a271c4fa6435cb8c0ee4d7cb'
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
      approvalContentHash: '3e0b1f9497d60a5c8e051a827d66778d56d5c7cd323085d46654a0bb3b65dd17'
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
      approvalContentHash: 'e0e556401d32dc7af388dd13b145349a2b90c2de91f751da46e099c860e3ec81'
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
      approvalContentHash: 'be4955f441b7ba0f25e3c10112df2fccaedd54f54c5f34ae967a17fef192cc0a'
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
      approvalContentHash: 'f7adb579233738ed7a582e5bddce877014d902826d62cca47eeab6baa5441f7c'
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
      approvalContentHash: 'd243c5d4d6420fe1848be4aae53d50b4e31db13f1b772d80b995fe5cabbfd319'
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
      approvalContentHash: '8811701acc394a9bcf861f5f8369c8d0ab52c59b6b88a46d90a0c7de163e1234'
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
      approvalContentHash: 'eeffae9123b9ac99e4868d6461ec4bee5d65f6c63946744167943436caf8d577'
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
      approvalContentHash: 'f7319510ab653e31b2b8911de5b53d18d07fe6c5d168a9e067d081503d35d6d5'
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
      approvalContentHash: 'a82e563b364ff6a68ae3042db6902d3cd801d5eed6cbbdeb0d010b9dfe772c12'
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
      approvalContentHash: '555efcf51f9eaea9e07e63adf0196328cf76f22f1c71257bda549d015c64987b'
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
      approvalContentHash: 'e53384b2bc5235fc80b3cb0ed803d8adb9184907a3788da8ed108c876d983d5b'
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
      approvalContentHash: '486e8a86c5c1d472f51b5374f318cf19c06b984804717cfbb01a0d0c83ded4ab'
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
      approvalContentHash: '677ed68c1620cfc825a5340e03b09489f898a0ede6a599f872fbaa91ae4ec77d'
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
      approvalContentHash: 'd87d6044252009897a8d8bbcfdc12ae2fefdad91bce2209532d3b53fa8c67eca'
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
      approvalContentHash: '3040f0ef4602732b328e884e2e53f413895aaf1b24b742fac675ef9eea9d32e2'
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
      approvalContentHash: '3e941dd080ebedf1cda2b1750f60ece293a43e2981d3b434057efe8c1db2c14f'
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
      approvalContentHash: '956446195aaf44e87c398eab23d25dc0f89881c2e79976a6d5bb02c096a87b36'
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
      approvalContentHash: '0228a3225103e6d28dec7c0ab84d698e76f2209f8c2404a0d1a83bcb3d695bd6'
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
      approvalContentHash: '34878458968d24925bf78a6c9406de66c816aaccca20c78627142aed9833084c'
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
      approvalContentHash: '1a124a91cc80758b093b51447cd211185d61a7314f4442ea7f2c160c94651bbd'
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
      approvalContentHash: 'f5e3e9a560344eb85614b0ab6f9262915562f85e65df417d46a81f9f9f7a050b'
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
      approvalContentHash: 'd49129fb66e28fe637778b3d0723714cfcee72fac8d7a2158573e3a2487b76b1'
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
      approvalContentHash: 'd3f59fd3518c14a98dbba879aebba8178f6d49493f0f4d3ee3335cf787d8733d'
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
      approvalContentHash: '5697bbfd2d0ad6e12427e01ba9fadfdc799ad9e3793c07a0138002d6f0231711'
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
      approvalContentHash: '6e904773448a0510d224e74e0c116ca37f9406ed4d0a78e8fa342058576b4d12'
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
      approvalContentHash: 'ede414d259c5b9ef810e900206db77a0acd4d0035034267c17b3f61f224d4859'
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
      approvalContentHash: '261930b4b5a903f6ec51c87804ffa8d202fcf627cfa50a29ebd5389a7521bfa4'
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
      approvalContentHash: 'a0d059597d39274bbaf8df81dbdceac6431fa67be928ac8bdb60bedf8a91b25e'
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
      approvalContentHash: 'd8a51e02bdd7b9b4e297a7130ea17f5374754884bba392e4bad0cd70e9e01a1c'
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
      approvalContentHash: '5fb453ea7a08ecf528b49b5bcdad992eda6f2f01988c8cb059b630863bb25dfe'
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
      approvalContentHash: '5d51ec7e8368f6cdda6108d55b9f91efd4b4acd1d6a32c88e2794bd1c3072a98'
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
      approvalContentHash: 'f00c210ae4b3d9c5cc8434df8d77beefe2a6cafa6efc97743f88871fcc48f012'
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
      approvalContentHash: 'a30dc1c79ca030b023ced2aa21722218b0e4fea525f3ea758708c64183e281a9'
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
      approvalContentHash: 'e4e7df73e97b55e34182cc49a35d71a62366b06bb9103b21ad3154b7fa0b8757'
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
      approvalContentHash: 'acd5a7c0452702fde83c45c2e9cc4a11e1b88a4e992a48231418665e0cf07681'
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
      approvalContentHash: '36ab000fc7062d665361addbb0beebc3294def735a9747c0b5ff7db4b22885b7'
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
      approvalContentHash: '8e38d250f9d927df6d91cc9f51fff76d8bf006ae11bf0432cc79af9a9c46eed6'
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
      approvalContentHash: '83288484c0f52a9743eeed7fed28094403a3171b035ab02f6f226ca7d3a6be38'
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
      approvalContentHash: '489dfaaefdc30d8c8dc141bafaacf18a23b80132e8e18bad82a96d652756625d'
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
      approvalContentHash: 'f376245cdbbfde02a6326abc7336c10be9cf9e3cd111b75abe98a97b1a7736ce'
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
      approvalContentHash: '655f3be7f271902aebb743acf3827de22c87195208bb4868a40b7ca4e3ec1f02'
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
      approvalContentHash: '215cf11700be9dd4f8e0548e33fa22d56fd42fa4105eeebecf0c4b47bb7d7d65'
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
      approvalContentHash: '23c4ae5b831f2b43c2a3b6dcc2600b4ffb3789be070102c43402b210a774af88'
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
      approvalContentHash: 'e05ea31f55e0e0cd7186f7f38de9dfe01dfe0001d268b9f05cc6eee3654f47ef'
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
      approvalContentHash: '879af05dd92bdb313ed0cc38d7f01335da5e1e624269aa7603e0ec2f5c299464'
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
      approvalContentHash: '5d8891fc78741092a5e26e708153e72704ce002de0f95e58b4a5d048a1d751ba'
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
      approvalContentHash: 'c8ff6754131986ccf64f9d66f4637bce7ba9268e5f73655fd82e5288e4dbd4c7'
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
      approvalContentHash: 'dee3fc05886fe80d56699929bf025b8fe7c56f739af88986dfcd7d26dbd23755'
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
      approvalContentHash: 'b5cb835c25e628ef26d5b251b321b52a6cdc9e8d6bad4b22d63b49507c064db0'
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
      approvalContentHash: '7e0b57fc72aeeed2e12640092a474da6560f2130c9f0f8f23a6a7bff6329453a'
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
      approvalContentHash: '4241b64fd4f52b46f269f2cd9f599954ce9f80e48fb7fac2253a5b7a7ae28dfc'
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
      approvalContentHash: 'b2666e7933173bdf649649e400bf641b03521ff74e10b8d235e21f00fecada86'
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
      approvalContentHash: 'b40ba79d88d32b87948bfae1a0bbf2241243a9120831c75b5f10849af2505ae9'
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
      approvalContentHash: '5b75b1065b6e526f2a6801cbfdbfcf535cdf5682679a9fea6e223deb7e11c6b0'
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
      approvalContentHash: 'cf401ff3a329b6fe10021c23a8522de5972c1e4623cb1a5153f4d2992f2dc9e2'
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
      approvalContentHash: '2af93b652e783c44d42574dcbe138d6be55ba28f28c646360e66a5fb677324bd'
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
      approvalContentHash: '781ea836dc93eacd38dde3ababa01b375e9616e51c00bf53ad5823025138cc99'
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
      approvalContentHash: '0e3f5effea5fccf171d88be31934114cde88704e35a3ffeabad7bb9d67ddff5e'
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
      approvalContentHash: 'f14955180c576f9363a114b887b25c4c998b587b7356a44640ca028784ce6872'
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
      approvalContentHash: '9d143874528a75dc07673f507dce47accbc930c44e3aca003772d850c903ccc0'
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
      approvalContentHash: '1257bb847f598560a5b80e56c616e0049a50ccdee87ef8370d5e49d4ecfb7010'
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
      approvalContentHash: '8f12db3af3198927a0b3c12eb58563d350d0daca582e9d81434371dba8d59a4c'
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
      approvalContentHash: 'e388c96428aec005ed0f0791445f2e8f0de143340d3baf3669a6ebb0db5ad2ab'
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
      approvalContentHash: '6dc11a5c2794169a9cec50b3f172dfc36982399e317047a3202cecdb10965670'
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
      approvalContentHash: '903fe462e9fe0c531ce6c42f39a6f3e4b503c58137e1121c4d42fba4f7f39501'
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
      approvalContentHash: '378711424e68564bcb25a71ec3ad2792d9b1f86a87d9a8b99b317f5c051de91d'
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
      approvalContentHash: '46e8d7a9ad6fef43f4c77dd43df277e0ac4da1050730254878a7503f18480e2c'
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
      approvalContentHash: '4d4386e160bd4b0f854ddcc7d050e9e05626ea4d14eb7e9bda492bb24288cd34'
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
      approvalContentHash: '5bc35a8aed44c958406414736c786dab559c4b17c695517993cbfec65960bbdb'
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
      approvalContentHash: '9ad2d7f79e1f6cf5b7db8f82f882979b3898ed002d03677ac9b6b5983a542e4a'
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
      approvalContentHash: '0f3bfd8e62e80ae1d5e2362b02cfc068abe223b70eba4ac5841b8692ce90e2e7'
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
      approvalContentHash: '3d1015b007c666e43258712fc391ed6747bf716a46d059686b3d4afbcf1aa124'
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
      approvalContentHash: 'fcac892f06ff4e960a27623bb4355564d49346746783a616b998272015726ae4'
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
      approvalContentHash: '46d7b3db035d8b829db782a05329f66237d0633e7537064a3e280440d78cee98'
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
      approvalContentHash: '58df84e3930e103b90c1b1b5aa1e6362ab7a36bc76d522090039db67ccace15c'
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
      approvalContentHash: '235ce055ac02798e9d0bb67fc0bcb7fc4b6cbfaa6902f3184e495aa0af16fff6'
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
      approvalContentHash: 'c4a0ca744dc321b9348f58805cd7e10b975e0cbfc3fefd0e0d6a2d23c2edc8ef',
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
      approvalContentHash: '915a3694a666a630968244bfe215ebcb6302b2c32856dd8542b2cd8a2fb8d768',
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
      approvalContentHash: 'fe329c9653b6222b300061c25a07de9a0cbd7727e231495d08bb879faef7a7d8',
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
      approvalContentHash: '6c009ca3a0f6dcd7d24fd60892de20cc1218f73233193b79abcd2cb8be3ed98a',
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
      approvalContentHash: '46abfece8b4e1650edd8591113ad5c6968f5c4794a4f84d0cbc4a906a216bf7b',
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
      approvalContentHash: '0c08c596141cc7e807bdbef090d727652bfb2bfe8ce8760a6655b15ffaba97f0',
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
      approvalContentHash: '9026d6894c6bd6bf1af9b98881279cb7c8c2e361e0c90c9c7bc7b4a09c5f3bd1',
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
      approvalContentHash: '8c133373c9625b28799349bb9e8cf35650d9134ffdf16b5a807938ebbdabba22',
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
      approvalContentHash: 'fb7da3a1fc219c31db5f3cbdc4e6d182d02d61e15e262c37c3056b8ff7749a9c',
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
      approvalContentHash: '819afe9ee4d78aacd90e69342c57df42593f96057c975f87bbeda4753380c586',
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
      approvalContentHash: '16cfa9442a31a4de4bdcd76682eefcf5e75939d85a96556b96d5bc47360b9dac',
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
      approvalContentHash: '9c3a0243a244c8d6c97ca3a3ac90e7bb287f4d147095dfc8662bbdad961149a9',
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
      approvalContentHash: 'be0fd229c42011a236b90510227ef15feba1426a105f2f1d8e9a4b99c2881709',
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
      approvalContentHash: 'f8c88867b56a837600350dcd92313d1628dd0b451f5c3ad4c9ec4079a2e9f92b',
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
      approvalContentHash: 'bd88afac9d9e0c157d8f96a4121033c94861d0acd312dd74849bafe38e210bab',
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
      approvalContentHash: 'b9afc42f232f036644fda29f3efd2af5d16a1a5a87cb7db79348acdf444152f4',
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
      approvalContentHash: '0e00d23d2189775ca34490f3ccad177d71054a478ea1b5b0e59761220583b9e0',
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
      approvalContentHash: '0a0e18be9d12b45f26a1c9de1b6a8de56e1580c0254541683df99ca6ff08abd4',
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
      approvalContentHash: '1b649b726ee458cdbf8a39c5bdf3a57ab63e85e767c45990a32d507c084ffce0',
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
      approvalContentHash: '8ddb0fdd3a51d6a339e8191e6a65d6f7e06643d13c6192b2a309f8db3a443d08',
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
      approvalContentHash: 'b6aa15bd90d0e0e0a608167e1185ce1276c9e46f686bad604c53ca94b6c6afef',
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
      approvalContentHash: 'd19dacadb7999c919811a8987876eb41bc582e009be718922b7a6fa10c6bd70d',
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
      approvalContentHash: '3cb3396874638d296e8f76c5227c2caf136316173ce99688d0225078216f02d4',
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
      approvalContentHash: 'a880d5001f21210f20df0830ef579ad7814c8467c7103c1aa41d4d4b2c8810f9',
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
      approvalContentHash: '72632329f8800c0bcc4fb333f8fd516768a3dd535907fc5b3362edd65f01238e',
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
      approvalContentHash: 'da40fb3fe14d205704a2e2d00b1ca383ac89f473542eb29cfdda4b69e80f1a69',
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
      approvalContentHash: '84ebeac6e3e6ea4bda1f50b553a7bb4e7bf1181f91ad92aa14e0b1320193153c',
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
      approvalContentHash: '73685481c3b303849a4d8a1f70acaaf6e9b2c925272d34fe92e470321586ead2',
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
      approvalContentHash: '4c37c7d0413e194ae4340fe0e83f10ccdebd6e6f8b92fb93200c10015abae472',
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
      approvalContentHash: '0359b482ca379741e8a1055d564acda4cf11e43d4941af0e17f2ea76a08c6095',
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
      approvalContentHash: '7a7c1339816a0b404283cb426cbec0ab5b3d25726bd16381b65a318facf4d092',
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
      approvalContentHash: '447263a8a1de02ced97137929dbed75f330d81fca79341ada3ddbb306a72fe6d',
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
      approvalContentHash: 'd0c4759221975116e901bdb49c5b1a6c868d210c774d5fc49fe19356a6ef5af3',
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
      approvalContentHash: 'd0d5f0844ac24d6842a344ad20ea0110d5e2c2c524f64dee50d10cf80ee867b2',
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
      approvalContentHash: 'f18c6dee79ccd2320b925dc3a626b56db643eba7fe46487bc87ee546864d509b',
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
      approvalContentHash: '7c4550e2526f4fec19621bba95e8ae2e8bcf5aceab74f41cce433010a2b5ab55',
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
      approvalContentHash: '034339aecb3507c256d5535055a408e44efbba97427f3c8de0f3a6672ce1b13d',
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
      approvalContentHash: '90336535571fe84165667d5e8cab0c83e13cbb67ac8ece9d3e898e236757ed35',
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
      approvalContentHash: 'dc0dd651bb5e2d51e8edba514289662119d6798c49623c3536b397e43b53b975',
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
      approvalContentHash: '9c534f5401b3de02de6f967ce144ab6646f78e7a6021cd3a8f26459cfb2a07e0',
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
      approvalContentHash: 'dd783bb4e46eb62ca95cd11e9fad630f665480dd404344d2f5cb244d0b369c7b',
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
