/**
 * Track B Chat B — Critical-Mass V3 evidence-gap investigation (7 candidates).
 * Does NOT modify Track A PR #568. CUSTOMER_BLOCKED.
 * Permanent rule: do not downgrade valid Qualified; do not promote Emerging without evidence.
 */
export const WAVE = "TRACK_B_CHAT_B_CM_V3_GAPS";
export const ASSESSMENT = Object.freeze({
  QUALIFIED_RETAINED_STRENGTHENED: "QUALIFIED_RETAINED_STRENGTHENED",
  EMERGING_RETAINED: "EMERGING_RETAINED",
  AMBIGUOUS_REVIEW: "AMBIGUOUS_REVIEW",
  UNDER_RESEARCH: "UNDER_RESEARCH",
  COMMITTED_CLOSED: "COMMITTED_CLOSED",
  HISTORICAL_EXECUTED: "HISTORICAL_EXECUTED",
});

export const GAP_FINDINGS = Object.freeze([
  {
    gapId: "cb_gap_oromana_pcap_ppt",
    trackAId: "cm3-oromana-concession",
    hotelName: "Hotel Oromana",
    city: "Alcalá de Guadaíra",
    stateProvince: "Sevilla",
    physicalCountry: "ES",
    agreementType: "CONCESSION_MANAGEMENT",
    priorTrackAClass: "QUALIFIED",
    chatBAssessment: "QUALIFIED_RETAINED_STRENGTHENED",
    verifiedContractTrigger: true,
    decisionUnresolved: true,
    competingPathway: true,
    decisionEntity: "Junta de Gobierno Local — Ayuntamiento de Alcalá de Guadaíra",
    bidDeadline: "2026-11-27T23:59:00+01:00",
    termYears: 40,
    estimatedValueEur: 29583285.72,
    datesPrecision: "DAY",
    evidenceSummary:
      "Restricted concession C-2026/032 (exp. 9998/2025) still accepting bids to 27 Nov 2026 23:59. " +
      "Object: integral hotel exploitation + mandatory rehab/reform/expansion to ~4★ (~31→~60 rooms). " +
      "PCAP/PPT published (PCAPN.pdf, PPT.pdf) with Anexo III staff-subrogation list (Hotel Legado Oromana). " +
      "Technical solvency: 5+ years operating a 3★+ hotel; complementary F&B/events experience; dedicated hotel director 3+ years. " +
      "Scoring: architecture/works 57 pts, canon 25, hotel ops plan 18. No mandated franchise brand identified — 4★ category is performance target. " +
      "TARCJA Resolución 610/2026 (9 Sep 2026) partially annulled prior pliegos; municipality re-approved corrected pliegos — current window is the re-tender after appeal.",
    additionalEvidenceStillNeeded: [
      "Full brand-affiliation clause text from PPT (confirm no soft-brand lock) — secondary aggregators summarize category target only",
      "Post-27-Nov shortlist / award notice when published",
    ],
    sources: [
      {
        url: "https://lalicitadora.es/licitacion/concesion-del-servicio-de-explotacion-del-hotel-oromana-c-2026-032-20269201",
        type: "PROCUREMENT_MIRROR",
        claim: "Open to 27/11/2026; PCAP/PPT/Anexos listed; solvency + scoring",
      },
      {
        url: "https://www.juntadeandalucia.es/sites/default/files/2026-10/Resoluci%C3%B3n_610_2026_Recurso_588_2026.pdf",
        type: "PRIMARY_ADMIN_TRIBUNAL",
        claim: "Partial annulment of earlier pliegos; 4★ transformation object; retroaction ordered",
      },
      {
        url: "https://contratacionabierta.es/licitacion/1633664",
        type: "PROCUREMENT_MIRROR",
        claim: "PLACSP-linked document set including PCAPN/PPT and appeal/suspension history",
      },
    ],
    persistEligible: true,
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
  },
  {
    gapId: "cb_gap_agueda_entity_hma",
    trackAId: "cm3-agueda-passivhaus",
    hotelName: "Águeda Passivhaus eco-hotel (Pateira de Fermentelos)",
    city: "Águeda",
    stateProvince: "Aveiro",
    physicalCountry: "PT",
    agreementType: "HYBRID_LEASE_MANAGEMENT",
    priorTrackAClass: "QUALIFIED",
    chatBAssessment: "QUALIFIED_RETAINED_STRENGTHENED",
    verifiedContractTrigger: true,
    decisionUnresolved: true,
    competingPathway: true,
    decisionEntity: "Portuguese sole-proprietorship rural-tourism developer (name not published on EEN); partner intros via EEN",
    bidDeadline: null,
    validityEnd: "2027-11-06",
    datesPrecision: "DAY",
    evidenceSummary:
      "EEN BOPT20261105031 (valid 6 Nov 2026–6 Nov 2027): developer seeks international boutique operator under long-term hybrid commercial lease/management for ~1,500 m² Passivhaus eco-hotel near Pateira de Fermentelos + equity co-investors. " +
      "Legal form disclosed as sole proprietorship created July 2026 in Águeda — **legal entity trade name / NIF not published** on the EEN profile. " +
      "Municipal licensing + architectural project approved (Águeda City Council). Portugal 2030 application >€4m pending. " +
      "Operator would run main hotel/restaurant/spa; developer retains peripheral leisure assets. Pathway open; partner not selected.",
    additionalEvidenceStillNeeded: [
      "Legal entity name / NIF (Track C / EEN partner intro)",
      "Draft hybrid lease/HMA term sheet (duration, fees, termination)",
      "CapEx residual schedule",
    ],
    sources: [
      {
        url: "https://een.ec.europa.eu/partnering-opportunities/luxury-eco-hotel-project-portugal-offers-opportunities-commercial",
        type: "PRIMARY_OWNER_OUTREACH",
        claim: "BOPT20261105031 hybrid lease/management operator search; sole proprietorship July 2026 Águeda",
      },
    ],
    persistEligible: true,
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
  },
  {
    gapId: "cb_gap_areia_branca_vortal",
    trackAId: "cm3-areia-branca-revive",
    hotelName: "Antiga Colónia Balnear da Areia Branca (REVIVE)",
    city: "Lourinhã",
    stateProvince: "Lisboa",
    physicalCountry: "PT",
    agreementType: "CONCESSION_HOTELARIA",
    priorTrackAClass: "QUALIFIED",
    chatBAssessment: "QUALIFIED_RETAINED_STRENGTHENED",
    verifiedContractTrigger: true,
    decisionUnresolved: true,
    competingPathway: true,
    decisionEntity: "Turismo de Portugal, I.P. (tramitação); property owner CIM Beira Baixa as concedente",
    bidDeadline: "2027-03-17T23:59:00+00:00",
    termYears: 50,
    minAnnualRentEur: 25637,
    rentHolidayYears: 4,
    datesPrecision: "DAY",
    evidenceSummary:
      "Diário da República anúncio n.º 27619/2026 (sent 17 Nov 2026): public concurso for 50-year concession to recover/requalify/operate Areia Branca former beach colony as hotelaria + F&B/culture/animation/commerce. " +
      "Proposals via VORTAL to 17 Mar 2027 23:59; negotiation phase yes; variants not allowed. Internal ref P(143/2026)01/DJU/CSP2026001-1. " +
      "Nota informativa: min annual contraprestida €25,637 with 4-year rent holiday; evaluation 50% price / 50% earlier ops start; max 4 years for works after handover. " +
      "Volumetry/implantation must be preserved. No mandated international brand in the published notice — hotelaria use with brand freedom implied for bidder.",
    additionalEvidenceStillNeeded: [
      "Download full peças from VORTAL (brand/ops constraints if any beyond hotelaria)",
      "Mandatory CapEx figure if locked in peças (press cites ~€4.2m historical estimate, not confirmed as mandatory floor)",
    ],
    sources: [
      {
        url: "https://files.diariodarepublica.pt/cp_hora/2026/11/225/419977887.pdf",
        type: "PRIMARY_PROCUREMENT",
        claim: "DR 27619/2026 — VORTAL; deadline 17-03-2027; 50 years; hotelaria CPV 55100000",
      },
      {
        url: "https://oregioes.pt/nota-informativa-sobre-o-imovel-e-o-procedimento-concursal-revive/",
        type: "PRIMARY_OWNER_CONTEXT",
        claim: "CIM Beira Baixa owner; €25,637 min rent; 4-year holiday; 50/50 evaluation",
      },
      {
        url: "https://community.vortal.biz/public/",
        type: "PRIMARY_PLATFORM",
        claim: "VORTAL eDelivery gateway named in DR notice",
      },
    ],
    persistEligible: true,
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
  },
  {
    gapId: "cb_gap_cap_cana_teaser",
    trackAId: "cm3-cap-cana-condohotel-selection",
    hotelName: "Cap Cana condohotel projects — Spain Inmobiliaria operator/brand search",
    city: "Cap Cana",
    stateProvince: "La Altagracia",
    physicalCountry: "DO",
    agreementType: null,
    priorTrackAClass: "EMERGING_PROSPECTIVE",
    chatBAssessment: "EMERGING_RETAINED",
    verifiedContractTrigger: false,
    decisionUnresolved: true,
    competingPathway: false,
    decisionEntity: "Spain Inmobiliaria / Mercedes Solís Díez (broker outreach)",
    bidDeadline: null,
    datesPrecision: "UNKNOWN",
    evidenceSummary:
      "LinkedIn outreach invites operators/brands to request a private teaser (mercedes@spaininmobiliaria.com) for Cap Cana Marina/Golf/Playa condohotels under CONFOTUR. " +
      "No public RFP, named parcel IDs, or downloadable teaser retrieved. " +
      "Do not conflate with committed Cap Cana projects: AC Residences by Marriott (Gesproin — brand already licensed) or Tonino Lamborghini Hotels by Huxley / Archipelago (announced FITUR 2026).",
    additionalEvidenceStillNeeded: [
      "Project teaser PDF with named parcels / SPE",
      "Proof of site control / Cap Cana master-plan entitlement",
      "Formal selection process rules and deadlines",
    ],
    sources: [
      {
        url: "https://es.linkedin.com/posts/mercedes-%F0%9F%8F%A8-sol%C3%ADs-diez-guti%C3%A9rrez-9670891a_capcana-hospitalityindustry-realestatedominicanrepublic-activity-7441441942065975296-IlMy",
        type: "SECONDARY_OUTREACH",
        claim: "Operator/brand selection opened; teaser via email",
      },
      {
        url: "https://inmobiliario.do/en/ac-residences-by-marriott-cap-cana-was-presented-to-european-investors-in-madrid/",
        type: "SECONDARY_TRADE",
        claim: "Separate Gesproin AC Residences by Marriott Cap Cana — brand committed (not this Emerging lead)",
      },
    ],
    persistEligible: true,
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
    note: "Do NOT promote to Qualified. Persist as Emerging contract-watch context only.",
  },
  {
    gapId: "cb_gap_finn_hma_dates",
    trackAId: "finn-residual-aimbridge",
    hotelName: "Fibra Inn — residual Aimbridge HMAs (Westin Monterrey Valle + JW Marriott Monterrey Arboleda/Valle)",
    city: "San Pedro Garza García",
    stateProvince: "Nuevo León",
    physicalCountry: "MX",
    agreementType: "MANAGEMENT",
    priorTrackAClass: "EMERGING / UNDER_RESEARCH",
    chatBAssessment: "UNDER_RESEARCH",
    verifiedContractTrigger: false,
    decisionUnresolved: true,
    competingPathway: false,
    decisionEntity: "Fibra Inn Technical Committee + co-owners",
    bidDeadline: null,
    datesPrecision: "UNKNOWN",
    evidenceSummary:
      "Track B prior waves already established: Aimbridge Latam still operates 2 co-owned hotels / 424 keys as of FINN 3T26; ASG lists Westin Valle + JW Arboleda under gestión. " +
      "Gov letters (xix) internalization-at-expiry for third-party HMAs; (xx) early Aimbridge termination only for 100%-owned. " +
      "**No hotel-level HMA expiration, notice, or performance-termination dates found** in free IR/governance sources. Cannot upgrade to Qualified. Competing MCT constrained by internalization policy.",
    additionalEvidenceStillNeeded: [
      "HMA exhibits / FID vehicle contracts with expiry dates",
      "Co-owner consent mechanics for manager change",
    ],
    sources: [
      {
        url: "https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf",
        type: "PRIMARY_IR",
        claim: "(xix) internalization at HMA expiry; (xx) early Aimbridge exit 100%-owned only",
      },
      {
        url: "reports/sec-edgar-hif-track-b-fwd-qualify-v4/FOUNDER_REPORT.md",
        type: "TRACK_B_PRIOR",
        claim: "Prior Chat B qualify pack — residual HMAs EMERGING; expiry unknown",
      },
    ],
    persistEligible: false,
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
    note: "Reuse prior CALA/fwd-qualify persisted IDs — no duplicate write.",
  },
  {
    gapId: "cb_gap_bergantin_residual",
    trackAId: "cm3-bergantin-residual-hilton-talks",
    hotelName: "Punta Bergantín — residual / fourth hotel parcel",
    city: "Villa Montellano / Puerto Plata",
    stateProvince: "Puerto Plata",
    physicalCountry: "DO",
    agreementType: null,
    priorTrackAClass: "EMERGING_PROSPECTIVE",
    chatBAssessment: "EMERGING_RETAINED",
    verifiedContractTrigger: false,
    decisionUnresolved: true,
    competingPathway: false,
    decisionEntity: "Banreservas / Punta Bergantín project SPE (state–private); Central Bank land",
    bidDeadline: null,
    datesPrecision: "UNKNOWN",
    evidenceSummary:
      "Sep 2026 Banreservas (Aguilera): Meliá groundbreaking done; Westin picazo early Dec 2026; Dreams Mar 2027 — three named; **fourth hotel details still awaited** despite Abinader four-hotel framing. " +
      "Jul 2025 public hearing listed proposed brands Westin/W/Meliá/Hilton across four named hotels — later coverage treats Hilton as conversations only. " +
      "No open RFP, residual parcel tender, or developer SPE process for a competing brand/operator found. Retain Emerging.",
    additionalEvidenceStillNeeded: [
      "Named fourth-hotel parcel + developer SPE",
      "Formal operator RFP or confirmed open shortlist",
    ],
    sources: [
      {
        url: "https://atardecer.com.do/punta-bergantin-iniciara-construccion-de-dos-nuevos-hoteles-entre-diciembre-y-marzo/",
        type: "SECONDARY_TRADE",
        claim: "Westin Dec 2026 / Dreams Mar 2027; fourth hotel details pending",
      },
      {
        url: "https://radardelespectaculo.com/escapadas/punta-bergantin-toma-forma-estos-son-los-hoteles-que-llegaran-a-puerto-plata/",
        type: "SECONDARY_TRADE",
        claim: "Hilton conversations; map not fully defined",
      },
      {
        url: "https://www.comunidadinformativa.com/los-hoteles-en-punta-bergantin-iniciaran-construccion-en-el-2026-tras-su-respaldo-en-vistas-publicas/",
        type: "SECONDARY_TRADE",
        claim: "Jul 2025 hearing: four hotels proposed under Westin/W/Meliá/Hilton",
      },
    ],
    persistEligible: true,
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
  },
  {
    gapId: "cb_gap_torre_uriz_award",
    trackAId: "cm3-torre-uriz-navarra",
    hotelName: "Hotel Torre de Uriz",
    city: "Uriz (Valle de Arce)",
    stateProvince: "Navarra",
    physicalCountry: "ES",
    agreementType: "CONCESSION_MANAGEMENT",
    priorTrackAClass: "AMBIGUOUS_REVIEW",
    chatBAssessment: "AMBIGUOUS_REVIEW",
    verifiedContractTrigger: true,
    decisionUnresolved: true,
    competingPathway: false,
    decisionEntity: "Dirección General de Turismo — Gobierno de Navarra",
    bidDeadline: "2026-11-19T13:15:00+01:00",
    termYears: 5,
    minCanonEur: 12100,
    estimatedValueEur: 878870.29,
    datesPrecision: "DAY",
    evidenceSummary:
      "SICP Navarra concession for gestión/explotación of Hotel Torre de Uriz (3★ rural; inventory 3384-03). Bid window **closed 19 Nov 2026 13:15**. " +
      "As of 20–23 Nov 2026 aggregators show **adjudicación pendiente / sin publicaciones**. " +
      "New competitors cannot enter. Not open Qualified. Not yet Historical/Committed until award notice. Monitor award or desertion/re-tender.",
    additionalEvidenceStillNeeded: ["Award notice or desertion/re-tender publication"],
    sources: [
      {
        url: "https://hacienda.navarra.es/sicpportal/mtoAnunciosModalidad.aspx?cod=261005112143BFAF8FF2",
        type: "PRIMARY_PROCUREMENT",
        claim: "Official SICP notice; bids 21 Oct–19 Nov 2026; pliego link",
      },
      {
        url: "https://hacienda.navarra.es/sicpportal/mtoGeneraDocumento.aspx?DOA=261005112143BFAF8FF2&DOL=1",
        type: "PRIMARY_PCAP",
        claim: "5-year MCT concession; min canon €12,100; EV €878,870.29",
      },
      {
        url: "https://www.alicitar.es/licitacion/22062061/concesion-de-servicios-para-la-gestion-y-explotacion-del-hotel-torre-de-uriz-sit",
        type: "PROCUREMENT_MIRROR",
        claim: "Award pending; last change 20/11/2026; no award publications",
      },
    ],
    persistEligible: true,
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
  },
]);

function resolveAgreementType(raw) {
  if (
    raw === "CONCESSION_MANAGEMENT" ||
    raw === "CONCESSION_HOTELARIA" ||
    raw === "HYBRID_LEASE_MANAGEMENT" ||
    raw === "MANAGEMENT"
  ) {
    return "MANAGEMENT";
  }
  return null;
}

export function buildChatBGapDossiers() {
  return GAP_FINDINGS.map((g) => {
    const agreementType = resolveAgreementType(g.agreementType);
    const persistEligible = g.persistEligible === true;
    const contractEligible = persistEligible && agreementType != null && g.verifiedContractTrigger === true;
    const milestoneRaw = g.bidDeadline || g.validityEnd || null;
    const milestoneDate =
      milestoneRaw && /^\d{4}-\d{2}-\d{2}/.test(String(milestoneRaw)) ? String(milestoneRaw).slice(0, 10) : null;
    return {
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: g.verifiedContractTrigger ? "HIGH" : "MEDIUM",
      persistEligible,
      contractEligible,
      discoveryMethod: WAVE,
      hpcMatchStatus: "PENDING_HPC_SEARCH",
      wave: "B26",
      publicationState: "CUSTOMER_BLOCKED",
      discoveryPropertyId: `disc_tb_cb_${g.gapId}`,
      findingId: `find_tb_cb_${g.gapId}`,
      contractId: contractEligible ? `ctr_tb_cb_${g.gapId}` : null,
      hotelName: g.hotelName,
      hotelNameNormalized: g.hotelName.toLowerCase(),
      city: g.city || null,
      stateProvince: g.stateProvince || null,
      physicalCountry: g.physicalCountry,
      agreementType,
      agreementRole: "ORIGINAL",
      contractStatus: g.decisionUnresolved ? "CURRENTLY_CORROBORATED" : "HISTORICALLY_VERIFIED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      counterpartyName: g.decisionEntity || null,
      expirationDate: null,
      milestoneDate: contractEligible ? milestoneDate : null,
      milestoneId: contractEligible && milestoneDate ? `ms_tb_cb_${g.gapId}_deadline` : null,
      milestoneType: milestoneDate ? "OTHER" : null,
      datePrecision: g.datesPrecision,
      sourceUrl: g.sources[0]?.url || null,
      sourcePublicationDate: "2026-11-23",
      evidenceSnippet: g.evidenceSummary,
      unresolvedTerms: g.additionalEvidenceStillNeeded,
      reconciliation: {
        currentStatusClass: g.decisionUnresolved ? "CURRENTLY_CORROBORATED" : "HISTORICALLY_VERIFIED",
      },
      chatBAssessment: g.chatBAssessment,
      priorTrackAClass: g.priorTrackAClass,
      trackAId: g.trackAId,
      trackA: g.chatBAssessment,
      trackAPayload: {
        gapId: g.gapId,
        trackAId: g.trackAId,
        assessment: g.chatBAssessment,
        verifiedContractTrigger: g.verifiedContractTrigger,
        decisionUnresolved: g.decisionUnresolved,
        competingPathway: g.competingPathway,
        decisionEntity: g.decisionEntity,
        bidDeadline: g.bidDeadline || g.validityEnd || null,
        doNotDowngradeQualified: g.priorTrackAClass === "QUALIFIED",
        doNotPromoteWithoutEvidence: true,
        limitation: g.additionalEvidenceStillNeeded.join("; "),
        sourceUrls: g.sources.map((s) => s.url),
      },
      hpcQuery: g.hotelName.slice(0, 48),
      note: g.note || null,
    };
  });
}

export function summarizeGapAssessments(findings = GAP_FINDINGS) {
  return findings.reduce((a, f) => {
    a[f.chatBAssessment] = (a[f.chatBAssessment] || 0) + 1;
    return a;
  }, {});
}
