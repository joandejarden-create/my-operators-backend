# Phase 1 Post-Apply Audit

```json
{
  "startedAt": "2026-08-10T15:21:07.583Z",
  "mode": "apply",
  "webhound": "Deferred supplemental enrichment",
  "schema": {
    "createdTables": [
      {
        "name": "Operator Intelligence - Assignments",
        "id": "tblKh5p0K1tNAUnkj",
        "fieldCount": 35
      },
      {
        "name": "Operator Intelligence - Brand Relationships",
        "id": "tblt2pMLBEcTdgwdD",
        "fieldCount": 19
      }
    ],
    "createdFields": [
      {
        "table": "Operator Setup - Master",
        "name": "Record Purpose",
        "id": "fldI8lwxwlefN1geK"
      },
      {
        "table": "Operator Setup - Master",
        "name": "Operating Model",
        "id": "fldGg9gr6212S6Zpb"
      },
      {
        "table": "Operator Setup - Master",
        "name": "Management Availability",
        "id": "fldcAsoeQvnXWHgk7"
      },
      {
        "table": "Operator Setup - Master",
        "name": "Operator Aliases",
        "id": "fldg9bSSHydNaUI2I"
      },
      {
        "table": "Operator Setup - Master",
        "name": "Operator Website",
        "id": "flduAsgKZVPw7hQOC"
      },
      {
        "table": "Operator Setup - Master",
        "name": "Operator Parent Company",
        "id": "fldt7r8iwhby1HX1k"
      },
      {
        "table": "Operator Intelligence - Claims",
        "name": "PI Source Library",
        "id": "fld5iIkwav4d6uASL"
      },
      {
        "table": "Operator Intelligence - Market Presence",
        "name": "City / Metro",
        "id": "fldFPcWrDjTNqag8Z"
      },
      {
        "table": "Operator Intelligence - Market Presence",
        "name": "Verified Assignment Count",
        "id": "fldrPuHLbvO9FMeJu"
      }
    ],
    "skippedFields": [],
    "failed": []
  },
  "masters": {
    "updated": [
      {
        "id": "rec3TUHT9Z4AnFp5P",
        "name": "Playa Hotels & Resorts",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Integrated Owner / Brand / Operator",
          "Management Availability": "Conditional / Scoped",
          "Operator Website": "https://www.playaresorts.com",
          "Operator Aliases": "Playa",
          "Operator Parent Company": "Playa Hotels & Resorts"
        }
      },
      {
        "id": "rec3Uwxe6ovpiokuN",
        "name": "Hilton (Managed)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Hybrid",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://www.hilton.com",
          "Operator Aliases": "Hilton; Hilton Worldwide; Hilton Management Services; HMS",
          "Operator Parent Company": "Hilton Worldwide"
        }
      },
      {
        "id": "rec6UB6RpMKSs2tAo",
        "name": "Remington Hospitality",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "rec7IXYQYpKMYsrDl",
        "name": "IHG Hotels & Resorts (Managed)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Hybrid",
          "Management Availability": "Conditional / Scoped",
          "Operator Website": "https://www.ihg.com",
          "Operator Aliases": "IHG; InterContinental Hotels Group",
          "Operator Parent Company": "IHG"
        }
      },
      {
        "id": "rec8SrT3VjRkkYTxm",
        "name": "Minor Hotels (Managed)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Hybrid",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://www.minorhotels.com",
          "Operator Aliases": "Minor Hotels; Minor Hotel Group; NH Hotels",
          "Operator Parent Company": "Minor International"
        }
      },
      {
        "id": "rec9JSyGQjvodsPSJ",
        "name": "AADESA",
        "fields": {
          "Record Purpose": "Research"
        }
      },
      {
        "id": "recBReJUmxdOUvQzp",
        "name": "Cordillera One Gestión",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "recF2WqLqNVyKGz9E",
        "name": "Accor (Managed)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Hybrid",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://group.accor.com/en/hotel-development",
          "Operator Aliases": "Accor; AccorHotels; Accor Group",
          "Operator Parent Company": "Accor"
        }
      },
      {
        "id": "recF5Z87OAqFgndoq",
        "name": "Arbor Lodging (CALA)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Third-Party",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://arborlodging.com",
          "Operator Aliases": "Arbor Lodging; Arbor Lodging Partners",
          "Operator Parent Company": "Arbor Lodging Partners"
        }
      },
      {
        "id": "recGWxIJqnYHkJZFD",
        "name": "Aimbridge Hospitality (LATAM)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Third-Party",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://www.aimbridge.com",
          "Operator Aliases": "Aimbridge; Aimbridge Hospitality",
          "Operator Parent Company": "Aimbridge Hospitality"
        }
      },
      {
        "id": "recGmiPhRt6hiayd9",
        "name": "Marriott International (Managed)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Hybrid",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott",
          "Operator Aliases": "Marriott; MxM; Managed by Marriott; Marriott International, Inc.",
          "Operator Parent Company": "Marriott International"
        }
      },
      {
        "id": "recHj56wpRLUnJ5Wx",
        "name": "Tremun Hoteles",
        "fields": {
          "Record Purpose": "Research"
        }
      },
      {
        "id": "recJ6NPSYveCTo3At",
        "name": "Tafer Hotels & Resorts",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "recJtFkhjaO57rSDC",
        "name": "Grupo Presidente",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "recKVILWcRLqrQlWs",
        "name": "Driftwood Hospitality Management",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Third-Party",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://www.driftwoodhospitality.com",
          "Operator Aliases": "Driftwood",
          "Operator Parent Company": "Driftwood Hospitality"
        }
      },
      {
        "id": "recLjxtxIIVJaGbXK",
        "name": "Highgate",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Third-Party",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://highgate.com",
          "Operator Aliases": "Highgate Hotels",
          "Operator Parent Company": "Highgate"
        }
      },
      {
        "id": "recOc5kpsg4Muip9Y",
        "name": "Royalton Hotels & Resorts",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "recQ6Cf8O2z0tiqBz",
        "name": "Cenote Azul Operadores",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Third-Party",
          "Management Availability": "Conditional / Scoped",
          "Operator Website": "https://cenoteazul.mx",
          "Operator Aliases": "Cenote Azul"
        }
      },
      {
        "id": "recTUjuDxL96yWcQA",
        "name": "Antillano Norte Hospitality Group",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "recWPKu5laVZxsvpn",
        "name": "Hotel Equities (CALA)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Third-Party",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://hotelequities.com/cala.htm",
          "Operator Aliases": "Hotel Equities",
          "Operator Parent Company": "Hotel Equities"
        }
      },
      {
        "id": "recZPHT2zqc8K6itx",
        "name": "Viento Sur Gestión Hotelera",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "recZgNR85WZKDItLF",
        "name": "Mangle Azul Hospitalidad",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "recbT3q8ApRIBu4j5",
        "name": "Panamerican Lodging Partners S.A.",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "receHCdI6CEsJqdG4",
        "name": "Brittain Resorts & Hotels (BRH)",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "recfwDdU5t9h4uFnZ",
        "name": "Atlantica Hotels International (AHI)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Hybrid",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://www.ahi.com.br",
          "Operator Aliases": "Atlantica; AHI",
          "Operator Parent Company": "Atlantica Hospitality International"
        }
      },
      {
        "id": "reciI2tYQBfMoMK9G",
        "name": "GHL Hoteles (GHL Holding)",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Third-Party",
          "Management Availability": "Confirmed Direct Management",
          "Operator Website": "https://www.ghlhoteles.com",
          "Operator Aliases": "GHL Hoteles; GHL",
          "Operator Parent Company": "GHL Holding"
        }
      },
      {
        "id": "recjgHXqTJktijFUR",
        "name": "Álvarez Argüelles Hoteles",
        "fields": {
          "Record Purpose": "Research",
          "Operating Model": "Third-Party",
          "Management Availability": "Confirmed Direct Management",
          "Operator Aliases": "Alvarez Arguelles"
        }
      },
      {
        "id": "reck6gjQd3wdeugmZ",
        "name": "Arriva Hospitality Group (AHG)",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "reckO98E46sKTn3F3",
        "name": "Río Plata Hotel Partners",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "reckyv9O0Y3auYpJJ",
        "name": "Grupo Hotelero Santa Fe",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Hybrid",
          "Management Availability": "Conditional / Scoped",
          "Operator Website": "https://www.gsf-hoteles.com",
          "Operator Aliases": "Santa Fe; GHSF",
          "Operator Parent Company": "Grupo Hotelero Santa Fe"
        }
      },
      {
        "id": "recq3NiRxOerg4kZU",
        "name": "Barrio Hotelero CDMX",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "rectsHzacZDFTH1Ze",
        "name": "OxoHotel",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "recuEDrp6oeJIEuRX",
        "name": "Grupo Marta Hospitality",
        "fields": {
          "Record Purpose": "Production"
        }
      },
      {
        "id": "recwEHUotSGpfkZEJ",
        "name": "Grupo Iberostar",
        "fields": {
          "Record Purpose": "Production",
          "Operating Model": "Integrated Owner / Brand / Operator",
          "Management Availability": "Conditional / Scoped",
          "Operator Website": "https://www.iberostar.com",
          "Operator Aliases": "Iberostar; Iberostar Hotels & Resorts",
          "Operator Parent Company": "Grupo Iberostar"
        }
      },
      {
        "id": "recwbyY4qfNP1bV3r",
        "name": "Metro Lodging São Paulo",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      },
      {
        "id": "recxAa86Qoc0nFRSt",
        "name": "Oro Verde Lodge & Hotel Operators",
        "fields": {
          "Record Purpose": "Test Fixture"
        }
      }
    ],
    "created": [
      {
        "provisionalId": "provisional_operator_hyatt",
        "finalMasterId": "reculkMOYWDxX14Pv",
        "canonicalName": "Hyatt (Managed)",
        "aliases": "Hyatt; Hyatt Hotels Corporation",
        "operatingModel": "Hybrid",
        "managementAvailability": "Confirmed Direct Management",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_sonesta",
        "finalMasterId": "recIq0XYgt5Ghvcsz",
        "canonicalName": "Sonesta International",
        "aliases": "Sonesta",
        "operatingModel": "Brand / Operator",
        "managementAvailability": "Confirmed Direct Management",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_four_seasons",
        "finalMasterId": "rechnXKjpeiNMaqjJ",
        "canonicalName": "Four Seasons Hotels and Resorts",
        "aliases": "Four Seasons",
        "operatingModel": "Brand / Operator",
        "managementAvailability": "Confirmed Direct Management",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_rosewood",
        "finalMasterId": "recji1awMffccwox2",
        "canonicalName": "Rosewood Hotel Group",
        "aliases": "Rosewood",
        "operatingModel": "Brand / Operator",
        "managementAvailability": "Confirmed Direct Management",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_mandarin_oriental",
        "finalMasterId": "rec5xdV2THfFjEUPk",
        "canonicalName": "Mandarin Oriental Hotel Group",
        "aliases": "MOHG; Mandarin Oriental",
        "operatingModel": "Integrated Brand / Operator",
        "managementAvailability": "Conditional / Scoped",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_radisson",
        "finalMasterId": "rec0AXje3BxPqIDnZ",
        "canonicalName": "Radisson Hotel Group",
        "aliases": "RHG; Radisson",
        "operatingModel": "Hybrid",
        "managementAvailability": "Conditional / Scoped",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_melia",
        "finalMasterId": "rec28eZ7ERwc92XWd",
        "canonicalName": "Meliá Hotels International",
        "aliases": "Melia; Meliá",
        "operatingModel": "Hybrid",
        "managementAvailability": "Conditional / Scoped",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_auberge",
        "finalMasterId": "recVtNxNeeYlngtUk",
        "canonicalName": "Auberge Resorts Collection",
        "aliases": "Auberge",
        "operatingModel": "Brand / Operator",
        "managementAvailability": "Confirmed Direct Management",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_shangri_la",
        "finalMasterId": "rec8XpNv6G0WOlMwu",
        "canonicalName": "Shangri-La Group",
        "aliases": "Shangri-La",
        "operatingModel": "Integrated Brand / Operator",
        "managementAvailability": "Conditional / Scoped",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      },
      {
        "provisionalId": "provisional_operator_barcelo",
        "finalMasterId": "rec04aLAfmupWG4ZK",
        "canonicalName": "Barceló Hotel Group",
        "aliases": "Barcelo; Barceló",
        "operatingModel": "Integrated Owner / Brand / Operator",
        "managementAvailability": "Conditional / Scoped",
        "recordPurpose": "Research",
        "lifecycle": "Research Stage",
        "duplicateCheck": "clear"
      }
    ],
    "held": [],
    "failed": []
  },
  "sources": {
    "reused": 4,
    "created": 41,
    "skippedWeak": 0,
    "failed": []
  },
  "assignments": {
    "proposed": 84,
    "created": 75,
    "held": 9,
    "failed": []
  },
  "brandRelationships": {
    "proposed": 51,
    "created": 51,
    "bmc": 24,
    "held": 0,
    "failed": []
  },
  "presence": {
    "created": 20,
    "updated": 0,
    "skipped": 40,
    "failed": []
  },
  "claims": {
    "created": 0,
    "updated": 0,
    "skipped": 25,
    "failed": []
  },
  "holdouts": [
    {
      "id": "hold_cenote_geo",
      "entity": "Cenote Azul Operadores",
      "entityId": "recQ6Cf8O2z0tiqBz",
      "domain": "geography",
      "conflict": "Prior Active Countries / presence claims may overstate vs assignment evidence",
      "whyWithheld": "Do not seed unsupported Active Countries; presence creates only where dry-run Proposed new with evidence",
      "resolution": "Expand named assignments; founder not required if policy followed"
    },
    {
      "id": "hold_playa_hyatt",
      "entity": "Playa Hotels & Resorts / Hyatt",
      "entityId": "rec3TUHT9Z4AnFp5P",
      "domain": "corporate_relationship",
      "conflict": "Hyatt acquisition adjacency vs Playa as distinct Track 1 management counterparty",
      "whyWithheld": "Do not merge Masters; do not seed ownership-change as Operating Model change without founder",
      "resolution": "Keep separate Masters; note corporate relationship in claims only when sourced",
      "founderNeeded": true
    },
    {
      "id": "hold_mxm_enterprise_cala",
      "entity": "Marriott International (Managed)",
      "entityId": "recGmiPhRt6hiayd9",
      "domain": "assignments",
      "conflict": "Enterprise managed scale vs sparse named CALA assignments",
      "whyWithheld": "Aggregate/representative assignment rows held",
      "resolution": "Seed only named hotels; Webhound supplemental later"
    },
    {
      "id": "hold_asg_asg_c01_provisional_operator_radisson_radisson_blu_examples_americas_europe_",
      "entity": "provisional_operator_radisson",
      "entityId": "provisional_operator_radisson",
      "domain": "assignments",
      "proposedRecord": "asg_c01_provisional_operator_radisson_radisson_blu_examples_americas_europe_",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_c01_rad_01",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_provisional_operator_sonesta_sonesta_select_examples_us_portfolio_",
      "entity": "provisional_operator_sonesta",
      "entityId": "provisional_operator_sonesta",
      "domain": "assignments",
      "proposedRecord": "asg_c01_provisional_operator_sonesta_sonesta_select_examples_us_portfolio_",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_c01_sonesta_01",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_rec3Uwxe6ovpiokuN_representative_hilton_managed_hotels_en",
      "entity": "rec3Uwxe6ovpiokuN",
      "entityId": "rec3Uwxe6ovpiokuN",
      "domain": "assignments",
      "proposedRecord": "asg_c01_rec3Uwxe6ovpiokuN_representative_hilton_managed_hotels_en",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_c01_hil_01",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_rec7IXYQYpKMYsrDl_intercontinental_managed_examples",
      "entity": "rec7IXYQYpKMYsrDl",
      "entityId": "rec7IXYQYpKMYsrDl",
      "domain": "assignments",
      "proposedRecord": "asg_c01_rec7IXYQYpKMYsrDl_intercontinental_managed_examples",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_c01_ihg_01",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_rec8SrT3VjRkkYTxm_nh_collection_anantara_representative_",
      "entity": "rec8SrT3VjRkkYTxm",
      "entityId": "rec8SrT3VjRkkYTxm",
      "domain": "assignments",
      "proposedRecord": "asg_c01_rec8SrT3VjRkkYTxm_nh_collection_anantara_representative_",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_c01_minor_01",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_recF2WqLqNVyKGz9E_representative_accor_managed_hotels",
      "entity": "recF2WqLqNVyKGz9E",
      "entityId": "recF2WqLqNVyKGz9E",
      "domain": "assignments",
      "proposedRecord": "asg_c01_recF2WqLqNVyKGz9E_representative_accor_managed_hotels",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_c01_acc_01",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_recfwDdU5t9h4uFnZ_atlantica_multi_brand_brazil_portfolio_",
      "entity": "recfwDdU5t9h4uFnZ",
      "entityId": "recfwDdU5t9h4uFnZ",
      "domain": "assignments",
      "proposedRecord": "asg_c01_recfwDdU5t9h4uFnZ_atlantica_multi_brand_brazil_portfolio_",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_w2_ah_001",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_cs_recv6jStwspE4c4Ca",
      "entity": "recfwDdU5t9h4uFnZ",
      "entityId": "recfwDdU5t9h4uFnZ",
      "domain": "assignments",
      "proposedRecord": "asg_c01_cs_recv6jStwspE4c4Ca",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    },
    {
      "id": "hold_asg_asg_c01_recGmiPhRt6hiayd9_representative_marriott_managed_luxury_u",
      "entity": "recGmiPhRt6hiayd9",
      "entityId": "recGmiPhRt6hiayd9",
      "domain": "assignments",
      "proposedRecord": "asg_c01_recGmiPhRt6hiayd9_representative_marriott_managed_luxury_u",
      "conflict": "Aggregate / representative portfolio row — not a named property SoT",
      "source": "src_c01_mxm_01",
      "whyWithheld": "Assignments SoT requires named properties",
      "resolution": "Replace with named hotels via supplemental research"
    }
  ],
  "provisionalCrosswalk": {
    "provisional_operator_hyatt": "reculkMOYWDxX14Pv",
    "provisional_operator_sonesta": "recIq0XYgt5Ghvcsz",
    "provisional_operator_four_seasons": "rechnXKjpeiNMaqjJ",
    "provisional_operator_rosewood": "recji1awMffccwox2",
    "provisional_operator_mandarin_oriental": "rec5xdV2THfFjEUPk",
    "provisional_operator_radisson": "rec0AXje3BxPqIDnZ",
    "provisional_operator_melia": "rec28eZ7ERwc92XWd",
    "provisional_operator_auberge": "recVtNxNeeYlngtUk",
    "provisional_operator_shangri_la": "rec8XpNv6G0WOlMwu",
    "provisional_operator_barcelo": "rec04aLAfmupWG4ZK"
  }
}
```