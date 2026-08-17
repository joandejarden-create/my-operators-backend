# Operator Fit — Comparable Relevance Simulation (Audit Only)

Generated: 2026-08-04T13:50:57.727Z

Not production. Does not change engine weights.

## Variant summary (Deal C Santa Fe vs Highgate)

| Variant | SF Displayed (sim) | HG Displayed (sim) | Diff | Directionally defensible? |
| ------- | -----------------: | -----------------: | ---: | ------------------------- |
| Conservative | 37.6 | 37.8 | -0.2 | See notes |
| Moderate | 36.4 | 37.3 | -0.9 | See notes |
| Strong | 35.5 | 36.9 | -1.4 | See notes |

## Detail

```json
{
  "A_conservative": {
    "label": "Conservative",
    "note": "Comparables create limited differentiation; capped influence.",
    "weights": {
      "geography": 18,
      "segment": 12,
      "development": 14,
      "asset": 10,
      "urbanResort": 8,
      "brand": 6,
      "keyCount": 4,
      "complexity": 8,
      "recency": 6,
      "comparabilityStrength": 8,
      "evidence": 6
    },
    "blendIntoAsset": 0.25,
    "multiCompBonus": 2,
    "santaFe": {
      "cri": {
        "best": 0.679,
        "avg": 0.679,
        "n": 1,
        "combined": 0.6111,
        "bestComp": {
          "property": "GSF Mexico third-party managed hotels (portfolio)",
          "country": "Multiple, Mexico",
          "score": 0.679,
          "dims": {
            "geography": 1,
            "segment": 0.2,
            "development": 1,
            "asset": 0.6,
            "urbanResort": 0.7,
            "brand": 0.5,
            "keyCount": 0.3,
            "complexity": 0.35,
            "recency": 0.75,
            "comparabilityStrength": 0.55,
            "evidence": 1
          }
        },
        "details": [
          {
            "property": "GSF Mexico third-party managed hotels (portfolio)",
            "country": "Multiple, Mexico",
            "score": 0.679,
            "dims": {
              "geography": 1,
              "segment": 0.2,
              "development": 1,
              "asset": 0.6,
              "urbanResort": 0.7,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 1
            }
          }
        ]
      },
      "simulatedAsset": 75.3,
      "opProject": 57.8,
      "primary": 47.6,
      "displayed": 37.6
    },
    "highgate": {
      "cri": {
        "best": 0.545,
        "avg": 0.44933333333333336,
        "n": 3,
        "combined": 0.5013666666666667,
        "bestComp": {
          "property": "Aloft Tulum",
          "country": "Tulum, Mexico",
          "score": 0.545,
          "dims": {
            "geography": 1,
            "segment": 0.2,
            "development": 0.25,
            "asset": 0.4,
            "urbanResort": 0.7,
            "brand": 0.5,
            "keyCount": 0.3,
            "complexity": 0.35,
            "recency": 0.75,
            "comparabilityStrength": 0.55,
            "evidence": 0.85
          }
        },
        "details": [
          {
            "property": "Aloft Tulum",
            "country": "Tulum, Mexico",
            "score": 0.545,
            "dims": {
              "geography": 1,
              "segment": 0.2,
              "development": 0.25,
              "asset": 0.4,
              "urbanResort": 0.7,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 0.85
            }
          },
          {
            "property": "The Ocean Club, a Luxury Collection Resort (DR)",
            "country": "Costa Norte, Dominican Republic",
            "score": 0.43300000000000005,
            "dims": {
              "geography": 0.4,
              "segment": 0.55,
              "development": 0.25,
              "asset": 0.25,
              "urbanResort": 0.2,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 1
            }
          },
          {
            "property": "Tambo del Inka, a Luxury Collection Resort & Spa",
            "country": "Urubamba, Peru",
            "score": 0.37,
            "dims": {
              "geography": 0.1,
              "segment": 0.55,
              "development": 0.25,
              "asset": 0.25,
              "urbanResort": 0.2,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 0.85
            }
          }
        ]
      },
      "simulatedAsset": 76.5,
      "opProject": 58.1,
      "primary": 47.8,
      "displayed": 37.8
    },
    "differenceDisplayed": -0.2,
    "doubleCountRisk": "Asset factor already includes comparable boost; blending CRI atop current asset can double-count unless Asset is rebuilt around CRI."
  },
  "B_moderate": {
    "label": "Moderate",
    "note": "Comparables important subcomponent of Asset/Development + complexity signal.",
    "weights": {
      "geography": 22,
      "segment": 14,
      "development": 16,
      "asset": 12,
      "urbanResort": 10,
      "brand": 6,
      "keyCount": 4,
      "complexity": 10,
      "recency": 6,
      "comparabilityStrength": 10,
      "evidence": 8
    },
    "blendIntoAsset": 0.55,
    "multiCompBonus": 5,
    "santaFe": {
      "cri": {
        "best": 0.6838983050847458,
        "avg": 0.6838983050847458,
        "n": 1,
        "combined": 0.6155084745762711,
        "bestComp": {
          "property": "GSF Mexico third-party managed hotels (portfolio)",
          "country": "Multiple, Mexico",
          "score": 0.6838983050847458,
          "dims": {
            "geography": 1,
            "segment": 0.2,
            "development": 1,
            "asset": 0.6,
            "urbanResort": 0.7,
            "brand": 0.5,
            "keyCount": 0.3,
            "complexity": 0.35,
            "recency": 0.75,
            "comparabilityStrength": 0.55,
            "evidence": 1
          }
        },
        "details": [
          {
            "property": "GSF Mexico third-party managed hotels (portfolio)",
            "country": "Multiple, Mexico",
            "score": 0.6838983050847458,
            "dims": {
              "geography": 1,
              "segment": 0.2,
              "development": 1,
              "asset": 0.6,
              "urbanResort": 0.7,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 1
            }
          }
        ]
      },
      "simulatedAsset": 69.9,
      "opProject": 56.4,
      "primary": 46.4,
      "displayed": 36.4
    },
    "highgate": {
      "cri": {
        "best": 0.5516949152542373,
        "avg": 0.45112994350282487,
        "n": 3,
        "combined": 0.506412429378531,
        "bestComp": {
          "property": "Aloft Tulum",
          "country": "Tulum, Mexico",
          "score": 0.5516949152542373,
          "dims": {
            "geography": 1,
            "segment": 0.2,
            "development": 0.25,
            "asset": 0.4,
            "urbanResort": 0.7,
            "brand": 0.5,
            "keyCount": 0.3,
            "complexity": 0.35,
            "recency": 0.75,
            "comparabilityStrength": 0.55,
            "evidence": 0.85
          }
        },
        "details": [
          {
            "property": "Aloft Tulum",
            "country": "Tulum, Mexico",
            "score": 0.5516949152542373,
            "dims": {
              "geography": 1,
              "segment": 0.2,
              "development": 0.25,
              "asset": 0.4,
              "urbanResort": 0.7,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 0.85
            }
          },
          {
            "property": "The Ocean Club, a Luxury Collection Resort (DR)",
            "country": "Costa Norte, Dominican Republic",
            "score": 0.4338983050847458,
            "dims": {
              "geography": 0.4,
              "segment": 0.55,
              "development": 0.25,
              "asset": 0.25,
              "urbanResort": 0.2,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 1
            }
          },
          {
            "property": "Tambo del Inka, a Luxury Collection Resort & Spa",
            "country": "Urubamba, Peru",
            "score": 0.3677966101694915,
            "dims": {
              "geography": 0.1,
              "segment": 0.55,
              "development": 0.25,
              "asset": 0.25,
              "urbanResort": 0.2,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 0.85
            }
          }
        ]
      },
      "simulatedAsset": 73.9,
      "opProject": 57.4,
      "primary": 47.3,
      "displayed": 37.3
    },
    "differenceDisplayed": -0.9,
    "doubleCountRisk": "Asset factor already includes comparable boost; blending CRI atop current asset can double-count unless Asset is rebuilt around CRI."
  },
  "C_strong": {
    "label": "Strong",
    "note": "Direct comparables become primary Asset/Development driver.",
    "weights": {
      "geography": 24,
      "segment": 12,
      "development": 18,
      "asset": 14,
      "urbanResort": 12,
      "brand": 6,
      "keyCount": 4,
      "complexity": 12,
      "recency": 8,
      "comparabilityStrength": 12,
      "evidence": 10
    },
    "blendIntoAsset": 0.85,
    "multiCompBonus": 8,
    "santaFe": {
      "cri": {
        "best": 0.6984848484848484,
        "avg": 0.6984848484848484,
        "n": 1,
        "combined": 0.6286363636363635,
        "bestComp": {
          "property": "GSF Mexico third-party managed hotels (portfolio)",
          "country": "Multiple, Mexico",
          "score": 0.6984848484848484,
          "dims": {
            "geography": 1,
            "segment": 0.2,
            "development": 1,
            "asset": 0.6,
            "urbanResort": 0.7,
            "brand": 0.5,
            "keyCount": 0.3,
            "complexity": 0.35,
            "recency": 0.75,
            "comparabilityStrength": 0.55,
            "evidence": 1
          }
        },
        "details": [
          {
            "property": "GSF Mexico third-party managed hotels (portfolio)",
            "country": "Multiple, Mexico",
            "score": 0.6984848484848484,
            "dims": {
              "geography": 1,
              "segment": 0.2,
              "development": 1,
              "asset": 0.6,
              "urbanResort": 0.7,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 1
            }
          }
        ]
      },
      "simulatedAsset": 65.4,
      "opProject": 55.3,
      "primary": 45.5,
      "displayed": 35.5
    },
    "highgate": {
      "cri": {
        "best": 0.5636363636363636,
        "avg": 0.4568181818181818,
        "n": 3,
        "combined": 0.5159090909090909,
        "bestComp": {
          "property": "Aloft Tulum",
          "country": "Tulum, Mexico",
          "score": 0.5636363636363636,
          "dims": {
            "geography": 1,
            "segment": 0.2,
            "development": 0.25,
            "asset": 0.4,
            "urbanResort": 0.7,
            "brand": 0.5,
            "keyCount": 0.3,
            "complexity": 0.35,
            "recency": 0.75,
            "comparabilityStrength": 0.55,
            "evidence": 0.85
          }
        },
        "details": [
          {
            "property": "Aloft Tulum",
            "country": "Tulum, Mexico",
            "score": 0.5636363636363636,
            "dims": {
              "geography": 1,
              "segment": 0.2,
              "development": 0.25,
              "asset": 0.4,
              "urbanResort": 0.7,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 0.85
            }
          },
          {
            "property": "The Ocean Club, a Luxury Collection Resort (DR)",
            "country": "Costa Norte, Dominican Republic",
            "score": 0.4363636363636364,
            "dims": {
              "geography": 0.4,
              "segment": 0.55,
              "development": 0.25,
              "asset": 0.25,
              "urbanResort": 0.2,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 1
            }
          },
          {
            "property": "Tambo del Inka, a Luxury Collection Resort & Spa",
            "country": "Urubamba, Peru",
            "score": 0.3704545454545454,
            "dims": {
              "geography": 0.1,
              "segment": 0.55,
              "development": 0.25,
              "asset": 0.25,
              "urbanResort": 0.2,
              "brand": 0.5,
              "keyCount": 0.3,
              "complexity": 0.35,
              "recency": 0.75,
              "comparabilityStrength": 0.55,
              "evidence": 0.85
            }
          }
        ]
      },
      "simulatedAsset": 71.9,
      "opProject": 56.9,
      "primary": 46.9,
      "displayed": 36.9
    },
    "differenceDisplayed": -1.4,
    "doubleCountRisk": "Asset factor already includes comparable boost; blending CRI atop current asset can double-count unless Asset is rebuilt around CRI."
  }
}
```
