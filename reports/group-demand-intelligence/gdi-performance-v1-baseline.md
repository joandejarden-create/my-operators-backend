# GDI Performance V1 — Baseline (before code changes)

**As of:** 2026-09-23  
**Branch:** `deploy/adp-final-trust-closure-20260910`  
**Starting SHA:** `cdbb419`  
**Deploy ID (baseline):** `428a6afc-9e34-4f80-aeb0-b6bdbad6c1bc`  
**Node:** v22.19.0  
**Runtime:** Railway `my-operators-backend` production  

## Method

Bethesda share surface. 5 samples per endpoint via HTTP (server-side latency proxy for View Details / list).

## API baseline (ms)

| Endpoint | P50 | P95 | MAX | Bytes |
| --- | ---: | ---: | ---: | ---: |
| share HTML | 131 | 155 | 306 | 1.7KB |
| resolve | 279 | 286 | 2836 | 1.3KB |
| opportunities list | **5147** | **5306** | 5462 | 62KB |
| opportunity detail | **4881** | **5159** | 5182 | 13KB |

## Initial load model (share)

Serial chain: `resolve` → `opportunities`  
Estimated useful content ≈ resolve + list ≈ **5.4s P50** (network only; + JS parse/render).

## View Details model

Model **C/D**: always calls detail GET; reloads full hotel opportunity doc (+ commercial progression).  
No click acknowledgement before fetch completes.  
Click-to-visible ≈ detail P50 **~4.9s**.

## Root cause (baseline)

1. Airtable `listOpportunitiesForHotel` on every list and detail (~5s).  
2. Commercial progression N+1 (sequential decision/event loads) on list + detail.  
3. Serial resolve→list on share bootstrap.  
4. No immediate UI shell on View Details.
