# GDI Discovery Quality + Persistence Recovery V4

**Date:** 2026-10-03

## A. Executive Summary

AssociationScout produced **48** detail hits in V3 SCOUT_YIELD but **0** were persisted to `ASSOCIATION_RESULTS.csv` (silent report-writer omission). Surgical recovery restored **45** Association detail rows. Admission gate separated SIGNAL vs CANDIDATE before completion. Bounded completion on admitted candidates only.

| Funnel | Count |
|--------|------:|
| Raw signals reviewed | 680 |
| Admitted candidates | 249 |
| Signal-only | 245 |
| Early rejected | 66 |
| Duplicates | 120 |
| Researched | 40 |
| Customer ready | 0 |
| Valid future watch | 0 |

## B. Association Persistence

See ASSOCIATION_PERSISTENCE_ROOT_CAUSE.md. Root cause: **scripts/gdi-discovery-expansion-v3-2026-10-03.mjs report writer — rowsForScout() never called for SCOUT_FAMILY.ASSOCIATION**.

## C. Recovery

Recovered=45 · invalid=0 · admitted from association family=10.

## D. Success Controls

Bethesda/NYC ready controls n=58. Pattern: named buyer + future timing + hotel-motion thesis + contact/housing path. V3 SERP hits rarely combine these.

## E. Admission Gate

Implemented. Thresholds unchanged.

## F. Jev Role

Priority/depth/language/feeder **DISABLED**. Next-blocker / source / stop-continue **CONDITIONAL** post-admission.

## G. Hotel Results

YOTEL: raw 179 · admitted 58 · researched 13 · ready 0 · watch 0
AC: raw 142 · admitted 45 · researched 2 · ready 0 · watch 0
SPICE: raw 179 · admitted 95 · researched 15 · ready 0 · watch 0
CAMBRIDGE: raw 96 · admitted 28 · researched 4 · ready 0 · watch 0
NOW_NOW: raw 84 · admitted 23 · researched 6 · ready 0 · watch 0

## H. Conversion

Signal→candidate 36.6% · Candidate(researched)→useful 0.0% · Cost/useful N/A

## Safety
Thresholds NO · Watch bypass NO · ADP NO · Shares NO · Broad discovery NO

**STOP.**
