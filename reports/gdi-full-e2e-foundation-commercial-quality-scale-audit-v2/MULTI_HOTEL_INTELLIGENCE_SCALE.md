# Multi-Hotel Intelligence Scale

## Goal

Reuse independently sourced public research across hotels in a market without sharing hotel-confidential commercial activity.

## Current state (LIKELY / CONFIRMED)

- Per-hotel bundled packages and hotel-specific qualification — **CONFIRMED**.  
- Shared segment research strategies / Exa missions can rediscover same public events for each hotel — cost duplication **LIKELY**.  
- No single global opportunity registry promoted as customer SoT in this audit.  
- CMI can represent competitor-held demand for a subject hotel without exposing another client’s CRM.

## Hotel-confidential (must isolate)

Pipeline status, feedback, outreach, rates, CRM notes, bookings, losses, client-provided intel.

## Minimal future architecture (prefer incremental)

1. Optional **shared evidence objects** (org/event/project + public sources) keyed independently of hotel.  
2. **Hotel match layer** applies geo/fit/timing → hotel-specific opportunity + drawer + publication state.  
3. Never auto-publish shared evidence to all hotels.  
4. API/export allowlists enforce confidentiality.

Do **not** create a competing global registry migration in the first implementation PRs — extend existing research ledgers carefully after identity work (P3→P4).
