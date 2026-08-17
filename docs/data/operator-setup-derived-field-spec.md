# Operator Setup — Derived Field Spec

## Active Countries

- **Source:** Market Presence where type ∈ {Current Operating Portfolio, Current Managed Property} ∪ Assignments with Assignment Status=Current
- **Exclude:** Strategic Interest, Claimed Capability, Historical-only
- **Transform:** distinct Country set
- **Min evidence:** ≥1 current presence or current named assignment
- **Conflict:** Prefer assignment-backed countries if Presence conflicts
- **Blank:** if no current evidence
- **Refresh:** on OE wave apply / nightly sync

## Brand Families Operated

- **Source:** Intel Brand Relationships (Currently Operates / BMC) ∪ Assignments.Brand
- **Min evidence:** ≥1 brand name
- **Taxonomy:** ensure select options before write

## Development experience flags/counts

- **Source:** Assignments.Development Context distinct values
- **Map:** New Build → newBuild; Conversion/Reflag/Repositioning → conversion; etc.

## Urban / Resort / AI / Extended Stay

- **Source:** Assignments Urban/Resort + checkbox flags
- **Rule:** Yes if ≥1 current assignment matches

## Management Structures Supported

- **Source:** distinct Assignments.Operating / Management Structure (+ Master.Operating Model)
- **Caution:** taxonomy mapping required
