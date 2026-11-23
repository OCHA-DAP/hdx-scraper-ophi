# 0001: Prefer official MPI rows over harmonised trend rows in HAPI output

## Status

Accepted — 2026-11-10

## Context

The HAPI poverty rate output is derived from the global MPI data, which combines official MPI rows with
harmonised trend rows. For the same location and survey end date, both can exist and would produce
duplicate HAPI rows. The 2026 update surfaced this (fee1f61).

## Decision

Rows are keyed on (country ISO3, provider admin 1 name, admin 1 p-code, reference period end). Official
MPI rows are processed first, and a later harmonised trend row with the same key is skipped.

## Consequences

- One HAPI row per location and survey end date, using the official figure.
- A harmonised value for a survey that also has an official row is not published to HAPI.
- Correctness depends on official rows being processed before trend rows.
