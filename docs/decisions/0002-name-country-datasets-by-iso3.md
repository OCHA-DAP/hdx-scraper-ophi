# 0002: Name country datasets and showcases by ISO3

## Status

Accepted — 2026-11-11

## Context

Country datasets and showcases were named from the country name (`{country} MPI`). Country names change
in the OCHA taxonomy: NRU became "Naoero", which created a duplicate `naoero-mpi` dataset (9b14349).

## Decision

Name country datasets and showcases `{ISO3} MPI`, for example `NRU MPI`. Titles still use the country name.

## Consequences

- Dataset names stay stable when a country is renamed in the taxonomy.
- Unclear how datasets already published under country-name slugs are matched or migrated. This needs
  checking against HDX before the next run.
