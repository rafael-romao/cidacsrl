# ADR 005: Introdução da documentação arquitetural no repositório

## Status

Aceito / Implementado

## Contexto

O foco inicial da documentação (docs/user-guide/) estava no usuário final do CIDACS-RL. Havia pouca visibilidade sobre a mecânica interna para os engenheiros de software.

## Decisão

A nova versão adota uma cultura de documentação da arquitetura incorporada ao repositório via MkDocs, com a introdução do diretório `docs/architecture/` (explicações gráficas e textuais como `overview.md` e `execution-flows.md`) e do diretório `docs/adr/`, contendo os registros de decisão arquitetural (ADRs) individuais, ambos registrados no `nav:` do `mkdocs.yml`.

## Consequências

**Positivas:** Facilita o onboarding e serve como fonte única de verdade (Single Source of Truth) para o design técnico — tanto o estado atual da arquitetura (`docs/architecture/`) quanto o histórico de decisões e seus porquês (`docs/adr/`).

**Negativas:** Requer disciplina na revisão de Pull Requests para garantir que a arquitetura, as ADRs e a documentação evoluam sincronizadamente — uma ADR criada sem ser adicionada ao `nav:` do MkDocs, por exemplo, fica invisível no site publicado mesmo existindo no repositório.

## Referências

- `docs/architecture/overview.md`, `docs/architecture/execution-flows.md`
- `docs/adr/`
- `mkdocs.yml` (seção `nav`)

