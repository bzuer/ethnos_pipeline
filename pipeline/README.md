# Pipeline

## Estrutura
Terminologia ELT: `pipeline/[extract|load|transform]/[entidade]/[fonte].py`.

```
pipeline/
├── extract/                       # Busca dados de APIs externas → JSON local
│   ├── common.py                  # Utilitários compartilhados (config, CSV, ISSN)
│   ├── works/
│   │   ├── missing_dois.py        # Compara DOIs Crossref vs DB → listas de DOIs ausentes
│   │   ├── crossref.py            # Fetch de metadados via Crossref para DOI lists
│   │   └── openalex.py            # Fetch de metadados via OpenAlex para DOI lists
│   └── venues/
│       ├── scopus.py              # Fetch de metadados via Scopus Serial Title API
│       └── openalex.py            # Fetch de metadados via OpenAlex Sources API
├── load/                          # Lê JSONs → insere no banco
│   ├── common.py                  # Filtros, cache, conexão
│   ├── works/
│   │   ├── openalex.py            # Load de works via OpenAlex
│   │   └── crossref.py            # Load de works via Crossref
│   └── venues/
│       ├── openalex.py            # Load de venues via OpenAlex
│       └── scopus.py              # Load de venues via Scopus
└── transform/                     # Limpeza/normalização de dados no banco
    ├── common.py                  # Re-export de DB helpers (pipeline.common)
    ├── cleanup.py                 # Limpeza textual + sentinelas + identificadores + perfil
    ├── postprocess.py             # Junk removal, orphans, OA propagation, persons dedup/signatures
    └── metrics.py                 # Ref/citation counts, h_index, venue stats/yearly/impact/ranking, sphinx summaries
```

## Diretórios de output (cache JSON)
Convenção: `entidade/fonte/`

```
works/
├── crossref/          # JSONs extraídos do Crossref
├── openalex/          # JSONs extraídos do OpenAlex
└── doi/
    ├── cache/         # Cache de DOIs por ISSN/ano (Crossref journal catalog)
    └── missing/       # Listas de DOIs ausentes no DB

venues/
├── scopus/            # JSONs extraídos do Scopus
└── openalex/          # JSONs extraídos do OpenAlex
```

## Extract

### Works — encontrar e buscar DOIs ausentes
```bash
venv/bin/python pipeline/extract/works/missing_dois.py -i venues_list.csv --from-year 2026
venv/bin/python pipeline/extract/works/missing_dois.py -i venues_list.csv --from-year 2025 --until-year 2025
venv/bin/python pipeline/extract/works/missing_dois.py -i venues_list.csv --force
venv/bin/python pipeline/extract/works/crossref.py works/doi/missing
venv/bin/python pipeline/extract/works/openalex.py works/doi/missing
```

### Venues — buscar metadados
```bash
venv/bin/python pipeline/extract/venues/scopus.py --input-file venues_list.csv --limit 10
venv/bin/python pipeline/extract/venues/openalex.py --input-file venues_list.csv --limit 10
venv/bin/python pipeline/extract/venues/scopus.py --input-file venues_list.csv --output-dir venues/scopus --force
venv/bin/python pipeline/extract/venues/openalex.py --input-file venues_list.csv --output-dir venues/openalex --force
```

## Load

### Works
```bash
venv/bin/python pipeline/load/works/openalex.py works/openalex
venv/bin/python pipeline/load/works/openalex.py works/openalex --limit 1000
venv/bin/python pipeline/load/works/crossref.py works/crossref
venv/bin/python pipeline/load/works/crossref.py works/crossref --mode full
```

### Venues
```bash
venv/bin/python pipeline/load/venues/openalex.py --json-dir venues/openalex
venv/bin/python pipeline/load/venues/openalex.py --json-dir venues/openalex --limit 1000
venv/bin/python pipeline/load/venues/scopus.py --json-dir venues/scopus
venv/bin/python pipeline/load/venues/scopus.py --json-dir venues/scopus --limit 1000
```

## Transform

### cleanup.py — normalização textual
```bash
venv/bin/python pipeline/transform/cleanup.py                              # perfil + todas as fases
venv/bin/python pipeline/transform/cleanup.py --mode profile               # apenas perfil
venv/bin/python pipeline/transform/cleanup.py --mode run                   # todas as fases
venv/bin/python pipeline/transform/cleanup.py --phase text --table works
venv/bin/python pipeline/transform/cleanup.py --dry-run
```

### postprocess.py — limpeza estrutural (junk, orphans, persons, métricas)
```bash
venv/bin/python pipeline/transform/postprocess.py                    # full (works + persons)
venv/bin/python pipeline/transform/postprocess.py --works            # works cleanup only
venv/bin/python pipeline/transform/postprocess.py --persons          # persons processing only
venv/bin/python pipeline/transform/postprocess.py --dry-run          # preview steps
```

### metrics.py — métricas e sumários sphinx
```bash
venv/bin/python pipeline/transform/metrics.py                        # partial, todas entidades
venv/bin/python pipeline/transform/metrics.py --full                  # recálculo completo
venv/bin/python pipeline/transform/metrics.py --works                 # sync ref/citation counts
venv/bin/python pipeline/transform/metrics.py --full --persons        # full person stats + h_index
venv/bin/python pipeline/transform/metrics.py --full --venues         # full venue stats + h_index + yearly + impact + ranking
venv/bin/python pipeline/transform/metrics.py --full --sphinx         # rebuild sphinx summaries
venv/bin/python pipeline/transform/metrics.py --dry-run               # preview steps
```

## Flags Comuns

### Extract venues
| Flag | Default | Descrição |
|------|---------|-----------|
| `--input-file` | `venues_list.csv` | CSV com `venue_id, venue_name, issn, eissn` |
| `--output-dir` | `venues/scopus` / `venues/openalex` | Diretório de cache JSON |
| `--config PATH` | `config.ini` | Caminho para config.ini |
| `--force` | off | Re-fetch mesmo se cache existir |
| `--limit N` | `0` (ilimitado) | Máximo de venues |

### Load (ingest)
| Flag | Default | Descrição |
|------|---------|-----------|
| `directory` | (positional) | Diretório raiz de JSONs |
| `--mode {new,full}` | `new` | `new` = só DOIs inéditos; `full` = reprocessa todos |
| `--limit N` | sem limite | Máximo de arquivos por subdiretório |
| `--commit-batch N` | `0` | `0` = commit por subdiretório; `N` = a cada N registros |
| `--config PATH` | `config.ini` | Caminho para config.ini |

## Configuração
Scripts esperam `config.ini` na raiz do repositório com credenciais de banco.
