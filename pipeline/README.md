# data

Pipeline bibliografico ELT (Extract, Load, Transform) para MariaDB.

## Estrutura

```
pipeline/               # codebase ativo — extract, load, transform
database/               # schema SQL e dumps
venues_list.csv         # lista de periodicos (input principal)
config.ini              # credenciais DB/API (nao versionado)
works/                  # cache JSON de works (crossref, openalex, doi)
venues/                 # cache JSON de venues (scopus, openalex)
arquivo/                # scripts legados (referencia, nao executar)
venv/                   # virtualenv Python
```

## Uso rapido

```bash
# extract
venv/bin/python pipeline/extract/works/missing_dois.py -i venues_list.csv --from-year 2026
venv/bin/python pipeline/extract/works/crossref.py works/doi/missing
venv/bin/python pipeline/extract/works/openalex.py works/doi/missing

# load
venv/bin/python pipeline/load/works/openalex.py works/openalex --limit 1000
venv/bin/python pipeline/load/works/crossref.py works/crossref --limit 1000

# transform (sempre --dry-run primeiro)
venv/bin/python pipeline/transform/cleanup.py --dry-run
venv/bin/python pipeline/transform/postprocess.py --dry-run
venv/bin/python pipeline/transform/metrics.py --dry-run
```

Documentacao completa em [`pipeline/README.md`](pipeline/README.md).
# ethnos_pipeline
