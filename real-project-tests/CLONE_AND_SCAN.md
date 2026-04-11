# Clone public samples and scan with LineageScope

All clones live under `real-project-tests/` and are **gitignored** (see repo `.gitignore`). Run commands from the **LineageScope repo root**.

## dbt — [dbt-labs/jaffle_shop](https://github.com/dbt-labs/jaffle_shop)

```powershell
cd real-project-tests
git clone --depth 1 https://github.com/dbt-labs/jaffle_shop.git
cd ..
lineagescope scan real-project-tests\jaffle_shop --dialect postgres --exclude node_modules,venv,.venv,.git
```

## Airflow — example DAGs only (sparse clone, **recommended**)

The full Airflow repo is very large. Sparse checkout pulls only `example_dags`:

```powershell
cd real-project-tests
Remove-Item -Recurse -Force airflow -ErrorAction SilentlyContinue
git clone --depth 1 --filter=blob:none --sparse https://github.com/apache/airflow.git airflow
cd airflow
git sparse-checkout init --no-cone
git sparse-checkout set airflow-core/src/airflow/example_dags
cd ..\..
lineagescope scan real-project-tests\airflow\airflow-core\src\airflow\example_dags --exclude node_modules,venv,.venv,.git,__pycache__
```

If you **already** have a full `airflow` clone, skip cloning and run only the last `lineagescope scan` line (path as above).

## Spark — [apache/spark](https://github.com/apache/spark) `examples` tree

```powershell
cd real-project-tests
git clone --depth 1 --filter=blob:none --sparse https://github.com/apache/spark.git spark
cd spark
git sparse-checkout init --no-cone
git sparse-checkout set examples
cd ..\..
lineagescope scan real-project-tests\spark\examples --exclude node_modules,venv,.venv,.git,__pycache__,target
```

## SQLMesh + dbt examples — [TobikoData/sqlmesh](https://github.com/TobikoData/sqlmesh)

Open-source SQLMesh monorepo (Python orchestration + dbt-style projects in `examples/` and test fixtures).

```powershell
cd real-project-tests
git clone --depth 1 https://github.com/TobikoData/sqlmesh.git
cd ..
lineagescope scan real-project-tests\sqlmesh --dialect postgres --exclude node_modules,venv,.venv,.git,__pycache__,.tox,.pytest_cache,dist,build,.eggs
```

## Larger dbt (optional)

- [brooklyn-data/dbt_artifacts](https://github.com/brooklyn-data/dbt_artifacts)  
- [dbt-labs/dbt-project-evaluator](https://github.com/dbt-labs/dbt-project-evaluator)  

Clone with `git clone --depth 1 <url>` into `real-project-tests/`, then `lineagescope scan <that-folder> --dialect postgres` (add `--exclude` as needed).

## Batch summaries

From repo root, after clones exist:

```powershell
pip install -e ".[dev]"
python real-project-tests\run_validation.py
```

See `VALIDATION.md` for metrics and `LINEAGESCOPE_PRIVATE_SCAN_PATH` for a private repo path.
