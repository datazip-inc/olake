# smt-poc

Proof of concept for the SMT + JSON flattening + Variant feature. Spec: `../smt.md`. Evidence and reasoning: `../smt-research.md`.

```sh
GOWORK=off go build -tags duckdb_arrow -o flatten-poc .     # CGO; arrow-go >= 18.8 for VARIANT

./flatten-poc -cases                                        # 26 edge cases (15 and 16 are refused by design)
./ex/run.sh                                                 # 10 examples -> ex/out/NN.txt + NN.parquet
./flatten-poc -source orders.jsonl -out /tmp/x.parquet \
  -query "select _id.\"\$oid\" as order_id, flatten(customer, 2), status, _olake_raw::VARIANT as doc from batch"
./flatten-poc -source orders.jsonl -shred -out /tmp/x.parquet -query "select _olake_raw::VARIANT as doc from batch"
```

| Flag          | Meaning                                                                                                                        |
|---------------|--------------------------------------------------------------------------------------------------------------------------------|
| `-source`     | JSONL: one JSON document (= one OLake record) per line, read as raw text                                                       |
| `-query`      | the transform: `flatten(path[, depth])`, `expr::VARIANT`, plain DuckDB SQL, `FROM batch`                                       |
| `-depth`      | default depth for `flatten()` without one                                                                                      |
| `-batch-size` | rows per batch (0 = all); shows structure merge + schema evolution across batches                                              |
| `-out`        | Parquet written from the Arrow batch with pqarrow (OLake-style names, field ids, VARIANT logical type); `_bN` suffix per batch |
| `-shred`      | shred `::VARIANT` columns: typed_value for stable primitive paths of the running structure                                     |
| `-v`          | print structures, depth cut, generated SQL, shredding schema                                                                   |

Input files: `orders.jsonl`, `ex/*.jsonl` — one document per line. A relational row with a JSON column is just a document whose key holds an object (`ex/products.jsonl`: `{"id":42,"name":"widget","attributes":{...}}` → `flatten(attributes, 2)`). The raw document is reachable in the query as `_olake_raw`.

`iceberg/` — registering a Parquet file into an Iceberg v3 table through the REST catalog (`Register.java`, uses the OLake Java writer fat jar) and DuckDB read scripts for the `destination/iceberg/local-test` stack (Lakekeeper ≥ 0.13 required for variant).
