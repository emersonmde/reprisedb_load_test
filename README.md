# Load Test For RepriseDB

## Usage

Use `cargo update` to update the RepriseDB dependency to the latest version.
```bash
cargo update && cargo run --release -- --duration 30 --num-tasks 300 --keyspace 1000000000
```

The `gen_graph.py` script will take the data output by the load test and generate a graph for it:

![graph](https://github.com/emersonmde/reprisedb_load_test/blob/main/saved_runs/2023-06-04/1000000000_keyspace_30_sec_300_threads_90_percent_writes.png)
