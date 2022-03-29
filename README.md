# `scrabsha/plays-with-clickhouse`

This repository contains a bunch of experiments on how to interact with the
clickhouse database. It aims to be a simple playground for me to experiment
with, possibly helping other people in the future.

Don't expect any kind of stability there.

## Experiments

This repository is a cargo workspace where each member corresponds to a
different experiment. They are listed above:

| Member name | What to see in there |
|-------------| -------------------- |
| `1-hello-world` | A super simple hello-world code where we insert hardcoded data in a database. |
| `2-dynamic-data` | An attempt to port the previous experiment to runtime-defined data structures. |
| `3-another-crate` | :tada: I got it working for runtime-defined data structures with `clickhouse-rs`. |
| `4-value-and-co` | Switched everything to `async-std`, column name and type known ahead of time, actual tremor values. |