# ribeye

BGP RIB processing framework written in Rust.

## Processors

- `peer-stats`: collector peer information from a given RIB dump file
- `pfx2as`: prefix-to-AS mapping from a given RIB dump file
- `as2rel`: AS-level relationship
- `pfx2dist`: prefix-to-collector-distance mapping, counting the minimum AS-path distance for every prefix to each route
  collector

## Installation

### From source

Checkout the repository and run the following command to install the binary:

```bash
cargo install --path .
```

## Run

Get help information:

```bash
ribeye cook --help

Process recent RIB dump files

Usage: ribeye cook [OPTIONS]

Options:
      --days <DAYS>
          Number of days to search back for
          
          [default: 1]

  -e, --env <ENV>
          Path to environment variables file

  -l, --limit <LIMIT>
          limit to process the smallest N RIB dump files

  -c, --collectors <COLLECTORS>
          Specify route collectors to use (e.g. route-views2, rrc00)

  -p, --processors <PROCESSORS>
          specify processors to use.
          
          Available processors: pfx2as, pfx2dist, as2rel, peer_stats
          
          If not specified, all processors will be used

  -t, --threads <THREADS>
          Number of threads to use

  -d, --dir <DIR>
          Root data directory
          
          [default: ./results]

      --summarize-only
          Only summarize latest results

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

## Cronjob setup

Set up a cronjob (ideally 2+ hours after UTC midnight) and run the following command to generate daily data:

```bash
ribeye cook --dir /DATA/PATH/TO/OUTPUT/DIRECTORY
```

See the Hashicorp Nomad [job spec](deployment/nomad_periodic_raw.hcl) for an example Nomad deployment.