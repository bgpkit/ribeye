# ribeye

BGP RIB processing framework written in Rust.

## Implemented RIB processors

- [X] peer-stats: collector peer information from a given RIB dump file
- [X] pfx2as: prefix-to-AS mapping from a given RIB dump file
- [ ] as2rel: AS-level relationship

## Run

Set up a cronjob (ideally 2+ hours after UTC midnight) and run the following command to generate daily data:

```bash
ribeye cook --dir /DATA/PATH/TO/OUTPUT/DIRECTORY
```

See the Hashicorp Nomad [job spec](deployment/nomad_periodic_raw.hcl) for an example Nomad deployment.

## Examples

See [`examples/`](examples) directory for usage examples.
