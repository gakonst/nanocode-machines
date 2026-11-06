# nanocode-machines

This repository is retired. `nanocode-mpp` is now part of the [Nanocode](https://github.com/gakonst/nanocode) CLI as `--gateway tempo` (gakonst/nanocode 9b3d914), so Nanocode pays Codex and Claude per request over MPP from a Tempo wallet without a separate binary.

```sh
cargo install --git https://github.com/gakonst/nanocode --locked nanocode-cli
tempo wallet login
nanocode --gateway tempo
nanocode --gateway tempo --gateway-base-url https://api.inference.boundless.network/mpp
```

The first runs on Tempo's proxies, with tool payments, Mercator and the Tempo gateway; the second pays one MPP endpoint, such as Boundless, and closes its session on exit. `NANOCODE_GATEWAY=tempo` sets the gateway for every run, and `/login tempo` signs in to the wallet from inside Nanocode. Until the next Nanocode release, install it from source as above; the install script's binary does not have it yet.

Sessions stay in the same Tempo wallet channel store and session slots, so channels nanocode-mpp opened carry over. Its settings map to these flags, each with a `NANOCODE_TEMPO_*` variable:

| nanocode-mpp | Nanocode |
| --- | --- |
| `MPP_ENDPOINT` | `--gateway-base-url` with `--gateway tempo` |
| `MPP_NETWORK` | `--tempo-network` |
| `MPP_CURRENCY` | `--tempo-currency` |
| `MPP_SESSION` | `--tempo-session` |
| `MPP_KEEP_SESSION=1` | `--tempo-keep-session` |
| `NANOCODE_MPP_DEPOSIT` | `--tempo-deposit` |
| `NANOCODE_MPP_TOOL_DEPOSIT` | `--tempo-tool-deposit` |

## License

MIT or Apache-2.0, at your option.
