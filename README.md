# nanocode-machines

`nanocode-mpp` is the [Nanocode](https://github.com/gakonst/nanocode) coding agent with its model requests paid per request over [MPP](https://mpp.dev) from a Tempo wallet, instead of a ChatGPT or Claude subscription login. It runs Nanocode's terminal interface and `exec` unchanged; this crate only wires Nanocode to [mpp-rs](https://github.com/tempoxyz/mpp-rs).

Payments are real: they spend your wallet's funds. Keep deposits small.

## Install

You need a Rust toolchain and a Tempo wallet:

```sh
curl -fsSL https://tempo.xyz/install | bash
tempo wallet login
cargo install --git https://github.com/gakonst/nanocode-machines --bin nanocode-mpp --locked
```

Then fund the wallet with a few dollars of MACH or USDC.e on Tempo mainnet (`tempo wallet fund --mach`). From a checkout, install with `cargo install --path . --bin nanocode-mpp --locked`. If cargo can't fetch a pinned git revision, set `CARGO_NET_GIT_FETCH_WITH_CLI=true`.

## Use

```sh
nanocode-mpp
nanocode-mpp exec -m luna "Reply with exactly: ok"
nanocode-mpp models
```

By default it pays Tempo's MPP proxies for OpenAI (the Codex models) and Anthropic (the Claude models), preferring MACH. Each service gets a session, a payment channel funded with a deposit, which later runs reuse. `/spend` shows what each model and tool has spent.

The same wallet also pays:

- HTTPS requests that shell tools make to services in the [mpp.dev directory](https://mpp.dev/api/services) and `*.mpp.tempo.xyz`, through a local proxy only tool processes see.
- The built-in Mercator and Tempo gateway MCP servers.

## Pay a custom endpoint

`MPP_ENDPOINT` points it at one OpenAI-compatible endpoint that takes MPP sessions, such as [Boundless](https://boundless.network):

```sh
export MPP_ENDPOINT=https://api.inference.boundless.network/mpp
nanocode-mpp models
nanocode-mpp exec -m deepseek/deepseek-v4.1-flash "Reply with exactly: ok"
```

In this mode:

- The Codex models are the ones the endpoint lists at `<url>/v1/models`. Claude models keep your own Claude login, and tool payments, Mercator and the gateway are off.
- The chain comes from the endpoint's payment challenge: Tempo mainnet pays USDC.e and testnet pathUSD by default. `MPP_NETWORK` pins it.
- The session closes cooperatively on exit, so the unspent deposit comes back at once. A run that made no paid request leaves its channel for the next run. `MPP_KEEP_SESSION=1` keeps it open.
- Concurrent runs each take one of 64 slots per endpoint and pay from separate channels.

## Settings

| Variable | Effect |
| --- | --- |
| `MPP_ENDPOINT` | Pay this endpoint instead of Tempo's proxies. A value without a scheme is HTTPS. |
| `MPP_NETWORK` | `mainnet` or `testnet`: the endpoint's chain, instead of reading it from its challenge. |
| `MPP_CURRENCY` | The token to prefer for an endpoint: `mach`, `usdc.e` or `ousd` (mainnet), `pathusd` (testnet), or a token address. An endpoint that doesn't offer it is paid in one it does. |
| `MPP_SESSION` | Pay an endpoint from this named channel instead of a slot. |
| `MPP_KEEP_SESSION` | `1` keeps an endpoint's session open on exit. |
| `NANOCODE_MPP_DEPOSIT` | Model session deposit and top-up, in base units (6 decimals). Default `2000000` (2 MACH) for Tempo's proxies, `1000000` (1 token) for an endpoint. |
| `NANOCODE_MPP_TOOL_DEPOSIT` | Tool session deposit and top-up, in base units. Default `100000` (0.1 MACH). |
| `TEMPO_HOME` | The wallet's directory (default `~/.tempo`). Channels are kept in `wallet/channels.db`. |

`tempo wallet sessions list` shows the wallet's open sessions, and `tempo wallet sessions close <channel>` closes one on chain.

## License

MIT or Apache-2.0, at your option.
