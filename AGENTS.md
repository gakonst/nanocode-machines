# nanocode-machines

MPP-paid model transports for Nanocode agents. Model requests are paid per request through [mpp-rs](https://github.com/tempoxyz/mpp-rs) instead of a ChatGPT or Claude subscription login, and Nanocode itself carries no payment code.

- `MppTransport<P>` implements Nanocode's public transport traits: `nanocode::codex::transport::host::HostTransport` for HTTPS Responses and `nanocode::claude::transport::HttpTransport` for Messages. Nanocode applies them through `OpenAiBuilder::host_transport` and `ClaudeClient::with_transport`; there is no plugin registry. Keep payment logic here and keep Nanocode changes limited to provider-neutral transport contracts.
- `P` is any `mpp::client::PaymentProvider`. Each request goes through mpp-rs's 402 flow: the first send returns challenges, the provider pays, and the same body is resent with the credential. Responses over MPP are HTTPS with full history replay; native host transports reject WebSocket. `events` forwards mpp-rs `ClientEvents` for payment observability.
- `tempo_sessions` attaches mpp-rs's MPPx-compatible SQLite channel store (`$TEMPO_HOME/wallet/channels.db`, default `~/.tempo/wallet/channels.db`) namespaced by service origin, so each service reuses its channel across processes. Deposit, top-up and maximum deposit are caller policy set with mpp-rs's `with_session_*` builders; the proxies send no `suggestedDeposit`. Pass the provider's `session()` to `MppTransport::session`.
- Metered session responses are mppx SSE streams. `src/stream.rs` answers `payment-need-voucher` events with `send_voucher`, reports streamed acceptance through `acknowledge_accepted` so pending opens resolve, removes both payment events, and strips mppx's generic `event: message` name so parsers that check event names against data types, such as Claude's, read the upstream `type`.
- Funding preference is `TempoAccountsProvider::with_preferred_currency`. With MACH preferred, a TIP-1034 session denominated in MACH wins, then a machine-token-enabled charge paid with MACH through the canonical swapper, then the first offer in Accept-Payment order (sessions before charges). This mirrors Mercator's MACH-first charge ordering.
- `OPENAI_API_BASE_URL` and `ANTHROPIC_MESSAGES_URL` are the `serviceUrl` entries for `openai` and `anthropic` in https://mpp.dev/api/services. Since tempoxyz/mpp-proxy#394 their model routes offer only `tempo/session`, in USDC.e and MACH, with `unitType: request`, `feePayer` and `feeToken` USDC.e. `tempo/charge` in USDC.e and pathUSD with `machineTokenEnabled` is offered only to requests carrying a charge credential or an `Accept-Payment` header that ranks `tempo/charge` above `tempo/session`.
- The proxy charges a request-priced session stream once and passes the upstream bytes through with their event names and a `Payment-Receipt` header. Its `Server-Timing` header reports `mpp;dur`, the proxy's own time, and `upstream;dur`, the provider's time to response headers.
- The default `AcceptPaymentPolicy::Always` applies because the transport only receives the model endpoints configured on Nanocode; redirects are disabled. Request failures are reconnectable and payment failures are not. A retried model request pays for its own unit again.

## MACH sessions

Since T12 (mainnet 2026-10-13), TIP-1095 checks a TIP-1034 channel's TIP-403 policy on the logical payer-to-payee path and refunds the payer without it. A MACH session is therefore an ordinary TIP-1034 channel in MACH to a payee on MACH's recipient policy 582. tempoxyz/mach#93 added the MPP proxy payee `0xca4e835F803cB0b7C428222B3A3B98518d4779Fe` to that policy.

- A MACH channel cannot pay its own gas. Sponsored opens pay gas in the challenge's `feeToken` (tempoxyz/mpp-rs#582), and unsponsored opens need `with_session_fee_token`.
- mppx `compose()` dispatched every session credential to the first session handler, so the proxy rejected MACH opens with a currency mismatch. tempoxyz/mpp-proxy#395 routes MACH session credentials itself; wevm/mppx#1060 fixes `compose()`.

## Development

- Do not add code comments, doc comments, README files, tests or test-only dependencies. The user maintains README files.
- Nanocode and mpp-rs are pinned by git revision. mpp-rs points at a tempoxyz/mpp-rs main revision that includes #581 and #582. The root and `output/validation` manifests must pin the same revision. Bump the pins when the transport traits change. If cargo cannot fetch a revision, set `CARGO_NET_GIT_FETCH_WITH_CLI=true`.
- Check with `cargo fmt --check`, `cargo check --examples` and `cargo clippy --all-targets`.
- The consumer in ignored `output/validation` has these modes:
  - `cargo run`: a local 402 mock with a mock provider. Expect one unpaid and one paid request per backend.
  - `live`: real proxies with a provider that declines to pay.
  - `timing PREFERRED N`: phase timings from `ClientEvents` for tiny real requests.
  - `paid PREFERRED BACKEND DEPOSIT PROMPT...`: real agent turns through `MppTransport` with a memory durability store.
  - `openraw PREFERRED [N]`: N sequential paid OpenAI requests without `MppTransport`, printing the 402, payment, header and first-byte times with the proxy's `Server-Timing`, and the problem details of a rejected payment, which it rolls back.

  Real modes spend the wallet's funds; run them only with the wallet owner's approval.
- 2026-10-19 mainnet results with wallet `0x9759…e122` and its P-256 access key:
  - MACH-funded charges completed Codex (Luna) and Claude (Haiku 5.5) turns, spending 0.049925 and 0.074382 MACH.
  - USDC.e TIP-1034 sessions completed Codex and Claude turns, including top-ups.
  - After mpp-rs reused RPC connections, challenge-to-credential time for MACH charges fell from 1.11–1.18 s to 0.25–0.31 s.
  - An unpaid 402 from the proxy took 0.5–3 s, and `/.well-known/mpp` took 0.5–0.8 s.
  - MACH sessions opened sponsored 0.30 MACH channels for Codex and Claude and completed turns, spending 0.051685 and 0.148863 MACH. The wallet's MACH balance fell by exactly the two deposits.
  - Proxy time (`mpp;dur`) for a warm paid session request fell from 1263 ms to 178–199 ms after tempoxyz/mpp-proxy#397, #399 and #400. What remains is the forced on-chain channel read (80–110 ms to `rpc.tempo.xyz`) and one Durable Object compare-and-swap (~60 ms). A warm voucher takes 4–16 ms on the client; the first payment in a process restores the channel from SQLite and chain state.
- Keep logs and validation artifacts in ignored `output/`. Never commit wallet stores, channel databases or credentials.
