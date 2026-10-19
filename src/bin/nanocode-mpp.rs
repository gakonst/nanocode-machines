use std::process::ExitCode;

use mpp::{
    client::{
        AcceptPaymentPolicy, ClientEvents, DEFAULT_MAX_PAYMENT_RETRIES, Exchange, FlowError,
        TempoAccountsProvider, TempoSessionProvider,
    },
    protocol::methods::tempo::{CHAIN_ID, machine_token},
};
use nanocode::{
    ClaudeModel, Model,
    http::{self as layer, BoxFuture, Event, EventHook, Layer, Next},
};
use nanocode_cli::ModelClients;
use reqwest::ResponseBuilderExt;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const OPENAI: &str = "https://openai.mpp.tempo.xyz/v1";
const ANTHROPIC: &str = "https://anthropic.mpp.tempo.xyz/v1/messages";
const MACH: u128 = 1_000_000;
const PREPAID_REQUESTS: u32 = 2;

#[derive(Clone)]
struct Mpp {
    wallet: TempoAccountsProvider,
    session: TempoSessionProvider,
    client: reqwest::Client,
}

impl Mpp {
    fn new(wallet: &TempoAccountsProvider, service: &str, deposit: u128) -> Result<Self> {
        let wallet = wallet
            .clone()
            .with_sqlite_session_store(&service.parse()?)?
            .with_session_prepaid_requests(PREPAID_REQUESTS)
            .with_session_default_deposit(deposit)
            .with_session_top_up_amount(deposit);
        Ok(Self {
            session: wallet.session()?,
            wallet,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    async fn preconnect(self, url: String, model: &str) {
        let body = serde_json::json!({ "model": model }).to_string();
        let _ = self.wallet.preconnect(&self.client, &url, body).await;
    }

    fn status(&self, name: &str) -> Option<String> {
        let spend = self.session.spend();
        Some(format!(
            "{name} spent {} MACH (last {}) · {} MACH left",
            mach(spend.spent),
            mach(spend.last?),
            mach(spend.remaining)
        ))
    }
}

impl Layer for Mpp {
    fn send<'a>(
        &'a self,
        request: layer::Request,
        next: Next<'a>,
    ) -> BoxFuture<'a, std::result::Result<layer::Response, layer::Error>> {
        Box::pin(async move {
            let url = reqwest::Url::parse(&request.url)
                .map_err(|error| layer::Error::new(error.to_string()).unsent(true))?;
            let mut paid = reqwest::Request::new(request.method, url);
            *paid.headers_mut() = request.headers;
            *paid.body_mut() = Some(request.body.into());
            let mut exchange = NextExchange {
                next,
                client: self.client.clone(),
                body: None,
            };
            let response = mpp::client::send_with_exchange(
                &mut exchange,
                paid,
                &self.wallet,
                &AcceptPaymentPolicy::Always,
                &ClientEvents::default(),
                DEFAULT_MAX_PAYMENT_RETRIES,
            )
            .await
            .map_err(|error| match error {
                FlowError::Send(error) => error,
                FlowError::Payment(error) => layer::Error::new(error.to_string()),
            })?;
            let body = exchange
                .body
                .ok_or_else(|| layer::Error::new("the payment flow returned no response"))?;
            Ok(
                layer::Response::new(response.status(), response.headers().clone(), body)
                    .with_events(SessionEvents {
                        mpp: self.clone(),
                        url: request.url,
                    }),
            )
        })
    }
}

struct SessionEvents {
    mpp: Mpp,
    url: String,
}

impl EventHook for SessionEvents {
    fn event(
        &mut self,
        event: Event<'_>,
    ) -> Option<BoxFuture<'static, std::result::Result<(), layer::Error>>> {
        let work =
            self.mpp
                .session
                .stream_event(&self.mpp.client, &self.url, event.name?, event.data)?;
        Some(Box::pin(async move {
            work.await
                .map_err(|error| layer::Error::new(format!("MPP session stream: {error}")))
        }))
    }
}

struct NextExchange<'a> {
    next: Next<'a>,
    client: reqwest::Client,
    body: Option<layer::Body>,
}

impl Exchange for NextExchange<'_> {
    type Error = layer::Error;

    fn http_client(&self) -> Option<reqwest::Client> {
        Some(self.client.clone())
    }

    async fn send(
        &mut self,
        request: reqwest::Request,
    ) -> std::result::Result<reqwest::Response, layer::Error> {
        let sent = layer::Request {
            method: request.method().clone(),
            url: request.url().to_string(),
            headers: request.headers().clone(),
            body: request
                .body()
                .and_then(reqwest::Body::as_bytes)
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
        };
        let (status, headers, body, _) = self.next.send(sent).await?.into_parts();
        self.body = Some(body);
        let mut response = http::Response::builder()
            .status(status)
            .url(request.url().clone())
            .body(reqwest::Body::from(Vec::new()))
            .map_err(|error| layer::Error::new(error.to_string()))?;
        *response.headers_mut() = headers;
        Ok(response.into())
    }
}

fn mach(amount: u128) -> String {
    format!("{}.{:06}", amount / MACH, amount % MACH)
}

fn clients() -> Result<ModelClients> {
    nanocode::codex::transport::install_default_rustls_crypto_provider();
    let deposit = match std::env::var_os("NANOCODE_MPP_DEPOSIT") {
        Some(deposit) => deposit
            .to_str()
            .ok_or("NANOCODE_MPP_DEPOSIT is not UTF-8")?
            .parse()?,
        None => 2 * MACH,
    };
    let wallet = TempoAccountsProvider::from_default_store()?
        .with_expected_chain_id(CHAIN_ID)
        .with_preferred_currency(
            machine_token::token(CHAIN_ID).ok_or("MACH is not deployed on Tempo mainnet")?,
        );
    let openai = Mpp::new(&wallet, OPENAI, deposit)?;
    let claude = Mpp::new(&wallet, ANTHROPIC, deposit)?;
    let meters = [("openai", openai.clone()), ("anthropic", claude.clone())];
    let warm = meters.clone();
    Ok(ModelClients::new()
        .openai_endpoint(OPENAI, openai)
        .claude_endpoint(ANTHROPIC, claude)
        .warm_up(move || {
            let [(_, openai), (_, claude)] = warm.clone();
            async move {
                futures_util::future::join(
                    openai.preconnect(format!("{OPENAI}/responses"), Model::Luna.as_str()),
                    claude.preconnect(ANTHROPIC.to_owned(), ClaudeModel::Haiku55.as_str()),
                )
                .await;
            }
        })
        .status(move || {
            let parts: Vec<_> = meters
                .iter()
                .filter_map(|(name, mpp)| mpp.status(name))
                .collect();
            (!parts.is_empty()).then(|| parts.join(" | "))
        }))
}

fn main() -> ExitCode {
    match clients() {
        Ok(clients) => nanocode_cli::run_with(clients),
        Err(error) => {
            eprintln!("nanocode-mpp: {error}");
            ExitCode::FAILURE
        }
    }
}
