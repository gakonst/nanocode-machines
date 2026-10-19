use std::{
    collections::BTreeMap,
    process::ExitCode,
    sync::{Arc, Mutex, OnceLock},
};

use futures_util::Stream;
use mpp::{
    Address,
    client::{
        AcceptPaymentPolicy, ClientEventSubscription, ClientEvents, DEFAULT_MAX_PAYMENT_RETRIES,
        Exchange, FlowError, ForwardProxy, PaymentResponseContext, TempoAccountsProvider,
        TempoSessionProvider,
        directory::{self, MPP_DIRECTORY_URL},
    },
    protocol::methods::tempo::{CHAIN_ID, USDC, machine_token},
};
use nanocode::{
    ClaudeModel, Model,
    http::{self as layer, BoxFuture, Event, EventHook, Layer, Next},
};
use nanocode_cli::ModelClients;
use reqwest::ResponseBuilderExt;
use tokio::sync::mpsc::UnboundedSender;

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
        let wallet = sessions(wallet, &service.parse()?, deposit)?;
        Ok(Self {
            session: wallet.session()?,
            wallet,
            client: reqwest::Client::builder()
                .no_proxy()
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

fn sessions(
    wallet: &TempoAccountsProvider,
    service: &reqwest::Url,
    deposit: u128,
) -> Result<TempoAccountsProvider> {
    Ok(wallet
        .clone()
        .with_sqlite_session_store(service)?
        .with_session_prepaid_requests(PREPAID_REQUESTS)
        .with_session_default_deposit(deposit)
        .with_session_top_up_amount(deposit))
}

#[derive(Clone)]
struct ToolPayments {
    ledger: Arc<Mutex<Ledger>>,
    failure: Arc<OnceLock<String>>,
    _paid: Arc<ClientEventSubscription>,
}

#[derive(Default)]
struct Ledger {
    totals: BTreeMap<String, u128>,
    last: Option<(u128, String)>,
    listeners: Vec<UnboundedSender<String>>,
}

impl ToolPayments {
    fn start(wallet: &TempoAccountsProvider, machine: Address, deposit: u128) -> Result<Self> {
        let mut models = Vec::new();
        for url in [OPENAI, ANTHROPIC] {
            models.extend(reqwest::Url::parse(url)?.host_str().map(str::to_owned));
        }
        let events = ClientEvents::default();
        let ledger = Arc::new(Mutex::new(Ledger::default()));
        let recorded = ledger.clone();
        let paid = events.on_payment_response(move |context| {
            record(&recorded, &context, machine);
            async {}
        });
        let tools = wallet.clone();
        let proxy = ForwardProxy::bind(move |url| sessions(&tools, url, deposit))?
            .bypass(models)
            .events(events);
        for (name, value) in proxy.environment() {
            unsafe { std::env::set_var(name, value) };
        }
        let failure = Arc::new(OnceLock::new());
        let (client, failed) = (
            reqwest::Client::builder().no_proxy().build()?,
            failure.clone(),
        );
        proxy.spawn(async move {
            match directory::service_urls(&client, MPP_DIRECTORY_URL).await {
                Ok(services) => AcceptPaymentPolicy::Origins(services),
                Err(error) => {
                    let _ = failed.set(error.to_string());
                    AcceptPaymentPolicy::Never
                }
            }
        })?;
        Ok(Self {
            ledger,
            failure,
            _paid: Arc::new(paid),
        })
    }

    fn notices(&self) -> impl Stream<Item = String> + use<> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.ledger.lock().unwrap().listeners.push(sender);
        futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|notice| (notice, receiver))
        })
    }

    fn status(&self) -> Option<String> {
        if let Some(error) = self.failure.get() {
            return Some(format!("tool payments off: {error}"));
        }
        let ledger = self.ledger.lock().unwrap();
        let (last, symbol) = ledger.last.as_ref()?;
        let totals: Vec<_> = ledger
            .totals
            .iter()
            .map(|(symbol, total)| format!("{} {symbol}", mach(*total)))
            .collect();
        Some(format!(
            "tools spent {} (last {} {symbol})",
            totals.join(" + "),
            mach(*last)
        ))
    }
}

fn record(
    ledger: &Mutex<Ledger>,
    context: &PaymentResponseContext,
    machine: Address,
) -> Option<()> {
    let request = context
        .challenge
        .request
        .decode::<serde_json::Value>()
        .ok()?;
    let amount = request.get("amount")?.as_str()?.parse().ok()?;
    let currency = context
        .receipt
        .as_ref()
        .and_then(|receipt| receipt.extensions.get("fundingCurrency"))
        .or_else(|| request.get("currency"))?
        .as_str()?
        .parse::<Address>()
        .ok()?;
    let symbol = if currency == machine {
        "MACH".to_owned()
    } else if USDC.parse() == Ok(currency) {
        "USDC.e".to_owned()
    } else {
        currency.to_string()
    };
    let reference = context
        .receipt
        .as_ref()
        .map(|receipt| format!(" · {}", receipt.reference))
        .unwrap_or_default();
    let notice = format!(
        "paid {} {symbol} to {} ({}{reference})",
        mach(amount),
        context.url.host_str().unwrap_or_default(),
        context.challenge.intent
    );
    let mut ledger = ledger.lock().unwrap();
    *ledger.totals.entry(symbol.clone()).or_default() += amount;
    ledger.last = Some((amount, symbol));
    ledger
        .listeners
        .retain(|listener| listener.send(notice.clone()).is_ok());
    Some(())
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
    let machine = machine_token::token(CHAIN_ID).ok_or("MACH is not deployed on Tempo mainnet")?;
    let wallet = TempoAccountsProvider::from_default_store()?
        .with_expected_chain_id(CHAIN_ID)
        .with_preferred_currency(machine);
    let tools = ToolPayments::start(&wallet, machine, deposit)?;
    let notices = tools.clone();
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
        .notices(move || notices.notices())
        .status(move || {
            let parts: Vec<_> = meters
                .iter()
                .filter_map(|(name, mpp)| mpp.status(name))
                .chain(tools.status())
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
