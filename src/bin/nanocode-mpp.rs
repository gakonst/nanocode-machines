use std::{
    collections::{BTreeMap, HashMap},
    process::ExitCode,
    sync::{Arc, Mutex, OnceLock},
};

use async_trait::async_trait;
use futures_util::Stream;
use mpp::{
    Address, PaymentChallenge,
    client::{
        AcceptPaymentPolicy, ClientEventSubscription, ClientEvents, DEFAULT_MAX_PAYMENT_RETRIES,
        DepositContext, Exchange, FlowError, ForwardProxy, PaymentContext, PaymentProvider,
        PendingPayment, TempoAccountsProvider, TempoSessionProvider,
        directory::{self, MPP_DIRECTORY_URL},
    },
    mcp::client::McpPayment,
    protocol::methods::tempo::{CHAIN_ID, USDC, machine_token},
};
use nanocode::{
    ClaudeModel, Model,
    http::{self as layer, BoxFuture, Event, EventHook, Layer, Next},
    tools::mcp::{McpPaymentProvider, McpPendingPayment, McpServer},
};
use nanocode_cli::{ModelClients, Panel};
use reqwest::ResponseBuilderExt;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const OPENAI: &str = "https://openai.mpp.tempo.xyz/v1";
const ANTHROPIC: &str = "https://anthropic.mpp.tempo.xyz/v1/messages";
const MERCATOR: &str = "https://mercator.sh/mcp";
const TEMPO_SERVICES: &str = "*.mpp.tempo.xyz";
const MACH: u128 = 1_000_000;
const PREPAID_REQUESTS: u32 = 2;

#[derive(Clone)]
struct Mpp {
    wallet: TempoAccountsProvider,
    client: reqwest::Client,
    events: ClientEvents,
}

impl Mpp {
    fn new(wallet: &TempoAccountsProvider, service: &str, payments: &Payments) -> Result<Self> {
        let wallet = payments.sessions(wallet, &reqwest::Url::parse(service)?)?;
        Ok(Self {
            wallet,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            events: payments.events.clone(),
        })
    }

    async fn preconnect(self, url: String, model: &str) {
        let body = serde_json::json!({ "model": model }).to_string();
        let _ = self.wallet.preconnect(&self.client, &url, body).await;
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
                &self.events,
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
                    .with_events(StreamPayments {
                        mpp: self.clone(),
                        url: request.url,
                    }),
            )
        })
    }
}

struct StreamPayments {
    mpp: Mpp,
    url: String,
}

impl EventHook for StreamPayments {
    fn event(
        &mut self,
        event: Event<'_>,
    ) -> Option<BoxFuture<'static, std::result::Result<(), layer::Error>>> {
        let work =
            self.mpp
                .wallet
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

#[derive(Clone)]
struct Payments {
    ledger: Arc<Mutex<Ledger>>,
    events: ClientEvents,
    failure: Arc<OnceLock<String>>,
    _observers: Arc<[ClientEventSubscription; 2]>,
}

#[derive(Default)]
struct Ledger {
    machine: Address,
    models: Vec<String>,
    services: BTreeMap<String, Service>,
    sessions: HashMap<String, TempoSessionProvider>,
    listeners: Vec<UnboundedSender<String>>,
}

struct Service {
    model: bool,
    symbol: String,
    spent: u128,
    requests: u64,
    last: u128,
}

struct Paid<'a> {
    url: &'a reqwest::Url,
    challenge: &'a PaymentChallenge,
    amount: u128,
    funding: Option<String>,
    reference: &'a str,
}

impl Payments {
    fn new(machine: Address, models: Vec<String>) -> Self {
        let events = ClientEvents::default();
        let ledger = Arc::new(Mutex::new(Ledger {
            machine,
            models,
            ..Ledger::default()
        }));
        let (paid, deposited) = (ledger.clone(), ledger.clone());
        let observers = [
            events.on_payment_response(move |context| {
                if let Some(amount) = context.amount {
                    let receipt = context.receipt.as_deref();
                    paid.lock().unwrap().record(Paid {
                        url: &context.url,
                        challenge: &context.challenge,
                        amount,
                        funding: context.funding_currency.clone().or_else(|| {
                            Some(
                                receipt?
                                    .extensions
                                    .get("fundingCurrency")?
                                    .as_str()?
                                    .to_owned(),
                            )
                        }),
                        reference: receipt.map_or("", |receipt| receipt.reference.as_str()),
                    });
                }
                async {}
            }),
            events.on_deposit(move |context| {
                let mut ledger = deposited.lock().unwrap();
                let notice = deposit_notice(&context, ledger.machine);
                ledger.notify(notice);
                async {}
            }),
        ];
        Self {
            ledger,
            events,
            failure: Arc::default(),
            _observers: Arc::new(observers),
        }
    }

    fn sessions(
        &self,
        wallet: &TempoAccountsProvider,
        service: &reqwest::Url,
    ) -> Result<TempoAccountsProvider> {
        let wallet = wallet.clone().with_sqlite_session_store(service)?;
        let host = service.host_str().unwrap_or_default().to_owned();
        let session = wallet.session()?;
        self.ledger.lock().unwrap().sessions.insert(host, session);
        Ok(wallet)
    }

    fn pay_tools(
        &self,
        wallet: &TempoAccountsProvider,
        models: Vec<String>,
    ) -> Result<Vec<(&'static str, String)>> {
        let (payments, tools) = (self.clone(), wallet.clone());
        let proxy = ForwardProxy::bind(move |url| payments.sessions(&tools, url))?
            .bypass(models)
            .events(self.events.clone());
        let environment = proxy.environment();
        let (client, failed) = (reqwest::Client::new(), self.failure.clone());
        proxy.spawn(async move {
            let services = directory::service_urls(&client, MPP_DIRECTORY_URL)
                .await
                .unwrap_or_else(|error| {
                    let _ = failed.set(error.to_string());
                    Vec::new()
                });
            AcceptPaymentPolicy::Origins(
                services
                    .into_iter()
                    .chain([TEMPO_SERVICES.to_owned()])
                    .collect(),
            )
        })?;
        Ok(environment)
    }

    fn mcp(&self, wallet: &TempoAccountsProvider, url: &str) -> Result<McpServer> {
        Ok(McpServer::http(url).payment_provider(Arc::new(McpPayments {
            wallet: wallet.clone(),
            client: reqwest::Client::new(),
            url: url.parse()?,
            payments: self.clone(),
        })))
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
            return Some(format!("mpp.dev directory unavailable: {error}"));
        }
        let ledger = self.ledger.lock().unwrap();
        (!ledger.services.is_empty()).then(|| format!("spent {}", ledger.totals()))
    }

    fn panel(&self) -> Option<Panel> {
        let ledger = self.ledger.lock().unwrap();
        let mut services: Vec<_> = ledger.services.iter().collect();
        if services.is_empty() {
            return None;
        }
        services.sort_by_key(|(_, service)| std::cmp::Reverse(service.spent));
        let rows = services
            .into_iter()
            .map(|(host, service)| {
                let left = ledger
                    .sessions
                    .get(host)
                    .map(|session| session.spend().remaining)
                    .filter(|left| *left > 0)
                    .map_or_else(String::new, |left| format!("{} left", mach(left)));
                vec![
                    host.clone(),
                    if service.model { "model" } else { "tool" }.to_owned(),
                    format!("{} {}", mach(service.spent), service.symbol),
                    format!("{}×", service.requests),
                    format!("last {}", mach(service.last)),
                    left,
                ]
            })
            .collect();
        Some(Panel {
            title: format!("spend · {}", ledger.totals()),
            rows,
        })
    }
}

impl Ledger {
    fn record(&mut self, paid: Paid<'_>) -> Option<()> {
        let request = paid.challenge.request.decode::<Value>().ok()?;
        let currency = match paid.funding {
            Some(funding) => funding,
            None => request.get("currency")?.as_str()?.to_owned(),
        };
        let symbol = symbol(&currency, self.machine);
        let host = paid.url.host_str()?.to_owned();
        let model = self.models.contains(&host);
        let service = self.services.entry(host.clone()).or_insert(Service {
            model,
            symbol: symbol.clone(),
            spent: 0,
            requests: 0,
            last: 0,
        });
        service.spent += paid.amount;
        service.requests += 1;
        service.last = paid.amount;
        if !model {
            let (intent, label) = match paid.challenge.intent.as_str() {
                "session" => ("session", "channel"),
                intent => (intent, "tx"),
            };
            self.notify(format!(
                "paid {} {symbol} to {host} ({intent}, {label} {})",
                mach(paid.amount),
                paid.reference
            ));
        }
        Some(())
    }

    fn notify(&mut self, notice: String) {
        self.listeners
            .retain(|listener| listener.send(notice.clone()).is_ok());
    }

    fn totals(&self) -> String {
        let mut totals = BTreeMap::<&str, u128>::new();
        for service in self.services.values() {
            *totals.entry(&service.symbol).or_default() += service.spent;
        }
        totals
            .into_iter()
            .map(|(symbol, total)| format!("{} {symbol}", mach(total)))
            .collect::<Vec<_>>()
            .join(" + ")
    }
}

struct McpPayments {
    wallet: TempoAccountsProvider,
    client: reqwest::Client,
    url: reqwest::Url,
    payments: Payments,
}

#[async_trait]
impl McpPaymentProvider for McpPayments {
    async fn prepare(
        &self,
        payment_required: &Value,
    ) -> std::result::Result<Option<Box<dyn McpPendingPayment>>, String> {
        let Some(challenges) = mpp::mcp::extract_challenges_from_data(payment_required) else {
            return Ok(None);
        };
        let context = PaymentContext {
            url: self.url.clone(),
            headers: Default::default(),
            http_client: Some(self.client.clone()),
        };
        let pending = McpPayment::new(self.wallet.clone(), context)
            .prepare(&challenges)
            .await
            .map_err(|error| error.to_string())?;
        let Some(pending) = pending else {
            return Ok(None);
        };
        Ok(Some(Box::new(McpPending {
            credential: mpp::mcp::credential_value(pending.credential())
                .map_err(|error| error.to_string())?,
            pending,
            wallet: self.wallet.clone(),
            receipt: None,
            url: self.url.clone(),
            payments: self.payments.clone(),
        })))
    }
}

struct McpPending {
    wallet: TempoAccountsProvider,
    pending: PendingPayment<TempoAccountsProvider>,
    credential: Value,
    receipt: Option<Value>,
    url: reqwest::Url,
    payments: Payments,
}

#[async_trait]
impl McpPendingPayment for McpPending {
    fn credential(&self) -> &Value {
        &self.credential
    }

    fn receipt(&mut self, receipt: &Value) {
        self.receipt = Some(receipt.clone());
    }

    async fn commit(self: Box<Self>) -> std::result::Result<(), String> {
        let challenge = self.pending.challenge();
        let amount = challenge
            .request
            .decode::<Value>()
            .ok()
            .and_then(|request| request.get("amount")?.as_str()?.parse().ok());
        if let Some(amount) = amount {
            let receipt = self.receipt.as_ref();
            self.payments.ledger.lock().unwrap().record(Paid {
                url: &self.url,
                challenge,
                amount,
                funding: self.wallet.funding_currency(challenge),
                reference: receipt
                    .and_then(|receipt| receipt.get("reference")?.as_str())
                    .unwrap_or_default(),
            });
        }
        self.pending
            .commit()
            .await
            .map_err(|error| error.to_string())
    }

    async fn rollback(self: Box<Self>) -> std::result::Result<(), String> {
        self.pending
            .rollback()
            .await
            .map_err(|error| error.to_string())
    }
}

fn deposit_notice(context: &DepositContext, machine: Address) -> String {
    let symbol = context
        .currency
        .as_deref()
        .map(|currency| symbol(currency, machine))
        .unwrap_or_default();
    let host = context.url.host_str().unwrap_or_default();
    match context.opened {
        true => format!(
            "opened a {} {symbol} session with {host} (channel {})",
            mach(context.amount),
            context.channel_id
        ),
        false => format!(
            "topped up the {host} session by {} {symbol} (deposit {} {symbol})",
            mach(context.amount),
            mach(context.deposit)
        ),
    }
}

fn symbol(currency: &str, machine: Address) -> String {
    match currency.parse::<Address>() {
        Ok(currency) if currency == machine => "MACH".to_owned(),
        Ok(currency) if USDC.parse() == Ok(currency) => "USDC.e".to_owned(),
        _ => currency.to_owned(),
    }
}

fn amount(name: &str, default: u128) -> Result<u128> {
    match std::env::var_os(name) {
        Some(value) => Ok(value
            .to_str()
            .ok_or_else(|| format!("{name} is not UTF-8"))?
            .parse()?),
        None => Ok(default),
    }
}

fn mach(amount: u128) -> String {
    format!("{}.{:06}", amount / MACH, amount % MACH)
}

fn clients() -> Result<ModelClients> {
    nanocode::codex::transport::install_default_rustls_crypto_provider();
    let deposit = amount("NANOCODE_MPP_DEPOSIT", 2 * MACH)?;
    let tool_deposit = amount("NANOCODE_MPP_TOOL_DEPOSIT", MACH / 10)?;
    let machine = machine_token::token(CHAIN_ID).ok_or("MACH is not deployed on Tempo mainnet")?;
    let mut models = Vec::new();
    for url in [OPENAI, ANTHROPIC] {
        models.extend(reqwest::Url::parse(url)?.host_str().map(str::to_owned));
    }
    let payments = Payments::new(machine, models.clone());
    let wallet = TempoAccountsProvider::from_default_store()?
        .with_expected_chain_id(CHAIN_ID)
        .with_preferred_currency(machine)
        .with_session_prepaid_requests(PREPAID_REQUESTS)
        .with_session_events(payments.events.clone());
    let tool_wallet = wallet
        .clone()
        .with_session_default_deposit(tool_deposit)
        .with_session_top_up_amount(tool_deposit);
    let tool_environment = payments.pay_tools(&tool_wallet, models)?;
    let mercator = payments.mcp(&wallet, MERCATOR)?;
    let wallet = wallet
        .with_session_default_deposit(deposit)
        .with_session_top_up_amount(deposit);
    let openai = Mpp::new(&wallet, OPENAI, &payments)?;
    let claude = Mpp::new(&wallet, ANTHROPIC, &payments)?;
    let warm = (openai.clone(), claude.clone());
    let (notices, status) = (payments.clone(), payments.clone());
    Ok(ModelClients::new()
        .openai_endpoint(OPENAI, openai)
        .claude_endpoint(ANTHROPIC, claude)
        .warm_up(move || {
            let (openai, claude) = warm.clone();
            async move {
                futures_util::future::join(
                    openai.preconnect(format!("{OPENAI}/responses"), Model::Luna.as_str()),
                    claude.preconnect(ANTHROPIC.to_owned(), ClaudeModel::Haiku55.as_str()),
                )
                .await;
            }
        })
        .tool_environment(tool_environment)
        .mcp_server("mercator", mercator)
        .notices(move || notices.notices())
        .status(move || status.status())
        .panel(
            "/spend",
            "show or hide what models and tools have spent",
            move || payments.panel(),
        ))
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
