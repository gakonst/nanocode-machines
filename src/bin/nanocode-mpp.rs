use std::{
    collections::{BTreeMap, HashMap},
    process::ExitCode,
    sync::{Arc, Mutex, OnceLock},
};

use futures_util::Stream;
use mpp::{
    Address,
    client::{
        AcceptPaymentPolicy, ClientEventSubscription, ClientEvents, DEFAULT_MAX_PAYMENT_RETRIES,
        DepositContext, Exchange, FlowError, ForwardProxy, PaymentProvider, PaymentResponseContext,
        TempoAccountsProvider, TempoSessionProvider,
        directory::{self, MPP_DIRECTORY_URL},
    },
    protocol::methods::tempo::{CHAIN_ID, USDC, machine_token},
};
use nanocode::{
    ClaudeModel, Model,
    http::{self as layer, BoxFuture, Event, EventHook, Layer, Next},
};
use nanocode_cli::{ModelClients, Panel};
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
    client: reqwest::Client,
    events: ClientEvents,
}

impl Mpp {
    fn new(
        wallet: &TempoAccountsProvider,
        service: &str,
        deposit: u128,
        payments: &Payments,
    ) -> Result<Self> {
        let service = reqwest::Url::parse(service)?;
        let wallet = payments.sessions(wallet, &service, deposit)?;
        Ok(Self {
            wallet,
            client: reqwest::Client::builder()
                .no_proxy()
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

impl Payments {
    fn new(machine: Address, models: Vec<String>) -> Self {
        let events = ClientEvents::default();
        let ledger = Arc::new(Mutex::new(Ledger::default()));
        let (paid, deposited) = (ledger.clone(), ledger.clone());
        let observers = [
            events.on_payment_response(move |context| {
                record(&mut paid.lock().unwrap(), &context, machine, &models);
                async {}
            }),
            events.on_deposit(move |context| {
                notify(
                    &mut deposited.lock().unwrap(),
                    deposit_notice(&context, machine),
                );
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
        deposit: u128,
    ) -> Result<TempoAccountsProvider> {
        let wallet = wallet
            .clone()
            .with_sqlite_session_store(service)?
            .with_session_prepaid_requests(PREPAID_REQUESTS)
            .with_session_default_deposit(deposit)
            .with_session_top_up_amount(deposit)
            .with_session_events(self.events.clone());
        let host = service.host_str().unwrap_or_default().to_owned();
        let session = wallet.session()?;
        self.ledger.lock().unwrap().sessions.insert(host, session);
        Ok(wallet)
    }

    fn pay_tools(
        &self,
        wallet: &TempoAccountsProvider,
        deposit: u128,
        models: Vec<String>,
    ) -> Result<()> {
        let (payments, tools) = (self.clone(), wallet.clone());
        let proxy = ForwardProxy::bind(move |url| payments.sessions(&tools, url, deposit))?
            .bypass(models)
            .events(self.events.clone());
        for (name, value) in proxy.environment() {
            unsafe { std::env::set_var(name, value) };
        }
        let (client, failed) = (
            reqwest::Client::builder().no_proxy().build()?,
            self.failure.clone(),
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
        Ok(())
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
        (!ledger.services.is_empty()).then(|| format!("spent {}", totals(&ledger)))
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
            title: format!("spend · {}", totals(&ledger)),
            rows,
        })
    }
}

fn record(
    ledger: &mut Ledger,
    context: &PaymentResponseContext,
    machine: Address,
    models: &[String],
) -> Option<()> {
    let amount = context.amount?;
    let request = context
        .challenge
        .request
        .decode::<serde_json::Value>()
        .ok()?;
    let currency = context
        .receipt
        .as_ref()
        .and_then(|receipt| receipt.extensions.get("fundingCurrency"))
        .or_else(|| request.get("currency"))?
        .as_str()?;
    let symbol = symbol(currency, machine);
    let host = context.url.host_str()?.to_owned();
    let model = models.contains(&host);
    let service = ledger.services.entry(host.clone()).or_insert(Service {
        model,
        symbol: symbol.clone(),
        spent: 0,
        requests: 0,
        last: 0,
    });
    service.spent += amount;
    service.requests += 1;
    service.last = amount;
    if !model {
        let reference = context
            .receipt
            .as_ref()
            .map_or("", |receipt| receipt.reference.as_str());
        let (intent, label) = match context.challenge.intent.as_str() {
            "session" => ("session", "channel"),
            intent => (intent, "tx"),
        };
        notify(
            ledger,
            format!(
                "paid {} {symbol} to {host} ({intent}, {label} {reference})",
                mach(amount)
            ),
        );
    }
    Some(())
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

fn notify(ledger: &mut Ledger, notice: String) {
    ledger
        .listeners
        .retain(|listener| listener.send(notice.clone()).is_ok());
}

fn totals(ledger: &Ledger) -> String {
    let mut totals = BTreeMap::<&str, u128>::new();
    for service in ledger.services.values() {
        *totals.entry(&service.symbol).or_default() += service.spent;
    }
    totals
        .into_iter()
        .map(|(symbol, total)| format!("{} {symbol}", mach(total)))
        .collect::<Vec<_>>()
        .join(" + ")
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
    let wallet = TempoAccountsProvider::from_default_store()?
        .with_expected_chain_id(CHAIN_ID)
        .with_preferred_currency(machine);
    let mut models = Vec::new();
    for url in [OPENAI, ANTHROPIC] {
        models.extend(reqwest::Url::parse(url)?.host_str().map(str::to_owned));
    }
    let payments = Payments::new(machine, models.clone());
    payments.pay_tools(&wallet, tool_deposit, models)?;
    let openai = Mpp::new(&wallet, OPENAI, deposit, &payments)?;
    let claude = Mpp::new(&wallet, ANTHROPIC, deposit, &payments)?;
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
