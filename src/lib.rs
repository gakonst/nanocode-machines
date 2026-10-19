mod events;

use std::sync::Arc;

use http::{HeaderMap, HeaderValue, header::CONTENT_TYPE, header::WWW_AUTHENTICATE};
use mpp::client::{
    AcceptPaymentPolicy, ClientEvents, DEFAULT_MAX_PAYMENT_RETRIES, Exchange, FlowError,
    PaymentProvider, TempoAccountsProvider,
    tempo::session::{
        TempoSessionProvider,
        store::{ChannelStoreError, SqliteChannelStore, SqliteChannelStoreOptions},
    },
};
use nanocode::{
    ClaudeClient, OpenAi,
    codex::{auth::OpenAiAuth, transport::ResponsesTransport},
    http::{self as layer, BoxFuture, Layer, Next},
};
use reqwest::ResponseBuilderExt;

pub use mpp;

pub const OPENAI_API_BASE_URL: &str = "https://openai.mpp.tempo.xyz/v1";
pub const ANTHROPIC_MESSAGES_URL: &str = "https://anthropic.mpp.tempo.xyz/v1/messages";
pub const PREPAID_REQUESTS: u32 = 2;

#[derive(Clone)]
pub struct MppLayer<P> {
    provider: P,
    client: reqwest::Client,
    policy: AcceptPaymentPolicy,
    events: ClientEvents,
    session: Option<TempoSessionProvider>,
}

impl<P: PaymentProvider + 'static> MppLayer<P> {
    pub fn new(provider: P) -> Result<Self, reqwest::Error> {
        nanocode::codex::transport::install_default_rustls_crypto_provider();
        Ok(Self {
            provider,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            policy: AcceptPaymentPolicy::Always,
            events: ClientEvents::default(),
            session: None,
        })
    }

    #[must_use]
    pub fn events(mut self, events: ClientEvents) -> Self {
        self.events = events;
        self
    }

    #[must_use]
    pub fn session(mut self, session: TempoSessionProvider) -> Self {
        self.session = Some(session);
        self
    }

    #[must_use]
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    #[must_use]
    pub fn policy(mut self, policy: AcceptPaymentPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub const fn provider(&self) -> &P {
        &self.provider
    }

    pub fn openai(&self) -> nanocode::codex::OpenAiBuilder {
        OpenAi::builder(OpenAiAuth::gateway_managed())
            .api_base_url(OPENAI_API_BASE_URL)
            .transport(ResponsesTransport::Https)
            .http_layer(self.clone())
    }

    pub fn claude(&self) -> ClaudeClient {
        ClaudeClient::with_auth_headers(
            self.client.clone(),
            ANTHROPIC_MESSAGES_URL,
            HeaderMap::new(),
        )
        .with_http_layer(self.clone())
    }

    pub async fn preconnect(&self, endpoint: &str, model: &str) -> Result<(), String> {
        let response = self
            .client
            .post(endpoint)
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::json!({ "model": model }).to_string())
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let challenges: Vec<_> = mpp::protocol::core::parse_www_authenticate_all_bytes(
            response
                .headers()
                .get_all(WWW_AUTHENTICATE)
                .iter()
                .map(HeaderValue::as_bytes),
        )
        .into_iter()
        .filter_map(Result::ok)
        .filter(|challenge| {
            self.provider
                .supports(challenge.method.as_str(), challenge.intent.as_str())
        })
        .collect();
        let candidates: Vec<_> = challenges.iter().collect();
        let (Some(session), Some(challenge)) =
            (&self.session, self.provider.select_challenge(&candidates))
        else {
            return Ok(());
        };
        if challenge.intent.as_str() != "session" {
            return Ok(());
        }
        session
            .restore(challenge)
            .await
            .map(drop)
            .map_err(|error| error.to_string())
    }
}

pub fn tempo_sessions(
    wallet: TempoAccountsProvider,
    service: &reqwest::Url,
) -> Result<TempoAccountsProvider, ChannelStoreError> {
    nanocode::codex::transport::install_default_rustls_crypto_provider();
    let store = SqliteChannelStore::open(SqliteChannelStoreOptions {
        namespace: service.origin().ascii_serialization(),
        path: None,
        request_url: Some(service.to_string()),
    })?;
    Ok(wallet
        .with_session_store(Arc::new(store))
        .with_session_prepaid_requests(PREPAID_REQUESTS))
}

impl<P: PaymentProvider + 'static> Layer for MppLayer<P> {
    fn send<'a>(
        &'a self,
        request: layer::Request,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<layer::Response, layer::Error>> {
        Box::pin(async move {
            let url = reqwest::Url::parse(&request.url)
                .map_err(|error| layer::Error::new(error.to_string()).unsent(true))?;
            let mut paid = reqwest::Request::new(request.method, url.clone());
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
                &self.provider,
                &self.policy,
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
                .take()
                .ok_or_else(|| layer::Error::new("the payment flow returned no response"))?;
            let response =
                layer::Response::new(response.status(), response.headers().clone(), body);
            Ok(match &self.session {
                Some(session) => response.with_events(events::SessionEvents::new(
                    session.clone(),
                    self.client.clone(),
                    url.to_string(),
                )),
                None => response,
            })
        })
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

    async fn send(&mut self, request: reqwest::Request) -> Result<reqwest::Response, layer::Error> {
        let url = request.url().clone();
        let sent = layer::Request {
            method: request.method().clone(),
            url: url.to_string(),
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
            .url(url)
            .body(reqwest::Body::from(Vec::new()))
            .map_err(|error| layer::Error::new(error.to_string()))?;
        *response.headers_mut() = headers;
        Ok(reqwest::Response::from(response))
    }
}
