mod stream;

use std::{sync::Arc, time::Duration};

use http::{
    HeaderMap, HeaderName, HeaderValue,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER},
};
use mpp::client::{
    AcceptPaymentPolicy, ClientEvents, DEFAULT_MAX_PAYMENT_RETRIES, Fetch, PaymentProvider,
    TempoAccountsProvider,
    tempo::session::{
        TempoSessionProvider,
        store::{ChannelStoreError, SqliteChannelStore, SqliteChannelStoreOptions},
    },
};
use nanocode::{
    ClaudeClient, OpenAi,
    claude::transport::{HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport},
    codex::{
        auth::OpenAiAuth,
        transport::{
            ResponsesTransport, RetryAfter,
            host::{
                ConnectedHost, HostConnectRequest, HostConnectionMetadata, HostError, HostFuture,
                HostHttpBody, HostHttpResponse, HostTransport,
            },
        },
    },
};
use stream::{BodyError, PaidBody};

pub use mpp;

pub const OPENAI_API_BASE_URL: &str = "https://openai.mpp.tempo.xyz/v1";
pub const ANTHROPIC_MESSAGES_URL: &str = "https://anthropic.mpp.tempo.xyz/v1/messages";

const TURN_STATE_HEADER: &str = "x-codex-turn-state";

#[derive(Clone)]
pub struct MppTransport<P> {
    provider: P,
    client: reqwest::Client,
    policy: AcceptPaymentPolicy,
    events: ClientEvents,
    session: Option<TempoSessionProvider>,
}

impl<P: PaymentProvider + 'static> MppTransport<P> {
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
            .host_transport(self.clone())
    }

    pub fn claude(&self) -> ClaudeClient {
        ClaudeClient::with_auth_headers(
            self.client.clone(),
            ANTHROPIC_MESSAGES_URL,
            HeaderMap::new(),
        )
        .with_transport(self.clone())
    }

    async fn send(
        &self,
        endpoint: &str,
        headers: HeaderMap,
        body: String,
    ) -> Result<reqwest::Response, mpp::client::HttpError> {
        self.client
            .post(endpoint)
            .headers(headers)
            .body(body)
            .send_with_payment_options_max_retries(
                &self.provider,
                &self.policy,
                self.events.clone(),
                DEFAULT_MAX_PAYMENT_RETRIES,
            )
            .await
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
    Ok(wallet.with_session_store(Arc::new(store)))
}

impl<P: PaymentProvider + 'static> HostTransport for MppTransport<P> {
    fn connect<'a>(
        &'a self,
        _request: HostConnectRequest<'a>,
    ) -> HostFuture<'a, Result<ConnectedHost, HostError>> {
        Box::pin(async { Err(HostError::new("MPP serves the Responses API over HTTPS")) })
    }

    fn http<'a>(
        &'a self,
        request: HostConnectRequest<'a>,
        body: &'a str,
    ) -> HostFuture<'a, Result<HostHttpResponse, HostError>> {
        Box::pin(async move {
            let headers = responses_headers(&request).map_err(HostError::new)?;
            let response = self
                .send(request.endpoint(), headers, body.to_owned())
                .await
                .map_err(payment_host_error)?;
            let status = response.status().as_u16();
            if !response.status().is_success() {
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(RetryAfter::from_header);
                let body = response.text().await.unwrap_or_default();
                return Err(HostError::handshake_rejected(status, body, retry_after));
            }
            let headers = response.headers();
            let mut metadata = HostConnectionMetadata::new(status)
                .with_reasoning_included(headers.contains_key("x-reasoning-included"));
            if let Some(request_id) = header_text(headers, "x-request-id") {
                metadata = metadata.with_request_id(request_id);
            }
            if let Some(turn_state) = header_text(headers, TURN_STATE_HEADER) {
                metadata = metadata.with_turn_state(turn_state);
            }
            Ok(HostHttpResponse {
                body: Box::new(PaidBody::new(
                    response,
                    self.session.as_ref(),
                    &self.client,
                    request.endpoint(),
                )),
                metadata,
            })
        })
    }

    fn sleep<'a>(&'a self, _session_id: &'a str, duration: Duration) -> HostFuture<'a, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

impl<P: PaymentProvider + 'static> HttpTransport for MppTransport<P> {
    fn post(&self, request: HttpRequest) -> HttpFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let mut response = self
                .send(&request.endpoint, request.headers, request.body)
                .await
                .map_err(payment_http_error)?;
            let status = response.status();
            let headers = std::mem::take(response.headers_mut());
            let body = PaidBody::new(
                response,
                self.session.as_ref(),
                &self.client,
                &request.endpoint,
            );
            Ok(HttpResponse::new(
                status,
                headers,
                Box::pin(futures_util::stream::unfold(
                    Some(body),
                    |body| async move {
                        let mut body = body?;
                        match body.next().await {
                            Ok(Some(chunk)) => Some((Ok(chunk), Some(body))),
                            Ok(None) => None,
                            Err(error) => Some((Err(error.into()), None)),
                        }
                    },
                )),
            ))
        })
    }
}

impl HostHttpBody for PaidBody {
    fn next(&mut self) -> HostFuture<'_, Result<Option<Vec<u8>>, HostError>> {
        Box::pin(async move { Self::next(self).await.map_err(HostError::from) })
    }
}

impl From<BodyError> for HostError {
    fn from(error: BodyError) -> Self {
        Self::new(error.detail).with_reconnectable(error.retryable)
    }
}

impl From<BodyError> for HttpError {
    fn from(error: BodyError) -> Self {
        Self::Transport {
            detail: error.detail,
            retryable: error.retryable,
            timeout: error.timeout,
        }
    }
}

fn responses_headers(request: &HostConnectRequest<'_>) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    headers.insert("session-id", header_value(request.session_id())?);
    headers.insert("x-client-request-id", header_value(request.thread_id())?);
    if request.sends_bearer_token() {
        let mut value = header_value(&format!("Bearer {}", request.bearer_token()))?;
        value.set_sensitive(true);
        headers.insert(AUTHORIZATION, value);
    }
    if let Some(turn_state) = request.turn_state() {
        headers.insert(TURN_STATE_HEADER, header_value(turn_state)?);
    }
    for header in request.headers() {
        let name = HeaderName::from_bytes(header.name().as_bytes())
            .map_err(|error| format!("invalid Responses header name: {error}"))?;
        headers.insert(name, header_value(header.value())?);
    }
    Ok(headers)
}

fn header_value(value: &str) -> Result<HeaderValue, String> {
    HeaderValue::from_str(value).map_err(|error| format!("invalid Responses header value: {error}"))
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

const fn reconnectable(error: &mpp::client::HttpError) -> bool {
    matches!(error, mpp::client::HttpError::Request(_))
}

fn payment_host_error(error: mpp::client::HttpError) -> HostError {
    let reconnectable = reconnectable(&error);
    HostError::new(error.to_string()).with_reconnectable(reconnectable)
}

fn payment_http_error(error: mpp::client::HttpError) -> HttpError {
    HttpError::Transport {
        retryable: reconnectable(&error),
        timeout: matches!(&error, mpp::client::HttpError::Request(error) if error.is_timeout()),
        detail: error.to_string(),
    }
}
