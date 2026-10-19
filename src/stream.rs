use memchr::memmem;
use mpp::{client::tempo::session::TempoSessionProvider, protocol::methods::tempo::SessionReceipt};
use serde::Deserialize;

pub(crate) struct PaidBody {
    response: reqwest::Response,
    events: Option<SessionEvents>,
}

pub(crate) struct BodyError {
    pub(crate) detail: String,
    pub(crate) retryable: bool,
    pub(crate) timeout: bool,
}

struct SessionEvents {
    session: TempoSessionProvider,
    client: reqwest::Client,
    url: String,
    buffer: Vec<u8>,
    ended: bool,
}

enum Event {
    Payment,
    Message,
    Other,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NeedVoucher {
    channel_id: String,
    required_cumulative: String,
    accepted_cumulative: String,
}

impl PaidBody {
    pub(crate) fn new(
        response: reqwest::Response,
        session: Option<&TempoSessionProvider>,
        client: &reqwest::Client,
        url: &str,
    ) -> Self {
        Self {
            response,
            events: session.map(|session| SessionEvents {
                session: session.clone(),
                client: client.clone(),
                url: url.to_owned(),
                buffer: Vec::new(),
                ended: false,
            }),
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Vec<u8>>, BodyError> {
        let Some(events) = self.events.as_mut() else {
            return self
                .response
                .chunk()
                .await
                .map(|chunk| chunk.map(Vec::from))
                .map_err(BodyError::http);
        };
        loop {
            let mut output = Vec::new();
            let mut start = 0;
            while let Some(length) = event_length(&events.buffer[start..]) {
                let event = &events.buffer[start..start + length];
                match events.handle(event).await? {
                    Event::Payment => {}
                    Event::Message => output.extend(
                        event
                            .split_inclusive(|byte| *byte == b'\n')
                            .filter(|line| !line.starts_with(b"event:"))
                            .flatten(),
                    ),
                    Event::Other => output.extend_from_slice(event),
                }
                start += length;
            }
            events.buffer.drain(..start);
            if !output.is_empty() {
                return Ok(Some(output));
            }
            if events.ended {
                return Ok((!events.buffer.is_empty()).then(|| std::mem::take(&mut events.buffer)));
            }
            match self.response.chunk().await.map_err(BodyError::http)? {
                Some(chunk) => events.buffer.extend_from_slice(&chunk),
                None => events.ended = true,
            }
        }
    }
}

impl SessionEvents {
    async fn handle(&self, event: &[u8]) -> Result<Event, BodyError> {
        let Ok(event) = std::str::from_utf8(event) else {
            return Ok(Event::Other);
        };
        let mut name = None;
        let mut data = String::new();
        for line in event.lines() {
            if let Some(value) = line.strip_prefix("event:") {
                name = Some(value.trim());
            } else if let Some(value) = line.strip_prefix("data:") {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(value.strip_prefix(' ').unwrap_or(value));
            }
        }
        match name {
            Some("payment-need-voucher") => {
                let event: NeedVoucher = serde_json::from_str(&data).map_err(BodyError::payment)?;
                self.acknowledge(&event.channel_id, &event.accepted_cumulative)
                    .await?;
                let required = amount(&event.required_cumulative)?;
                self.session
                    .send_voucher(&self.client, &self.url, &event.channel_id, required)
                    .await
                    .map_err(BodyError::payment)?;
                Ok(Event::Payment)
            }
            Some("payment-receipt") => {
                let receipt: SessionReceipt =
                    serde_json::from_str(&data).map_err(BodyError::payment)?;
                self.acknowledge(&receipt.channel_id, &receipt.accepted_cumulative)
                    .await?;
                Ok(Event::Payment)
            }
            Some("message") => Ok(Event::Message),
            _ => Ok(Event::Other),
        }
    }

    async fn acknowledge(&self, channel_id: &str, accepted: &str) -> Result<(), BodyError> {
        self.session
            .acknowledge_accepted(channel_id, amount(accepted)?)
            .await
            .map(drop)
            .map_err(BodyError::payment)
    }
}

impl BodyError {
    fn http(error: reqwest::Error) -> Self {
        Self {
            retryable: error.is_body() || error.is_request(),
            timeout: error.is_timeout(),
            detail: error.to_string(),
        }
    }

    fn payment(error: impl std::fmt::Display) -> Self {
        Self {
            detail: format!("MPP session stream: {error}"),
            retryable: false,
            timeout: false,
        }
    }
}

fn amount(value: &str) -> Result<u128, BodyError> {
    value.parse().map_err(BodyError::payment)
}

fn event_length(buffer: &[u8]) -> Option<usize> {
    let lf = memmem::find(buffer, b"\n\n").map(|index| index + 2);
    let crlf = memmem::find(buffer, b"\r\n\r\n").map(|index| index + 4);
    match (lf, crlf) {
        (Some(lf), Some(crlf)) => Some(lf.min(crlf)),
        (lf, crlf) => lf.or(crlf),
    }
}
