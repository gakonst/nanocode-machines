use mpp::{client::tempo::session::TempoSessionProvider, protocol::methods::tempo::SessionReceipt};
use nanocode::http::{self as layer, BoxFuture, Event, EventHook};
use serde::Deserialize;

pub(crate) struct SessionEvents {
    session: TempoSessionProvider,
    client: reqwest::Client,
    url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NeedVoucher {
    channel_id: String,
    required_cumulative: String,
    accepted_cumulative: String,
}

impl SessionEvents {
    pub(crate) const fn new(
        session: TempoSessionProvider,
        client: reqwest::Client,
        url: String,
    ) -> Self {
        Self {
            session,
            client,
            url,
        }
    }
}

impl EventHook for SessionEvents {
    fn event(&mut self, event: Event<'_>) -> Option<BoxFuture<'static, Result<(), layer::Error>>> {
        let session = self.session.clone();
        match event.name? {
            "payment-need-voucher" => {
                let parsed = serde_json::from_str::<NeedVoucher>(event.data);
                let (client, url) = (self.client.clone(), self.url.clone());
                Some(Box::pin(async move {
                    let voucher = parsed.map_err(payment)?;
                    acknowledge(&session, &voucher.channel_id, &voucher.accepted_cumulative)
                        .await?;
                    session
                        .send_voucher(
                            &client,
                            &url,
                            &voucher.channel_id,
                            amount(&voucher.required_cumulative)?,
                        )
                        .await
                        .map_err(payment)
                }))
            }
            "payment-receipt" => {
                let parsed = serde_json::from_str::<SessionReceipt>(event.data);
                Some(Box::pin(async move {
                    let receipt = parsed.map_err(payment)?;
                    acknowledge(&session, &receipt.channel_id, &receipt.accepted_cumulative).await
                }))
            }
            _ => None,
        }
    }
}

async fn acknowledge(
    session: &TempoSessionProvider,
    channel_id: &str,
    accepted: &str,
) -> Result<(), layer::Error> {
    session
        .acknowledge_accepted(channel_id, amount(accepted)?)
        .await
        .map(drop)
        .map_err(payment)
}

fn amount(value: &str) -> Result<u128, layer::Error> {
    value.parse().map_err(payment)
}

fn payment(error: impl std::fmt::Display) -> layer::Error {
    layer::Error::new(format!("MPP session stream: {error}"))
}
