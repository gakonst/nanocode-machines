use mpp::{client::TempoAccountsProvider, protocol::methods::tempo::CHAIN_ID};
use nanocode::{ClaudeModel, Model, Nanocode, Thinking};
use nanocode_machines::{
    ANTHROPIC_MESSAGES_URL, MppTransport, OPENAI_API_BASE_URL, tempo_sessions,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const HELP: &str = "cargo run --example agent -- DEPOSIT codex|claude [PROMPT...]
Runs one Nanocode agent whose model requests are paid with Tempo MPP sessions.
DEPOSIT is the session channel deposit and top-up, in payment-token base units (6 decimals).
The wallet is the Tempo Accounts store at $TEMPO_HOME/wallet/store.json, or ~/.tempo/wallet/store.json.";

fn sessions(
    wallet: &TempoAccountsProvider,
    service: &str,
    deposit: u128,
) -> Result<MppTransport<TempoAccountsProvider>> {
    let provider = tempo_sessions(wallet.clone(), &service.parse()?)?
        .with_session_default_deposit(deposit)
        .with_session_top_up_amount(deposit);
    Ok(MppTransport::new(provider)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (Some(deposit), Some(backend)) = (args.next(), args.next()) else {
        println!("{HELP}");
        return Ok(());
    };
    let deposit: u128 = deposit.parse()?;
    let prompt = args.collect::<Vec<_>>().join(" ");
    let prompt = if prompt.is_empty() {
        "Reply with one sentence about paying for inference per request.".to_owned()
    } else {
        prompt
    };
    let wallet = TempoAccountsProvider::from_default_store()?.with_expected_chain_id(CHAIN_ID);
    let runtime = Nanocode::builder()
        .openai(
            sessions(&wallet, OPENAI_API_BASE_URL, deposit)?
                .openai()
                .model(Model::Luna)
                .build()?,
        )
        .claude(sessions(&wallet, ANTHROPIC_MESSAGES_URL, deposit)?.claude())
        .without_cua()
        .runner_management(false)
        .thinking(Thinking::Low)
        .build_async()
        .await?;
    let outcome: Result<()> = async {
        let (agent, _events) = match backend.as_str() {
            "codex" => {
                runtime
                    .spawn()
                    .backend::<nanocode::Codex>()
                    .model(Model::Luna)
                    .build()
                    .await?
            }
            "claude" => {
                runtime
                    .spawn()
                    .backend::<nanocode::Claude>()
                    .model(ClaudeModel::Haiku55)
                    .build()
                    .await?
            }
            _ => return Err("the backend must be codex or claude".into()),
        };
        let result = agent.prompt(prompt).await?.result().await?;
        println!("{}", result.final_message());
        Ok(())
    }
    .await;
    let shutdown = runtime.shutdown().await;
    outcome?;
    shutdown?;
    Ok(())
}
