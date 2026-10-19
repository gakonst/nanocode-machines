use mpp::{
    Address,
    client::TempoAccountsProvider,
    protocol::methods::tempo::{CHAIN_ID, machine_token},
};
use nanocode::{ClaudeModel, Model, Nanocode, Thinking};
use nanocode_machines::{ANTHROPIC_MESSAGES_URL, MppLayer, OPENAI_API_BASE_URL, tempo_sessions};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const HELP: &str = "cargo run --example agent -- [--prefer TOKEN] DEPOSIT codex|claude [PROMPT...]
Runs one Nanocode agent whose model requests are paid over MPP from a Tempo Accounts access key.
Offers funded from TOKEN win: a TIP-1034 session denominated in it, then, for MACH, a MACH-funded
charge. TOKEN defaults to MACH. Otherwise the service's first offer is paid, sessions before charges.
DEPOSIT is the session channel deposit and top-up, in payment-token base units (6 decimals).
The wallet is the Tempo Accounts store at $TEMPO_HOME/wallet/store.json, or ~/.tempo/wallet/store.json.";

fn layer(
    wallet: &TempoAccountsProvider,
    service: &str,
    deposit: u128,
) -> Result<MppLayer<TempoAccountsProvider>> {
    let provider = tempo_sessions(wallet.clone(), &service.parse()?)?
        .with_session_default_deposit(deposit)
        .with_session_top_up_amount(deposit);
    let session = provider.session()?;
    Ok(MppLayer::new(provider)?.session(session))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1).peekable();
    let preferred = if args.peek().map(String::as_str) == Some("--prefer") {
        args.next();
        args.next()
            .ok_or("--prefer requires a token address")?
            .parse::<Address>()?
    } else {
        machine_token::token(CHAIN_ID).ok_or("MACH is not deployed on Tempo mainnet")?
    };
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
    let wallet = TempoAccountsProvider::from_default_store()?
        .with_expected_chain_id(CHAIN_ID)
        .with_preferred_currency(preferred);
    let openai = layer(&wallet, OPENAI_API_BASE_URL, deposit)?;
    let claude = layer(&wallet, ANTHROPIC_MESSAGES_URL, deposit)?;
    let responses = format!("{OPENAI_API_BASE_URL}/responses");
    let preconnect = async {
        match backend.as_str() {
            "codex" => openai.preconnect(&responses, Model::Luna.as_str()).await,
            _ => {
                claude
                    .preconnect(ANTHROPIC_MESSAGES_URL, ClaudeModel::Haiku55.as_str())
                    .await
            }
        }
    };
    let build = Nanocode::builder()
        .openai(openai.openai().model(Model::Luna).build()?)
        .claude(claude.claude())
        .without_cua()
        .runner_management(false)
        .thinking(Thinking::Low)
        .build_async();
    let (preconnected, runtime) = tokio::join!(preconnect, build);
    preconnected?;
    let runtime = runtime?;
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
