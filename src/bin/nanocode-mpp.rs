use std::{env::VarError, process::ExitCode};

use mpp::{
    Address,
    client::TempoAccountsProvider,
    protocol::methods::tempo::{CHAIN_ID, machine_token},
};
use nanocode::{ClaudeModel, Model};
use nanocode_cli::ModelClients;
use nanocode_machines::{ANTHROPIC_MESSAGES_URL, MppLayer, OPENAI_API_BASE_URL, tempo_sessions};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_DEPOSIT: u128 = 2_000_000;

fn env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("{name}: {error}").into()),
    }
}

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

fn clients() -> Result<ModelClients> {
    let preferred = match env("NANOCODE_MPP_TOKEN")? {
        Some(token) => token.parse::<Address>()?,
        None => machine_token::token(CHAIN_ID).ok_or("MACH is not deployed on Tempo mainnet")?,
    };
    let deposit = match env("NANOCODE_MPP_DEPOSIT")? {
        Some(deposit) => deposit.parse()?,
        None => DEFAULT_DEPOSIT,
    };
    let wallet = TempoAccountsProvider::from_default_store()?
        .with_expected_chain_id(CHAIN_ID)
        .with_preferred_currency(preferred);
    let openai = layer(&wallet, OPENAI_API_BASE_URL, deposit)?;
    let claude = layer(&wallet, ANTHROPIC_MESSAGES_URL, deposit)?;
    let warm = (openai.clone(), claude.clone());
    Ok(ModelClients::new()
        .openai(move || openai.openai())
        .claude(move || claude.claude())
        .warm_up(move || {
            let (openai, claude) = warm.clone();
            async move {
                let responses = format!("{OPENAI_API_BASE_URL}/responses");
                let _ = futures_util::future::join(
                    openai.preconnect(&responses, Model::Luna.as_str()),
                    claude.preconnect(ANTHROPIC_MESSAGES_URL, ClaudeModel::Haiku55.as_str()),
                )
                .await;
            }
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
