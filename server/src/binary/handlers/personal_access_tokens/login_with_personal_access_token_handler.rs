use crate::binary::mapper;
use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_login_with_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: LoginWithPersonalAccessToken,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let user = system
        .login_with_personal_access_token(&command.token, Some(session))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to login with personal access token: {}, session: {session}",
                command.token
            )
        })?;
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(&identity_info).await?;
    Ok(())
}
