use crate::id::CommandName;

/// Trait implemented by command enums. The derive macro generates the
/// `command_name()` discriminator from variant names (kebab-case, prefixed).
pub trait Command: Send + 'static {
    fn command_name(&self) -> CommandName;
}
