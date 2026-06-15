mod capability_attr;
mod handler_attr;
mod loader_attr;
mod service_macro;

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input};

/// Annotates an environment capability trait and emits adapter glue.
///
/// Capability methods are ordinary domain dependencies. Async methods are
/// shaped as `impl Future + Send` so they can be used from service handlers.
#[proc_macro_attribute]
pub fn capability(args: TokenStream, input: TokenStream) -> TokenStream {
    capability_attr::expand(args, input)
}

/// Annotates a command handler function and emits a companion spec struct.
///
/// # Syntax
///
/// ```text
/// #[wee_events::handler(command = MyCommand, requires(HasSomeTrait))]
/// pub async fn my_handler<R: HasSomeTrait>(
///     env: &R,
///     entity: &wee_events::Entity<MyState>,
///     cmd: MyCommand,
/// ) -> wee_events::Result<()> { ... }
/// ```
///
/// Emits:
/// - The original function, unchanged
/// - `pub struct my_handler_Spec` implementing `wee_events::HandlerSpec`
/// - `pub trait __my_handler_Requires` with a blanket impl
#[proc_macro_attribute]
pub fn handler(args: TokenStream, input: TokenStream) -> TokenStream {
    handler_attr::expand(args, input)
}

/// Annotates a state loader function and emits a companion spec struct.
///
/// # Syntax
///
/// ```text
/// #[wee_events::loader(requires(HasStore))]
/// pub async fn load_my_state<R: HasStore>(
///     env: &R,
///     id: &wee_events::AggregateId,
/// ) -> wee_events::Result<wee_events::Entity<MyState>> { ... }
/// ```
///
/// Emits:
/// - The original function, unchanged
/// - `pub struct load_my_state_Spec` implementing `wee_events::LoaderSpec`
/// - `pub trait __load_my_state_Requires` with a blanket impl
#[proc_macro_attribute]
pub fn loader(args: TokenStream, input: TokenStream) -> TokenStream {
    loader_attr::expand(args, input)
}

/// Derives the `Command` trait for an enum.
///
/// Each variant generates a `CommandName` from the enum name and variant name,
/// both converted to kebab-case and joined with `:`.
///
/// Use `#[command(prefix = "custom")]` on the enum to override the prefix
/// (defaults to the enum name in kebab-case with "-command" suffix stripped).
#[proc_macro_derive(Command, attributes(command))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let prefix = match extract_prefix(&input, "command") {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };

    let Data::Enum(data_enum) = &input.data else {
        return syn::Error::new_spanned(name, "Command can only be derived for enums")
            .to_compile_error()
            .into();
    };

    let arms = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let kebab = variant_name.to_string().to_case(Case::Kebab);
        let command_name = format!("{prefix}:{kebab}");

        match &variant.fields {
            Fields::Unit => quote! {
                Self::#variant_name => wee_events::CommandName::new_const(#command_name),
            },
            Fields::Named(_) => quote! {
                Self::#variant_name { .. } => wee_events::CommandName::new_const(#command_name),
            },
            Fields::Unnamed(_) => quote! {
                Self::#variant_name(..) => wee_events::CommandName::new_const(#command_name),
            },
        }
    });

    // For enums, NAME is the prefix — per-variant dispatch uses command_name(&self).
    let expanded = quote! {
        impl wee_events::Command for #name {
            const NAME: &'static str = #prefix;

            fn command_name(&self) -> wee_events::CommandName {
                match self {
                    #(#arms)*
                }
            }
        }
    };

    expanded.into()
}

/// Derives the `DomainEvent` trait for an enum.
///
/// Each variant generates an `EventType` from the enum name and variant name,
/// both converted to kebab-case and joined with `:`.
///
/// Use `#[domain_event(prefix = "custom")]` on the enum to override the prefix
/// (defaults to the enum name in kebab-case with "-event" suffix stripped).
///
/// Also generates associated constants for each variant's `EventType`, named
/// in `SCREAMING_SNAKE_CASE` (e.g., `CampaignEvent::CREW_INJURED`).
#[proc_macro_derive(DomainEvent, attributes(domain_event))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let prefix = match extract_prefix(&input, "event") {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };

    let Data::Enum(data_enum) = &input.data else {
        return syn::Error::new_spanned(name, "DomainEvent can only be derived for enums")
            .to_compile_error()
            .into();
    };

    let match_arms = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let kebab = variant_name.to_string().to_case(Case::Kebab);
        let event_type = format!("{prefix}:{kebab}");

        match &variant.fields {
            Fields::Unit => quote! {
                Self::#variant_name => wee_events::EventType::new_const(#event_type),
            },
            Fields::Named(_) => quote! {
                Self::#variant_name { .. } => wee_events::EventType::new_const(#event_type),
            },
            Fields::Unnamed(_) => quote! {
                Self::#variant_name(..) => wee_events::EventType::new_const(#event_type),
            },
        }
    });

    let consts = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let screaming = variant_name.to_string().to_case(Case::UpperSnake);
        let const_name = syn::Ident::new(&screaming, variant_name.span());
        let kebab = variant_name.to_string().to_case(Case::Kebab);
        let event_type = format!("{prefix}:{kebab}");

        quote! {
            pub const #const_name: &'static str = #event_type;
        }
    });

    let expanded = quote! {
        impl wee_events::DomainEvent for #name {
            fn event_type(&self) -> wee_events::EventType {
                match self {
                    #(#match_arms)*
                }
            }
        }

        impl #name {
            #(#consts)*
        }
    };

    expanded.into()
}

/// Extracts the `prefix` attribute value, or derives it from the enum name.
///
/// `attr_name` is the attribute identifier to look for (e.g. `"command"` or
/// `"event"`). The auto-derived prefix strips `"-{attr_name}"` from the
/// kebab-case enum name.
///
/// Returns an error if the attribute is present but malformed.
fn extract_prefix(input: &DeriveInput, attr_name: &str) -> syn::Result<String> {
    // Determine the outer attribute name that marks this derive's configuration.
    // DomainEvent uses `#[domain_event(...)]`, Command uses `#[command(...)]`.
    let outer_attr = if attr_name == "event" {
        "domain_event"
    } else {
        attr_name
    };

    for attr in &input.attrs {
        if attr.path().is_ident(outer_attr) {
            // Attribute is present — parse it strictly; malformed content is an error.
            let args = attr.parse_args::<PrefixArgs>().map_err(|e| {
                syn::Error::new_spanned(attr.path(), format!("invalid {outer_attr} attribute: {e}"))
            })?;
            return Ok(args.prefix);
        }
    }

    let raw = input.ident.to_string().to_case(Case::Kebab);
    // Strip the suffix that matches the derive name (e.g., "-command", "-event")
    Ok(raw
        .strip_suffix(&format!("-{attr_name}"))
        .unwrap_or(&raw)
        .to_string())
}

/// Generates a service environment trait and namespace struct from
/// `#[handler]`/`#[loader]`-annotated functions.
///
/// The `handlers` list contains bare function names; command types and
/// capability requirements are derived from the companion `HandlerSpec`/
/// `LoaderSpec` items emitted by the attribute macros.
///
/// # Syntax
///
/// ```text
/// wee_events::service! {
///     pub CounterService for Counter {
///         loader: load_counter,
///         handlers: [increment, adjust],
///     }
/// }
/// ```
///
/// # Generated items
///
/// - `pub trait CounterServiceEnv` — union of all capability requirements;
///   any type satisfying the constituent traits satisfies this trait via
///   a blanket impl
/// - `pub struct CounterService` — service namespace (zero-size)
/// - `wee_events::create(CounterService).with_store(store).with_env(services).build()`
///   — builds a typed in-process service
/// - `impl DispatchCommand<C>` + `impl Handles<C>` on the generated in-process
///   service for each registered command
/// - Restate registration helpers when used with `wee-events-restate`
#[proc_macro]
pub fn service(input: TokenStream) -> TokenStream {
    service_macro::expand(input)
}

struct PrefixArgs {
    prefix: String,
}

impl syn::parse::Parse for PrefixArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ident: syn::Ident = input.parse()?;
        if ident != "prefix" {
            return Err(syn::Error::new_spanned(ident, "expected `prefix`"));
        }
        let _: syn::Token![=] = input.parse()?;
        let lit: LitStr = input.parse()?;
        if !input.is_empty() {
            return Err(input.error("unexpected tokens after prefix value"));
        }
        Ok(PrefixArgs {
            prefix: lit.value(),
        })
    }
}
