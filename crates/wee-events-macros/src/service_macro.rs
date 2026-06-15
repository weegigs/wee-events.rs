use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Ident, LitStr, Path, Token, Visibility, braced, bracketed,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

// ---------------------------------------------------------------------------
// Custom keywords
// ---------------------------------------------------------------------------

syn::custom_keyword!(loader);
syn::custom_keyword!(handlers);
syn::custom_keyword!(effects);
syn::custom_keyword!(all);
syn::custom_keyword!(predicate);

// ---------------------------------------------------------------------------
// AST types
// ---------------------------------------------------------------------------

/// A single handler entry: `<fn_path>` optionally followed by `as "wire_name"`.
struct HandlerEntry {
    fn_path: Path,
    #[allow(dead_code)]
    wire_name: Option<LitStr>,
}

impl Parse for HandlerEntry {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let fn_path: Path = input.parse()?;
        let wire_name: Option<LitStr> = if input.peek(Token![as]) {
            let _as: Token![as] = input.parse()?;
            Some(input.parse::<LitStr>()?)
        } else {
            None
        };
        Ok(HandlerEntry { fn_path, wire_name })
    }
}

/// A single effect entry: `<WorkflowIdent> on <filter>`.
#[allow(dead_code)]
struct EffectEntry {
    workflow_ident: Ident,
    filter: EffectFilterSpec,
}

#[allow(dead_code)]
enum EffectFilterSpec {
    /// `on all`
    All,
    /// `on [Cmd1, Cmd2, ...]` - command type paths.
    Commands(Vec<Path>),
    /// `on predicate(|n| ...)` - a closure expression evaluating on `&ExecuteNotification`.
    Predicate(syn::ExprClosure),
}

impl Parse for EffectEntry {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let workflow_ident: Ident = input.parse()?;
        let on_kw: Ident = input.parse()?;
        if on_kw != "on" {
            return Err(syn::Error::new(on_kw.span(), "expected `on`"));
        }

        let filter = if input.peek(all) {
            let _: all = input.parse()?;
            EffectFilterSpec::All
        } else if input.peek(predicate) {
            let _: predicate = input.parse()?;
            let inner;
            syn::parenthesized!(inner in input);
            let closure: syn::ExprClosure = inner.parse()?;
            if !inner.is_empty() {
                return Err(inner.error("unexpected token in predicate filter"));
            }
            EffectFilterSpec::Predicate(closure)
        } else if input.peek(syn::token::Bracket) {
            let buf;
            bracketed!(buf in input);
            let cmds: Punctuated<Path, Token![,]> = buf.parse_terminated(Path::parse, Token![,])?;
            EffectFilterSpec::Commands(cmds.into_iter().collect())
        } else {
            return Err(input.error("expected `all`, `predicate(...)`, or `[Cmd, ...]` after `on`"));
        };

        Ok(EffectEntry {
            workflow_ident,
            filter,
        })
    }
}

/// The loader entry mirrors a handler entry but is parsed inline (no braces).
struct LoaderEntry {
    fn_path: Path,
    #[allow(dead_code)]
    wire_name: Option<LitStr>,
}

impl LoaderEntry {
    fn parse_inline(input: ParseStream) -> syn::Result<Self> {
        let fn_path: Path = input.parse()?;
        let wire_name: Option<LitStr> = if input.peek(Token![as]) {
            let _as: Token![as] = input.parse()?;
            Some(input.parse::<LitStr>()?)
        } else {
            None
        };
        Ok(LoaderEntry { fn_path, wire_name })
    }
}

/// The two forms accepted by `service!`:
///
/// - **`DefinitionOnly`**: `pub Name("logical") for State [Cmd, ...]`
///   Emits only `ServiceDefinition` + `HasCommand<C>` impls.
///
/// - **Full**: `pub Name for State { loader: .., handlers: [..] }`
///   Emits env traits, an in-process runtime, and Restate binding helpers.
enum ServiceInput {
    DefinitionOnly(DefinitionOnlyInput),
    Full(FullServiceInput),
}

impl Parse for ServiceInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let vis: Visibility = input.parse()?;
        let name: Ident = input.parse()?;

        // Optional logical name literal: `("some-name")`
        let logical_name: Option<LitStr> = if input.peek(syn::token::Paren) {
            let inner;
            syn::parenthesized!(inner in input);
            Some(inner.parse::<LitStr>()?)
        } else {
            None
        };

        let _for: Token![for] = input.parse()?;
        let state_type: Path = input.parse()?;

        // Distinguish the two forms by what follows the state type.
        if input.peek(syn::token::Bracket) {
            // Definition-only: [ Cmd, ... ]
            let cmds_buf;
            bracketed!(cmds_buf in input);
            let commands: Punctuated<Path, Token![,]> =
                cmds_buf.parse_terminated(Path::parse, Token![,])?;

            let service_name = match logical_name {
                Some(lit) => lit.value(),
                None => to_snake_case(&name.to_string()),
            };

            Ok(ServiceInput::DefinitionOnly(DefinitionOnlyInput {
                vis,
                name,
                service_name,
                state_type,
                commands: commands.into_iter().collect(),
            }))
        } else {
            // Full form: { loader: .., handlers: [..] }
            let body;
            braced!(body in input);

            // loader: <path> [as "wire"] ,
            let _loader_kw: loader = body.parse()?;
            let _colon: Token![:] = body.parse()?;
            let loader_entry = LoaderEntry::parse_inline(&body)?;
            let _comma: Token![,] = body.parse()?;

            // handlers: [ ... ],
            let _handlers_kw: handlers = body.parse()?;
            let _colon2: Token![:] = body.parse()?;
            let entries_buf;
            bracketed!(entries_buf in body);
            let entries: Punctuated<HandlerEntry, Token![,]> =
                entries_buf.parse_terminated(HandlerEntry::parse, Token![,])?;

            // optional trailing comma after the bracket
            let _ = body.parse::<Token![,]>();

            let effect_entries: Vec<EffectEntry> = if body.peek(effects) {
                let _: effects = body.parse()?;
                let _: Token![:] = body.parse()?;
                let eff_buf;
                bracketed!(eff_buf in body);
                let entries: Punctuated<EffectEntry, Token![,]> =
                    eff_buf.parse_terminated(EffectEntry::parse, Token![,])?;
                let _ = body.parse::<Token![,]>();
                entries.into_iter().collect()
            } else {
                Vec::new()
            };

            if !body.is_empty() {
                return Err(body.error("unexpected token in service body"));
            }

            let service_name = match logical_name {
                Some(lit) => lit.value(),
                None => to_snake_case(&name.to_string()),
            };

            Ok(ServiceInput::Full(FullServiceInput {
                vis,
                name,
                service_name,
                state_type,
                loader_entry,
                handler_entries: entries.into_iter().collect(),
                effect_entries,
            }))
        }
    }
}

/// Input for the definition-only form.
struct DefinitionOnlyInput {
    vis: Visibility,
    name: Ident,
    service_name: String,
    state_type: Path,
    commands: Vec<Path>,
}

/// Input for the full form (original struct, renamed for clarity).
struct FullServiceInput {
    vis: Visibility,
    name: Ident,
    service_name: String,
    state_type: Path,
    loader_entry: LoaderEntry,
    handler_entries: Vec<HandlerEntry>,
    effect_entries: Vec<EffectEntry>,
}

// ---------------------------------------------------------------------------
// snake_case helper
//
// Rule: insert `_` before each uppercase character that follows a lowercase
// character or digit, then lowercase everything.
// Examples: CounterService → counter_service, HTTPService → h_t_t_p_service
// (simple per-uppercase-transition rule; no acronym special-casing)
// ---------------------------------------------------------------------------

fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let mut prev_lower = false;
    for ch in s.chars() {
        if ch.is_uppercase() {
            if prev_lower {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
            prev_lower = false;
        } else {
            out.push(ch);
            prev_lower = ch.is_lowercase() || ch.is_ascii_digit();
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Identifier synthesis helpers
// ---------------------------------------------------------------------------

/// Given a function path `foo::bar::baz`, produce the spec struct path
/// `foo::bar::baz_Spec` by appending `_Spec` to the last segment ident.
fn spec_path(fn_path: &Path) -> Path {
    let mut spec = fn_path.clone();
    let last = spec
        .segments
        .last_mut()
        .expect("path must have at least one segment");
    last.ident = format_ident!("{}_Spec", last.ident);
    spec
}

/// Given a function path `foo::bar::baz`, produce the `__Requires` trait path
/// `foo::bar::__baz_Requires` by prepending `__` and appending `_Requires` to the
/// last segment ident.
fn requires_path(fn_path: &Path) -> Path {
    let mut req = fn_path.clone();
    let last = req
        .segments
        .last_mut()
        .expect("path must have at least one segment");
    last.ident = format_ident!("__{}_Requires", last.ident);
    req
}

fn remaining_type_for_subset(effect_markers: &[Ident], subset: usize) -> TokenStream2 {
    effect_markers
        .iter()
        .enumerate()
        .rev()
        .filter(|(idx, _)| subset & (1usize << idx) != 0)
        .fold(
            quote! { ::wee_events_restate::Ready },
            |tail, (_, marker)| {
                quote! { ::wee_events_restate::Needs<#marker, #tail> }
            },
        )
}

// ---------------------------------------------------------------------------
// Code generation
// ---------------------------------------------------------------------------

pub fn expand(input: TokenStream) -> TokenStream {
    match syn::parse::<ServiceInput>(input) {
        Ok(ServiceInput::DefinitionOnly(defn)) => generate_definition_only(defn).into(),
        Ok(ServiceInput::Full(full)) => generate_full(full).into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// ---------------------------------------------------------------------------
// Definition-only emission
// ---------------------------------------------------------------------------

fn generate_definition_only(input: DefinitionOnlyInput) -> TokenStream2 {
    let DefinitionOnlyInput {
        vis,
        name,
        service_name,
        state_type,
        commands,
    } = input;

    let service_name_lit = LitStr::new(&service_name, proc_macro2::Span::call_site());

    quote! {
        /// Service definition for #name.
        ///
        /// State: `#state_type`
        #vis struct #name;

        impl ::wee_events::ServiceDefinition for #name {
            type State = #state_type;
            const SERVICE_NAME: &'static str = #service_name_lit;
        }

        #( impl ::wee_events::HasCommand<#commands> for #name {} )*

        impl #name {
            pub fn restate_client(
                ingress: impl Into<String>,
            ) -> ::wee_events_restate::RestateClient<Self> {
                ::wee_events_restate::RestateClient::new(ingress)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Full-form emission (original logic, refactored into its own function)
// ---------------------------------------------------------------------------

fn generate_full(service: FullServiceInput) -> TokenStream2 {
    let FullServiceInput {
        vis,
        name,
        service_name,
        state_type,
        loader_entry,
        handler_entries,
        effect_entries,
    } = service;

    // Effective Restate method name for the loader.
    let loader_wire_name = loader_entry
        .wire_name
        .as_ref()
        .map_or_else(|| "load".to_string(), syn::LitStr::value);

    // Effective Restate method name for each handler, in input order.
    let handler_wire_names: Vec<String> = handler_entries
        .iter()
        .map(|e| match &e.wire_name {
            Some(lit) => lit.value(),
            None => to_snake_case(
                &e.fn_path
                    .segments
                    .last()
                    .expect("fn path has at least one segment")
                    .ident
                    .to_string(),
            ),
        })
        .collect();

    // Collision check: loader name must not collide with any handler name.
    let mut seen = std::collections::HashSet::new();
    seen.insert(loader_wire_name.clone());
    for (entry, wire) in handler_entries.iter().zip(handler_wire_names.iter()) {
        if !seen.insert(wire.clone()) {
            return syn::Error::new_spanned(
                &entry.fn_path,
                format!(
                    "wire name `{wire}` collides with another handler or with the reserved loader name `{loader}`. \
                     Use `{fn} as \"…\"` to pick a different wire name.",
                    wire = wire,
                    loader = loader_wire_name,
                    fn = entry
                        .fn_path
                        .segments
                        .last()
                        .map(|s| s.ident.to_string())
                        .unwrap_or_default(),
                ),
            )
            .to_compile_error();
        }
    }

    let loader_fn_path = &loader_entry.fn_path;

    // Derive spec and requires paths for the loader
    let loader_spec_path = spec_path(loader_fn_path);
    let loader_requires_path = requires_path(loader_fn_path);

    // Derive spec and requires paths for each handler
    let handler_spec_paths: Vec<Path> = handler_entries
        .iter()
        .map(|e| spec_path(&e.fn_path))
        .collect();
    let handler_requires_paths: Vec<Path> = handler_entries
        .iter()
        .map(|e| requires_path(&e.fn_path))
        .collect();
    let handler_fn_paths: Vec<&Path> = handler_entries.iter().map(|e| &e.fn_path).collect();

    // Name for the generated service-specific env trait: {Name}Env
    let env_trait_name = format_ident!("{}Env", name);
    let handler_env_trait_name = format_ident!("{}HandlerEnv", name);

    // All __Requires traits combined (loader + handlers)
    let all_requires_paths: Vec<&Path> = std::iter::once(&loader_requires_path)
        .chain(handler_requires_paths.iter())
        .collect();

    // -----------------------------------------------------------------------
    // 1. Service-specific env trait
    // -----------------------------------------------------------------------

    let env_trait = quote! {
        /// Environment contract for #name.
        ///
        /// Union of all capability requirements declared by the loader and handlers.
        /// Any type implementing the required capability traits automatically
        /// satisfies this trait via the blanket impl.
        #[allow(non_camel_case_types)]
        #vis trait #env_trait_name:
            #(#all_requires_paths +)*
            ::std::marker::Send + ::std::marker::Sync {}

        impl<__T> #env_trait_name for __T
        where
            __T: #(#all_requires_paths +)*
                 ::std::marker::Send + ::std::marker::Sync {}

        /// Handler environment contract for #name.
        ///
        /// Union of capability requirements declared by command handlers only.
        /// Restate bindings supply loader requirements through the store.
        #[allow(non_camel_case_types)]
        #vis trait #handler_env_trait_name:
            #(#handler_requires_paths +)*
            ::std::marker::Send + ::std::marker::Sync {}

        impl<__T> #handler_env_trait_name for __T
        where
            __T: #(#handler_requires_paths +)*
                 ::std::marker::Send + ::std::marker::Sync {}
    };

    // -----------------------------------------------------------------------
    // 2. Namespace struct
    // -----------------------------------------------------------------------

    let namespace_struct = quote! {
        /// Generated service namespace.
        #vis struct #name;
    };

    // -----------------------------------------------------------------------
    // 3. Core in-process service emitted for wee_events::create(...)
    // -----------------------------------------------------------------------

    let core_mod_name = format_ident!("__wee_events_{}_core", to_snake_case(&name.to_string()));

    let core_dispatch_impls: Vec<TokenStream2> = handler_spec_paths
        .iter()
        .zip(handler_requires_paths.iter())
        .map(|(sp, handler_requires)| {
            quote! {
                impl<__Store, __Services> ::wee_events::__private::DispatchCommand<
                    <#sp as ::wee_events::HandlerSpec>::Command,
                >
                    for Service<__Store, __Services>
                where
                    __Store:
                        #loader_requires_path
                        + ::std::clone::Clone
                        + ::std::marker::Send
                        + ::std::marker::Sync
                        + 'static,
                    __Services:
                        ::std::clone::Clone
                        + ::std::marker::Send
                        + ::std::marker::Sync
                        + 'static,
                    ::wee_events::HandlerEnv<__Store, __Services>:
                        #handler_requires + ::std::marker::Send + ::std::marker::Sync + 'static,
                    #loader_spec_path: ::wee_events::LoaderRuntimeSpec<__Store>,
                    #sp: ::wee_events::HandlerRuntimeSpec<
                        ::wee_events::HandlerEnv<__Store, __Services>,
                    >,
                    <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::Error:
                        ::std::marker::Send + ::std::marker::Sync + 'static,
                    <#sp as ::wee_events::HandlerRuntimeSpec<
                        ::wee_events::HandlerEnv<__Store, __Services>,
                    >>::Error:
                        ::std::convert::From<
                            <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::Error,
                        >
                        + ::std::marker::Send
                        + ::std::marker::Sync
                        + 'static,
                    #state_type: ::std::marker::Send + ::std::marker::Sync + 'static,
                {
                    type Error = <#sp as ::wee_events::HandlerRuntimeSpec<
                        ::wee_events::HandlerEnv<__Store, __Services>,
                    >>::Error;

                    fn dispatch_command(
                        &self,
                        id: ::wee_events::AggregateId,
                        cmd: <#sp as ::wee_events::HandlerSpec>::Command,
                    ) -> impl ::std::future::Future<
                        Output = ::std::result::Result<
                            ::wee_events::Entity<#state_type>,
                            Self::Error,
                        >,
                    > + ::std::marker::Send {
                        let store = self.store.clone();
                        let services = self.services.clone();
                        async move {
                            let entity =
                                <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::load(
                                    &store,
                                    &id,
                                )
                                .await
                                .map_err(<Self::Error as ::std::convert::From<
                                    <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::Error,
                                >>::from)?;
                            let env = ::wee_events::HandlerEnv::new(store.clone(), services);
                            match <#sp as ::wee_events::HandlerRuntimeSpec<
                                ::wee_events::HandlerEnv<__Store, __Services>,
                            >>::handle(
                                    &env,
                                    &entity,
                                    cmd,
                                )
                                .await?
                            {
                                ::wee_events::HandlerOutcome::Entity(entity) => Ok(entity),
                                ::wee_events::HandlerOutcome::Reload => {
                                    <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::load(
                                        &store,
                                        &id,
                                    )
                                        .await
                                        .map_err(<Self::Error as ::std::convert::From<
                                            <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::Error,
                                        >>::from)
                                }
                            }
                        }
                    }
                }

                impl<__Store, __Services> ::wee_events::Handles<
                    <#sp as ::wee_events::HandlerSpec>::Command,
                >
                    for Service<__Store, __Services>
                where
                    Service<__Store, __Services>: ::wee_events::__private::DispatchCommand<
                        <#sp as ::wee_events::HandlerSpec>::Command,
                    >,
                {}
            }
        })
        .collect();

    let core_service = quote! {
        #vis mod #core_mod_name {
            use super::*;

            pub struct Service<__Store, __Services> {
                store: __Store,
                services: __Services,
            }

            impl<__Store, __Services> Service<__Store, __Services> {
                pub fn new(store: __Store, services: __Services) -> Self {
                    Self { store, services }
                }
            }

            impl<__Store, __Services> ::wee_events::__private::ServiceState
                for Service<__Store, __Services>
            where
                #state_type: ::std::marker::Send + ::std::marker::Sync,
                __Store: ::std::marker::Send + ::std::marker::Sync,
                __Services: ::std::marker::Send + ::std::marker::Sync,
            {
                type State = #state_type;
            }

            impl<__Store, __Services> ::wee_events::TypedService<#state_type>
                for Service<__Store, __Services>
            where
                __Store:
                    #loader_requires_path
                    + ::std::clone::Clone
                    + ::std::marker::Send
                    + ::std::marker::Sync
                    + 'static,
                __Services:
                    ::std::clone::Clone
                    + ::std::marker::Send
                    + ::std::marker::Sync
                    + 'static,
                #loader_spec_path: ::wee_events::LoaderRuntimeSpec<__Store>,
                <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::Error:
                    ::std::marker::Send + ::std::marker::Sync + 'static,
                #state_type: ::std::marker::Send + ::std::marker::Sync + 'static,
            {
                type Error = <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::Error;

                fn load(
                    &self,
                    id: ::wee_events::AggregateId,
                ) -> impl ::std::future::Future<
                    Output = ::std::result::Result<
                        ::wee_events::Entity<#state_type>,
                        Self::Error,
                    >,
                > + ::std::marker::Send {
                    let store = self.store.clone();
                    async move {
                        <#loader_spec_path as ::wee_events::LoaderRuntimeSpec<__Store>>::load(
                            &store,
                            &id,
                        )
                        .await
                    }
                }
            }

            #(#core_dispatch_impls)*
        }

        impl ::wee_events::InProcessServiceDefinition for #name {
            type Built<__Store, __Services> =
                #core_mod_name::Service<__Store, __Services>;

            fn build_in_process<__Store, __Services>(
                store: __Store,
                services: __Services,
            ) -> Self::Built<__Store, __Services> {
                #core_mod_name::Service::new(store, services)
            }
        }
    };

    // -----------------------------------------------------------------------
    // 4. Suppress the loader_spec_path unused warning (it's referenced only
    //    for the env trait, not for dispatch). Actually it is used — never mind.
    // -----------------------------------------------------------------------

    // Silence unused import: reference loader spec in a doc comment / type alias.
    // Actually loader_spec_path is used in the env trait only indirectly via
    // loader_requires_path. We don't reference it in generated code, so suppress
    // the compiler lint with a type assertion hidden in a const.
    let loader_spec_assertion = quote! {
        const _: () = {
            fn _assert_loader_spec() {
                fn _check<T: wee_events::LoaderSpec>() {}
                _check::<#loader_spec_path>();
            }
        };
    };

    // -----------------------------------------------------------------------
    // 5. ServiceDefinition + HasCommand<C> impls (definition traits)
    // -----------------------------------------------------------------------

    let service_name_lit = LitStr::new(&service_name, proc_macro2::Span::call_site());

    let binder_trait_name = format_ident!("{}Binder", name);
    let binding_name = format_ident!("{}Binding", name);
    let registration_name = format_ident!("{}Registration", name);
    let restate_mod_name =
        format_ident!("__wee_events_{}_restate", to_snake_case(&name.to_string()));
    let loader_wire_name_lit = LitStr::new(&loader_wire_name, proc_macro2::Span::call_site());
    let loader_method_ident = format_ident!("__wee_events_load");
    let handler_wire_name_lits: Vec<LitStr> = handler_wire_names
        .iter()
        .map(|wire| LitStr::new(wire, proc_macro2::Span::call_site()))
        .collect();
    let handler_method_idents: Vec<Ident> = handler_entries
        .iter()
        .enumerate()
        .map(|(i, _)| format_ident!("__wee_events_handler_{}", i))
        .collect();
    let effect_filter_idents: Vec<Ident> = effect_entries
        .iter()
        .enumerate()
        .map(|(i, _)| {
            format_ident!(
                "__wee_events_{}_effect_filter_{}",
                to_snake_case(&name.to_string()),
                i
            )
        })
        .collect();
    let effect_marker_idents: Vec<Ident> = effect_entries
        .iter()
        .map(|entry| format_ident!("{}{}Effect", name, entry.workflow_ident))
        .collect();

    let effect_filter_functions: Vec<TokenStream2> = effect_entries
        .iter()
        .zip(effect_filter_idents.iter())
        .map(|(entry, filter_ident)| {
            let filter = match &entry.filter {
                EffectFilterSpec::All => {
                    quote! {
                        ::wee_events_restate::SideEffectFilter::All
                    }
                }
                EffectFilterSpec::Commands(commands) => {
                    quote! {
                        ::wee_events_restate::SideEffectFilter::Names(::std::vec![
                            #(
                                ::wee_events::CommandName::from(
                                    <#commands as ::wee_events::Command>::NAME,
                                ),
                            )*
                        ])
                    }
                }
                EffectFilterSpec::Predicate(predicate) => {
                    quote! {
                        ::wee_events_restate::SideEffectFilter::Predicate(
                            ::std::boxed::Box::new(#predicate),
                        )
                    }
                }
            };

            quote! {
                fn #filter_ident(
                    notification: &::wee_events_restate::ExecuteNotification,
                ) -> bool {
                    let filter = #filter;
                    filter.matches(notification)
                }
            }
        })
        .collect();

    let effect_dispatch_calls: Vec<TokenStream2> = effect_entries
        .iter()
        .zip(effect_filter_idents.iter())
        .map(|(entry, filter_ident)| {
            let effect_client_ident = format_ident!("{}Client", entry.workflow_ident);
            quote! {
                if #filter_ident(&notification) {
                    let effect_client =
                        ::wee_events_restate::__private::context::ContextClient::workflow_client::<
                            #effect_client_ident<'_>,
                        >(&ctx, notification.metadata.correlation_id.clone());
                    let _ = effect_client
                        .run(::wee_events_restate::__private::serde::Json(notification.clone()))
                        .send();
                }
            }
        })
        .collect();
    let effect_command_bounds: Vec<TokenStream2> = if effect_entries.is_empty() {
        Vec::new()
    } else {
        handler_spec_paths
            .iter()
            .map(|sp| {
                quote! {
                    <#sp as ::wee_events::HandlerSpec>::Command:
                        ::std::clone::Clone + ::serde::Serialize,
                }
            })
            .collect()
    };

    let binder_trait_methods: Vec<TokenStream2> = handler_method_idents
        .iter()
        .zip(handler_wire_name_lits.iter())
        .zip(handler_spec_paths.iter())
        .map(|((method_ident, wire_lit), sp)| {
            quote! {
                #[name = #wire_lit]
                async fn #method_ident(
                    command: ::wee_events_restate::__private::serde::Json<
                        <#sp as ::wee_events::HandlerSpec>::Command,
                    >,
                ) -> ::std::result::Result<
                    ::wee_events_restate::EntityResponse,
                    ::wee_events_restate::__private::errors::HandlerError,
                >;
            }
        })
        .collect();

    let binder_impl_methods: Vec<TokenStream2> = handler_method_idents
        .iter()
        .zip(handler_fn_paths.iter())
        .zip(handler_spec_paths.iter())
        .map(|((method_ident, fn_path), sp)| {
            let handle_command = if effect_entries.is_empty() {
                quote! {
                    let outcome = ::wee_events::IntoHandlerOutcome::into_handler_outcome(
                        #fn_path(&env, &entity, command.into_inner()).await
                    )
                        .map_err(::wee_events_restate::__private::to_handler_error)?;
                    let entity = match outcome {
                        ::wee_events::HandlerOutcome::Entity(entity) => entity,
                        ::wee_events::HandlerOutcome::Reload => {
                            #loader_fn_path(&store, &id)
                                .await
                                .map_err(::wee_events_restate::__private::to_handler_error)?
                        }
                    };
                    ::wee_events_restate::__private::to_entity_response(entity)
                }
            } else {
                quote! {
                    let command = command.into_inner();
                    let command_for_notification = command.clone();
                    let outcome = ::wee_events::IntoHandlerOutcome::into_handler_outcome(
                        #fn_path(&env, &entity, command).await
                    )
                        .map_err(::wee_events_restate::__private::to_handler_error)?;
                    let entity = match outcome {
                        ::wee_events::HandlerOutcome::Entity(entity) => entity,
                        ::wee_events::HandlerOutcome::Reload => {
                            #loader_fn_path(&store, &id)
                                .await
                                .map_err(::wee_events_restate::__private::to_handler_error)?
                        }
                    };
                    let response = ::wee_events_restate::__private::to_entity_response(entity)?;
                    let command_name = ::wee_events::CommandName::from(
                        <<#sp as ::wee_events::HandlerSpec>::Command as ::wee_events::Command>::NAME,
                    );
                    let correlation = ::wee_events_restate::correlation_id(&id, &command_name);
                    let notification = ::wee_events_restate::ExecuteNotification {
                        command: ::wee_events_restate::CommandRequest {
                            name: command_name,
                            target: id.clone(),
                            command: ::wee_events_restate::__private::serde_json::to_value(
                                &command_for_notification,
                            )
                            .map_err(|e| {
                                ::wee_events_restate::__private::errors::TerminalError::new(
                                    e.to_string(),
                                )
                            })?,
                        },
                        response: response.clone(),
                        metadata: ::wee_events_restate::Metadata {
                            correlation_id: correlation.clone(),
                            causation_id: None,
                            idempotency_key: None,
                        },
                    };
                    Ok(notification)
                }
            };

            let run_result = if effect_entries.is_empty() {
                quote! {
                    ::wee_events_restate::__private::context::ContextSideEffects::run(
                        &ctx,
                        move || async move {
                            let store = store;
                            let services = services;
                            let entity = #loader_fn_path(&store, &id)
                                .await
                                .map_err(::wee_events_restate::__private::to_handler_error)?;
                            let env = ::wee_events_restate::HandlerEnv::new(store.clone(), services);
                            #handle_command
                        },
                    )
                    .await
                    .map_err(::wee_events_restate::__private::errors::HandlerError::from)
                }
            } else {
                quote! {
                    let notification = ::wee_events_restate::__private::context::ContextSideEffects::run(
                        &ctx,
                        move || async move {
                            let store = store;
                            let services = services;
                            let entity = #loader_fn_path(&store, &id)
                                .await
                                .map_err(::wee_events_restate::__private::to_handler_error)?;
                            let env = ::wee_events_restate::HandlerEnv::new(store.clone(), services);
                            #handle_command
                        },
                    )
                    .await
                    .map_err(::wee_events_restate::__private::errors::HandlerError::from)?;
                    #(#effect_dispatch_calls)*
                    Ok(notification.response)
                }
            };

            quote! {
                async fn #method_ident(
                    &self,
                    ctx: ::wee_events_restate::__private::context::ObjectContext<'_>,
                    command: ::wee_events_restate::__private::serde::Json<
                        <#sp as ::wee_events::HandlerSpec>::Command,
                    >,
                ) -> ::std::result::Result<
                    ::wee_events_restate::EntityResponse,
                    ::wee_events_restate::__private::errors::HandlerError,
                > {
                    let id = ctx
                        .key()
                        .parse::<::wee_events::AggregateId>()
                        .map_err(|e| {
                            ::wee_events_restate::__private::errors::TerminalError::new(e.to_string())
                        })?;
                    let store = self.store.clone();
                    let services = self.services.clone();
                    #run_result
                }
            }
        })
        .collect();

    let initial_remaining_effects = remaining_type_for_subset(
        &effect_marker_idents,
        if effect_entries.is_empty() {
            0
        } else {
            (1usize << effect_entries.len()) - 1
        },
    );

    let mut with_effect_impls = Vec::new();
    if !effect_entries.is_empty() {
        let all_effects = (1usize << effect_entries.len()) - 1;
        for subset in 1usize..=all_effects {
            let input_remaining = remaining_type_for_subset(&effect_marker_idents, subset);

            for effect_index in 0..effect_entries.len() {
                if subset & (1usize << effect_index) == 0 {
                    continue;
                }

                let output_subset = subset & !(1usize << effect_index);
                let output_remaining =
                    remaining_type_for_subset(&effect_marker_idents, output_subset);
                let workflow_ident = &effect_entries[effect_index].workflow_ident;
                let effect_marker = &effect_marker_idents[effect_index];

                with_effect_impls.push(quote! {
                    impl<__Store, __Services, __Effect>
                        ::wee_events_restate::WithEffect<__Effect, #effect_marker>
                        for Registration<__Store, __Services, #input_remaining>
                    where
                        __Effect: #workflow_ident
                            + ::std::marker::Send
                            + ::std::marker::Sync
                            + 'static,
                    {
                        type Output = Registration<
                            __Store,
                            __Services,
                            #output_remaining,
                        >;

                        fn with_effect(mut self, effect: __Effect) -> Self::Output {
                            self.effects.push(::std::boxed::Box::new(
                                move |builder: ::restate_sdk::endpoint::Builder| {
                                    builder.bind(<__Effect as #workflow_ident>::serve(effect))
                                },
                            ));
                            Registration {
                                binding: self.binding,
                                effects: self.effects,
                                _remaining_effects: ::std::marker::PhantomData,
                            }
                        }
                    }
                });
            }
        }
    }

    let binder = quote! {
        #(
            #vis struct #effect_marker_idents;
        )*

        #vis use #restate_mod_name::Registration as #registration_name;

        mod #restate_mod_name {
            use super::*;

            #[doc = "Restate Virtual Object service trait generated by `wee_events::service!`."]
            #[::wee_events_restate::__private::object]
            #[name = #service_name_lit]
            trait #binder_trait_name {
                #[shared]
                #[name = #loader_wire_name_lit]
                async fn #loader_method_ident() -> ::std::result::Result<
                    ::wee_events_restate::EntityResponse,
                    ::wee_events_restate::__private::errors::HandlerError,
                >;

                #(#binder_trait_methods)*
            }

            struct #binding_name<__Store, __Services> {
                store: __Store,
                services: __Services,
            }

            pub struct Registration<
                __Store,
                __Services,
                __RemainingEffects,
            > {
                binding: #binding_name<__Store, __Services>,
                effects: ::std::vec::Vec<::wee_events_restate::__private::AttachEffect>,
                _remaining_effects:
                    ::std::marker::PhantomData<fn() -> __RemainingEffects>,
            }

            impl<__Store, __Services, __RemainingEffects>
                Registration<
                    __Store,
                    __Services,
                    __RemainingEffects,
                >
            {
                pub fn with_effect<__Effect, __Requirement>(
                    self,
                    effect: __Effect,
                ) -> <Self as ::wee_events_restate::WithEffect<
                    __Effect,
                    __Requirement,
                >>::Output
                where
                    Self: ::wee_events_restate::WithEffect<__Effect, __Requirement>,
                {
                    <Self as ::wee_events_restate::WithEffect<
                        __Effect,
                        __Requirement,
                    >>::with_effect(
                        self,
                        effect,
                    )
                }
            }

            #(#with_effect_impls)*

            impl<__Store, __Services>
                Registration<
                    __Store,
                    __Services,
                    ::wee_events_restate::Ready,
                >
            where
                #binding_name<__Store, __Services>: #binder_trait_name,
                __Store: ::std::marker::Send + ::std::marker::Sync + 'static,
                __Services: ::std::marker::Send + ::std::marker::Sync + 'static,
            {
                pub fn attach_to(
                    self,
                    builder: ::restate_sdk::endpoint::Builder,
                ) -> ::restate_sdk::endpoint::Builder {
                    ::wee_events_restate::__private::attach_effects_to(
                        builder.bind(self.binding.serve()),
                        self.effects,
                    )
                }
            }

            impl<__Store, __Services> #binder_trait_name for #binding_name<__Store, __Services>
            where
                __Store:
                    #loader_requires_path
                    + ::std::clone::Clone
                    + ::std::marker::Send
                    + ::std::marker::Sync
                    + 'static,
                __Services:
                    ::std::clone::Clone
                    + ::std::marker::Send
                    + ::std::marker::Sync
                    + 'static,
                ::wee_events_restate::HandlerEnv<
                    __Store,
                    __Services,
                >: #handler_env_trait_name,
                #state_type: ::serde::Serialize,
                #(#effect_command_bounds)*
            {
                async fn #loader_method_ident(
                    &self,
                    ctx: ::wee_events_restate::__private::context::SharedObjectContext<'_>,
                ) -> ::std::result::Result<
                    ::wee_events_restate::EntityResponse,
                    ::wee_events_restate::__private::errors::HandlerError,
                > {
                    let id = ctx
                        .key()
                        .parse::<::wee_events::AggregateId>()
                        .map_err(|e| {
                            ::wee_events_restate::__private::errors::TerminalError::new(e.to_string())
                        })?;
                    let store = self.store.clone();
                    let services = self.services.clone();
                    ::wee_events_restate::__private::context::ContextSideEffects::run(
                        &ctx,
                        move || async move {
                            let entity = #loader_fn_path(&store, &id)
                                .await
                                .map_err(::wee_events_restate::__private::to_handler_error)?;
                            ::wee_events_restate::__private::to_entity_response(entity)
                        },
                    )
                    .await
                    .map_err(::wee_events_restate::__private::errors::HandlerError::from)
                }

                #(#binder_impl_methods)*
            }

            impl ::wee_events_restate::RestateServiceDefinition for super::#name {
                type Registration<__Store, __Services> = Registration<
                    __Store,
                    __Services,
                    #initial_remaining_effects,
                >;

                fn register<__Store, __Services>(
                    store: __Store,
                    services: __Services,
                ) -> Self::Registration<__Store, __Services> {
                    Registration {
                        binding: #binding_name { store, services },
                        effects: ::std::vec::Vec::new(),
                        _remaining_effects: ::std::marker::PhantomData,
                    }
                }
            }
        }
    };

    let definition_impls = quote! {
        impl ::wee_events::ServiceDefinition for #name {
            type State = #state_type;
            const SERVICE_NAME: &'static str = #service_name_lit;
        }

        #(
            impl ::wee_events::HasCommand<<#handler_spec_paths as ::wee_events::HandlerSpec>::Command>
                for #name {}
        )*

        impl #name {
            pub fn restate_client(
                ingress: impl Into<String>,
            ) -> ::wee_events_restate::RestateClient<Self> {
                ::wee_events_restate::RestateClient::new(ingress)
            }
        }
    };

    quote! {
        #env_trait

        #namespace_struct

        #core_service

        #loader_spec_assertion

        #definition_impls

        #(#effect_filter_functions)*

        #binder
    }
}
