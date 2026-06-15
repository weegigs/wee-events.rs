//! Implementation of the `#[handler(...)]` attribute macro.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Error, FnArg, GenericArgument, ItemFn, PatType, Path, PathArguments, ReturnType, Token, Type,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

// ---------------------------------------------------------------------------
// Attribute argument parsing
// ---------------------------------------------------------------------------

/// Parsed arguments for `#[handler(command = Path, requires(T, ...))]`.
struct HandlerArgs {
    command: Path,
    requires: Vec<Path>,
}

impl Parse for HandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut command: Option<Path> = None;
        let mut requires: Vec<Path> = Vec::new();

        while !input.is_empty() {
            let lookahead = input.lookahead1();

            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;

                if ident == "command" {
                    let _: Token![=] = input.parse()?;
                    let path: Path = input.parse()?;
                    command = Some(path);
                } else if ident == "requires" {
                    let content;
                    syn::parenthesized!(content in input);
                    let paths: Punctuated<Path, Token![,]> =
                        content.parse_terminated(Path::parse, Token![,])?;
                    requires = paths.into_iter().collect();
                } else {
                    return Err(Error::new_spanned(
                        ident,
                        "expected `command` or `requires`",
                    ));
                }
            } else {
                return Err(lookahead.error());
            }

            // Consume optional trailing comma between top-level args
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }

        let command = command.ok_or_else(|| {
            Error::new(
                proc_macro2::Span::call_site(),
                "`command = <Type>` is required for #[handler]",
            )
        })?;

        Ok(HandlerArgs { command, requires })
    }
}

// ---------------------------------------------------------------------------
// State extraction from return type
// ---------------------------------------------------------------------------

fn result_ok_type(return_type: &ReturnType) -> syn::Result<&Type> {
    let ReturnType::Type(_, box_ty) = return_type else {
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            "handler must return `wee_events::Result<()>` or `wee_events::Result<Entity<State>>`",
        ));
    };

    let Type::Path(type_path) = box_ty.as_ref() else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `wee_events::Result<()>` or `wee_events::Result<Entity<State>>`",
        ));
    };

    // Navigate the path segments to find Result<...>
    // Accept: Result<...>, wee_events::Result<...>
    let last_seg = type_path.path.segments.last().ok_or_else(|| {
        Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `wee_events::Result<()>` or `wee_events::Result<Entity<State>>`",
        )
    })?;

    if last_seg.ident != "Result" {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `wee_events::Result<()>` or `wee_events::Result<Entity<State>>`",
        ));
    }

    // Extract the single generic argument of Result<T>
    let PathArguments::AngleBracketed(result_args) = &last_seg.arguments else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `wee_events::Result<()>` or `wee_events::Result<Entity<State>>`",
        ));
    };

    result_args
        .args
        .iter()
        .find_map(|arg| {
            if let GenericArgument::Type(ty) = arg {
                Some(ty)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            Error::new_spanned(
                box_ty.as_ref(),
                "handler must return `wee_events::Result<()>` or `wee_events::Result<Entity<State>>`",
            )
        })
}

fn result_error_type(return_type: &ReturnType) -> syn::Result<Type> {
    let ReturnType::Type(_, box_ty) = return_type else {
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            "handler must return `Result<T, Error>` or `wee_events::Result<T>`",
        ));
    };

    let Type::Path(type_path) = box_ty.as_ref() else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `Result<T, Error>` or `wee_events::Result<T>`",
        ));
    };

    let last_seg = type_path.path.segments.last().ok_or_else(|| {
        Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `Result<T, Error>` or `wee_events::Result<T>`",
        )
    })?;

    if last_seg.ident != "Result" {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `Result<T, Error>` or `wee_events::Result<T>`",
        ));
    }

    let PathArguments::AngleBracketed(result_args) = &last_seg.arguments else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `Result<T, Error>` or `wee_events::Result<T>`",
        ));
    };

    let mut type_args = result_args.args.iter().filter_map(|arg| {
        if let GenericArgument::Type(ty) = arg {
            Some(ty.clone())
        } else {
            None
        }
    });

    let _ok = type_args.next().ok_or_else(|| {
        Error::new_spanned(
            box_ty.as_ref(),
            "handler must return `Result<T, Error>` or `wee_events::Result<T>`",
        )
    })?;

    Ok(type_args
        .next()
        .unwrap_or_else(|| syn::parse_quote!(wee_events::Error)))
}

fn extract_state_from_entity_type(entity_ty: &Type) -> syn::Result<Type> {
    let Type::Path(entity_path) = entity_ty else {
        return Err(Error::new_spanned(entity_ty, "expected `Entity<State>`"));
    };

    let entity_seg = entity_path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(entity_ty, "expected `Entity<State>`"))?;

    if entity_seg.ident != "Entity" {
        return Err(Error::new_spanned(entity_ty, "expected `Entity<State>`"));
    }

    let PathArguments::AngleBracketed(entity_args) = &entity_seg.arguments else {
        return Err(Error::new_spanned(entity_ty, "expected `Entity<State>`"));
    };

    let state_ty = entity_args
        .args
        .iter()
        .find_map(|arg| {
            if let GenericArgument::Type(ty) = arg {
                Some(ty.clone())
            } else {
                None
            }
        })
        .ok_or_else(|| Error::new_spanned(entity_ty, "expected `Entity<State>`"))?;

    Ok(state_ty)
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

fn extract_entity_arg_state(func: &ItemFn) -> syn::Result<Type> {
    for input in &func.sig.inputs {
        let FnArg::Typed(PatType { ty, .. }) = input else {
            continue;
        };
        let Type::Reference(reference) = ty.as_ref() else {
            continue;
        };
        if let Ok(state_ty) = extract_state_from_entity_type(&reference.elem) {
            return Ok(state_ty);
        }
    }

    Err(Error::new_spanned(
        &func.sig,
        "handler returning `wee_events::Result<()>` must take an `&Entity<State>` argument",
    ))
}

fn extract_state_type(func: &ItemFn) -> syn::Result<Type> {
    let ok_ty = result_ok_type(&func.sig.output)?;
    if is_unit_type(ok_ty) {
        extract_entity_arg_state(func)
    } else {
        extract_state_from_entity_type(ok_ty).map_err(|_| {
            Error::new_spanned(
                ok_ty,
                "handler must return `wee_events::Result<()>` or `wee_events::Result<Entity<State>>`",
            )
        })
    }
}

// ---------------------------------------------------------------------------
// Macro expansion
// ---------------------------------------------------------------------------

pub fn expand(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(args as HandlerArgs);
    let func = syn::parse_macro_input!(input as ItemFn);

    match expand_inner(&args, &func) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_inner(args: &HandlerArgs, func: &ItemFn) -> syn::Result<TokenStream2> {
    // Validate: function must have at least one generic type parameter
    if func.sig.generics.type_params().next().is_none() {
        return Err(Error::new_spanned(
            &func.sig,
            "#[handler] requires a generic type parameter for the context (e.g., `<R: MyTrait>`)",
        ));
    }

    let state_ty = extract_state_type(func)?;
    let error_ty = result_error_type(&func.sig.output)?;
    let ctx_ident = func
        .sig
        .generics
        .type_params()
        .next()
        .expect("validated above")
        .ident
        .clone();

    let fn_name = &func.sig.ident;
    let vis = &func.vis;
    let command_path = &args.command;
    let requires = &args.requires;
    let generics = &func.sig.generics;
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    // Spec struct name: {fn_name}_Spec
    let spec_name = syn::Ident::new(&format!("{fn_name}_Spec"), fn_name.span());

    // Composite requires trait name: __{fn_name}_Requires
    let requires_trait_name = syn::Ident::new(&format!("__{fn_name}_Requires"), fn_name.span());

    // Build the supertraits for the composite requires trait
    let requires_supertraits: TokenStream2 = if requires.is_empty() {
        quote! { ::std::marker::Send + ::std::marker::Sync }
    } else {
        let paths = requires.iter();
        quote! { #(#paths +)* ::std::marker::Send + ::std::marker::Sync }
    };

    // Build the where clause for the blanket impl
    let requires_where: TokenStream2 = if requires.is_empty() {
        quote! { T: ::std::marker::Send + ::std::marker::Sync + ?Sized }
    } else {
        let paths = requires.iter();
        quote! { T: #(#paths +)* ::std::marker::Send + ::std::marker::Sync + ?Sized }
    };

    Ok(quote! {
        #func

        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        #vis struct #spec_name;

        impl wee_events::HandlerSpec for #spec_name {
            type Command = #command_path;
            type State = #state_ty;
        }

        impl #impl_generics wee_events::HandlerRuntimeSpec<#ctx_ident> for #spec_name #where_clause {
            type Error = #error_ty;

            fn handle<'a>(
                env: &'a #ctx_ident,
                entity: &'a wee_events::Entity<Self::State>,
                command: Self::Command,
            ) -> ::std::pin::Pin<
                ::std::boxed::Box<
                    dyn ::std::future::Future<
                            Output = ::std::result::Result<
                                wee_events::HandlerOutcome<Self::State>,
                                Self::Error,
                            >,
                        > + ::std::marker::Send
                        + 'a,
                >,
            >
            where
                #ctx_ident: 'a,
                Self::State: 'a,
                Self::Error: 'a,
                Self::Command: 'a,
            {
                ::std::boxed::Box::pin(async move {
                    wee_events::IntoHandlerOutcome::into_handler_outcome(
                        #fn_name::<#ctx_ident>(env, entity, command).await,
                    )
                })
            }
        }

        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        #vis trait #requires_trait_name: #requires_supertraits {}

        impl<T> #requires_trait_name for T
        where #requires_where {}
    })
}
