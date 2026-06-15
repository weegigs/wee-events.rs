//! Implementation of the `#[loader(...)]` attribute macro.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Error, GenericArgument, ItemFn, Path, PathArguments, ReturnType, Token, Type,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

// ---------------------------------------------------------------------------
// Attribute argument parsing
// ---------------------------------------------------------------------------

/// Parsed arguments for `#[loader(requires(T, ...))]`.
struct LoaderArgs {
    requires: Vec<Path>,
}

impl Parse for LoaderArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut requires: Vec<Path> = Vec::new();

        while !input.is_empty() {
            let lookahead = input.lookahead1();

            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;

                if ident == "requires" {
                    let content;
                    syn::parenthesized!(content in input);
                    let paths: Punctuated<Path, Token![,]> =
                        content.parse_terminated(Path::parse, Token![,])?;
                    requires = paths.into_iter().collect();
                } else {
                    return Err(Error::new_spanned(ident, "expected `requires`"));
                }
            } else {
                return Err(lookahead.error());
            }

            // Consume optional trailing comma between top-level args
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }

        Ok(LoaderArgs { requires })
    }
}

// ---------------------------------------------------------------------------
// State extraction from return type
// ---------------------------------------------------------------------------

/// Extract the `State` from `wee_events::Result<Entity<State>>` or
/// `wee_events::Result<wee_events::Entity<State>>`.
fn extract_state_type(return_type: &ReturnType) -> syn::Result<Type> {
    let ReturnType::Type(_, box_ty) = return_type else {
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            "loader must return `wee_events::Result<Entity<State>>`",
        ));
    };

    let Type::Path(type_path) = box_ty.as_ref() else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `wee_events::Result<Entity<State>>`",
        ));
    };

    let last_seg = type_path.path.segments.last().ok_or_else(|| {
        Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `wee_events::Result<Entity<State>>`",
        )
    })?;

    if last_seg.ident != "Result" {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `wee_events::Result<Entity<State>>`",
        ));
    }

    let PathArguments::AngleBracketed(result_args) = &last_seg.arguments else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `wee_events::Result<Entity<State>>`",
        ));
    };

    let entity_ty = result_args
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
                "loader must return `wee_events::Result<Entity<State>>`",
            )
        })?;

    let Type::Path(entity_path) = entity_ty else {
        return Err(Error::new_spanned(
            entity_ty,
            "loader must return `wee_events::Result<Entity<State>>`",
        ));
    };

    let entity_seg = entity_path.path.segments.last().ok_or_else(|| {
        Error::new_spanned(
            entity_ty,
            "loader must return `wee_events::Result<Entity<State>>`",
        )
    })?;

    if entity_seg.ident != "Entity" {
        return Err(Error::new_spanned(
            entity_ty,
            "loader must return `wee_events::Result<Entity<State>>`",
        ));
    }

    let PathArguments::AngleBracketed(entity_args) = &entity_seg.arguments else {
        return Err(Error::new_spanned(
            entity_ty,
            "loader must return `wee_events::Result<Entity<State>>`",
        ));
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
        .ok_or_else(|| {
            Error::new_spanned(
                entity_ty,
                "loader must return `wee_events::Result<Entity<State>>`",
            )
        })?;

    Ok(state_ty)
}

fn extract_error_type(return_type: &ReturnType) -> syn::Result<Type> {
    let ReturnType::Type(_, box_ty) = return_type else {
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            "loader must return `Result<Entity<State>, Error>` or `wee_events::Result<Entity<State>>`",
        ));
    };

    let Type::Path(type_path) = box_ty.as_ref() else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `Result<Entity<State>, Error>` or `wee_events::Result<Entity<State>>`",
        ));
    };

    let last_seg = type_path.path.segments.last().ok_or_else(|| {
        Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `Result<Entity<State>, Error>` or `wee_events::Result<Entity<State>>`",
        )
    })?;

    if last_seg.ident != "Result" {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `Result<Entity<State>, Error>` or `wee_events::Result<Entity<State>>`",
        ));
    }

    let PathArguments::AngleBracketed(result_args) = &last_seg.arguments else {
        return Err(Error::new_spanned(
            box_ty.as_ref(),
            "loader must return `Result<Entity<State>, Error>` or `wee_events::Result<Entity<State>>`",
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
            "loader must return `Result<Entity<State>, Error>` or `wee_events::Result<Entity<State>>`",
        )
    })?;

    Ok(type_args
        .next()
        .unwrap_or_else(|| syn::parse_quote!(wee_events::Error)))
}

// ---------------------------------------------------------------------------
// Macro expansion
// ---------------------------------------------------------------------------

pub fn expand(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(args as LoaderArgs);
    let func = syn::parse_macro_input!(input as ItemFn);

    match expand_inner(&args, &func) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_inner(args: &LoaderArgs, func: &ItemFn) -> syn::Result<TokenStream2> {
    // Validate: function must have at least one generic type parameter
    if func.sig.generics.type_params().next().is_none() {
        return Err(Error::new_spanned(
            &func.sig,
            "#[loader] requires a generic type parameter for the context (e.g., `<R: MyTrait>`)",
        ));
    }

    let state_ty = extract_state_type(&func.sig.output)?;
    let error_ty = extract_error_type(&func.sig.output)?;
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

        impl wee_events::LoaderSpec for #spec_name {
            type State = #state_ty;
        }

        impl #impl_generics wee_events::LoaderRuntimeSpec<#ctx_ident> for #spec_name #where_clause {
            type Error = #error_ty;

            fn load<'a>(
                env: &'a #ctx_ident,
                id: &'a wee_events::AggregateId,
            ) -> ::std::pin::Pin<
                ::std::boxed::Box<
                    dyn ::std::future::Future<
                            Output = ::std::result::Result<
                                wee_events::Entity<Self::State>,
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
            {
                ::std::boxed::Box::pin(#fn_name::<#ctx_ident>(env, id))
            }
        }

        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        #vis trait #requires_trait_name: #requires_supertraits {}

        impl<T> #requires_trait_name for T
        where #requires_where {}
    })
}
