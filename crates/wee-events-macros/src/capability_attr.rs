use proc_macro::TokenStream;
use quote::quote;
use syn::{Error, FnArg, ItemTrait, ReturnType, TraitItem, TraitItemFn, Type, parse_quote};

pub fn expand(_args: TokenStream, input: TokenStream) -> TokenStream {
    let item = syn::parse_macro_input!(input as ItemTrait);

    match expand_inner(item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_inner(mut item: ItemTrait) -> syn::Result<proc_macro2::TokenStream> {
    for trait_item in &mut item.items {
        let TraitItem::Fn(method) = trait_item else {
            continue;
        };

        let result_ty = method_output_type(method)?;

        method.sig.asyncness = None;
        method.sig.output = parse_quote! {
            -> impl ::std::future::Future<Output = #result_ty> + ::std::marker::Send
        };

        add_send_bound(method)?;
    }

    Ok(quote! {
        #item
    })
}

fn method_output_type(method: &TraitItemFn) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = &method.sig.output else {
        return Err(Error::new_spanned(
            &method.sig,
            "#[wee_events::capability] async methods must return a value",
        ));
    };
    Ok(ty.as_ref().clone())
}

fn add_send_bound(method: &mut TraitItemFn) -> syn::Result<()> {
    let Some(receiver) = method.sig.inputs.first() else {
        return Err(Error::new_spanned(
            &method.sig,
            "#[wee_events::capability] methods must take &self",
        ));
    };

    if !matches!(receiver, FnArg::Receiver(receiver) if receiver.reference.is_some()) {
        return Err(Error::new_spanned(
            receiver,
            "#[wee_events::capability] methods must take &self",
        ));
    }

    Ok(())
}
