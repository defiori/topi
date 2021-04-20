// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse2, parse_macro_input, parse_quote, Arm, AttributeArgs, DeriveInput, ExprMatch, ItemTrait,
    Meta, NestedMeta, Path, PathSegment, Type,
};

#[proc_macro_attribute]
pub fn py_concretify(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttributeArgs);
    let item = parse_macro_input!(item as DeriveInput);
    let mut stream = quote! {
        #item
    };
    for (ty, ty_str) in attr.into_iter().filter_map(|a| {
        if let NestedMeta::Meta(Meta::Path(Path { mut segments, .. })) = a {
            if let Some(PathSegment { ident, .. }) = segments.first_mut() {
                let ty: Type = parse_quote!(#ident);
                Some((ty, ident.to_string()))
            } else {
                None
            }
        } else {
            panic!("Unexpected attribute argument.");
        }
    }) {
        let old_ident = item.ident.clone();
        let new_ident = format_ident!("{}_{}", item.ident, ty_str);
        let add = quote! {

            #[allow(non_camel_case_types)]
            #[pyclass(unsendable)]
            pub struct #new_ident {
                inner: #old_ident<#ty>,
            }

            #[pymethods]
            impl #new_ident {
                #[new]
                fn new(shape: Vec<usize>) -> PyResult<Self> {
                    Ok(Self{inner: <#old_ident<#ty>>::new(shape)?})
                }

                fn new_numpy_array<'py>(&mut self, py: Python<'py>) -> PyResult<&'py numpy::PyArrayDyn<#ty>> {
                    self.inner.new_numpy_array(py)
                }

                fn is_valid<'py>(&self, py: Python<'py>) -> &'py pyo3::types::PyBool {
                    self.inner.is_valid(py)
                }

                fn update_strideshape(&mut self, view: &numpy::PyArrayDyn<#ty>) -> PyResult<()> {
                    self.inner.update_strideshape(view)
                }
            }

            impl #new_ident {
                fn new_from_shared(shared: sharify::SharedArrayMut<#ty, ndarray::IxDyn>) -> PyResult<Self> {
                    Ok(Self {
                        inner: <#old_ident<#ty>>::new_from_shared(shared)?,
                    })
                }

                fn invalidate(&mut self) -> Result<sharify::SharedArrayMut<#ty, ndarray::IxDyn>, ()> {
                    self.inner.invalidate()
                }
            }
        };
        stream.extend(add);
    }
    TokenStream::from(stream)
}

#[proc_macro]
pub fn py_send_topitype(_item: TokenStream) -> TokenStream {
    let stream = quote! {
        match object {
            TopiType::String(s) => messenger.send(recv_id, s, metadata).map_err(topi_base_err_to_py_err),
        }
    };
    let mut expr_match: ExprMatch = parse2(stream).unwrap();
    let numpy_variants = [
        "NumpyArrayBool",
        "NumpyArrayUInt8",
        "NumpyArrayUInt16",
        "NumpyArrayUInt32",
        "NumpyArrayUInt64",
        "NumpyArrayInt8",
        "NumpyArrayInt16",
        "NumpyArrayInt32",
        "NumpyArrayInt64",
        "NumpyArrayFloat32",
        "NumpyArrayFloat64",
    ];
    let shared_mut_variants = [
        "SharedArrayMutBool",
        "SharedArrayMutUInt8",
        "SharedArrayMutUInt16",
        "SharedArrayMutUInt32",
        "SharedArrayMutUInt64",
        "SharedArrayMutInt8",
        "SharedArrayMutInt16",
        "SharedArrayMutInt32",
        "SharedArrayMutInt64",
        "SharedArrayMutFloat32",
        "SharedArrayMutFloat64",
    ];
    for &variant in numpy_variants.iter() {
        let ident = format_ident!("{}", variant);
        let add = quote! {
            TopiType::#ident(arr) => {messenger
                .send(recv_id, arr.as_array(), metadata)
                .map_err(topi_base_err_to_py_err)}
        };
        expr_match.arms.push(parse2::<Arm>(add).unwrap());
    }
    for &variant in shared_mut_variants.iter() {
        let ident = format_ident!("{}", variant);
        let add = quote! {
            TopiType::#ident(mut cell) => {
                if let Ok(arr) = cell.invalidate() {
                    messenger
                        .send(recv_id, arr, metadata)
                        .map_err(topi_base_err_to_py_err)
                } else {
                    Err(PyException::new_err("Can't send shared array twice."))
                }
            }
        };
        expr_match.arms.push(parse2::<Arm>(add).unwrap());
    }
    let stream = quote! {#expr_match};
    TokenStream::from(stream)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn impl_TopiType_for_arrays(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttributeArgs);
    let item = parse_macro_input!(item as ItemTrait);
    if item.ident != "TopiType" {
        panic!("Macro can only be applied to the 'TopiType' trait.")
    }
    let mut stream = quote! {
        #item
    };
    for (ty, ty_str) in attr.into_iter().filter_map(|a| {
        if let NestedMeta::Meta(Meta::Path(Path { mut segments, .. })) = a {
            if let Some(PathSegment { ident, .. }) = segments.first_mut() {
                let ty: Type = parse_quote!(#ident);
                Some((ty, ident.to_string()))
            } else {
                None
            }
        } else {
            panic!("Unexpected attribute argument.");
        }
    }) {
        let array_mut = format_ident!("ArrayMut{}", type_str_to_content_variant_suffix(&ty_str));
        let array_shared =
            format_ident!("SharedArray{}", type_str_to_content_variant_suffix(&ty_str));
        let array_shared_mut = format_ident!(
            "SharedArrayMut{}",
            type_str_to_content_variant_suffix(&ty_str)
        );
        let add = quote! {
            impl<U> TopiType<U> for ndarray::Array<#ty, IxDyn> {
                fn into_content(self) -> Content<U> {
                    Content::#array_mut(self)
                }
            }

            impl<'a, U> TopiType<U> for ndarray::ArrayView<'a, #ty, IxDyn> {
                fn into_content(self) -> Content<U> {
                    Content::#array_mut(self.into_owned())
                }
            }

            impl<'a, U> TopiType<U> for ndarray::ArrayViewMut<'a, #ty, IxDyn> {
                fn into_content(self) -> Content<U> {
                    Content::#array_mut(self.into_owned())
                }
            }

            impl<U> TopiType<U> for SharedArray<#ty, IxDyn> {
                fn into_content(self) -> Content<U> {
                    Content::#array_shared(self)
                }
            }

            impl<U> TopiType<U> for SharedArrayMut<#ty, IxDyn> {
                fn into_content(self) -> Content<U> {
                    Content::#array_shared_mut(self)
                }
            }
        };
        stream.extend(add);
    }
    TokenStream::from(stream)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn impl_Sealed_for_arrays(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttributeArgs);
    let item = parse_macro_input!(item as ItemTrait);
    if item.ident != "Sealed" {
        panic!("Macro can only be applied to the 'Sealed' trait.")
    }
    let mut stream = quote! {
        #item
    };
    for ty in attr.into_iter().filter_map(|a| {
        if let NestedMeta::Meta(Meta::Path(Path { mut segments, .. })) = a {
            if let Some(PathSegment { ident, .. }) = segments.first_mut() {
                let ty: Type = parse_quote!(#ident);
                Some(ty)
            } else {
                None
            }
        } else {
            panic!("Unexpected attribute argument.");
        }
    }) {
        let add = quote! {
            impl<D: Dimension> Sealed for ndarray::Array<#ty, D> {}

            impl<'a, D: Dimension> Sealed for ndarray::ArrayView<'a, #ty, D> {}

            impl<'a, D: Dimension> Sealed for ndarray::ArrayViewMut<'a, #ty, D> {}

            impl<D: Dimension + Serialize + DeserializeOwned> Sealed for SharedArray<#ty, D> {}

            impl<D: Dimension + Serialize + DeserializeOwned> Sealed for SharedArrayMut<#ty, D> {}
        };
        stream.extend(add);
    }
    TokenStream::from(stream)
}

fn type_str_to_content_variant_suffix(ty_str: &str) -> &'static str {
    match ty_str {
        "bool" => "Bool",
        "u8" => "UInt8",
        "u16" => "UInt16",
        "u32" => "UInt32",
        "u64" => "UInt64",
        "i8" => "Int8",
        "i16" => "Int16",
        "i32" => "Int32",
        "i64" => "Int64",
        "f32" => "Float32",
        "f64" => "Float64",
        _ => panic!("Unexpected type string."),
    }
}
