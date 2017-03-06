#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#![feature(try_from)]
#![feature(integer_atomics)]

// #![warn(missing_docs)]
#![deny(unused_must_use)]
#![deny(const_err)]
#![deny(legacy_directory_ownership)]
#![deny(legacy_imports)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
#![deny(non_upper_case_globals)]
#![deny(patterns_in_fns_without_body)]
#![deny(private_in_public)]
#![deny(unused_must_use)]
#![deny(while_true)]

// clippy lints
#![cfg_attr(feature = "clippy", warn(cast_possible_truncation))]
#![cfg_attr(feature = "clippy", warn(cast_possible_wrap))]
#![cfg_attr(feature = "clippy", warn(cast_precision_loss))]
#![cfg_attr(feature = "clippy", warn(cast_sign_loss))]
#![cfg_attr(feature = "clippy", warn(empty_enum))]
#![cfg_attr(feature = "clippy", warn(enum_glob_use))]
#![cfg_attr(feature = "clippy", warn(float_arithmetic))]
#![cfg_attr(feature = "clippy", warn(if_not_else))]
// #![cfg_attr(feature = "clippy", warn(indexing_slicing))]
#![cfg_attr(feature = "clippy", deny(mem_forget))]
#![cfg_attr(feature = "clippy", warn(mut_mut))]
#![cfg_attr(feature = "clippy", warn(nonminimal_bool))]
#![cfg_attr(feature = "clippy", warn(option_map_unwrap_or))]
#![cfg_attr(feature = "clippy", warn(option_map_unwrap_or_else))]
// #![cfg_attr(feature = "clippy", warn(option_unwrap_used))]  // FIXME: we should enable this at some point
#![cfg_attr(feature = "clippy", warn(print_stdout))]
// #![cfg_attr(feature = "clippy", warn(result_unwrap_used))]  // FIXME: we should enable this at some point
#![cfg_attr(feature = "clippy", deny(unicode_not_nfc))]
#![cfg_attr(feature = "clippy", deny(unseparated_literal_suffix))]
#![cfg_attr(feature = "clippy", deny(used_underscore_binding))]
#![cfg_attr(feature = "clippy", deny(wrong_self_convention))]

extern crate aws_sdk_rust;
extern crate mktemp;
extern crate postgres;
extern crate postgres_large_object;
extern crate rustc_serialize as serialize;
extern crate sha1;
extern crate sha2;
extern crate memmap;
extern crate hyper;
extern crate base64;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
extern crate two_lock_queue;

pub mod common;
pub mod lo;
pub mod retrieve;
pub mod store;
pub mod commit;
pub mod thread;

pub use common::Result;
pub use aws_sdk_rust::aws::s3;
pub use aws_sdk_rust::aws;
