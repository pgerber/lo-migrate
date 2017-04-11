//! Postgres to S3 migration tool for the Tocco Business Framework
//!
//! # Frankly, this library is design to do this using multiple threads
//!
//! * Fetch large object from Postgres
//! * Store binaries in S3
//! * Calculate sha2 hashes
//! * commit sha2 hashes to Postgres

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#![cfg_attr(feature = "try_from", feature(try_from))]
#![cfg_attr(feature = "integer_atomics", feature(integer_atomics))]

#![warn(trivial_casts)]
#![warn(trivial_numeric_casts)]
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
#![cfg_attr(feature = "clippy", allow(new_without_default_derive))]
#![cfg_attr(feature = "clippy", warn(cast_possible_truncation))]
#![cfg_attr(feature = "clippy", warn(cast_possible_wrap))]
#![cfg_attr(feature = "clippy", warn(cast_precision_loss))]
#![cfg_attr(feature = "clippy", warn(cast_sign_loss))]
#![cfg_attr(feature = "clippy", warn(empty_enum))]
#![cfg_attr(feature = "clippy", warn(enum_glob_use))]
#![cfg_attr(feature = "clippy", warn(float_arithmetic))]
#![cfg_attr(feature = "clippy", warn(items_after_statements))]
#![cfg_attr(feature = "clippy", warn(if_not_else))]
#![cfg_attr(feature = "clippy", deny(mem_forget))]
#![cfg_attr(feature = "clippy", warn(mut_mut))]
#![cfg_attr(feature = "clippy", warn(nonminimal_bool))]
#![cfg_attr(feature = "clippy", warn(option_map_unwrap_or))]
#![cfg_attr(feature = "clippy", warn(option_map_unwrap_or_else))]
#![cfg_attr(feature = "clippy", warn(option_unwrap_used))]
#![cfg_attr(feature = "clippy", warn(print_stdout))]
#![cfg_attr(feature = "clippy", warn(result_unwrap_used))]
#![cfg_attr(feature = "clippy", deny(unicode_not_nfc))]
#![cfg_attr(feature = "clippy", deny(unseparated_literal_suffix))]
#![cfg_attr(feature = "clippy", warn(single_match_else))]
#![cfg_attr(feature = "clippy", deny(used_underscore_binding))]
#![cfg_attr(feature = "clippy", deny(wrong_pub_self_convention))]
#![cfg_attr(feature = "clippy", deny(wrong_self_convention))]

extern crate aws_sdk_rust;
extern crate chrono;
#[macro_use]
extern crate derive_error;
extern crate digest;
extern crate fallible_iterator;
extern crate mkstemp;
extern crate postgres;
extern crate postgres_large_object;
extern crate rustc_serialize as serialize;
extern crate sha2;
extern crate memmap;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate two_lock_queue;

mod commit;
pub mod error;
mod lo;
mod store;
mod receive;
pub mod thread;

use aws_sdk_rust::aws::s3;
use aws_sdk_rust::aws;

pub use lo::Lo;
