#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

// #![feature(try_from)]

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

pub mod common;
pub mod lo;
pub mod retrieve;
pub mod store;
pub mod commit;

pub use common::Result;
pub use aws_sdk_rust::aws::s3;
pub use aws_sdk_rust::aws;
