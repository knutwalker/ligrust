#![feature(array_windows, never_type, new_uninit, vec_into_raw_parts)]

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

#[macro_use]
extern crate eyre;

use eyre::Result;

pub mod algos;
mod cli;
pub mod graph;
pub mod ligra;

fn main() -> Result<()> {
    cli::main()
}
