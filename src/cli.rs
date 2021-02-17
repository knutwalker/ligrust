use crate::{algos, graph, Result};
use pico_args::Arguments;
use std::{ffi::OsStr, path::PathBuf};

struct Opts {
    command: Command,
}

impl Opts {
    fn parse_from_pico() -> Result<Self> {
        let mut args = Arguments::from_env();

        fn as_path_buf(arg: &OsStr) -> Result<PathBuf, !> {
            Ok(PathBuf::from(arg))
        }

        match args.subcommand()? {
            Some(c) if c.as_str() == "parse" => {
                let output = args.value_from_os_str(["-o", "--output"], as_path_buf)?;
                let input = args.free_from_os_str(as_path_buf)?;
                let free = args.finish();
                if !free.is_empty() {
                    bail!("Unexpected arguments: {:?}", free);
                }
                let command = Command::Parse(ParseInput { input, output });
                Ok(Self { command })
            }
            Some(c) if c.as_str() == "cc" => {
                let input = args.free_from_os_str(as_path_buf)?;
                let free = args.finish();
                if !free.is_empty() {
                    bail!("Unexpected arguments: {:?}", free);
                }
                let command = Command::CC(RunCC { input });
                Ok(Self { command })
            }
            Some(c) if c.as_str() == "bfs" => {
                let source: usize = args.value_from_str(["-s", "--source"])?;
                let input = args.free_from_os_str(as_path_buf)?;
                let free = args.finish();
                if !free.is_empty() {
                    bail!("Unexpected arguments: {:?}", free);
                }
                let command = Command::BFS(RunBFS { input, source });
                Ok(Self { command })
            }
            Some(c) if c.as_str() == "prd" => {
                let max_iterations: usize = args.value_from_str(["-i", "--iterations"])?;
                let input = args.free_from_os_str(as_path_buf)?;
                let free = args.finish();
                if !free.is_empty() {
                    bail!("Unexpected arguments: {:?}", free);
                }
                let command = Command::PageRankDelta(RunPageRankDelta {
                    input,
                    max_iterations,
                });
                Ok(Self { command })
            }
            _ => {
                bail!("invalid command, use either parse, cc or bfs")
            }
        }
    }
}

enum Command {
    Parse(ParseInput),
    CC(RunCC),
    BFS(RunBFS),
    PageRankDelta(RunPageRankDelta),
}

/// Parses an input file and dump a binary representation of the graph
struct ParseInput {
    /// input file in "AdjacencyGraph" format
    input: PathBuf,

    /// output file where to dump the graph to
    output: PathBuf,
}

/// Run conncected components on a parsed input
struct RunCC {
    /// input file in "AdjacencyGraph" format
    input: PathBuf,
}

/// Run BFS on a parsed input
struct RunBFS {
    /// input file in "AdjacencyGraph" format
    input: PathBuf,
    /// source node to run BFS from
    source: usize,
}

/// Run PageRankDelta on a parsed input
struct RunPageRankDelta {
    /// input file in "AdjacencyGraph" format
    input: PathBuf,
    /// maximum number of iterations to run
    max_iterations: usize,
}

pub fn main() -> Result<()> {
    let opts = Opts::parse_from_pico()?;
    match opts.command {
        Command::Parse(opts) => graph::parse(opts.input, opts.output),
        Command::CC(opts) => algos::run_cc(opts.input),
        Command::BFS(opts) => algos::run_bfs(opts.input, opts.source),
        Command::PageRankDelta(opts) => algos::run_page_rank_delta(opts.input, opts.max_iterations),
    }
}
