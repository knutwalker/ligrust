#![feature(array_windows)]

use clap::{
    AppSettings::{ColoredHelp, DeriveDisplayOrder, UnifiedHelpMessage},
    Clap,
};
use eyre::Result;
use fs::File;
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader},
    iter::{once, FromIterator},
    path::PathBuf,
};

#[derive(Clap, Debug)]
#[cfg_attr(test, derive(Default))]
#[clap(version, about, setting = ColoredHelp, setting = DeriveDisplayOrder, setting = UnifiedHelpMessage)]
pub(crate) struct Opts {
    /// input file
    #[clap()]
    input: PathBuf,
}

#[derive(Debug)]
struct Graph {
    node_count: usize,
    rel_count: usize,
    out: AdjacencyList,
    inc: AdjacencyList,
}

#[derive(Debug)]
struct AdjacencyList {
    offsets: Vec<usize>,
    targets: Vec<usize>,
}

#[derive(Debug)]
struct Node {
    degree: usize,
    offset: usize,
}

impl FromIterator<usize> for Graph {
    fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        let node_count = iter.next().expect("node count");
        let rel_count = iter.next().expect("rel count");
        let out_offsets = iter.by_ref().take(node_count).collect::<Vec<_>>();
        let out_targets = iter.by_ref().take(rel_count).collect::<Vec<_>>();
        assert_eq!(iter.count(), 0, "more stuff in the input");

        let mut incoming = HashMap::<_, Vec<_>>::new();
        let last_entry = [*out_offsets.last().unwrap(), rel_count];
        for (source, &[offset, next_off]) in out_offsets
            .array_windows::<2>()
            .chain(once(&last_entry))
            .enumerate()
        {
            for &target in &out_targets[offset..next_off] {
                incoming.entry(target).or_default().push(source);
            }
        }

        let mut in_offsets = Vec::with_capacity(node_count);
        let mut in_targets = Vec::with_capacity(rel_count);

        for n in 0..node_count {
            in_offsets.push(in_targets.len());
            if let Some(mut sources) = incoming.remove(&n) {
                sources.sort_unstable();
                in_targets.extend_from_slice(&sources);
            }
        }

        let out = AdjacencyList {
            offsets: out_offsets,
            targets: out_targets,
        };
        let inc = AdjacencyList {
            offsets: in_offsets,
            targets: in_targets,
        };

        Graph {
            node_count,
            rel_count,
            out,
            inc,
        }
    }
}

fn main() -> Result<()> {
    // if Term::stdout().features().is_attended() {
    //     color_eyre::config::HookBuilder::default()
    //         .display_env_section(false)
    //         .install()?
    // }

    let opts = Opts::parse();
    let file = File::open(opts.input)?;
    let input = BufReader::new(file);

    let mut input = input.lines();
    assert_eq!(
        "AdjacencyGraph",
        input.next().expect("empty input")?.as_str(),
        "Can only read AdjacencyGraph files"
    );

    fn parse_line(line: std::io::Result<String>) -> Result<Option<usize>> {
        let line = line?;
        let line = line.trim();
        Ok(if !line.is_empty() {
            let line = line.parse::<usize>()?;
            Some(line)
        } else {
            None
        })
    }

    let input = input
        .map(parse_line)
        .filter_map(|line| line.transpose())
        .collect::<Result<Vec<_>>>()?;

    let graph = input.into_iter().collect::<Graph>();

    eprintln!("graph = {:#?}", graph);

    Ok(())
}
