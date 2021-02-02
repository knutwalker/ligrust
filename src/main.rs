#![feature(array_windows)]

use argh::FromArgs;
use eyre::Result;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    iter::{once, FromIterator},
    path::PathBuf,
};

#[derive(FromArgs)]
/// opts
struct Opts {
    /// input file
    #[argh(option)]
    input: PathBuf,
}

#[derive(Debug)]
struct Graph {
    node_count: usize,
    rel_count: usize,
    out: AdjacencyList,
    inc: AdjacencyList,
}

impl Graph {
    fn out(&self, node: usize) -> &[usize] {
        self.out.rels(node)
    }

    fn inc(&self, node: usize) -> &[usize] {
        self.inc.rels(node)
    }

    fn out_degree(&self, node: usize) -> usize {
        self.out.degree(node)
    }

    fn inc_degree(&self, node: usize) -> usize {
        self.inc.degree(node)
    }

    fn threshold(&self) -> usize {
        self.rel_count / 20
    }
}

#[derive(Debug)]
struct AdjacencyList {
    offsets: Vec<usize>,
    targets: Vec<usize>,
}

impl AdjacencyList {
    fn degree(&self, node: usize) -> usize {
        let start = self.offsets[node];
        let end = self
            .offsets
            .get(node + 1)
            .copied()
            .unwrap_or(self.targets.len());
        end - start
    }
    fn rels(&self, node: usize) -> &[usize] {
        let start = self.offsets[node];
        let end = self
            .offsets
            .get(node + 1)
            .copied()
            .unwrap_or(self.targets.len());
        &self.targets[start..end]
    }
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

pub(crate) mod ligra {
    use super::*;
    use downcast_rs::{impl_downcast, Downcast};

    pub trait NodeSet: Downcast {
        fn empty(node_count: usize) -> Self
        where
            Self: Sized;

        fn len(&self) -> usize;

        fn add(&mut self, target: usize);

        fn contains(&self, value: usize) -> bool;

        fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a>;
    }

    impl_downcast!(NodeSet);

    #[derive(Debug, Default)]
    struct SparseNodeSet {
        values: Vec<usize>,
    }

    impl NodeSet for SparseNodeSet {
        fn empty(_node_count: usize) -> Self {
            Self::default()
        }

        fn len(&self) -> usize {
            self.values.len()
        }

        fn add(&mut self, target: usize) {
            self.values.push(target);
        }

        fn contains(&self, value: usize) -> bool {
            self.values.contains(&value)
        }

        fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a> {
            Box::new(self.values.iter().copied())
        }
    }

    pub(crate) struct DenseNodeSet {
        values: Vec<bool>,
        cardinality: usize,
    }

    impl DenseNodeSet {
        pub(crate) fn full(node_count: usize) -> Self {
            Self {
                values: vec![true; node_count],
                cardinality: node_count,
            }
        }
    }

    impl NodeSet for DenseNodeSet {
        fn empty(node_count: usize) -> Self {
            Self {
                values: vec![false; node_count],
                cardinality: 0,
            }
        }

        fn len(&self) -> usize {
            self.cardinality
        }

        fn add(&mut self, target: usize) {
            self.cardinality += 1;
            self.values[target] = true;
        }

        fn contains(&self, value: usize) -> bool {
            self.values[value]
        }

        fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a> {
            Box::new(
                self.values
                    .iter()
                    .enumerate()
                    .filter_map(|(value, contains)| if *contains { Some(value) } else { None }),
            )
        }
    }

    #[allow(non_snake_case)]
    pub(crate) fn relationship_map(
        G: &Graph,
        U: &dyn NodeSet,
        F: impl FnMut(usize, usize) -> bool,
        C: impl Fn(usize) -> bool,
    ) -> Box<dyn NodeSet> {
        let cost = U.len() + U.iter().map(|node| G.out_degree(node)).sum::<usize>();
        if cost > G.threshold() {
            Box::new(relationship_map_dense(G, U, F, C))
        } else {
            Box::new(relationship_map_sparse(G, U, F, C))
        }
    }

    #[allow(non_snake_case)]
    fn relationship_map_sparse(
        G: &Graph,
        U: &dyn NodeSet,
        mut F: impl FnMut(usize, usize) -> bool,
        C: impl Fn(usize) -> bool,
    ) -> SparseNodeSet {
        let mut result = SparseNodeSet::empty(G.node_count);
        for source in U.iter() {
            for &target in G.out(source) {
                if C(target) && F(source, target) {
                    result.add(target);
                }
            }
        }

        // TODO: distinct

        result
    }

    #[allow(non_snake_case)]
    fn relationship_map_dense(
        G: &Graph,
        U: &dyn NodeSet,
        mut F: impl FnMut(usize, usize) -> bool,
        C: impl Fn(usize) -> bool,
    ) -> DenseNodeSet {
        let mut result = DenseNodeSet::empty(G.node_count);
        for target in 0..G.node_count {
            if C(target) {
                for &source in G.inc(target) {
                    if U.contains(source) && F(source, target) {
                        result.add(target);
                    }
                    if !C(target) {
                        break;
                    }
                }
            }
        }

        result
    }

    #[allow(non_snake_case)]
    pub(crate) fn node_map(
        G: &Graph,
        U: &dyn NodeSet,
        F: impl FnMut(usize) -> bool,
    ) -> Box<dyn NodeSet> {
        if let Some(sparse) = U.downcast_ref::<SparseNodeSet>() {
            Box::new(node_map_sparse(G, sparse, F))
        } else if let Some(dense) = U.downcast_ref::<DenseNodeSet>() {
            Box::new(node_map_dense(G, dense, F))
        } else {
            unreachable!("there is a new node set in town")
        }
    }

    macro_rules! node_map_impl {
        ($name:ident: $node_map:ty) => {
            #[allow(non_snake_case)]
            fn $name(G: &Graph, U: &$node_map, mut F: impl FnMut(usize) -> bool) -> $node_map {
                let mut result = <$node_map>::empty(G.node_count);
                for node in U.iter() {
                    if F(node) {
                        result.add(node);
                    }
                }
                result
            }
        };
    }

    node_map_impl!(node_map_sparse: SparseNodeSet);
    node_map_impl!(node_map_dense: DenseNodeSet);
}

mod cc {
    use super::*;

    struct CC {
        ids: Vec<usize>,
        prev_ids: Vec<usize>,
    }

    impl CC {
        fn new(node_count: usize) -> Self {
            Self {
                ids: (0..node_count).collect(),
                prev_ids: vec![0; node_count],
            }
        }

        fn update(&mut self, source: usize, target: usize) -> bool {
            let original_id = self.ids[target];
            if self.ids[source] < original_id {
                self.ids[target] = self.ids[source];
                original_id == self.prev_ids[target]
            } else {
                false
            }
        }

        fn copy(&mut self, node: usize) -> bool {
            self.prev_ids[node] = self.ids[node];
            true
        }
    }

    #[allow(non_snake_case)]
    pub(crate) fn cc(G: Graph) -> Vec<usize> {
        let mut cc = CC::new(G.node_count);

        let frontier = ligra::DenseNodeSet::full(G.node_count);
        let mut frontier: Box<dyn ligra::NodeSet> = Box::new(frontier);

        while frontier.len() != 0 {
            frontier = ligra::node_map(&G, &*frontier, |node| cc.copy(node));
            frontier = ligra::relationship_map(&G, &*frontier, |s, t| cc.update(s, t), |_| true);
        }

        cc.ids
    }
}

fn main() -> Result<()> {
    let opts: Opts = argh::from_env();
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
    let cc = cc::cc(graph);

    eprintln!("cc = {:#?}", cc);

    Ok(())
}
