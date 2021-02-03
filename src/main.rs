#![feature(array_windows)]

#[macro_use]
extern crate eyre;

use argh::FromArgs;
use atoi::FromRadix10;
use eyre::Result;
use linereader::LineReader;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fs::File,
    iter::{once, FromIterator},
    path::PathBuf,
    time::Instant,
};

#[derive(FromArgs)]
/// opts
struct Opts {
    /// input file
    #[argh(positional)]
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

    #[allow(dead_code)]
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
    nodes: Vec<Node>,
}

impl AdjacencyList {
    fn degree(&self, node: usize) -> usize {
        self.nodes[node].degree
    }

    fn rels(&self, node: usize) -> &[usize] {
        let node = self.nodes[node];
        let start = node.offset;
        let end = node.degree + start;
        &self.targets[start..end]
    }
}

#[derive(Debug, Clone, Copy)]
struct Node {
    degree: usize,
    offset: usize,
}

impl<R> TryFrom<LineReader<R>> for AdjacencyList
where
    R: std::io::Read,
{
    type Error = eyre::Report;

    fn try_from(mut lines: LineReader<R>) -> Result<Self> {
        let header = lines.next_line().expect("missing header line")?;
        ensure!(
            header == b"AdjacencyGraph\n",
            "Can only read AdjacencyGraph files but got {:?}",
            std::str::from_utf8(header)
        );

        let node_count = lines.next_line().expect("missing node count")?;
        let node_count = atoi::atoi::<usize>(node_count).expect("invalid node count");

        let rel_count = lines.next_line().expect("missing relationship count")?;
        let rel_count = atoi::atoi::<usize>(rel_count).expect("invalid relationship count");

        let mut offsets = Vec::with_capacity(node_count);
        let mut targets = Vec::with_capacity(rel_count);

        let mut batch = lines.next_batch().expect("missing graph data")?;

        while offsets.len() < node_count {
            match usize::from_radix_10(batch) {
                (_, 0) => {
                    batch = lines.next_batch().expect("missing offsets")?;
                }
                (num, used) => {
                    offsets.push(num);
                    batch = &batch[used + 1..];
                }
            };
        }

        while targets.len() < rel_count {
            match usize::from_radix_10(batch) {
                (_, 0) => {
                    batch = lines.next_batch().expect("missing targets")?;
                }
                (num, used) => {
                    targets.push(num);
                    batch = &batch[used + 1..];
                }
            };
        }

        Ok(Self::from((offsets, targets)))
    }
}

impl From<(Vec<usize>, Vec<usize>)> for AdjacencyList {
    fn from((offsets, targets): (Vec<usize>, Vec<usize>)) -> Self {
        let node_count = offsets.len();
        let rel_count = targets.len();

        let last_offset = *offsets.last().unwrap();
        let last_node = Node {
            offset: last_offset,
            degree: rel_count - last_offset,
        };

        let mut nodes = Vec::with_capacity(node_count);
        offsets
            .par_windows(2)
            .map(|offset_pair| match offset_pair {
                &[offset, next_offset] => Node {
                    offset,
                    degree: next_offset - offset,
                },
                _ => unreachable!("windows size is 2"),
            })
            .collect_into_vec(&mut nodes);
        nodes.push(last_node);

        AdjacencyList {
            offsets,
            targets,
            nodes,
        }
    }
}

impl From<AdjacencyList> for Graph {
    fn from(out: AdjacencyList) -> Self {
        let inc = out.invert();
        Graph {
            node_count: out.offsets.len(),
            rel_count: out.targets.len(),
            out,
            inc,
        }
    }
}

impl AdjacencyList {
    fn invert(&self) -> Self {
        let node_count = self.nodes.len();
        let rel_count = self.targets.len();

        let mut temp = Vec::with_capacity(rel_count);
        temp.resize(rel_count, (usize::max_value(), usize::max_value()));

        self.nodes
            .par_iter()
            .enumerate()
            .for_each(|(source, node)| {
                let offset = node.offset;
                let next_off = offset + node.degree;
                for (&target, tmp) in self.targets[offset..next_off]
                    .iter()
                    .zip(&mut temp[offset..next_off])
                {
                    *tmp = (target, source);
                }
            });

        // let last_entry = [*self.offsets.last().unwrap(), rel_count];
        // for (source, &[offset, next_off]) in self
        //     .offsets
        //     .array_windows::<2>()
        //     .chain(once(&last_entry))
        //     .enumerate()
        // {
        //     for (&target, tmp) in self.targets[offset..next_off]
        //         .iter()
        //         .zip(&mut temp[offset..next_off])
        //     {
        //         *tmp = (target, source);
        //     }
        // }

        temp.sort_by_key(|(target, _)| *target);

        let mut offsets = Vec::with_capacity(node_count);
        let mut targets = Vec::with_capacity(rel_count);

        let mut last_target = usize::max_value();

        for (target, source) in temp.into_iter() {
            while target != last_target {
                offsets.push(targets.len());
                last_target = last_target.wrapping_add(1);
            }

            targets.push(source);
        }

        offsets.extend(std::iter::repeat(targets.len()).take(node_count - last_target));

        Self::from((offsets, targets))
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
    let mut start = Instant::now();

    let opts: Opts = argh::from_env();
    let file = File::open(opts.input)?;

    println!("preparing input: {:?}", start.elapsed());
    start = Instant::now();

    let adjacencies = AdjacencyList::try_from(LineReader::new(file))?;

    println!("parsing input: {:?}", start.elapsed());
    start = Instant::now();

    let graph = Graph::from(adjacencies);

    println!("building full graph: {:?}", start.elapsed());
    start = Instant::now();

    let cc = cc::cc(graph);

    println!("cc done with {} nodes: {:?}", cc.len(), start.elapsed());
    // eprintln!("cc = {:#?}", cc);

    Ok(())
}
