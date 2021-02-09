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
    let mut result = SparseNodeSet::empty(G.node_count());
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
    let mut result = DenseNodeSet::empty(G.node_count());
    for target in 0..G.node_count() {
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
            let mut result = <$node_map>::empty(G.node_count());
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
