use super::*;
use downcast_rs::{impl_downcast, Downcast};
use rayon::prelude::*;
use std::sync::{Arc, Mutex};

#[path = "node_set.rs"]
mod node_set;

pub(crate) use node_set::NodeSubset;

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
pub(crate) struct SparseNodeSet {
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

pub fn par_vec<T: Send>(len: usize, f: impl Fn(usize) -> T + Send + Sync) -> Vec<T> {
    let mut data = Vec::with_capacity(len);
    (0..len).into_par_iter().map(f).collect_into_vec(&mut data);
    data
}

pub fn par_vec_from<T: Send + Sync>(len: usize, f: impl Fn() -> T + Send + Sync) -> Vec<T> {
    let mut data = Vec::with_capacity(len);
    (0..len)
        .into_par_iter()
        .map(|_| f())
        .collect_into_vec(&mut data);
    data
}

#[allow(non_snake_case)]
pub(crate) fn relationship_map(
    G: &Graph,
    mut U: NodeSubset,
    F: impl Fn(usize, usize) -> bool + Send + Sync,
    C: impl Fn(usize) -> bool + Send + Sync,
) -> NodeSubset {
    let subset_size = U.len(); // m

    // TODO: dense iter implementation
    U.to_sparse();
    let degrees = par_vec(subset_size, |i| {
        let node_id = U.node(i);
        G.out_degree(node_id)
    });

    let out_degrees = degrees.par_iter().sum::<usize>();

    if out_degrees > G.threshold() {
        U.to_dense();
        relationship_map_dense(G, U, F, C)
    } else {
        relationship_map_sparse(G, U, degrees, F, C)
    }
}

#[allow(non_snake_case)]
fn relationship_map_sparse(
    G: &Graph,
    U: NodeSubset,
    mut degrees: Vec<usize>,
    F: impl Fn(usize, usize) -> bool + Send + Sync,
    C: impl Fn(usize) -> bool + Send + Sync,
) -> NodeSubset {
    let subset_size = U.len();

    // TODO: parallel
    let out_rel_count = degrees
        .par_iter_mut()
        .fold_with(0_usize, |sum, degree| {
            *degree += sum;
            *degree
        })
        .sum::<usize>();
    let offsets = degrees;

    // TODO: we could technically collect into the final
    // let mut out_rels = Vec::with_capacity(out_rel_count);
    let subset_nodes = U.nodes();

    // subset_nodes
    //     .par_iter()
    //     .zip(offsets.into_par_iter())
    //     .for_each(|(&node_id, offset)| {

    //         G.out(node_id)
    //             .par_iter()
    //             .enumerate()
    //             .for_each(op)

    //     });

    // TODO: replace with atomic usize and lock-free writes
    let out_rels = Arc::new(Mutex::new(Vec::<usize>::with_capacity(out_rel_count)));

    subset_nodes
        .par_iter()
        // .zip(offsets.into_par_iter())
        .for_each_with(Arc::clone(&out_rels), |out, &node_id| {
            // let source = node_id.clone();
            // TODO: parallel if d > 1000
            G.out(node_id)
                .par_iter()
                // .enumerate()
                .filter(|&&target| C(target) && F(node_id, target))
                .for_each(|&target| {
                    let mut out = out.lock().unwrap();
                    out.push(target);
                })
        });

    NodeSubset::sparse(
        U.node_count(),
        Arc::try_unwrap(out_rels).unwrap().into_inner().unwrap(),
    )
}

#[allow(non_snake_case)]
fn relationship_map_dense(
    G: &Graph,
    U: NodeSubset,
    mut F: impl FnMut(usize, usize) -> bool,
    C: impl Fn(usize) -> bool,
) -> NodeSubset {
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

    result;

    todo!()
}

#[allow(non_snake_case)]
pub(crate) fn node_map(
    G: &Graph,
    U: NodeSubset,
    F: impl Fn(usize) -> bool + Send + Sync,
) -> NodeSubset {
    let node_count = U.node_count();
    let subset_count = U.non_zeroes_count();

    if U.is_dense() {
        let dense = par_vec(node_count, F);
        NodeSubset::dense(node_count, dense)
    } else {
        let mut sparse = par_vec(
            subset_count,
            |node_id| if F(node_id) { node_id } else { usize::MAX },
        );

        let mut write_index = 0;
        for read_index in 0..subset_count {
            let value = sparse[read_index];
            if value != usize::MAX {
                // if write_index < read_index {
                sparse[write_index] = value;
                // }
                write_index += 1;
            }
        }

        sparse.truncate(write_index);
        NodeSubset::sparse_counted(node_count, write_index, sparse)
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
