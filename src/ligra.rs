use super::*;
use rayon::prelude::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

#[path = "node_set.rs"]
mod node_set;

pub(crate) use node_set::NodeSubset;

pub trait NodeSet {
    fn empty(node_count: usize) -> Self
    where
        Self: Sized;

    fn len(&self) -> usize;

    fn add(&mut self, target: usize);

    fn contains(&self, value: usize) -> bool;

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a>;
}

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

pub fn par_vec_with<T: Send + Sync>(len: usize, f: impl Fn() -> T + Send + Sync) -> Vec<T> {
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
    degrees: Vec<usize>,
    F: impl Fn(usize, usize) -> bool + Send + Sync,
    C: impl Fn(usize) -> bool + Send + Sync,
) -> NodeSubset {
    // TODO: parallel
    let out_rel_count = degrees.into_par_iter().sum::<usize>();

    // TODO: replace with atomic usize and lock-free writes
    let out_rels = Arc::new(Mutex::new(Vec::<usize>::with_capacity(out_rel_count)));

    U.nodes()
        .par_iter()
        .for_each_with(Arc::clone(&out_rels), |out, &node_id| {
            // TODO: parallel if d > 1000
            G.out(node_id)
                .par_iter()
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
    F: impl Fn(usize, usize) -> bool + Send + Sync,
    C: impl Fn(usize) -> bool + Send + Sync,
) -> NodeSubset {
    let node_count = G.node_count();

    let next = par_vec_with(node_count, AtomicBool::default);

    (0..node_count).into_par_iter().for_each(|target| {
        if C(target) {
            for &source in G.inc(target) {
                if U.contains(source) && F(source, target) {
                    next[source].store(true, Ordering::SeqCst);
                }
                if !C(target) {
                    break;
                }
            }
        }
    });

    let next = unsafe { std::mem::transmute::<Vec<AtomicBool>, Vec<bool>>(next) };

    NodeSubset::dense(node_count, next)
}

#[allow(non_snake_case)]
pub(crate) fn node_map(U: NodeSubset, F: impl Fn(usize) -> bool + Send + Sync) -> NodeSubset {
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
