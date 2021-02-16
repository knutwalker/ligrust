use super::*;
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    let degrees = if U.is_dense() {
        par_vec(U.node_count(), |node_id| {
            if U.contains(node_id) {
                G.out_degree(node_id)
            } else {
                0
            }
        })
    } else {
        par_vec(subset_size, |i| {
            let node_id = U.node(i);
            G.out_degree(node_id)
        })
    };

    let out_degrees = degrees.par_iter().sum::<usize>();

    if out_degrees > G.threshold() {
        U.to_dense();
        relationship_map_dense(G, U, F, C)
    } else {
        relationship_map_sparse(G, U, degrees, F, C)
    }
}

#[allow(non_snake_case)]
#[cfg(feature = "sparse_atomic_pack")]
fn relationship_map_sparse(
    G: &Graph,
    U: NodeSubset,
    degrees: Vec<usize>,
    F: impl Fn(usize, usize) -> bool + Send + Sync,
    C: impl Fn(usize) -> bool + Send + Sync,
) -> NodeSubset {
    let out_rel_count = degrees.into_par_iter().sum::<usize>();
    let out_rels = par_vec_with(out_rel_count, || AtomicUsize::new(usize::MAX));

    let write_idx = AtomicUsize::default();
    U.nodes().par_iter().for_each(|&source| {
        G.out(source).par_iter().for_each(|&target| {
            if C(target) && F(source, target) {
                let idx = write_idx.fetch_add(1, Ordering::SeqCst);
                out_rels[idx].store(target, Ordering::SeqCst);
            }
        })
    });

    let mut out_rels = unsafe { std::mem::transmute::<Vec<AtomicUsize>, Vec<usize>>(out_rels) };
    let write_idx = write_idx.load(Ordering::SeqCst);

    out_rels.truncate(write_idx);

    NodeSubset::sparse(U.node_count(), out_rels)
}

#[allow(non_snake_case)]
#[cfg(not(feature = "sparse_atomic_pack"))]
fn relationship_map_sparse(
    G: &Graph,
    U: NodeSubset,
    mut degrees: Vec<usize>,
    F: impl Fn(usize, usize) -> bool + Send + Sync,
    C: impl Fn(usize) -> bool + Send + Sync,
) -> NodeSubset {
    // before [1 3 3  7]
    // after  [1 4 7 14]
    let out_rel_count = degrees
        .par_iter_mut()
        .fold_with(0_usize, |sum, degree| {
            *degree += sum;
            *degree
        })
        .sum::<usize>();

    let offsets = degrees;

    let out_rels = par_vec_with(out_rel_count, || AtomicUsize::new(usize::MAX));

    U.nodes()
        .par_iter()
        .zip(offsets.into_par_iter())
        .for_each(|(node_id, offset)| {
            let source = *node_id;
            G.out(source)
                .par_iter()
                .enumerate()
                .for_each(|(j, &target)| {
                    if C(target) && F(source, target) {
                        out_rels[offset + j].store(target, Ordering::SeqCst)
                    }
                })
        });

    let mut out_rels = unsafe { std::mem::transmute::<Vec<AtomicUsize>, Vec<usize>>(out_rels) };
    let mut write_idx = 0;

    // pack non-max values together
    for i in 0..out_rels.len() {
        let target = out_rels[i];
        if target != usize::MAX {
            out_rels[write_idx] = target;
            write_idx += 1;
        }
    }

    out_rels.truncate(write_idx);

    NodeSubset::sparse(U.node_count(), out_rels)
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
