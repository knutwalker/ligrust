use crate::graph::Graph;
pub use node_set::NodeSubset;
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[path = "node_set.rs"]
mod node_set;

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

#[allow(dead_code)]
pub fn par_vec_copy<T: Clone + Send + Sync>(len: usize, initial_value: T) -> Vec<T> {
    let mut data = Vec::with_capacity(len);
    (0..len)
        .into_par_iter()
        .map(|_| initial_value.clone())
        .collect_into_vec(&mut data);
    data
}

pub trait RelationshipMapper {
    fn update(&self, source: usize, target: usize) -> bool;

    fn update_non_atomic(&self, source: usize, target: usize) -> bool {
        self.update(source, target)
    }

    fn check(&self, _target: usize) -> bool {
        true
    }

    fn update_always_returns_true(&self) -> bool {
        false
    }

    fn check_always_returns_true(&self) -> bool {
        false
    }

    fn has_no_result(&self) -> bool {
        self.update_always_returns_true()
    }
}

pub fn relationship_map<G, T>(graph: &G, node_subset: &mut NodeSubset, mapper: &T)
where
    G: Graph + Sync + ?Sized,
    T: RelationshipMapper + Sync + ?Sized,
{
    let subset_size = node_subset.subset_count();

    let degrees = if node_subset.is_dense() {
        par_vec(node_subset.node_count(), |node_id| {
            if node_subset.contains(node_id) {
                graph.out_degree(node_id)
            } else {
                0
            }
        })
    } else {
        par_vec(subset_size, |i| {
            let node_id = node_subset.node(i);
            graph.out_degree(node_id)
        })
    };

    let out_degrees = degrees.par_iter().sum::<usize>();

    if out_degrees > graph.threshold() {
        node_subset.to_dense();
        relationship_map_dense(graph, node_subset, mapper)
    } else {
        relationship_map_sparse(graph, node_subset, degrees, mapper)
    }
}

fn relationship_map_sparse<G, T>(
    graph: &G,
    node_subset: &mut NodeSubset,
    degrees: Vec<usize>,
    mapper: &T,
) where
    G: Graph + Sync + ?Sized,
    T: RelationshipMapper + Sync + ?Sized,
{
    if mapper.has_no_result() {
        node_subset.nodes().par_iter().for_each(|&source| {
            graph.out(source).par_iter().for_each(|&target| {
                if mapper.check(target) {
                    mapper.update(source, target);
                }
            })
        });
    } else {
        *node_subset = relationship_map_sparse_output(graph, node_subset, degrees, mapper);
    }
}

#[cfg(feature = "sparse_atomic_pack")]
fn relationship_map_sparse_output<G, T>(
    graph: &G,
    node_subset: &NodeSubset,
    degrees: Vec<usize>,
    mapper: &T,
) -> NodeSubset
where
    G: Graph + Sync + ?Sized,
    T: RelationshipMapper + Sync + ?Sized,
{
    let out_rel_count = degrees.into_par_iter().sum::<usize>();
    let out_rels = par_vec_with(out_rel_count, || AtomicUsize::new(usize::MAX));

    let write_idx = AtomicUsize::default();
    node_subset.nodes().par_iter().for_each(|&source| {
        graph.out(source).par_iter().for_each(|&target| {
            if mapper.check(target) && mapper.update(source, target) {
                let idx = write_idx.fetch_add(1, Ordering::SeqCst);
                out_rels[idx].store(target, Ordering::SeqCst);
            }
        })
    });

    let mut out_rels = unsafe { std::mem::transmute::<Vec<AtomicUsize>, Vec<usize>>(out_rels) };
    let write_idx = write_idx.load(Ordering::SeqCst);

    out_rels.truncate(write_idx);

    NodeSubset::sparse(node_subset.node_count(), out_rels)
}

#[cfg(not(feature = "sparse_atomic_pack"))]
fn relationship_map_sparse_output<G, T>(
    graph: &G,
    node_subset: NodeSubset,
    mut degrees: Vec<usize>,
    mapper: &T,
) -> NodeSubset
where
    G: Graph + Sync + ?Sized,
    T: RelationshipMapper + Sync + ?Sized,
{
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

    node_subset
        .nodes()
        .par_iter()
        .zip(offsets.into_par_iter())
        .for_each(|(node_id, offset)| {
            let source = *node_id;
            graph
                .out(source)
                .par_iter()
                .enumerate()
                .for_each(|(j, &target)| {
                    if mapper.check(target) && mapper.update(source, target) {
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

    NodeSubset::sparse(node_subset.node_count(), out_rels)
}

fn relationship_map_dense<G, T>(graph: &G, node_subset: &mut NodeSubset, mapper: &T)
where
    G: Graph + Sync + ?Sized,
    T: RelationshipMapper + Sync + ?Sized,
{
    let node_count = graph.node_count();

    if mapper.has_no_result() {
        (0..node_count).into_par_iter().for_each(|target| {
            if mapper.check(target) {
                for &source in graph.inc(target) {
                    if node_subset.contains(source) {
                        mapper.update(source, target);
                    }
                    if !mapper.check(target) {
                        break;
                    }
                }
            }
        });
    } else {
        let next = par_vec_with(node_count, AtomicBool::default);

        (0..node_count).into_par_iter().for_each(|target| {
            if mapper.check(target) {
                for &source in graph.inc(target) {
                    if node_subset.contains(source) && mapper.update(source, target) {
                        next[source].store(true, Ordering::SeqCst);
                    }
                    if !mapper.check(target) {
                        break;
                    }
                }
            }
        });

        let next = unsafe { std::mem::transmute::<Vec<AtomicBool>, Vec<bool>>(next) };
        *node_subset = NodeSubset::dense(node_count, next);
    }
}

pub trait NodeMapper {
    fn update(&self, node: usize) -> bool;

    fn update_always_returns_true(&self) -> bool {
        false
    }
}

pub fn node_map<T: NodeMapper + Sync + ?Sized>(node_subset: &NodeSubset, mapper: &T) {
    let node_count = node_subset.node_count();

    if node_subset.is_dense() {
        (0..node_count).into_par_iter().for_each(|node| {
            if node_subset.contains(node) {
                mapper.update(node);
            }
        });
    } else {
        node_subset.nodes().par_iter().for_each(|&node| {
            mapper.update(node);
        });
    }
}

pub fn node_filter<T: NodeMapper + Sync + ?Sized>(
    node_subset: &NodeSubset,
    mapper: &T,
) -> NodeSubset {
    let node_count = node_subset.node_count();
    let subset_count = node_subset.subset_count();

    if node_subset.is_dense() {
        let dense = par_vec(node_count, |node| {
            node_subset.contains(node) && mapper.update(node)
        });
        NodeSubset::dense(node_count, dense)
    } else {
        let mut sparse = Vec::with_capacity(subset_count);
        node_subset
            .nodes()
            .par_iter()
            .map(|&node| {
                if mapper.update(node) {
                    node
                } else {
                    usize::MAX
                }
            })
            .collect_into_vec(&mut sparse);

        let mut write_index = 0;
        for read_index in 0..subset_count {
            let value = sparse[read_index];
            if value != usize::MAX {
                sparse[write_index] = value;
                write_index += 1;
            }
        }

        sparse.truncate(write_index);
        NodeSubset::sparse_counted(node_count, write_index, sparse)
    }
}
