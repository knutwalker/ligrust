use crate::{graph::load_graph, Result};
use std::{path::PathBuf, time::Instant};

pub fn run_cc(input: PathBuf) -> Result<()> {
    let graph = load_graph(input)?;

    let start = Instant::now();

    let cc = cc::cc(graph);

    println!("cc done with {} nodes: {:?}", cc.len(), start.elapsed());

    Ok(())
}

pub fn run_bfs(input: PathBuf, source: usize) -> Result<()> {
    let graph = load_graph(input)?;

    let start = Instant::now();

    let parents = bfs::bfs(graph, source);

    println!(
        "bfs done with {} nodes: {:?}",
        parents.len(),
        start.elapsed()
    );

    Ok(())
}

pub fn run_page_rank_delta(input: PathBuf, max_iterations: usize) -> Result<()> {
    let graph = load_graph(input)?;

    let start = Instant::now();

    let pr = pagerank_delta::page_rank_delta(graph, max_iterations);

    println!(
        "page rank done with {} nodes: {:?}",
        pr.len(),
        start.elapsed()
    );

    Ok(())
}

mod cc {
    use crate::{
        graph::Graph,
        ligra::{self, NodeMapper, RelationshipMapper},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CC {
        ids: Vec<AtomicUsize>,
        prev_ids: Vec<AtomicUsize>,
    }

    impl NodeMapper for CC {
        fn update(&self, node: usize) -> bool {
            self.prev_ids[node].store(self.ids[node].load(Ordering::SeqCst), Ordering::SeqCst);
            true
        }

        fn update_always_returns_true(&self) -> bool {
            true
        }
    }

    impl RelationshipMapper for CC {
        fn update(&self, source: usize, target: usize) -> bool {
            let atom = &self.ids[target];
            let original_id = atom.load(Ordering::SeqCst);

            Self::write_min(atom, self.ids[source].load(Ordering::SeqCst))
                && original_id == self.prev_ids[target].load(Ordering::SeqCst)
        }

        fn check_always_returns_true(&self) -> bool {
            true
        }
    }

    impl CC {
        fn new(node_count: usize) -> Self {
            Self {
                ids: ligra::par_vec(node_count, AtomicUsize::new),
                prev_ids: ligra::par_vec_with(node_count, || AtomicUsize::new(0)),
            }
        }

        fn write_min(atom: &AtomicUsize, value: usize) -> bool {
            loop {
                let current = atom.load(Ordering::SeqCst);
                if value < current {
                    if atom
                        .compare_exchange_weak(current, value, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }
    }

    pub(crate) fn cc<G: Graph + Sync>(graph: G) -> Vec<AtomicUsize> {
        let cc = CC::new(graph.node_count());

        let mut frontier = ligra::NodeSubset::full(graph.node_count());

        while frontier.subset_count() != 0 {
            frontier = ligra::node_filter(&frontier, &cc);
            ligra::relationship_map(&graph, &mut frontier, &cc);
        }

        cc.ids
    }
}

mod bfs {
    use crate::{
        graph::Graph,
        ligra::{self, RelationshipMapper},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct BFS {
        parents: Vec<AtomicUsize>,
    }

    impl RelationshipMapper for BFS {
        fn update(&self, source: usize, target: usize) -> bool {
            self.parents[target]
                .compare_exchange(usize::MAX, source, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        }

        fn check(&self, node: usize) -> bool {
            self.parents[node].load(Ordering::SeqCst) == usize::MAX
        }
    }

    impl BFS {
        fn new(node_count: usize) -> Self {
            let mut parents = Vec::with_capacity(node_count);
            parents.resize_with(node_count, || AtomicUsize::new(usize::MAX));

            Self { parents }
        }
    }

    pub(crate) fn bfs<G: Graph + Sync>(graph: G, root: usize) -> Vec<AtomicUsize> {
        let mut bfs = BFS::new(graph.node_count());
        bfs.parents[root] = AtomicUsize::new(root);

        let mut frontier = ligra::NodeSubset::single(graph.node_count(), root);
        while frontier.subset_count() != 0 {
            ligra::relationship_map(&graph, &mut frontier, &bfs);
        }

        bfs.parents
    }
}

mod pagerank_delta {
    use crate::{
        graph::Graph,
        ligra::{self, par_vec_with, NodeMapper, RelationshipMapper},
    };
    use atomic_float::AtomicF64;
    use ligra::NodeSubset;
    use std::sync::atomic::Ordering;

    const DAMPING_FACTOR: f64 = 0.85;
    const TOLERANCE: f64 = 1E-7;
    const DELTA_THRESHOLD: f64 = 1E-2;
    const ALPHA: f64 = 1.0 - DAMPING_FACTOR;

    struct PageRankDelta<'g, G> {
        graph: &'g G,
        deltas: Vec<AtomicF64>,
        neighbors_rank: Vec<AtomicF64>,
        page_rank: Vec<AtomicF64>,
        one_over_n: f64,
        sum_of_delta: AtomicF64,
    }

    struct FirstRound<'a, 'g, G>(&'a PageRankDelta<'g, G>);

    impl<'a, 'g, G> NodeMapper for FirstRound<'a, 'g, G> {
        fn update(&self, node: usize) -> bool {
            // TODO ALPHA / node_count for normalization
            let mut delta =
                self.0.neighbors_rank[node].swap(0.0, Ordering::SeqCst) * DAMPING_FACTOR + ALPHA;
            let current_rank = self.0.page_rank[node].fetch_add(delta, Ordering::SeqCst) + delta;
            delta -= self.0.one_over_n;
            self.0.deltas[node].store(delta, Ordering::SeqCst);
            self.0.sum_of_delta.fetch_add(delta, Ordering::SeqCst);
            delta.abs() > (current_rank + DELTA_THRESHOLD)
        }
    }

    impl<'g, G> NodeMapper for PageRankDelta<'g, G> {
        fn update(&self, node: usize) -> bool {
            let delta = self.neighbors_rank[node].swap(0.0, Ordering::SeqCst) * DAMPING_FACTOR;
            self.deltas[node].store(delta, Ordering::SeqCst);
            self.sum_of_delta.fetch_add(delta, Ordering::SeqCst);

            let current_rank = self.page_rank[node].load(Ordering::SeqCst);

            if current_rank.abs() > (current_rank * DELTA_THRESHOLD) {
                self.page_rank[node].store(current_rank + delta, Ordering::SeqCst);
                true
            } else {
                false
            }
        }
    }

    impl<'g, G: Graph> RelationshipMapper for PageRankDelta<'g, G> {
        fn update(&self, source: usize, target: usize) -> bool {
            let delta =
                self.deltas[source].load(Ordering::SeqCst) / self.graph.out_degree(source) as f64;
            let rank = self.neighbors_rank[target].fetch_add(delta, Ordering::SeqCst);

            rank == 0.0
        }

        fn check_always_returns_true(&self) -> bool {
            true
        }

        fn has_no_result(&self) -> bool {
            true
        }
    }

    impl<'g, G: Graph> PageRankDelta<'g, G> {
        fn new(graph: &'g G) -> Self {
            let node_count = graph.node_count();
            let initial_value = 1.0 / node_count as f64;

            let deltas = par_vec_with(node_count, || AtomicF64::new(initial_value));
            let neighbors_rank = par_vec_with(node_count, AtomicF64::default);
            let page_rank = par_vec_with(node_count, AtomicF64::default);

            let one_over_n = 1.0 / node_count as f64;

            PageRankDelta {
                graph,
                deltas,
                neighbors_rank,
                page_rank,
                one_over_n,
                sum_of_delta: AtomicF64::default(),
            }
        }

        fn sum_of_delta_and_reset(&self) -> f64 {
            self.sum_of_delta.swap(0.0, Ordering::SeqCst)
        }
    }

    pub(crate) fn page_rank_delta<G: Graph + Sync>(
        graph: G,
        mut max_iterations: usize,
    ) -> Vec<AtomicF64> {
        let pr = PageRankDelta::new(&graph);

        let all_nodes = NodeSubset::full(graph.node_count());
        let mut frontier = NodeSubset::full(graph.node_count());

        ligra::relationship_map(&graph, &mut frontier, &pr);
        frontier = ligra::node_filter(&all_nodes, &FirstRound(&pr));

        // remaining iterations
        loop {
            let error = pr.sum_of_delta_and_reset();
            max_iterations -= 1;

            if error < TOLERANCE || max_iterations == 0 {
                break;
            }

            ligra::relationship_map(&graph, &mut frontier, &pr);
            frontier = ligra::node_filter(&all_nodes, &pr);
        }

        pr.page_rank
    }
}
