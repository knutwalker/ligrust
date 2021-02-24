use rayon::iter::IndexedParallelIterator;

pub struct NodeSubset {
    node_count: usize,
    subset_count: usize,
    dense: Option<Box<[bool]>>,
    sparse: Option<Box<[usize]>>,
    is_dense: bool,
}

impl Default for NodeSubset {
    fn default() -> Self {
        Self {
            node_count: 0,
            subset_count: 0,
            dense: None,
            sparse: None,
            is_dense: false,
        }
    }
}

/// Constructors
impl NodeSubset {
    pub fn empty(node_count: usize) -> Self {
        Self {
            node_count,
            subset_count: 0,
            dense: None,
            sparse: None,
            is_dense: false,
        }
    }

    pub fn single(node_count: usize, element: usize) -> Self {
        let sparse = vec![element];
        Self::sparse_counted(node_count, 1, sparse)
    }

    pub fn full(node_count: usize) -> Self {
        let mut dense = Vec::with_capacity(node_count);
        rayon::iter::repeatn(true, node_count).collect_into_vec(&mut dense);
        Self::dense_counted(node_count, node_count, dense)
    }

    pub fn sparse(node_count: usize, sparse: impl Into<Box<[usize]>>) -> Self {
        let sparse = sparse.into();
        Self::sparse_counted(node_count, sparse.len(), sparse)
    }

    pub fn sparse_counted(
        node_count: usize,
        subset_count: usize,
        sparse: impl Into<Box<[usize]>>,
    ) -> Self {
        Self {
            node_count,
            subset_count,
            dense: None,
            sparse: Some(sparse.into()),
            is_dense: false,
        }
    }

    pub fn dense(node_count: usize, dense: impl Into<Box<[bool]>>) -> Self {
        let dense = dense.into();
        let rel_count = dense.iter().filter(|d| **d).count();
        Self::dense_counted(node_count, rel_count, dense)
    }

    pub fn dense_counted(
        node_count: usize,
        subset_count: usize,
        dense: impl Into<Box<[bool]>>,
    ) -> Self {
        Self {
            node_count,
            subset_count,
            dense: Some(dense.into()),
            sparse: None,
            is_dense: true,
        }
    }
}

/// Common stuff
impl NodeSubset {
    pub fn is_empty(&self) -> bool {
        self.subset_count == 0
    }

    pub fn is_dense(&self) -> bool {
        self.is_dense
    }

    pub fn node_count(&self) -> usize {
        self.node_count
    }

    pub fn subset_count(&self) -> usize {
        self.subset_count
    }
}

/// Sparse NodeSet
impl NodeSubset {
    pub fn node(&self, index: usize) -> usize {
        self.sparse
            .as_ref()
            .expect("Dense NodeSubset does not support node(idx)")[index]
    }

    pub fn nodes(&self) -> &[usize] {
        self.sparse
            .as_deref()
            .expect("Dense NodeSubset does not support nodes()")
    }

    pub fn iter(&self) -> impl Iterator<Item = &usize> {
        self.into_iter()
    }

    pub fn to_dense(&mut self) {
        if self.dense.is_none() {
            let mut dense = vec![false; self.node_count];
            if let Some(sparse) = self.sparse.take() {
                for node in sparse.to_vec() {
                    dense[node] = true;
                }
            }
            self.dense = Some(dense.into_boxed_slice());
        }
        self.is_dense = true;
    }
}

/// Dense NodeSet
impl NodeSubset {
    pub fn contains(&self, value: usize) -> bool {
        self.dense
            .as_ref()
            .expect("Sparse NodeSubset does not support contains(node_id)")[value]
    }

    pub fn to_sparse(&mut self) {
        if self.sparse.is_none() && self.subset_count > 0 {
            let mut sparse = Vec::with_capacity(self.subset_count);
            if let Some(dense) = self.dense.take() {
                for (node, _) in dense.to_vec().into_iter().enumerate().filter(|(_, d)| *d) {
                    sparse.push(node);
                }
            }
            assert_eq!(sparse.len(), self.subset_count);
            self.sparse = Some(sparse.into_boxed_slice());
        }
        self.is_dense = false;
    }
}

impl IntoIterator for NodeSubset {
    type Item = usize;

    type IntoIter = <Vec<usize> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        assert!(
            self.is_dense == false,
            "Dense NodeSubset does not support into_iter()"
        );
        self.sparse.unwrap_or_default().into_vec().into_iter()
    }
}

impl<'a> IntoIterator for &'a NodeSubset {
    type Item = &'a usize;

    type IntoIter = <&'a [usize] as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        assert!(
            self.is_dense == false,
            "Dense NodeSubset does not support into_iter()"
        );
        let sparse = match &self.sparse {
            Some(sparse) => sparse.as_ref(),
            None => &[],
        };
        sparse.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_subset_empty() {
        let node_count = 42;
        let node_subset = NodeSubset::empty(node_count);
        assert_eq!(node_subset.node_count(), node_count);
        assert_eq!(node_subset.subset_count(), 0);
    }

    #[test]
    fn node_subset_single() {
        let node_count = 42;
        let element = 1337;
        let node_subset = NodeSubset::single(node_count, element);
        assert_eq!(node_subset.node_count(), node_count);
        assert_eq!(node_subset.subset_count(), 1);
        assert_eq!(node_subset.nodes(), &[element])
    }

    #[test]
    fn node_subset_full() {
        let node_count = 42;
        let node_subset = NodeSubset::full(node_count);
        assert_eq!(node_subset.node_count(), node_count);
        assert_eq!(node_subset.subset_count(), node_count);
        assert!(node_subset.is_dense());
        for node_id in 0..node_count {
            assert!(node_subset.contains(node_id));
        }
    }

    #[test]
    fn node_subset_sparse() {
        let node_count = 42;
        let subset_count = 23;
        let sparse = (0..subset_count).collect::<Vec<_>>();
        let node_subset = NodeSubset::sparse(node_count, sparse);
        assert!(!node_subset.is_dense());
        assert_eq!(node_subset.node_count(), node_count);
        assert_eq!(node_subset.subset_count(), subset_count);
    }

    #[test]
    fn node_subset_sparse_counted() {
        let node_count = 42;
        let subset_count = 23;
        let sparse = (0..subset_count).collect::<Vec<_>>();
        let node_subset = NodeSubset::sparse_counted(node_count, subset_count, sparse);
        assert!(!node_subset.is_dense());
        assert_eq!(node_subset.node_count(), node_count);
        assert_eq!(node_subset.subset_count(), subset_count);
    }

    #[test]
    fn node_subset_dense() {
        let node_count = 42;
        let subset_count = 23;
        let dense = (0..subset_count).map(|_| true).collect::<Vec<_>>();
        let node_subset = NodeSubset::dense(node_count, dense);
        assert!(node_subset.is_dense());
        assert_eq!(node_subset.node_count(), node_count);
        assert_eq!(node_subset.subset_count(), subset_count);
    }

    #[test]
    fn node_subset_dense_counted() {
        let node_count = 42;
        let subset_count = 23;
        let dense = (0..subset_count).map(|_| true).collect::<Vec<_>>();
        let node_subset = NodeSubset::dense_counted(node_count, subset_count, dense);
        assert!(node_subset.is_dense());
        assert_eq!(node_subset.node_count(), node_count);
        assert_eq!(node_subset.subset_count(), subset_count);
    }

    #[test]
    fn node_subset_is_empty() {
        let node_subset = NodeSubset::empty(42);
        assert!(node_subset.is_empty());
        let node_subset = NodeSubset::single(42, 1337);
        assert!(node_subset.is_empty() == false)
    }

    #[test]
    fn node_subset_is_dense() {
        let mut node_subset = NodeSubset::full(42);
        assert!(node_subset.is_dense());
        node_subset.to_sparse();
        assert!(node_subset.is_dense() == false);
    }

    #[test]
    fn node_subset_node() {
        let node_subset = NodeSubset::single(42, 1337);
        assert_eq!(node_subset.node(0), 1337);

        let node_subset = NodeSubset::sparse(42, vec![1, 9, 8, 4]);
        assert_eq!(node_subset.node(0), 1);
        assert_eq!(node_subset.node(1), 9);
        assert_eq!(node_subset.node(2), 8);
        assert_eq!(node_subset.node(3), 4);
    }

    #[test]
    fn node_subset_nodes() {
        let nodes = (0..42).collect::<Vec<_>>();
        let node_subset = NodeSubset::sparse(42, nodes.clone());
        assert_eq!(node_subset.nodes(), nodes.as_slice())
    }

    #[test]
    fn node_subset_to_dense() {
        let nodes = (0..42).collect::<Vec<_>>();
        let mut node_subset = NodeSubset::sparse(42, nodes.clone());
        node_subset.to_dense();
        for node_id in nodes.iter() {
            assert!(node_subset.contains(*node_id))
        }
        node_subset.to_sparse();
        assert_eq!(node_subset.nodes(), nodes.as_slice())
    }

    #[test]
    fn node_subset_contains() {
        let mut node_subset = NodeSubset::sparse(42, vec![1, 3, 4, 7]);
        node_subset.to_dense();

        assert!(node_subset.contains(1));
        assert!(node_subset.contains(3));
        assert!(node_subset.contains(4));
        assert!(node_subset.contains(7));
        assert!(node_subset.contains(0) == false);
        assert!(node_subset.contains(2) == false);
        assert!(node_subset.contains(5) == false);
        assert!(node_subset.contains(6) == false);
    }

    #[test]
    fn node_subset_to_sparse() {
        let nodes = (0..23).map(|_| true).collect::<Vec<_>>();
        let mut node_subset = NodeSubset::dense(42, nodes);
        node_subset.to_sparse();
        assert_eq!(node_subset.nodes(), (0..23).collect::<Vec<_>>().as_slice());
        node_subset.to_dense();
        for node_id in 0..23 {
            assert!(node_subset.contains(node_id));
        }
    }

    #[test]
    fn node_subset_into_iter() {
        let nodes = vec![1, 9, 8, 4];
        let node_subset = NodeSubset::sparse(42, nodes.clone());
        assert!(nodes.into_iter().eq(node_subset.into_iter()));
    }

    #[test]
    fn node_subset_iter() {
        let nodes = vec![1, 9, 8, 4];
        let node_subset = NodeSubset::sparse(42, nodes.clone());
        assert!(nodes.iter().eq(node_subset.iter()));
    }

    // panicking stuff

    #[test]
    #[should_panic(expected = "Dense NodeSubset does not support nodes()")]
    fn dense_nodes() {
        let node_subset = NodeSubset::full(42);
        node_subset.nodes();
    }

    #[test]
    #[should_panic(expected = "Dense NodeSubset does not support node(idx)")]
    fn dense_node() {
        let node_subset = NodeSubset::full(42);
        node_subset.node(0);
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 1 but the index is 1")]
    fn sparse_node() {
        let node_subset = NodeSubset::single(42, 1337);
        node_subset.node(1);
    }

    #[test]
    #[should_panic(expected = "Sparse NodeSubset does not support contains(node_id)")]
    fn sparse_contains() {
        let node_subset = NodeSubset::single(42, 1337);
        node_subset.contains(1337);
    }

    #[test]
    #[should_panic(expected = "Dense NodeSubset does not support into_iter()")]
    fn dense_into_iter() {
        let node_subset = NodeSubset::full(42);
        node_subset.into_iter();
    }

    #[test]
    #[should_panic(expected = "Dense NodeSubset does not support into_iter()")]
    fn dense_into_iter_ref() {
        let node_subset = NodeSubset::full(42);
        (&node_subset).into_iter();
    }
}
