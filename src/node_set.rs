use super::*;

struct NodeSubset {
    node_count: usize,
    rel_count: usize,
    dense: Option<Box<[bool]>>,
    sparse: Option<Box<[usize]>>,
    is_dense: bool,
}

impl Default for NodeSubset {
    fn default() -> Self {
        Self {
            node_count: 0,
            rel_count: 0,
            dense: None,
            sparse: None,
            is_dense: false,
        }
    }
}

/// Constructors
impl NodeSubset {
    fn empty(node_count: usize) -> Self {
        Self {
            node_count,
            rel_count: 0,
            dense: None,
            sparse: None,
            is_dense: false,
        }
    }

    fn sparse(node_count: usize, rel_count: usize, sparse: impl Into<Box<[usize]>>) -> Self {
        Self {
            node_count,
            rel_count,
            dense: None,
            sparse: Some(sparse.into()),
            is_dense: false,
        }
    }

    fn dense_counted(node_count: usize, rel_count: usize, dense: impl Into<Box<[bool]>>) -> Self {
        Self {
            node_count,
            rel_count,
            dense: Some(dense.into()),
            sparse: None,
            is_dense: true,
        }
    }

    fn dense(node_count: usize, dense: impl Into<Box<[bool]>>) -> Self {
        let dense = dense.into();
        let rel_count = dense.iter().filter(|d| **d).count();
        Self::dense_counted(node_count, rel_count, dense)
    }
}

/// Common stuff
impl NodeSubset {
    fn size(&self) -> usize {
        self.rel_count
    }

    fn len(&self) -> usize {
        self.rel_count
    }

    fn is_empty(&self) -> bool {
        self.rel_count == 0
    }

    fn is_dense(&self) -> bool {
        self.is_dense
    }

    fn node_count(&self) -> usize {
        self.node_count
    }

    fn row_count(&self) -> usize {
        self.node_count
    }

    fn non_zeroes_count(&self) -> usize {
        self.rel_count
    }
}

/// Sparse NodeSet
impl NodeSubset {
    fn node(&self, index: usize) -> usize {
        self.sparse.as_ref().expect("sparse")[index]
    }

    fn to_dense(&mut self) {
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
    fn contains(&self, value: usize) -> bool {
        self.dense.as_ref().expect("dense")[value]
    }

    fn to_sparse(&mut self) {
        if self.sparse.is_none() && self.rel_count > 0 {
            let mut sparse = Vec::with_capacity(self.rel_count);
            if let Some(dense) = self.dense.take() {
                for (node, _) in dense.to_vec().into_iter().enumerate().filter(|(_, d)| *d) {
                    sparse.push(node);
                }
            }
            assert_eq!(sparse.len(), self.rel_count);
            self.sparse = Some(sparse.into_boxed_slice());
        }
        self.is_dense = false;
    }
}

type S<T> = (usize, T);
type D<T> = (bool, T);

struct NodeSubsetData<T> {
    node_count: usize,
    rel_count: usize,
    dense: Option<Box<[D<T>]>>,
    sparse: Option<Box<[S<T>]>>,
    is_dense: bool,
}

impl<D> Default for NodeSubsetData<D> {
    fn default() -> Self {
        Self {
            node_count: 0,
            rel_count: 0,
            dense: None,
            sparse: None,
            is_dense: false,
        }
    }
}

/// Constructors
impl<T> NodeSubsetData<T> {
    fn empty(node_count: usize) -> Self {
        Self {
            node_count,
            rel_count: 0,
            dense: None,
            sparse: None,
            is_dense: false,
        }
    }

    fn sparse(node_count: usize, rel_count: usize, sparse: impl Into<Box<[S<T>]>>) -> Self {
        Self {
            node_count,
            rel_count,
            dense: None,
            sparse: Some(sparse.into()),
            is_dense: false,
        }
    }

    fn dense_counted(node_count: usize, rel_count: usize, dense: impl Into<Box<[D<T>]>>) -> Self {
        Self {
            node_count,
            rel_count,
            dense: Some(dense.into()),
            sparse: None,
            is_dense: true,
        }
    }

    fn dense(node_count: usize, dense: impl Into<Box<[D<T>]>>) -> Self {
        let dense = dense.into();
        let rel_count = dense.iter().filter(|(d, _)| *d).count();
        Self::dense_counted(node_count, rel_count, dense)
    }
}

/// Common stuff
impl<T> NodeSubsetData<T> {
    fn size(&self) -> usize {
        self.rel_count
    }

    fn len(&self) -> usize {
        self.rel_count
    }

    fn is_empty(&self) -> bool {
        self.rel_count == 0
    }

    fn is_dense(&self) -> bool {
        self.is_dense
    }

    fn node_count(&self) -> usize {
        self.node_count
    }

    fn row_count(&self) -> usize {
        self.node_count
    }

    fn non_zeroes_count(&self) -> usize {
        self.rel_count
    }
}

/// Sparse NodeSet
impl<T> NodeSubsetData<T> {
    fn node(&self, index: usize) -> usize {
        self.sparse.as_ref().expect("sparse")[index].0
    }

    fn node_data(&self, index: usize) -> &T {
        &self.sparse.as_ref().expect("sparse")[index].1
    }

    fn node_with_data(&self, index: usize) -> (usize, &T) {
        let (node, ref data) = self.sparse.as_ref().expect("sparse")[index];
        (node, data)
    }
}

/// Dense NodeSet
impl<T> NodeSubsetData<T> {
    fn contains(&self, value: usize) -> bool {
        self.dense.as_ref().expect("dense")[value].0
    }

    fn nth_Data(&self, value: usize) -> &T {
        &self.dense.as_ref().expect("dense")[value].1
    }
}
