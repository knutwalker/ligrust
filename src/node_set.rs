#![allow(unused)]
use super::*;

pub(crate) struct NodeSubset {
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
    pub(crate) fn empty(node_count: usize) -> Self {
        Self {
            node_count,
            subset_count: 0,
            dense: None,
            sparse: None,
            is_dense: false,
        }
    }

    pub(crate) fn sparse_counted(
        node_count: usize,
        rel_count: usize,
        sparse: impl Into<Box<[usize]>>,
    ) -> Self {
        Self {
            node_count,
            subset_count: rel_count,
            dense: None,
            sparse: Some(sparse.into()),
            is_dense: false,
        }
    }

    pub(crate) fn sparse(node_count: usize, sparse: impl Into<Box<[usize]>>) -> Self {
        let sparse = sparse.into();
        Self::sparse_counted(node_count, sparse.len(), sparse)
    }

    pub(crate) fn dense_counted(
        node_count: usize,
        rel_count: usize,
        dense: impl Into<Box<[bool]>>,
    ) -> Self {
        Self {
            node_count,
            subset_count: rel_count,
            dense: Some(dense.into()),
            sparse: None,
            is_dense: true,
        }
    }

    pub(crate) fn dense(node_count: usize, dense: impl Into<Box<[bool]>>) -> Self {
        let dense = dense.into();
        let rel_count = dense.iter().filter(|d| **d).count();
        Self::dense_counted(node_count, rel_count, dense)
    }
}

/// Common stuff
impl NodeSubset {
    pub(crate) fn size(&self) -> usize {
        self.subset_count
    }

    pub(crate) fn len(&self) -> usize {
        self.subset_count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.subset_count == 0
    }

    pub(crate) fn is_dense(&self) -> bool {
        self.is_dense
    }

    pub(crate) fn node_count(&self) -> usize {
        self.node_count
    }

    pub(crate) fn row_count(&self) -> usize {
        self.node_count
    }

    pub(crate) fn non_zeroes_count(&self) -> usize {
        self.subset_count
    }
}

/// Sparse NodeSet
impl NodeSubset {
    pub(crate) fn node(&self, index: usize) -> usize {
        self.sparse.as_ref().expect("sparse")[index]
    }

    pub(crate) fn nodes(&self) -> &[usize] {
        self.sparse.as_deref().unwrap_or_default()
    }

    pub(crate) fn to_dense(&mut self) {
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

impl IntoIterator for NodeSubset {
    type Item = usize;

    type IntoIter = <Vec<usize> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        assert!(
            self.is_dense == false,
            "iter is only defined on sparse node subsets"
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
            "iter is only defined on sparse node subsets"
        );
        let sparse = match &self.sparse {
            Some(sparse) => sparse.as_ref(),
            None => &[],
        };
        sparse.iter()
    }
}

/// Dense NodeSet
impl NodeSubset {
    pub(crate) fn contains(&self, value: usize) -> bool {
        self.dense.as_ref().expect("dense")[value]
    }

    pub(crate) fn to_sparse(&mut self) {
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

    // fn to_dense(&mut self) {
    //     if self.dense.is_none() {
    //         let mut dense = Box::<[D<T>]>::new_uninit_slice(self.node_count);

    //         for n in 0..self.node_count {
    //             unsafe {
    //                 dense[n].as_mut_ptr().write((false, ???));
    //             }
    //         }

    //         // let dense = dense.as_mut_ptr();
    //         if let Some(sparse) = self.sparse.take() {
    //             for (node, data) in sparse.into_vec() {
    //                 unsafe {
    //                     dense[node].as_mut_ptr().write((true, data));
    //                 }
    //             }
    //         }
    //         let dense = unsafe { dense.assume_init() };
    //         self.dense = Some(dense);
    //     }
    //     self.is_dense = true;
    // }
}

/// Dense NodeSet
impl<T> NodeSubsetData<T> {
    fn contains(&self, value: usize) -> bool {
        self.dense.as_ref().expect("dense")[value].0
    }

    fn nth_data(&self, value: usize) -> &T {
        &self.dense.as_ref().expect("dense")[value].1
    }
}
