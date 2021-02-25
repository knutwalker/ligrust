use crate::Result;
use atoi::FromRadix10;
use byte_slice_cast::*;
use linereader::LineReader;
#[cfg(feature = "mapped_graph")]
use memmap::Mmap;
#[cfg(feature = "mapped_graph")]
use std::convert::TryInto;
use std::{
    convert::TryFrom,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    slice,
    time::Instant,
};

pub trait Graph {
    fn node_count(&self) -> usize;

    fn rel_count(&self) -> usize;

    fn out(&self, node: usize) -> &[usize];

    fn inc(&self, node: usize) -> &[usize];

    fn out_degree(&self, node: usize) -> usize;

    fn inc_degree(&self, node: usize) -> usize;

    fn threshold(&self) -> usize {
        self.rel_count() / 20
    }
}
#[cfg(feature = "mapped_graph")]
#[derive(Debug)]
pub struct MappedGraph {
    map: Mmap,
    node_count: usize,
    rel_count: usize,
    out_nodes: &'static [Node],
    out_targets: &'static [usize],
    in_nodes: &'static [Node],
    in_targets: &'static [usize],
}

#[cfg(feature = "mapped_graph")]
impl Graph for MappedGraph {
    fn node_count(&self) -> usize {
        self.node_count
    }

    fn rel_count(&self) -> usize {
        self.rel_count
    }

    fn out(&self, node: usize) -> &[usize] {
        let node = self.out_nodes[node];
        let start = node.offset;
        let end = node.degree + start;
        &self.out_targets[start..end]
    }

    fn inc(&self, node: usize) -> &[usize] {
        let node = self.in_nodes[node];
        let start = node.offset;
        let end = node.degree + start;
        &self.in_targets[start..end]
    }

    fn out_degree(&self, node: usize) -> usize {
        self.out_nodes[node].degree
    }

    fn inc_degree(&self, node: usize) -> usize {
        self.in_nodes[node].degree
    }
}

#[derive(Debug)]
pub struct AdjacencyGraph {
    out: AdjacencyList,
    inc: AdjacencyList,
}

impl Graph for AdjacencyGraph {
    fn node_count(&self) -> usize {
        self.out.node_count()
    }

    fn rel_count(&self) -> usize {
        self.out.rel_count()
    }

    fn out(&self, node: usize) -> &[usize] {
        self.out.rels(node)
    }

    fn inc(&self, node: usize) -> &[usize] {
        self.inc.rels(node)
    }

    fn out_degree(&self, node: usize) -> usize {
        self.out.degree(node)
    }

    fn inc_degree(&self, node: usize) -> usize {
        self.inc.degree(node)
    }
}

#[derive(Debug)]
pub struct AdjacencyList {
    nodes: Box<[Node]>,
    targets: Box<[usize]>,
}

impl AdjacencyList {
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn rel_count(&self) -> usize {
        self.targets.len()
    }

    pub fn degree(&self, node: usize) -> usize {
        self.nodes[node].degree
    }

    pub fn rels(&self, node: usize) -> &[usize] {
        let node = self.nodes[node];
        let start = node.offset;
        let end = node.degree + start;
        &self.targets[start..end]
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Node {
    degree: usize,
    offset: usize,
}

impl<R> TryFrom<LineReader<R>> for AdjacencyList
where
    R: Read,
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
        for &[offset, next_offset] in offsets.array_windows::<2>() {
            let node = Node {
                offset,
                degree: next_offset - offset,
            };
            nodes.push(node);
        }

        // offsets
        //     .par_windows(2)
        //     .map(|offset_pair| match offset_pair {
        //         &[offset, next_offset] => Node {
        //             offset,
        //             degree: next_offset - offset,
        //         },
        //         _ => unreachable!("windows size is 2"),
        //     })
        //     .collect_into_vec(&mut nodes);

        nodes.push(last_node);

        AdjacencyList {
            nodes: nodes.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
        }
    }
}

impl From<AdjacencyList> for AdjacencyGraph {
    fn from(out: AdjacencyList) -> Self {
        let inc = out.invert();
        AdjacencyGraph { out, inc }
    }
}

impl AdjacencyList {
    pub fn invert(&self) -> Self {
        let node_count = self.nodes.len();
        let rel_count = self.targets.len();

        let mut temp = Vec::with_capacity(rel_count);
        temp.resize(rel_count, (usize::max_value(), usize::max_value()));

        self.nodes
            .iter()
            .enumerate()
            .for_each(|(source, &Node { offset, degree })| {
                let end = offset + degree;
                for (&target, tmp) in self.targets[offset..end].iter().zip(&mut temp[offset..end]) {
                    *tmp = (target, source);
                }
            });

        // let (temp, len, cap) = temp.into_raw_parts();
        // let temp = temp as usize;

        // self.nodes
        //     .par_iter()
        //     .enumerate()
        //     .for_each(|(source, node)| {
        //         let offset = node.offset;
        //         let temp = temp as *mut (usize, usize);
        //         let temp = unsafe { slice::from_raw_parts_mut(temp.add(offset), node.degree) };
        //         for (&target, tmp) in self.targets[offset..offset + node.degree].iter().zip(temp) {
        //             *tmp = (target, source);
        //         }
        //     });

        // let mut temp = unsafe { Vec::from_raw_parts(temp as *mut (usize, usize), len, cap) };

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

pub fn parse(input: PathBuf, output: PathBuf) -> Result<()> {
    let start = Instant::now();
    let file = File::open(input)?;
    let output = File::create(output)?;

    println!("preparing input: {:?}", start.elapsed());
    let start = Instant::now();

    let adjacencies = AdjacencyList::try_from(LineReader::new(file))?;

    println!("parsing input: {:?}", start.elapsed());
    let start = Instant::now();

    let graph = AdjacencyGraph::from(adjacencies);

    println!("building full graph: {:?}", start.elapsed());

    dump(graph, output)
}

pub fn dump(graph: AdjacencyGraph, mut output: impl Write) -> Result<()> {
    let start = Instant::now();

    let node_count = graph.node_count();
    let rel_count = graph.rel_count();
    let meta = [node_count, rel_count];
    output.write_all(meta.as_byte_slice())?;

    let AdjacencyGraph { out, inc } = graph;

    let AdjacencyList {
        nodes: out_nodes,
        targets: out_targets,
    } = out;

    let out_nodes = Box::into_raw(out_nodes) as *mut usize;
    let out_nodes = unsafe { slice::from_raw_parts(out_nodes, node_count * 2) };

    output.write_all(out_nodes.as_byte_slice())?;
    output.write_all(out_targets.as_byte_slice())?;

    let AdjacencyList {
        nodes: in_nodes,
        targets: in_targets,
    } = inc;

    let in_nodes = Box::into_raw(in_nodes) as *mut usize;
    let in_nodes = unsafe { slice::from_raw_parts(in_nodes, node_count * 2) };

    output.write_all(in_nodes.as_byte_slice())?;
    output.write_all(in_targets.as_byte_slice())?;

    println!("serializing graph : {:?}", start.elapsed());

    Ok(())
}

pub fn load_graph(input: PathBuf) -> Result<impl Graph + Sync> {
    let start = Instant::now();
    let file = File::open(input)?;

    println!("preparing input: {:?}", start.elapsed());
    let start = Instant::now();

    let graph = {
        #[cfg(feature = "mapped_graph")]
        {
            load_map(file)
        }

        #[cfg(not(feature = "mapped_graph"))]
        {
            load(file)
        }
    }?;

    println!("building full graph: {:?}", start.elapsed());
    Ok(graph)
}

#[cfg(feature = "mapped_graph")]
pub fn load_map(input: File) -> Result<MappedGraph> {
    let start = Instant::now();
    let map = unsafe { Mmap::map(&input)? };

    let (node_count_bytes, rest) = map.split_at(std::mem::size_of::<usize>());
    let (rel_count_bytes, rest) = rest.split_at(std::mem::size_of::<usize>());
    let node_count = usize::from_le_bytes(node_count_bytes.try_into().unwrap());
    let rel_count = usize::from_le_bytes(rel_count_bytes.try_into().unwrap());

    let (out_nodes_bytes, rest) = rest.split_at(node_count * std::mem::size_of::<Node>());
    let (out_targets_bytes, rest) = rest.split_at(rel_count * std::mem::size_of::<usize>());

    let (in_nodes_bytes, rest) = rest.split_at(node_count * std::mem::size_of::<Node>());
    let (in_targets_bytes, rest) = rest.split_at(rel_count * std::mem::size_of::<usize>());

    ensure!(rest.is_empty(), "extra data");

    let out_nodes: &'static [Node] = unsafe { std::mem::transmute(out_nodes_bytes) };
    let out_targets: &'static [usize] = unsafe { std::mem::transmute(out_targets_bytes) };

    let in_nodes: &'static [Node] = unsafe { std::mem::transmute(in_nodes_bytes) };
    let in_targets: &'static [usize] = unsafe { std::mem::transmute(in_targets_bytes) };

    println!("deserializing graph : {:?}", start.elapsed());

    Ok(MappedGraph {
        map,
        node_count,
        rel_count,
        out_nodes,
        out_targets,
        in_nodes,
        in_targets,
    })
}

pub fn load(mut input: impl Read) -> Result<AdjacencyGraph> {
    let start = Instant::now();

    let mut meta = [0_usize; 2];
    input.read_exact(meta.as_mut_byte_slice())?;

    let [node_count, rel_count] = meta;

    let mut out_nodes = Box::<[Node]>::new_uninit_slice(node_count);
    let out_nodes_ref = out_nodes.as_mut_ptr() as *mut usize;
    let out_nodes_ref = unsafe { slice::from_raw_parts_mut(out_nodes_ref, node_count * 2) };
    input.read_exact(out_nodes_ref.as_mut_byte_slice())?;

    let out_targets = Box::<[usize]>::new_uninit_slice(rel_count);
    let mut out_targets = unsafe { out_targets.assume_init() };
    input.read_exact(out_targets.as_mut_byte_slice())?;

    let mut in_nodes = Box::<[Node]>::new_uninit_slice(node_count);
    let in_nodes_ref = in_nodes.as_mut_ptr() as *mut usize;
    let in_nodes_ref = unsafe { slice::from_raw_parts_mut(in_nodes_ref, node_count * 2) };
    input.read_exact(in_nodes_ref.as_mut_byte_slice())?;

    let in_targets = Box::<[usize]>::new_uninit_slice(rel_count);
    let mut in_targets = unsafe { in_targets.assume_init() };
    input.read_exact(in_targets.as_mut_byte_slice())?;

    let out = AdjacencyList {
        nodes: unsafe { out_nodes.assume_init() },
        targets: out_targets,
    };
    let inc = AdjacencyList {
        nodes: unsafe { in_nodes.assume_init() },
        targets: in_targets,
    };

    println!("deserializing graph : {:?}", start.elapsed());

    Ok(AdjacencyGraph { out, inc })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub trait FilterGraph: Graph {
        type Delegate: Graph;

        fn delegate(&self) -> &Self::Delegate;

        fn node_count(&self) -> usize {
            Graph::node_count(self.delegate())
        }

        fn rel_count(&self) -> usize {
            Graph::rel_count(self.delegate())
        }

        fn out(&self, node: usize) -> &[usize] {
            Graph::out(self.delegate(), node)
        }

        fn inc(&self, node: usize) -> &[usize] {
            Graph::inc(self.delegate(), node)
        }

        fn out_degree(&self, node: usize) -> usize {
            Graph::out_degree(self.delegate(), node)
        }

        fn inc_degree(&self, node: usize) -> usize {
            Graph::inc_degree(self.delegate(), node)
        }

        fn threshold(&self) -> usize {
            Graph::threshold(self.delegate())
        }
    }

    impl<T> Graph for T
    where
        T: FilterGraph,
    {
        fn node_count(&self) -> usize {
            FilterGraph::node_count(self)
        }

        fn rel_count(&self) -> usize {
            FilterGraph::rel_count(self)
        }

        fn out(&self, node: usize) -> &[usize] {
            FilterGraph::out(self, node)
        }

        fn inc(&self, node: usize) -> &[usize] {
            FilterGraph::inc(self, node)
        }

        fn out_degree(&self, node: usize) -> usize {
            FilterGraph::out_degree(self, node)
        }

        fn inc_degree(&self, node: usize) -> usize {
            FilterGraph::inc_degree(self, node)
        }
    }

    #[derive(Default)]
    pub(crate) struct MockGraph {
        out: Vec<Vec<usize>>,
        inc: Vec<Vec<usize>>,
    }

    impl MockGraph {
        pub fn new(out: Vec<Vec<usize>>) -> Self {
            let mut inc = vec![];
            for (source, targets) in out.iter().enumerate() {
                for target in targets.iter() {
                    if *target >= inc.len() {
                        inc.resize_with(target + 1, Vec::new);
                    }
                    inc[*target].push(source)
                }
            }
            MockGraph { out, inc }
        }
    }

    impl Graph for MockGraph {
        fn node_count(&self) -> usize {
            self.out.len()
        }

        fn rel_count(&self) -> usize {
            self.out.iter().map(|targets| targets.len()).sum()
        }

        fn out(&self, node: usize) -> &[usize] {
            match self.out.get(node) {
                Some(targets) => targets.as_slice(),
                None => &[],
            }
        }

        fn inc(&self, node: usize) -> &[usize] {
            match self.inc.get(node) {
                Some(sources) => sources.as_slice(),
                None => &[],
            }
        }

        fn out_degree(&self, node: usize) -> usize {
            self.out(node).len()
        }

        fn inc_degree(&self, node: usize) -> usize {
            self.inc(node).len()
        }
    }

    #[test]
    fn adjacency_graph() {}
}
