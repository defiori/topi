// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use topi::test_utils::{BenchMessage, TestConfig};
use topi_base::{
    data::{Content, DataMessage, DataMessenger},
    messaging::Envelope,
    ndarray,
    nodes::{Node, NodeInfo, NodeSender},
    process::{ProcessCollectionBuilder, ProcessCollectionWithLocalThreads},
    sharify::SharedArrayMut,
    test_utils::TestMessage,
    thread::ThreadCollectionBuilder,
};

struct IpcBench<T> {
    procs: Option<ProcessCollectionWithLocalThreads<BenchMessage, TestConfig>>,
    array_type: std::marker::PhantomData<T>,
    send_node: crossbeam_channel::Sender<BenchThreadMessage>,
    recv_node: crossbeam_channel::Receiver<BenchThreadMessage>,
}

impl<T> IpcBench<T> {
    fn create_builder() -> ProcessCollectionBuilder<BenchMessage, TestConfig> {
        let node_ids: Vec<u32> = (0..10).collect();
        let mut builder = ProcessCollectionBuilder::new();
        for &id_num in node_ids.iter() {
            let id = format!("P{}", id_num);
            builder = builder
                .add_process(
                    &id,
                    [(
                        id.clone(),
                        TestConfig::BenchTransfer {
                            next_hop: format!("P{}", id_num + 1),
                            should_be: id_num as f64,
                        },
                    )]
                    .iter()
                    .cloned()
                    .collect(),
                    Some("topi_proc_test"),
                )
                .unwrap();
        }
        builder
    }

    fn init(&mut self, n_elements: usize) {
        self.send_node
            .send(BenchThreadMessage::BenchInit(n_elements))
            .unwrap();
    }

    fn run(&mut self) {
        self.send_node.send(BenchThreadMessage::Run).unwrap();
        if let BenchThreadMessage::Done = self.recv_node.recv().unwrap() {
        } else {
            panic!("Expected 'Done' message variant.")
        }
    }
}

impl IpcBench<SharedArrayMut<f64, ndarray::IxDyn>> {
    fn new() -> Self {
        let builder = Self::create_builder();
        let (send_node, recv_main) = crossbeam_channel::bounded(1);
        let (send_main, recv_node) = crossbeam_channel::bounded(1);
        let node: IpcBenchNode<SharedArrayMut<f64, ndarray::IxDyn>> =
            IpcBenchNode::new(recv_main, send_main);
        let thread = ThreadCollectionBuilder::new()
            .add_thread("P10", node)
            .unwrap();
        let procs = builder.build_with_local_threads(thread).unwrap();
        IpcBench {
            procs: Some(procs),
            array_type: std::marker::PhantomData,
            send_node,
            recv_node,
        }
    }
}

impl IpcBench<ndarray::Array<f64, ndarray::IxDyn>> {
    fn new() -> Self {
        let builder = Self::create_builder();
        let (send_node, recv_main) = crossbeam_channel::bounded(1);
        let (send_main, recv_node) = crossbeam_channel::bounded(1);
        let node: IpcBenchNode<ndarray::Array<f64, ndarray::IxDyn>> =
            IpcBenchNode::new(recv_main, send_main);
        let thread = ThreadCollectionBuilder::new()
            .add_thread("P10", node)
            .unwrap();
        let procs = builder.build_with_local_threads(thread).unwrap();
        IpcBench {
            procs: Some(procs),
            array_type: std::marker::PhantomData,
            send_node,
            recv_node,
        }
    }
}

impl<T> Drop for IpcBench<T> {
    fn drop(&mut self) {
        let procs = self.procs.take().unwrap();
        for destination in procs.contained_nodes() {
            procs
                .ipc_sender(destination)
                .unwrap()
                .send(DataMessage::new_from_user(TestMessage::Quit, None))
                .unwrap();
        }
        self.send_node.send(BenchThreadMessage::Quit).unwrap();
        procs.wait_for_all().unwrap();
    }
}

enum BenchThreadMessage {
    Run,
    BenchInit(usize),
    Done,
    Quit,
}

struct IpcBenchNode<T> {
    inner: Option<T>,
    recv_main: crossbeam_channel::Receiver<BenchThreadMessage>,
    send_main: crossbeam_channel::Sender<BenchThreadMessage>,
}

impl<T> IpcBenchNode<T> {
    fn new(
        recv_main: crossbeam_channel::Receiver<BenchThreadMessage>,
        send_main: crossbeam_channel::Sender<BenchThreadMessage>,
    ) -> Self {
        IpcBenchNode {
            inner: None,
            recv_main,
            send_main,
        }
    }
}

impl Node<BenchMessage, DataMessenger<TestMessage, ()>>
    for IpcBenchNode<SharedArrayMut<f64, ndarray::IxDyn>>
{
    fn entrypoint(&mut self, messenger: &mut DataMessenger<TestMessage, ()>) -> Result<(), String> {
        loop {
            match self.recv_main.recv().unwrap() {
                BenchThreadMessage::Run => {
                    messenger
                        .send("P0", self.inner.take().unwrap(), None)
                        .unwrap();
                    if let Envelope::Message(DataMessage {
                        content: Content::SharedArrayMutFloat64(mut array),
                        ..
                    }) = messenger.recv().unwrap()
                    {
                        {
                            let mut view = array.as_view_mut();
                            assert!((view[0] - 10.0).abs() < f64::EPSILON);
                            view[0] = 0.0;
                        }
                        self.inner = Some(array);
                        self.send_main.send(BenchThreadMessage::Done).unwrap();
                    } else {
                        panic!("Expected SharedMutArray.")
                    }
                }
                BenchThreadMessage::BenchInit(n_elements) => {
                    self.inner =
                        Some(SharedArrayMut::new(&(0.0, ndarray::IxDyn(&[n_elements]))).unwrap());
                }
                BenchThreadMessage::Quit => break,
                BenchThreadMessage::Done => panic!("IpcBenchNode received unexpected message."),
            }
        }
        Ok(())
    }
}

impl Node<BenchMessage, DataMessenger<TestMessage, ()>>
    for IpcBenchNode<ndarray::Array<f64, ndarray::IxDyn>>
{
    fn entrypoint(&mut self, messenger: &mut DataMessenger<TestMessage, ()>) -> Result<(), String> {
        loop {
            match self.recv_main.recv().unwrap() {
                BenchThreadMessage::Run => {
                    messenger
                        .send("P0", self.inner.take().unwrap(), None)
                        .unwrap();
                    if let Envelope::Message(DataMessage {
                        content: Content::ArrayMutFloat64(mut array),
                        ..
                    }) = messenger.recv().unwrap()
                    {
                        assert!((array[0] - 10.0).abs() < f64::EPSILON);
                        array[0] = 0.0;
                        self.inner = Some(array);
                        self.send_main.send(BenchThreadMessage::Done).unwrap();
                    } else {
                        panic!("Expected MutArray.")
                    }
                }
                BenchThreadMessage::BenchInit(n_elements) => {
                    self.inner = Some(ndarray::Array::zeros(ndarray::IxDyn(&[n_elements])));
                }
                BenchThreadMessage::Quit => break,
                BenchThreadMessage::Done => panic!("IpcBenchNode received unexpected message."),
            }
        }
        Ok(())
    }
}

pub fn ipc_benchmark(c: &mut Criterion) {
    let mut shared_bench = IpcBench::<SharedArrayMut<f64, ndarray::IxDyn>>::new();
    let mut local_bench = IpcBench::<ndarray::Array<f64, ndarray::IxDyn>>::new();

    let mut group = c.benchmark_group("IPC");
    for &n_elements in [100, 500_usize, 1_000, 2_000, 3_000, 4_000].iter() {
        shared_bench.init(n_elements);
        local_bench.init(n_elements);
        group.throughput(Throughput::Bytes((n_elements * 8) as u64));
        group.bench_function(BenchmarkId::new("Shared_memory", n_elements), |b| {
            b.iter(|| shared_bench.run())
        });
        group.bench_function(BenchmarkId::new("Serialized", n_elements), |b| {
            b.iter(|| local_bench.run())
        });
    }
    group.finish();
}

criterion_group!(benches, ipc_benchmark);
criterion_main!(benches);
