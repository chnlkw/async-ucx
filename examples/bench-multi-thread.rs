use async_ucx::ucp::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Result;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::sync::atomic::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use bytesize::ByteSize;

const MAX_MSG_SIZE: usize = ByteSize::mb(128).0 as usize; // 4 MB

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let local = tokio::task::LocalSet::new();
    if let Some(server_addr) = std::env::args().nth(1) {
        let msg_size = std::env::args()
            .nth(2)
            .map_or(8usize, |n| n.parse::<usize>().unwrap());
        local.run_until(client(server_addr, msg_size)).await;
    } else {
        local.run_until(server()).await;
    }
}

async fn client(server_addr: String, msg_size: usize) -> ! {
    println!(
        "client: connect to {:?} with msg_size {:?}",
        server_addr, msg_size
    );

    let context = Context::new().unwrap();
    let worker = context.create_worker().unwrap();
    #[cfg(not(feature = "event"))]
    tokio::task::spawn_local(worker.clone().polling());
    #[cfg(feature = "event")]
    tokio::task::spawn_local(worker.clone().event_poll());

    let endpoint = worker
        .connect_socket(server_addr.parse().unwrap())
        .await
        .unwrap();
    endpoint.print_to_stderr();

    let mut tag = [MaybeUninit::uninit(); 8];
    endpoint.worker().tag_recv(100, &mut tag).await.unwrap();
    let tag: u64 = unsafe { std::mem::transmute(tag) };
    println!("client: got tag {:#x}", tag);

    let long_msg: Vec<u8> = (0..msg_size).map(|x| x as u8).collect();
    loop {
        endpoint.tag_send(tag, &long_msg).await.unwrap();
        // for _ in 0..500 {
        //     std::hint::spin_loop();
        // }
        // endpoint
        //     .worker()
        //     .tag_recv(tag, &mut [MaybeUninit::uninit()])
        //     .await;
    }
}

async fn server() -> ! {
    println!("server");
    let context = Context::new().unwrap();
    let mut worker_threads = vec![];
    let mut counters = vec![];
    let mut summers = vec![];
    for _ in 0..4 {
        let wt = WorkerThread::new(&context, |ep, addr, counter, sum| {
            println!("accept: {:?}", addr);
            tokio::task::spawn_local(async move {
                let mut hasher = DefaultHasher::new();
                addr.hash(&mut hasher);
                let tag = hasher.finish();
                ep.tag_send(100, &tag.to_ne_bytes()).await.unwrap();

                let mut buf = vec![MaybeUninit::uninit(); MAX_MSG_SIZE];
                loop {
                    let sz = ep.worker().tag_recv(tag, &mut buf).await.unwrap();
                    // ep.tag_send(tag, &[0]).await;
                    counter.fetch_add(1, Ordering::Relaxed);
                    sum.fetch_add(sz, Ordering::Relaxed);
                }
            });
        });
        counters.push(wt.counter.clone());
        summers.push(wt.sum.clone());
        worker_threads.push(wt);
    }

    let worker = context.create_worker().unwrap();
    #[cfg(not(feature = "event"))]
    tokio::task::spawn_local(worker.clone().polling());
    #[cfg(feature = "event")]
    tokio::task::spawn_local(worker.clone().event_poll());

    let mut listener = worker
        .create_listener("0.0.0.0:10001".parse().unwrap())
        .unwrap();
    tokio::task::spawn_local(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let count: usize = counters.iter().map(|c| c.swap(0, Ordering::SeqCst)).sum();
            let sum: usize = summers.iter().map(|s| s.swap(0, Ordering::SeqCst)).sum();
            println!("{} IOPS\t{}/s", count, ByteSize::b(sum as u64).display().si());
        }
    });
    println!("listening on {}", listener.socket_addr().unwrap());

    for i in 0.. {
        let conn = listener.next().await;
        let n = worker_threads.len();
        worker_threads[i % n].accept(conn);
    }
    unreachable!()
}

struct WorkerThread {
    sender: mpsc::UnboundedSender<ConnectionRequest>,
    counter: Arc<AtomicUsize>,
    sum: Arc<AtomicUsize>,
}

impl WorkerThread {
    fn new(context: &Arc<Context>, handle_ep: fn(Endpoint, SocketAddr, Arc<AtomicUsize>, Arc<AtomicUsize>)) -> Self {
        let context = context.clone();
        let (sender, mut recver) = mpsc::unbounded_channel::<ConnectionRequest>();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter1 = counter.clone();
        let sum = Arc::new(AtomicUsize::new(0));
        let sum1 = sum.clone();
        std::thread::spawn(move || {
            let worker = context.create_worker().unwrap();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            #[cfg(not(event))]
            local.spawn_local(worker.clone().polling());
            #[cfg(feature = "event")]
            local.spawn_local(worker.clone().event_poll());
            local.block_on(&rt, async move {
                while let Some(conn) = recver.recv().await {
                    let addr = conn.remote_addr().unwrap();
                    let ep = worker.accept(conn).await.unwrap();
                    handle_ep(ep, addr, counter1.clone(), sum1.clone());
                }
            });
        });
        WorkerThread { sender, counter, sum }
    }

    fn accept(&mut self, conn: ConnectionRequest) {
        self.sender.send(conn).unwrap();
    }
}
