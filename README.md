# async-ucx

[![Crate](https://img.shields.io/crates/v/async-ucx.svg)](https://crates.io/crates/async-ucx)
[![Docs](https://docs.rs/async-ucx/badge.svg)](https://docs.rs/async-ucx)
[![CI](https://github.com/madsys-dev/async-ucx/workflows/CI/badge.svg?branch=main)](https://github.com/madsys-dev/async-ucx/actions)

Async Rust UCX bindings providing high-performance networking capabilities for distributed systems and HPC applications.

## Features

- **Asynchronous UCP Operations**: Full async/await support for UCX operations
- **Multiple Communication Models**: Support for RMA, Stream, Tag, and Active Message APIs
- **High Performance**: Optimized for low-latency, high-throughput communication
- **Tokio Integration**: Seamless integration with Tokio async runtime
- **Comprehensive Examples**: Ready-to-use examples for various UCX patterns

## Optional features

- `event`: Enable UCP wakeup mechanism for event-driven applications
- `am`: Enable UCP Active Message API for flexible message handling
- `util`: Enable additional utility functions for UCX integration

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
async-ucx = "0.2"
tokio = { version = "1.0", features = ["rt", "net"] }
```

Basic usage example:

```rust
use async_ucx::ucp::*;
use std::mem::MaybeUninit;
use std::net::SocketAddr;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create UCP contexts and workers
    let context1 = Context::new()?;
    let worker1 = context1.create_worker()?;
    let context2 = Context::new()?;
    let worker2 = context2.create_worker()?;
    
    // Start polling for both workers
    tokio::task::spawn_local(worker1.clone().polling());
    tokio::task::spawn_local(worker2.clone().polling());

    // Create listener on worker1
    let mut listener = worker1
        .create_listener("0.0.0.0:0".parse().unwrap())?;
    let listen_port = listener.socket_addr()?.port();
    
    // Connect worker2 to worker1
    let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    addr.set_port(listen_port);

    let (endpoint1, endpoint2) = tokio::join!(
        async {
            let conn1 = listener.next().await;
            worker1.accept(conn1).await.unwrap()
        },
        async { worker2.connect_socket(addr).await.unwrap() },
    );

    // Send and receive tag message
    tokio::join!(
        async {
            let msg = b"Hello UCX!";
            endpoint2.tag_send(1, msg).await.unwrap();
            println!("Message sent");
        },
        async {
            let mut buf = vec![MaybeUninit::<u8>::uninit(); 10];
            worker1.tag_recv(1, &mut buf).await.unwrap();
            println!("Message received");
        }
    );
    
    Ok(())
}
```

## Examples

Check the `examples/` directory for comprehensive examples:
- `rma.rs`: Remote Memory Access operations
- `stream.rs`: Stream-based communication
- `tag.rs`: Tag-based message matching
- `bench.rs`: Performance benchmarking
- `bench-multi-thread.rs`: Multi-threaded benchmarking

## License

MIT License
