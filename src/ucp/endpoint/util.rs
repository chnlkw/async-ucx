use super::*;
use futures::FutureExt;
use pin_project::pin_project;
use std::task::ready;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

impl Endpoint {
    /// make write stream
    pub fn write_stream(&self) -> WriteStream {
        WriteStream {
            endpoint: self,
            request: None,
        }
    }
    /// make read stream
    pub fn read_stream(&self) -> ReadStream {
        ReadStream {
            endpoint: self,
            request: None,
        }
    }
    /// make tag write stream
    pub fn tag_write_stream(&self, tag: u64) -> TagWriteStream {
        TagWriteStream {
            endpoint: self,
            tag,
            request: None,
        }
    }
}

impl Worker {
    /// make tag read stream
    pub fn tag_read_stream(&self, tag: u64) -> TagReadStream {
        TagReadStream {
            worker: self,
            tag,
            tag_mask: u64::max_value(),
            request: None,
        }
    }
    /// make tag read stream with mask
    pub fn tag_read_stream_mask(&self, tag: u64, tag_mask: u64) -> TagReadStream {
        TagReadStream {
            worker: self,
            tag,
            tag_mask,
            request: None,
        }
    }
}

#[pin_project]
/// A stream for writing data asynchronously to an `Endpoint` stream.
pub struct WriteStream<'a> {
    endpoint: &'a Endpoint,
    #[pin]
    request: Option<RequestHandle<Result<(), Error>>>,
}

impl<'a> AsyncWrite for WriteStream<'a> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if let Some(mut req) = self.as_mut().project().request.as_pin_mut() {
            let r = match ready!(req.poll_unpin(cx)) {
                Ok(_) => Ok(buf.len()),
                Err(e) => Err(e.into()),
            };
            self.request = None;
            Poll::Ready(r)
        } else {
            match self.endpoint.stream_send_impl(buf) {
                Ok(Status::Completed(r)) => Poll::Ready(r.map(|_| buf.len()).map_err(|e| e.into())),
                Ok(Status::Scheduled(request_handle)) => {
                    self.request = Some(request_handle);
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e.into())),
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo!()
    }
}

/// A stream for reading data asynchronously from an `Endpoint` stream.
#[pin_project]
pub struct ReadStream<'a> {
    endpoint: &'a Endpoint,
    #[pin]
    request: Option<RequestHandle<Result<usize, Error>>>,
}

impl<'a> AsyncRead for ReadStream<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out_buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(mut req) = self.as_mut().project().request.as_pin_mut() {
            let r = match ready!(req.poll_unpin(cx)) {
                Ok(n) => {
                    // Safety: The buffer was filled by the recv operation.
                    unsafe {
                        out_buf.assume_init(n);
                        out_buf.advance(n);
                    }
                    Ok(())
                }
                Err(e) => Err(e.into()),
            };
            self.request = None;
            Poll::Ready(r)
        } else {
            let buf = unsafe { out_buf.unfilled_mut() };
            match self.endpoint.stream_recv_impl(buf) {
                Ok(Status::Completed(n_result)) => {
                    match n_result {
                        Ok(n) => {
                            // Safety: The buffer was filled by the recv operation.
                            unsafe {
                                out_buf.assume_init(n);
                                out_buf.advance(n);
                            }
                            Poll::Ready(Ok(()))
                        }
                        Err(e) => Poll::Ready(Err(e.into())),
                    }
                }
                Ok(Status::Scheduled(request_handle)) => {
                    self.request = Some(request_handle);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e.into())),
            }
        }
    }
}

#[pin_project]
/// A stream for writing data asynchronously to an `Endpoint` using tag.
pub struct TagWriteStream<'a> {
    endpoint: &'a Endpoint,
    tag: u64,
    #[pin]
    request: Option<RequestHandle<Result<(), Error>>>,
}

impl<'a> AsyncWrite for TagWriteStream<'a> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if let Some(mut req) = self.as_mut().project().request.as_pin_mut() {
            let r = match ready!(req.poll_unpin(cx)) {
                Ok(_) => Ok(buf.len()),
                Err(e) => Err(e.into()),
            };
            self.request = None;
            Poll::Ready(r)
        } else {
            match self.endpoint.tag_send_impl(self.tag, buf) {
                Ok(Status::Completed(r)) => Poll::Ready(r.map(|_| buf.len()).map_err(|e| e.into())),
                Ok(Status::Scheduled(request_handle)) => {
                    self.request = Some(request_handle);
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e.into())),
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo!()
    }
}

/// A stream for reading data asynchronously from a `Worker` using tag.
#[pin_project]
pub struct TagReadStream<'a> {
    worker: &'a Worker,
    tag: u64,
    tag_mask: u64,
    #[pin]
    request: Option<RequestHandle<Result<(u64, usize), Error>>>,
}

impl<'a> AsyncRead for TagReadStream<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out_buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(mut req) = self.as_mut().project().request.as_pin_mut() {
            let r = match ready!(req.poll_unpin(cx)) {
                Ok((_, n)) => {
                    // Safety: The buffer was filled by the recv operation.
                    unsafe {
                        out_buf.assume_init(n);
                        out_buf.advance(n);
                    }
                    Ok(())
                }
                Err(e) => Err(e.into()),
            };
            self.request = None;
            Poll::Ready(r)
        } else {
            let buf = unsafe { out_buf.unfilled_mut() };
            match self.worker.tag_recv_impl(self.tag, self.tag_mask, buf) {
                Ok(Status::Completed(n_result)) => {
                    match n_result {
                        Ok((_, n)) => {
                            // Safety: The buffer was filled by the recv operation.
                            unsafe {
                                out_buf.assume_init(n);
                                out_buf.advance(n);
                            }
                            Poll::Ready(Ok(()))
                        }
                        Err(e) => Poll::Ready(Err(e.into())),
                    }
                }
                Ok(Status::Scheduled(request_handle)) => {
                    self.request = Some(request_handle);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e.into())),
            }
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test_log::test]
    fn stream_send_recv() {
        spawn_thread!(_stream_send_recv()).join().unwrap();
    }

    #[test_log::test]
    fn tag_send_recv() {
        spawn_thread!(_tag_send_recv()).join().unwrap();
    }

    async fn _stream_send_recv() {
        let context1 = Context::new().unwrap();
        let worker1 = context1.create_worker().unwrap();
        let context2 = Context::new().unwrap();
        let worker2 = context2.create_worker().unwrap();
        tokio::task::spawn_local(worker1.clone().polling());
        tokio::task::spawn_local(worker2.clone().polling());

        // connect with each other
        let mut listener = worker1
            .create_listener("0.0.0.0:0".parse().unwrap())
            .unwrap();
        let listen_port = listener.socket_addr().unwrap().port();
        let mut addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        addr.set_port(listen_port);

        let (endpoint1, endpoint2) = tokio::join!(
            async {
                let conn1 = listener.next().await;
                worker1.accept(conn1).await.unwrap()
            },
            async { worker2.connect_socket(addr).await.unwrap() },
        );

        // Test cases: (data, repeat count)
        let test_cases = vec![
            (vec![], 1),
            (vec![0u8], 10),
            (vec![1, 2, 3, 4, 5], 5),
            ((0..128).collect::<Vec<u8>>(), 3),
            ((0..1024).map(|x| (x % 256) as u8).collect::<Vec<u8>>(), 2),
            ((0..4096).map(|x| (x % 256) as u8).collect::<Vec<u8>>(), 1),
        ];
        for (data, repeat) in test_cases {
            for _ in 0..repeat {
                // send
                let send_buf = data.clone();
                let recv_len = send_buf.len();
                let mut recv_buf = vec![0u8; recv_len];
                tokio::join!(
                    async {
                        endpoint2.write_stream().write_all(&send_buf).await.unwrap();
                    },
                    async {
                        endpoint1
                            .read_stream()
                            .read_exact(&mut recv_buf)
                            .await
                            .unwrap();
                        assert_eq!(recv_buf, send_buf, "data mismatch for len={}", recv_len);
                    }
                );
            }
        }
    }

    async fn _tag_send_recv() {
        let context1 = Context::new().unwrap();
        let worker1 = context1.create_worker().unwrap();
        let context2 = Context::new().unwrap();
        let worker2 = context2.create_worker().unwrap();
        tokio::task::spawn_local(worker1.clone().polling());
        tokio::task::spawn_local(worker2.clone().polling());

        // connect with each other
        let mut listener = worker1
            .create_listener("0.0.0.0:0".parse().unwrap())
            .unwrap();
        let listen_port = listener.socket_addr().unwrap().port();
        let mut addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        addr.set_port(listen_port);

        let (endpoint1, endpoint2) = tokio::join!(
            async {
                let conn1 = listener.next().await;
                worker1.accept(conn1).await.unwrap()
            },
            async { worker2.connect_socket(addr).await.unwrap() },
        );

        // Test cases: (data, tag, repeat count)
        let test_cases = vec![
            (vec![], 1u64, 1),
            (vec![0u8], 2u64, 10),
            (vec![1, 2, 3, 4, 5], 3u64, 5),
            ((0..128).collect::<Vec<u8>>(), 4u64, 3),
            (
                (0..1024).map(|x| (x % 256) as u8).collect::<Vec<u8>>(),
                5u64,
                2,
            ),
            (
                (0..4096).map(|x| (x % 256) as u8).collect::<Vec<u8>>(),
                6u64,
                1,
            ),
        ];
        for (data, tag, repeat) in test_cases {
            for _ in 0..repeat {
                // send
                let send_buf = data.clone();
                let recv_len = send_buf.len();
                let mut recv_buf = vec![0u8; recv_len];
                tokio::join!(
                    async {
                        endpoint2
                            .tag_write_stream(tag)
                            .write_all(&send_buf)
                            .await
                            .unwrap();
                    },
                    async {
                        worker1
                            .tag_read_stream(tag)
                            .read_exact(&mut recv_buf)
                            .await
                            .unwrap();
                        assert_eq!(
                            recv_buf, send_buf,
                            "data mismatch for tag={}, len={}",
                            tag, recv_len
                        );
                    }
                );
            }
        }

        // Clean up
        let _ = endpoint1.close(false).await;
        let _ = endpoint2.close(false).await;
    }
}
