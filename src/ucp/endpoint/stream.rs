use super::param::RequestParam;
use super::*;

impl Endpoint {
    /// Sends data through stream.
    pub async fn stream_send(&self, buf: &[u8]) -> Result<usize, Error> {
        trace!("stream_send: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            _user_data: *mut c_void,
        ) {
            trace!(
                "stream_send: complete. req={:?}, status={:?}",
                request,
                status
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let param = RequestParam::new().cb_send(Some(callback));
        let status = unsafe {
            ucp_stream_send_nbx(
                self.get_handle()?,
                buf.as_ptr() as _,
                buf.len() as _,
                param.as_ref(),
            )
        };
        if status.is_null() {
            trace!("stream_send: complete");
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await?;
        } else {
            return Err(Error::from_ptr(status).unwrap_err());
        }
        Ok(buf.len())
    }

    /// Receives data from stream.
    pub async fn stream_recv(&self, buf: &mut [MaybeUninit<u8>]) -> Result<usize, Error> {
        trace!("stream_recv: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            length: usize,
            _user_data: *mut c_void,
        ) {
            trace!(
                "stream_recv: complete. req={:?}, status={:?}, len={}",
                request,
                status,
                length
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let mut length = MaybeUninit::<usize>::uninit();
        let param = RequestParam::new().cb_stream_recv(Some(callback));
        let status = unsafe {
            ucp_stream_recv_nbx(
                self.get_handle()?,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                length.as_mut_ptr(),
                param.as_ref(),
            )
        };
        if status.is_null() {
            let length = unsafe { length.assume_init() } as usize;
            trace!("stream_recv: complete. len={}", length);
            Ok(length)
        } else if UCS_PTR_IS_PTR(status) {
            Ok(RequestHandle {
                ptr: status,
                poll_fn: poll_stream,
            }
            .await)
        } else {
            Err(Error::from_ptr(status).unwrap_err())
        }
    }
}

unsafe fn poll_stream(ptr: ucs_status_ptr_t) -> Poll<usize> {
    let mut len = MaybeUninit::<usize>::uninit();
    let status = ucp_stream_recv_request_test(ptr as _, len.as_mut_ptr() as _);
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        Poll::Ready(len.assume_init())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test_log::test]
    fn stream() {
        for i in 0..20_usize {
            spawn_thread!(_stream(4 << i)).join().unwrap();
        }
    }

    async fn _stream(msg_size: usize) {
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
        println!("listen at port {}", listen_port);
        let mut addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        addr.set_port(listen_port);

        let (endpoint1, endpoint2) = tokio::join!(
            async {
                let conn1 = listener.next().await;
                worker1.accept(conn1).await.unwrap()
            },
            async { worker2.connect_socket(addr).await.unwrap() },
        );

        // send stream message
        tokio::join!(
            async {
                // send
                let buf = vec![42u8; msg_size];
                endpoint2.stream_send(&buf).await.unwrap();
                println!("stream sent");
            },
            async {
                // recv
                let mut buf = vec![std::mem::MaybeUninit::<u8>::uninit(); msg_size];
                let mut start = 0;
                while start < msg_size {
                    let len = endpoint1.stream_recv(&mut buf[start..]).await.unwrap();
                    if len == 0 {
                        break; // no more data
                    }
                    start += len;
                }
                let buf: Vec<u8> = unsafe { buf.into_iter().map(|b| b.assume_init()).collect() };
                assert_eq!(buf, vec![42u8; msg_size]);
                println!("stream received");
            }
        );

        assert_eq!(endpoint1.get_rc(), (1, 1));
        assert_eq!(endpoint2.get_rc(), (1, 1));
        assert_eq!(endpoint1.close(false).await, Ok(()));
        assert_eq!(endpoint2.close(false).await, Err(Error::ConnectionReset));
        assert_eq!(endpoint1.get_rc(), (1, 0));
        assert_eq!(endpoint2.get_rc(), (1, 1));
        assert_eq!(endpoint2.close(true).await, Ok(()));
        assert_eq!(endpoint2.get_rc(), (1, 0));
    }
}
