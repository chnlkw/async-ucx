use super::param::RequestParam;
use super::*;
use std::io::{IoSlice, IoSliceMut};

impl Worker {
    /// Receives a message with `tag`.
    pub async fn tag_recv(&self, tag: u64, buf: &mut [MaybeUninit<u8>]) -> Result<usize, Error> {
        self.tag_recv_mask(tag, u64::max_value(), buf)
            .await
            .map(|info| info.1)
    }

    /// Receives a message with `tag` and `tag_mask`.
    pub async fn tag_recv_mask(
        &self,
        tag: u64,
        tag_mask: u64,
        buf: &mut [MaybeUninit<u8>],
    ) -> Result<(u64, usize), Error> {
        match self.tag_recv_impl(tag, tag_mask, buf)? {
            Status::Completed(r) => r,
            Status::Scheduled(request_handle) => request_handle.await,
        }
    }

    /// Like `tag_recv`, except that it reads into a slice of buffers.
    pub async fn tag_recv_vectored(
        &self,
        tag: u64,
        iov: &mut [IoSliceMut<'_>],
    ) -> Result<usize, Error> {
        trace!(
            "tag_recv_vectored: worker={:?} iov.len={}",
            self.handle,
            iov.len()
        );
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            info: *const ucp_tag_recv_info,
            _user_data: *mut c_void,
        ) {
            let length = (*info).length;
            let tag = (*info).sender_tag;
            trace!(
                "tag_recv_vectored: complete. req={:?}, status={:?}, tag={}, len={}",
                request,
                status,
                tag,
                length
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        // Use RequestParam builder for iov
        let param = RequestParam::new().cb_tag_recv(Some(callback)).iov();
        let status = unsafe {
            ucp_tag_recv_nbx(
                self.handle,
                iov.as_ptr() as _,
                iov.len() as _,
                tag,
                u64::max_value(),
                param.as_ref(),
            )
        };
        Error::from_ptr(status)?;
        RequestHandle {
            ptr: status,
            poll_fn: poll_tag,
        }
        .await
        .map(|info| info.1)
    }

    pub(super) fn tag_recv_impl(
        &self,
        tag: u64,
        tag_mask: u64,
        buf: &mut [MaybeUninit<u8>],
    ) -> Result<Status<(u64, usize)>, Error> {
        trace!(
            "tag_recv: worker={:?}, tag={}, mask={:#x} len={}",
            self.handle,
            tag,
            tag_mask,
            buf.len()
        );
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            info: *const ucp_tag_recv_info,
            _user_data: *mut c_void,
        ) {
            let length = (*info).length;
            let sender_tag = (*info).sender_tag;
            trace!(
                "tag_recv: complete. req={:?}, status={:?}, tag={}, len={}",
                request,
                status,
                sender_tag,
                length
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let param = RequestParam::new().cb_tag_recv(Some(callback));
        let status = unsafe {
            ucp_tag_recv_nbx(
                self.handle,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                tag,
                tag_mask,
                param.as_ref(),
            )
        };
        Ok(Status::from(status, MaybeUninit::uninit(), poll_tag))
    }
}

impl Endpoint {
    pub(super) fn tag_send_impl(&self, tag: u64, buf: &[u8]) -> Result<Status<()>, Error> {
        trace!("tag_send: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            _user_data: *mut c_void,
        ) {
            trace!("tag_send: complete. req={:?}, status={:?}", request, status);
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let param = RequestParam::new().cb_send(Some(callback));
        let status = unsafe {
            ucp_tag_send_nbx(
                self.get_handle()?,
                buf.as_ptr() as _,
                buf.len() as _,
                tag,
                param.as_ref(),
            )
        };
        Ok(Status::from(status, MaybeUninit::uninit(), poll_normal))
    }

    /// Sends a messages with `tag`.
    pub async fn tag_send(&self, tag: u64, buf: &[u8]) -> Result<usize, Error> {
        match self.tag_send_impl(tag, buf)? {
            Status::Completed(r) => {
                match &r {
                    Ok(()) => trace!("tag_send: complete"),
                    Err(e) => error!("tag_send error : {:?}", e),
                }
                r.map(|_| buf.len())
            }
            Status::Scheduled(request_handle) => {
                request_handle.await?;
                Ok(buf.len())
            }
        }
    }

    /// Like `tag_send`, except that it reads into a slice of buffers.
    pub async fn tag_send_vectored(&self, tag: u64, iov: &[IoSlice<'_>]) -> Result<usize, Error> {
        trace!(
            "tag_send_vectored: endpoint={:?} iov.len={}",
            self.handle,
            iov.len()
        );
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            _user_data: *mut c_void,
        ) {
            trace!(
                "tag_send_vectored: complete. req={:?}, status={:?}",
                request,
                status
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        // Use RequestParam builder for iov
        let param = RequestParam::new().cb_send(Some(callback)).iov();
        let status = unsafe {
            ucp_tag_send_nbx(
                self.get_handle()?,
                iov.as_ptr() as _,
                iov.len() as _,
                tag,
                param.as_ref(),
            )
        };
        let total_len = iov.iter().map(|v| v.len()).sum();
        if status.is_null() {
            trace!("tag_send_vectored: complete");
        } else if UCS_PTR_IS_PTR(status) {
            RequestHandle {
                ptr: status,
                poll_fn: poll_normal,
            }
            .await?;
        } else {
            return Err(Error::from_ptr(status).unwrap_err());
        }
        Ok(total_len)
    }
}

fn poll_tag(ptr: ucs_status_ptr_t) -> Poll<Result<(u64, usize), Error>> {
    let mut info = MaybeUninit::<ucp_tag_recv_info>::uninit();
    let status = unsafe { ucp_tag_recv_request_test(ptr as _, info.as_mut_ptr() as _) };
    match status {
        ucs_status_t::UCS_INPROGRESS => Poll::Pending,
        ucs_status_t::UCS_OK => {
            let info = unsafe { info.assume_init() };
            Poll::Ready(Ok((info.sender_tag, info.length as usize)))
        }
        status => Poll::Ready(Err(Error::from_error(status))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test_log::test]
    fn tag() {
        for i in 0..20_usize {
            spawn_thread!(_tag(4 << i)).join().unwrap();
        }
    }

    async fn _tag(msg_size: usize) {
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
        let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        addr.set_port(listen_port);

        let (endpoint1, endpoint2) = tokio::join!(
            async {
                let conn1 = listener.next().await;
                worker1.accept(conn1).await.unwrap()
            },
            async { worker2.connect_socket(addr).await.unwrap() },
        );

        // send tag message
        tokio::join!(
            async {
                // send
                let mut buf = vec![0; msg_size];
                endpoint2.tag_send(1, &mut buf).await.unwrap();
                println!("tag sended");
            },
            async {
                // recv
                let mut buf = vec![MaybeUninit::<u8>::uninit(); msg_size];
                worker1.tag_recv(1, &mut buf).await.unwrap();
                println!("tag recved");
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
