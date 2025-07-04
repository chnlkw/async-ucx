use crossbeam::queue::SegQueue;
use tokio::sync::Notify;

use super::param::RequestParam;
use super::*;
use std::{
    io::{IoSlice, IoSliceMut},
    slice,
    sync::atomic::AtomicBool,
};

//// Active message protocol.
/// Active message protocol is a mechanism for sending and receiving messages
/// between processes in a distributed system.
/// It allows a process to send a message to another process, which can then
/// handle the message and perform some action based on its contents.
/// Active messages are typically used in high-performance computing (HPC)
/// applications, where low-latency communication is critical.
#[derive(Debug, PartialEq, Eq)]
pub enum AmDataType {
    /// Eager message
    Eager,
    /// Data message
    Data,
    /// Rendezvous message
    Rndv,
}

enum AmData {
    Eager(Vec<u8>),
    Data(&'static [u8]),
    Rndv(&'static [u8]),
}

impl AmData {
    fn from_raw(data: &'static [u8], attr: u64) -> Option<AmData> {
        if data.is_empty() {
            None
        } else if attr & ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_DATA as u64 != 0 {
            Some(AmData::Data(data))
        } else if attr & ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_RNDV as u64 != 0 {
            Some(AmData::Rndv(data))
        } else {
            Some(AmData::Eager(data.to_owned()))
        }
    }

    #[inline]
    fn data_type(&self) -> AmDataType {
        match self {
            AmData::Eager(_) => AmDataType::Eager,
            AmData::Data(_) => AmDataType::Data,
            AmData::Rndv(_) => AmDataType::Rndv,
        }
    }

    #[inline]
    fn data(&self) -> Option<&[u8]> {
        match self {
            AmData::Eager(data) => Some(data.as_slice()),
            AmData::Data(data) => Some(*data),
            _ => None,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            AmData::Eager(data) => data.len(),
            AmData::Data(data) => data.len(),
            AmData::Rndv(desc) => desc.len(),
        }
    }
}

struct RawMsg {
    id: u16,
    header: Vec<u8>,
    data: Option<AmData>,
    reply_ep: ucp_ep_h,
    attr: u64,
}

impl RawMsg {
    fn from_raw(
        id: u16,
        header: &[u8],
        data: &'static [u8],
        reply_ep: ucp_ep_h,
        attr: u64,
    ) -> Self {
        RawMsg {
            id,
            header: header.to_owned(),
            data: AmData::from_raw(data, attr),
            reply_ep,
            attr,
        }
    }
}

/// Active message message.
pub struct AmMsg<'a> {
    worker: &'a Worker,
    msg: RawMsg,
}

impl<'a> AmMsg<'a> {
    fn from_raw(worker: &'a Worker, msg: RawMsg) -> Self {
        AmMsg { worker, msg }
    }

    /// Get the ActiveStream id
    #[inline]
    pub fn id(&self) -> u16 {
        self.msg.id
    }

    /// Get the message header.
    #[inline]
    pub fn header(&self) -> &[u8] {
        self.msg.header.as_ref()
    }

    /// Returns `true` if the message contains data. Otherwise, `false`.
    #[inline]
    pub fn contains_data(&self) -> bool {
        self.msg.data.is_some()
    }

    /// Get the message data type.
    pub fn data_type(&self) -> Option<AmDataType> {
        self.msg.data.as_ref().map(|data| data.data_type())
    }

    /// Get the message data.
    /// Returns `None` if needs to receive data.
    /// Returns `Some(slice)` if the message contains concrete data.
    #[inline]
    pub fn get_data(&self) -> Option<&[u8]> {
        match self.msg.data {
            Some(ref amdata) => amdata.data(),
            None => Some(&[]),
        }
    }

    /// Get the message data length.
    /// Returns `0` if the message doesn't contain data.
    #[inline]
    pub fn data_len(&self) -> usize {
        self.msg.data.as_ref().map_or(0, |data| data.len())
    }

    /// Receive the message data.
    pub async fn recv_data(&mut self) -> Result<Vec<u8>, Error> {
        match self.msg.data.take() {
            None => Ok(Vec::new()),
            Some(AmData::Eager(vec)) => Ok(vec),
            Some(AmData::Data(data)) => {
                let v = data.to_vec();
                self.drop_msg(AmData::Data(data));
                Ok(v)
            }
            Some(data) => {
                self.msg.data = Some(data);
                let mut buf = Vec::with_capacity(self.data_len());
                unsafe {
                    buf.set_len(self.data_len());
                }
                let recv_size = self.recv_data_single(&mut buf).await?;
                buf.truncate(recv_size);
                Ok(buf)
            }
        }
    }

    /// Receive the message data.
    /// Returns `0` if the message doesn't contain data.
    /// Returns the number of bytes received.
    /// # Safety
    /// User needs to ensure that the buffer is large enough to hold the data.
    /// Otherwise, it will cause memory corruption.
    pub async fn recv_data_single(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        if !self.contains_data() {
            Ok(0)
        } else {
            let iov = [IoSliceMut::new(buf)];
            self.recv_data_vectored(&iov).await
        }
    }

    /// Receive the message data.
    pub async fn recv_data_vectored(&mut self, iov: &[IoSliceMut<'_>]) -> Result<usize, Error> {
        fn copy_data_to_iov(data: &[u8], iov: &[IoSliceMut<'_>]) -> Result<usize, Error> {
            // return error if buffer size < data length, same with ucx
            let cap = iov.iter().fold(0_usize, |cap, buf| cap + buf.len());
            if cap < data.len() {
                return Err(Error::MessageTruncated);
            }

            let mut copied = 0_usize;
            for buf in iov {
                let len = std::cmp::min(data.len() - copied, buf.len());
                if len == 0 {
                    break;
                }

                let buf = &buf[..len];
                unsafe {
                    std::ptr::copy_nonoverlapping(data[copied..].as_ptr(), buf.as_ptr() as _, len)
                }
                copied += len;
            }
            Ok(copied)
        }
        let data = self.msg.data.take();

        match data {
            Some(AmData::Eager(data)) => {
                // eager message, no need to receive
                copy_data_to_iov(&data, iov)
            }
            Some(AmData::Data(data)) => {
                // data message, no need to receive
                let size = copy_data_to_iov(&data, iov)?;
                self.drop_msg(AmData::Data(data));
                Ok(size)
            }
            Some(AmData::Rndv(desc)) => {
                // rndv message, need to receive
                let (data_desc, data_len) = (desc.as_ptr(), desc.len());

                unsafe extern "C" fn callback(
                    request: *mut c_void,
                    status: ucs_status_t,
                    _length: usize,
                    _data: *mut c_void,
                ) {
                    // todo: handle error & fix real data length
                    trace!(
                        "recv_data_vectored: complete, req={:?}, status={:?}",
                        request,
                        status
                    );
                    let request = &mut *(request as *mut Request);
                    request.waker.wake();
                }
                trace!(
                    "recv_data_vectored: worker={:?} iov.len={}",
                    self.worker.handle,
                    iov.len()
                );

                let param = RequestParam::new().cb_recv_am(Some(callback));
                let (buffer, count, param) = if iov.len() == 1 {
                    (iov[0].as_ptr(), iov[0].len(), param)
                } else {
                    (iov.as_ptr() as _, iov.len(), param.iov())
                };

                let status = unsafe {
                    ucp_am_recv_data_nbx(
                        self.worker.handle,
                        data_desc as _,
                        buffer as _,
                        count as _,
                        param.as_ref(),
                    )
                };
                if status.is_null() {
                    trace!("recv_data_vectored: complete");
                    Ok(data_len)
                } else if UCS_PTR_IS_PTR(status) {
                    RequestHandle {
                        ptr: status,
                        poll_fn: poll_normal,
                    }
                    .await?;
                    Ok(data_len)
                } else {
                    Err(Error::from_ptr(status).unwrap_err())
                }
            }
            None => {
                // no data
                Ok(0)
            }
        }
    }

    /// Check if the message needs a reply.
    #[inline]
    pub fn need_reply(&self) -> bool {
        self.msg.attr & (ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FIELD_REPLY_EP as u64) != 0
            && !self.msg.reply_ep.is_null()
    }

    /// Send reply
    /// # Safety
    /// User needs to ensure that the endpoint isn't closed.
    pub async unsafe fn reply(
        &self,
        id: u32,
        header: &[u8],
        data: &[u8],
        need_reply: bool,
        proto: Option<AmProto>,
    ) -> Result<(), Error> {
        // todo: we should prevent endpoint from being freed
        //       currently, ucx doesn't provide such function.
        assert!(self.need_reply());
        self.reply_vectorized(id, header, &[IoSlice::new(data)], need_reply, proto)
            .await
    }

    /// Send reply
    /// # Safety
    /// User needs to ensure that the endpoint isn't closed.
    pub async unsafe fn reply_vectorized(
        &self,
        id: u32,
        header: &[u8],
        data: &[IoSlice<'_>],
        need_reply: bool,
        proto: Option<AmProto>,
    ) -> Result<(), Error> {
        assert!(self.need_reply());
        am_send(self.msg.reply_ep, id, header, data, need_reply, proto).await
    }

    fn drop_msg(&mut self, data: AmData) {
        match data {
            AmData::Eager(_) => (),
            AmData::Data(data) => unsafe {
                ucp_am_data_release(self.worker.handle, data.as_ptr() as _);
            },
            AmData::Rndv(data) => unsafe {
                ucp_am_data_release(self.worker.handle, data.as_ptr() as _);
            },
        }
    }
}

impl<'a> Drop for AmMsg<'a> {
    fn drop(&mut self) {
        if let Some(data) = self.msg.data.take() {
            self.drop_msg(data);
        }
    }
}

/// Active message stream.
#[derive(Clone)]
pub struct AmStream<'a> {
    worker: &'a Worker,
    inner: Rc<AmStreamInner>,
}

impl<'a> AmStream<'a> {
    fn new(worker: &'a Worker, inner: Rc<AmStreamInner>) -> Self {
        AmStream { worker, inner }
    }

    /// Wait active message.
    pub async fn wait_msg(&self) -> Option<AmMsg<'_>> {
        self.inner.wait_msg(self.worker).await
    }
}

pub(crate) struct AmStreamInner {
    id: u16,
    msgs: SegQueue<RawMsg>,
    notify: Notify,
    unregistered: AtomicBool,
}

impl AmStreamInner {
    // new active message handler
    fn new(id: u16) -> Self {
        Self {
            id,
            msgs: SegQueue::new(),
            notify: Notify::new(),
            unregistered: AtomicBool::new(false),
        }
    }

    // unregister
    fn unregister(&self) {
        self.unregistered
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    // callback function
    fn callback(&self, header: &[u8], data: &'static [u8], reply: ucp_ep_h, attr: u64) {
        let msg = RawMsg::from_raw(self.id, header, data, reply, attr);
        self.msgs.push(msg);
        self.notify.notify_one();
    }

    /// Wait active message.
    async fn wait_msg<'a>(&self, worker: &'a Worker) -> Option<AmMsg<'a>> {
        // todo: how to make this thread safe?
        while !self.unregistered.load(std::sync::atomic::Ordering::Relaxed) {
            if let Some(msg) = self.msgs.pop() {
                return Some(AmMsg::from_raw(worker, msg));
            }

            self.notify.notified().await;
        }

        self.msgs.pop().map(|msg| AmMsg::from_raw(worker, msg))
    }
}

impl Worker {
    /// Register active message stream for `id`.
    /// Message of this `id` can be received with `am_recv`.
    pub fn am_stream(&self, id: u16) -> Result<AmStream<'_>, Error> {
        if let Some(inner) = self.am_streams.read().unwrap().get(&id) {
            return Ok(AmStream::new(self, inner.clone()));
        }

        unsafe extern "C" fn callback(
            arg: *mut c_void,
            header: *const c_void,
            header_len: usize,
            data: *mut c_void,
            data_len: usize,
            param: *const ucp_am_recv_param_t,
        ) -> ucs_status_t {
            let handler = &*(arg as *const AmStreamInner);
            let header = slice::from_raw_parts(header as *const u8, header_len as usize);
            let data = slice::from_raw_parts(data as *const u8, data_len as usize);

            let param = &*param;
            handler.callback(header, data, param.reply_ep, param.recv_attr);

            const DATA_FLAG: u64 = ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_DATA as u64
                | ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_RNDV as u64;
            if param.recv_attr & DATA_FLAG != 0 {
                ucs_status_t::UCS_INPROGRESS
            } else {
                ucs_status_t::UCS_OK
            }
        }

        let stream = Rc::new(AmStreamInner::new(id));
        unsafe {
            self.am_register(id, Some(callback), Rc::as_ptr(&stream) as _)?;
        }
        self.am_streams.write().unwrap().insert(id, stream.clone());

        return Ok(AmStream::new(self, stream));
    }

    /// Register active message handler for `id`.
    /// # Safety
    /// This method is not concurrent safe with `Worker::polling` or `Worker::event_poll`
    pub unsafe fn am_register(
        &self,
        id: u16,
        cb: ucp_am_recv_callback_t,
        arg: *mut c_void,
    ) -> Result<(), Error> {
        let param = ucp_am_handler_param_t {
            id: id as _,
            cb,
            arg,
            field_mask: (ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_ID
                | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_CB
                | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_ARG)
                .0 as _,
            flags: 0,
        };
        let status = ucp_worker_set_am_recv_handler(self.handle, &param as _);
        Error::from_status(status)?;
        if let Some(stream) = self.am_streams.write().unwrap().remove(&id) {
            stream.unregister();
        }

        Ok(())
    }
}

/// Active message endpoint.
impl Endpoint {
    /// Send active message.
    pub async fn am_send(
        &self,
        id: u32,
        header: &[u8],
        data: &[u8],
        need_reply: bool,
        proto: Option<AmProto>,
    ) -> Result<(), Error> {
        let data = [IoSlice::new(data)];
        self.am_send_vectorized(id, header, &data, need_reply, proto)
            .await
    }

    /// Send active message.
    pub async fn am_send_vectorized(
        &self,
        id: u32,
        header: &[u8],
        data: &[IoSlice<'_>],
        need_reply: bool,
        proto: Option<AmProto>,
    ) -> Result<(), Error> {
        let endpoint = self.get_handle()?;
        am_send(endpoint, id, header, data, need_reply, proto).await
    }
}

/// Active message protocol
#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub enum AmProto {
    /// Eager protocol
    Eager,
    /// Rendezvous protocol
    Rndv,
}

async fn am_send(
    endpoint: ucp_ep_h,
    id: u32,
    header: &[u8],
    data: &[IoSlice<'_>],
    need_reply: bool,
    proto: Option<AmProto>,
) -> Result<(), Error> {
    unsafe extern "C" fn callback(request: *mut c_void, _status: ucs_status_t, _data: *mut c_void) {
        trace!("am_send: complete");
        let request = &mut *(request as *mut Request);
        request.waker.wake();
    }

    // Use RequestParam builder for send
    let param = RequestParam::new().cb_send(Some(callback));
    let param = match proto {
        Some(AmProto::Eager) => param.set_flag_eager(),
        Some(AmProto::Rndv) => param.set_flag_rndv(),
        None => param,
    };
    let param = if need_reply {
        param.set_flag_reply()
    } else {
        param
    };
    let (buffer, count, param) = if data.len() == 1 {
        (data[0].as_ptr(), data[0].len(), param)
    } else {
        (data.as_ptr() as _, data.len(), param.iov())
    };

    let status = unsafe {
        ucp_am_send_nbx(
            endpoint,
            id,
            header.as_ptr() as _,
            header.len() as _,
            buffer as _,
            count as _,
            param.as_ref(),
        )
    };
    if status.is_null() {
        trace!("am_send: complete");
        Ok(())
    } else if UCS_PTR_IS_PTR(status) {
        RequestHandle {
            ptr: status,
            poll_fn: poll_normal,
        }
        .await
    } else {
        Err(Error::from_ptr(status).unwrap_err())
    }
}

#[cfg(test)]
#[cfg(feature = "am")]
mod tests {
    use super::*;

    #[test_log::test]
    fn am() {
        let protos = vec![None, Some(AmProto::Eager), Some(AmProto::Rndv)];
        for block_size_shift in 0..20_usize {
            for p in protos.iter() {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, send_recv(4 << block_size_shift, *p));
            }
        }
    }

    async fn send_recv(data_size: usize, proto: Option<AmProto>) {
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
        let mut addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        addr.set_port(listen_port);
        let (endpoint1, endpoint2) = tokio::join!(
            async {
                let conn1 = listener.next().await;
                worker1.accept(conn1).await.unwrap()
            },
            async { worker2.connect_socket(addr).await.unwrap() },
        );

        let stream1 = worker1.am_stream(16).unwrap();
        let stream2 = worker2.am_stream(12).unwrap();

        let header = vec![1, 2, 3, 4];
        let data = vec![1_u8; data_size];
        let (_, msg) = tokio::join!(
            async {
                // send msg
                let result = endpoint2
                    .am_send(16, header.as_slice(), data.as_slice(), true, proto)
                    .await;
                assert!(result.is_ok());
            },
            async {
                // recv msg
                let msg = stream1.wait_msg().await;
                let mut msg = msg.expect("no msg");
                assert_eq!(msg.header(), &header);
                assert_eq!(msg.contains_data(), true);
                assert_eq!(msg.data_len(), data.len());
                let mut recv_data = vec![0_u8; msg.data_len()];
                let recv_len = msg.recv_data_single(&mut recv_data).await.unwrap();
                assert_eq!(data.len(), recv_len);
                assert_eq!(data, recv_data);
                assert_eq!(msg.contains_data(), false);
                msg
            }
        );

        let header = vec![1, 3, 9, 10];
        let data = vec![2_u8; data_size];
        tokio::join!(
            async {
                // send reply
                let result = unsafe { msg.reply(12, &header, &data, false, proto).await };
                assert!(result.is_ok());
            },
            async {
                // recv reply
                let reply = stream2.wait_msg().await;
                let mut reply = reply.expect("no reply");
                assert_eq!(reply.header(), &header);
                assert_eq!(reply.contains_data(), true);
                assert_eq!(reply.data_len(), data.len());
                let recv_data = reply.recv_data().await.unwrap();
                assert_eq!(data, recv_data);
                assert_eq!(reply.contains_data(), false);
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
