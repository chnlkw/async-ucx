use ucx1_sys::*;

pub struct RequestParam {
    inner: ucp_request_param_t,
}

impl RequestParam {
    pub fn new() -> Self {
        // Zeroed for safety, as in C
        Self {
            inner: unsafe { std::mem::zeroed() },
        }
    }

    pub fn send_cb(mut self, callback: ucp_send_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
        unsafe {
            let mut cb: ucp_request_param_t__bindgen_ty_1 = std::mem::zeroed();
            cb.send = callback;
            self.inner.cb = cb;
        }
        self
    }

    pub fn cb_tag_recv(mut self, callback: ucp_tag_recv_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
        unsafe {
            let mut cb: ucp_request_param_t__bindgen_ty_1 = std::mem::zeroed();
            cb.recv = callback;
            self.inner.cb = cb;
        }
        self
    }

    pub fn cb_stream_recv(mut self, callback: ucp_stream_recv_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
        unsafe {
            let mut cb: ucp_request_param_t__bindgen_ty_1 = std::mem::zeroed();
            cb.recv_stream = callback;
            self.inner.cb = cb;
        }
        self
    }

    #[cfg(feature = "am")]
    pub fn cb_recv_am(mut self, callback: ucp_am_recv_data_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
        unsafe {
            let mut cb: ucp_request_param_t__bindgen_ty_1 = std::mem::zeroed();
            cb.recv_am = callback;
            self.inner.cb = cb;
        }
        self
    }

    pub fn iov(mut self) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32;
        self.inner.datatype = ucp_dt_type::UCP_DATATYPE_IOV as _;
        self
    }

    #[cfg(feature = "am")]
    fn set_flag(&mut self) {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS as u32;
    }

    #[cfg(feature = "am")]
    pub fn set_flag_eager(mut self) -> Self {
        self.set_flag();
        self.inner.flags |= ucp_send_am_flags::UCP_AM_SEND_FLAG_EAGER.0;
        self
    }

    #[cfg(feature = "am")]
    pub fn set_flag_rndv(mut self) -> Self {
        self.set_flag();
        self.inner.flags |= ucp_send_am_flags::UCP_AM_SEND_FLAG_RNDV.0;
        self
    }

    #[cfg(feature = "am")]
    pub fn set_flag_reply(mut self) -> Self {
        self.set_flag();
        self.inner.flags |= ucp_send_am_flags::UCP_AM_SEND_FLAG_REPLY.0;
        self
    }

    pub fn as_ref(&self) -> *const ucp_request_param_t {
        &self.inner as *const _
    }
}
