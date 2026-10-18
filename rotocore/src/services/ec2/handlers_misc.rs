//! EC2 handler group `misc`: the trailing families not yet owned by the
//! earlier groups. Every op that lands here before its real handler is
//! ported answers moto's Not Implemented (the parity floor).

#![allow(unused_variables)]

use super::Params;
use crate::{Request, Response};

type P = Params;

pub fn route(req: &Request, params: &P) -> Response {
    super::not_implemented(&req.operation)
}
