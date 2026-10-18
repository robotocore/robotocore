//! EC2 backend (in progress).

use crate::proto::error_response;
use crate::{Request, Response};

pub fn dispatch(req: &Request) -> Response {
    // Placeholder until the port lands: respond the way moto's sidecar did
    // for unimplemented ops.
    error_response(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}
