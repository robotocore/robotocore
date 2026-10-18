//! Service backends. Each module owns one AWS service and a `dispatch`.

use crate::{Request, Response};

pub mod acm;
pub mod cloudformation;
pub mod ec2;

pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "acm" => Some(self::acm::dispatch(req)),
        "cloudformation" => Some(self::cloudformation::dispatch(req)),
        "ec2" => Some(self::ec2::dispatch(req)),
        _ => None,
    }
}
