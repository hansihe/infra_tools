use std::{future::Future, pin::Pin};

#[pin_project::pin_project(project = EnumProj)]
pub enum MaybeFut<F> {
    Some(#[pin] F),
    None,
}

impl<F> MaybeFut<F> {
    pub fn new_if(inner: F, cond: bool) -> Self {
        match cond {
            true => Self::Some(inner),
            false => Self::None,
        }
    }

    pub fn from_option(inner: Option<F>) -> Self {
        match inner {
            Some(fut) => Self::Some(fut),
            None => Self::None,
        }
    }
}

impl<O, F: Future<Output = O>> Future for MaybeFut<F> {
    type Output = O;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.project() {
            EnumProj::Some(pin) => Future::poll(pin, cx),
            EnumProj::None => std::task::Poll::Pending,
        }
    }
}

pub fn round_to_nearest(value: i64, base: i64, round_up: bool) -> i64 {
    if base <= 0 {
        return value;
    }

    let remainder = value.rem_euclid(base); // This ensures positive remainder
    let rounded = value - remainder;

    if remainder == 0 {
        return value;
    }

    if round_up {
        rounded + base
    } else {
        if remainder >= base / 2 {
            rounded + base
        } else {
            rounded
        }
    }
}
