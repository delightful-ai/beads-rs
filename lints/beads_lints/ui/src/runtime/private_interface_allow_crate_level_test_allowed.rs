// compile-flags: --test
#![allow(dead_code)]
#![allow(private_interfaces)]

mod hidden {
    pub(crate) struct Token;
}

#[cfg(test)]
mod tests {
    use super::hidden;

    pub enum TestApi {
        V(hidden::Token),
    }

    #[test]
    fn smoke() {
        let _ = core::mem::size_of::<TestApi>();
    }
}