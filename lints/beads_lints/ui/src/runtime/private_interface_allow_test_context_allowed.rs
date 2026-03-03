// compile-flags: --test
#![allow(dead_code)]

mod hidden {
    pub(crate) struct Token;
}

#[cfg(test)]
mod tests {
    use super::hidden;

    #[allow(private_interfaces)]
    pub enum TestApi {
        V(hidden::Token),
    }

    #[allow(rustc::private_interfaces)]
    pub enum TestApiNamespaced {
        V(hidden::Token),
    }

    #[test]
    fn smoke() {
        let _ = core::mem::size_of::<TestApi>();
        let _ = core::mem::size_of::<TestApiNamespaced>();
    }
}