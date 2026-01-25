#[macro_export]
macro_rules! enum_str {
    (
        impl $name:ident {
            $as_vis:vis fn as_str(&self) -> &'static str;
            $parse_vis:vis fn parse_str($raw:ident : &str) -> Option<Self>;
            variants {
                $($variant:ident => [$first:expr $(, $alias:expr)*]),+ $(,)?
            }
        }
    ) => {
        impl $name {
            $as_vis fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => $first,)+
                }
            }

            #[allow(dead_code)]
            $parse_vis fn parse_str($raw: &str) -> Option<Self> {
                match $raw {
                    $($first $(| $alias)* => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }
    };
}
