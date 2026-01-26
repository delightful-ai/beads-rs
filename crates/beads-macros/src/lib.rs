//! Declarative macros for beads-rs.
//!
//! This crate provides utility macros used across the beads ecosystem.

/// Generates `as_str` and `parse_str` methods for a string-backed enum.
///
/// # Example
///
/// ```ignore
/// enum_str! {
///     impl MyEnum {
///         pub fn as_str(&self) -> &'static str;
///         pub fn parse_str(raw: &str) -> Option<Self>;
///         variants {
///             Foo => ["foo", "f"],
///             Bar => ["bar"],
///         }
///     }
/// }
/// ```
///
/// This generates:
/// - `as_str(&self)` returns the first string for each variant
/// - `parse_str(&str)` matches any alias and returns the variant
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
