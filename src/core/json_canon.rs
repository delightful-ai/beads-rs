//! Canonical JSON encoder for checkpoint hashing.

use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant, Serializer,
};
use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CanonJsonError {
    #[error("json encode failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("non-finite float values are not allowed")]
    NonFiniteFloat,
}

/// Serialize a value to canonical JSON bytes.
///
/// Canonical rules:
/// - object keys sorted by UTF-8 byte order, recursively
/// - no insignificant whitespace
/// - reject NaN/Infinity floats
pub fn to_canon_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, CanonJsonError> {
    ensure_finite(value)?;
    let value = serde_json::to_value(value)?;
    let canon = canon_value(value);
    Ok(serde_json::to_vec(&canon)?)
}

fn ensure_finite<T: Serialize>(value: &T) -> Result<(), CanonJsonError> {
    value
        .serialize(FiniteFloatSerializer)
        .map_err(|_| CanonJsonError::NonFiniteFloat)
}

#[derive(Debug)]
enum FiniteFloatError {
    NonFiniteFloat,
}

impl std::fmt::Display for FiniteFloatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("non-finite float")
    }
}

impl std::error::Error for FiniteFloatError {}

impl serde::ser::Error for FiniteFloatError {
    fn custom<T: std::fmt::Display>(_msg: T) -> Self {
        FiniteFloatError::NonFiniteFloat
    }
}

struct FiniteFloatSerializer;

struct FiniteCompound;

impl Serializer for FiniteFloatSerializer {
    type Ok = ();
    type Error = FiniteFloatError;
    type SerializeSeq = FiniteCompound;
    type SerializeTuple = FiniteCompound;
    type SerializeTupleStruct = FiniteCompound;
    type SerializeTupleVariant = FiniteCompound;
    type SerializeMap = FiniteCompound;
    type SerializeStruct = FiniteCompound;
    type SerializeStructVariant = FiniteCompound;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        if v.is_finite() {
            Ok(())
        } else {
            Err(FiniteFloatError::NonFiniteFloat)
        }
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        if v.is_finite() {
            Ok(())
        } else {
            Err(FiniteFloatError::NonFiniteFloat)
        }
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(FiniteCompound)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(FiniteCompound)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(FiniteCompound)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(FiniteCompound)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(FiniteCompound)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(FiniteCompound)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(FiniteCompound)
    }
}

impl SerializeSeq for FiniteCompound {
    type Ok = ();
    type Error = FiniteFloatError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteFloatSerializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for FiniteCompound {
    type Ok = ();
    type Error = FiniteFloatError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteFloatSerializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for FiniteCompound {
    type Ok = ();
    type Error = FiniteFloatError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteFloatSerializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleVariant for FiniteCompound {
    type Ok = ();
    type Error = FiniteFloatError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteFloatSerializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeMap for FiniteCompound {
    type Ok = ();
    type Error = FiniteFloatError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        key.serialize(FiniteFloatSerializer)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteFloatSerializer)
    }

    fn serialize_entry<K: ?Sized + Serialize, V: ?Sized + Serialize>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error> {
        key.serialize(FiniteFloatSerializer)?;
        value.serialize(FiniteFloatSerializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStruct for FiniteCompound {
    type Ok = ();
    type Error = FiniteFloatError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(FiniteFloatSerializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStructVariant for FiniteCompound {
    type Ok = ();
    type Error = FiniteFloatError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(FiniteFloatSerializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

fn canon_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut canon = Map::new();
            for (key, value) in entries {
                canon.insert(key, canon_value(value));
            }
            Value::Object(canon)
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().map(canon_value).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn canon_json_sorts_keys_recursively() {
        let value = json!({
            "b": 1,
            "a": {
                "d": 4,
                "c": 3
            },
            "aa": [
                {"z": 1, "y": 2}
            ]
        });

        let bytes = to_canon_json_bytes(&value).unwrap();
        let expected = br#"{"a":{"c":3,"d":4},"aa":[{"y":2,"z":1}],"b":1}"#;
        assert_eq!(bytes, expected);
    }

    #[test]
    fn canon_json_is_deterministic_for_hashmap() {
        let mut map_a = HashMap::new();
        map_a.insert("b".to_string(), 2u32);
        map_a.insert("a".to_string(), 1u32);

        let mut map_b = HashMap::new();
        map_b.insert("a".to_string(), 1u32);
        map_b.insert("b".to_string(), 2u32);

        let bytes_a = to_canon_json_bytes(&map_a).unwrap();
        let bytes_b = to_canon_json_bytes(&map_b).unwrap();
        assert_eq!(bytes_a, bytes_b);
    }

    #[derive(Serialize)]
    struct FloatSample {
        value: f64,
    }

    #[test]
    fn canon_json_rejects_non_finite_numbers() {
        let nan = FloatSample { value: f64::NAN };
        assert!(to_canon_json_bytes(&nan).is_err());

        let inf = FloatSample {
            value: f64::INFINITY,
        };
        assert!(to_canon_json_bytes(&inf).is_err());
    }
}
