use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;

pub const MAX_BODY_BYTES: usize = 65_536;
const MAX_DEPTH: usize = 64;

/// Raw bounded JSON parsing with recursive duplicate-key rejection. Content-Type
/// is deliberately ignored; DTO validation follows parsing at the route boundary.
pub fn parse_json(bytes: &[u8]) -> Result<Value, &'static str> {
    if bytes.len() > MAX_BODY_BYTES {
        return Err("body_too_large");
    }
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = JsonSeed(0)
        .deserialize(&mut decoder)
        .map_err(|_| "invalid_json")?;
    decoder.end().map_err(|_| "invalid_json")?;
    Ok(value)
}

struct JsonSeed(usize);
impl<'de> DeserializeSeed<'de> for JsonSeed {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<Value, D::Error> {
        decoder.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for JsonSeed {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded JSON without duplicate keys")
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        Number::from_f64(v)
            .map(Value::Number)
            .ok_or_else(|| E::custom("nonfinite number"))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.into()))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        if self.0 >= MAX_DEPTH {
            return Err(de::Error::custom("JSON nesting limit"));
        }
        let mut values = Vec::new();
        while let Some(value) = access.next_element_seed(JsonSeed(self.0 + 1))? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        if self.0 >= MAX_DEPTH {
            return Err(de::Error::custom("JSON nesting limit"));
        }
        let mut values = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON key"));
            }
            values.insert(key, access.next_value_seed(JsonSeed(self.0 + 1))?);
        }
        Ok(Value::Object(values))
    }
}
