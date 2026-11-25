//! defer timestamp rendering and string quoting until the logging thread consumes a record.
use chrono::{DateTime, SecondsFormat, Utc};
use insomnilog::{CustomEncode, Decoder};
use std::{fmt, ptr};

/// provide the native value or deferred encoding for a named field.
pub trait Argument {
    /// value copied into the producer queue.
    type Encoded<'a>
    where
        Self: 'a;
    /// prepare the value for its text or JSON placeholder.
    fn argument(&self) -> Self::Encoded<'_>;
}

/// copy native integer and boolean fields directly into the insomnilog queue.
macro_rules! native {
    ($($ty:ty),*) => {$(
        impl Argument for $ty {
            type Encoded<'a> = Self;
            fn argument(&self) -> Self { *self }
        }
    )*};
}
native!(usize, u64, bool);

/// borrowed UTF-8 field whose quoting is deferred to the backend.
pub struct Quoted<'a>(&'a str);

impl Argument for str {
    type Encoded<'a> = Quoted<'a>;

    fn argument(&self) -> Quoted<'_> {
        Quoted(self)
    }
}

// safety: the payload is exactly the UTF-8 bytes of the borrowed string.
unsafe impl CustomEncode for Quoted<'_> {
    fn payload_size(&self) -> usize {
        self.0.len()
    }

    unsafe fn encode_payload(&self, dst: *mut u8) {
        unsafe { ptr::copy_nonoverlapping(self.0.as_ptr(), dst, self.0.len()) }
    }

    fn decoder() -> Decoder {
        |bytes, out| {
            let text = str::from_utf8(bytes).map_err(|_| fmt::Error)?;
            out.write_str(&serde_json::to_string(text).map_err(|_| fmt::Error)?)
        }
    }
}

/// capture wall time on the producer and format it on the consumer.
pub struct Timestamp(i64);

impl Timestamp {
    /// sample wall time at the log call.
    pub fn now() -> Self {
        Self(Utc::now().timestamp_micros())
    }
}

// safety: both sides use the same eight-byte native-endian microsecond count.
unsafe impl CustomEncode for Timestamp {
    fn payload_size(&self) -> usize {
        size_of::<i64>()
    }

    unsafe fn encode_payload(&self, dst: *mut u8) {
        unsafe { dst.cast::<i64>().write_unaligned(self.0) }
    }

    fn decoder() -> Decoder {
        |bytes, out| {
            let micros = i64::from_ne_bytes(bytes.try_into().map_err(|_| fmt::Error)?);
            let time = DateTime::from_timestamp_micros(micros).ok_or(fmt::Error)?;
            out.write_str(&time.to_rfc3339_opts(SecondsFormat::Micros, true))
        }
    }
}
