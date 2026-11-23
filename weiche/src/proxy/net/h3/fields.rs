//! validate decoded HTTP/3 field sections and enforce the declared upload length.
use compio::quic::h3::error::Code;
use http::{HeaderMap, header};
use std::io;

/// check the connection-specific field rules shared by headers and trailers.
fn valid_fields(headers: &HeaderMap) -> bool {
    !["connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade"]
        .iter()
        .any(|&name| headers.contains_key(name))
        && headers
            .get_all(header::TE)
            .iter()
            .all(|v| v.as_bytes().eq_ignore_ascii_case(b"trailers"))
}

/// bytes still expected from DATA when the client supplied Content-Length.
pub struct ContentLength(Option<u64>);

impl ContentLength {
    /// validate the initial fields before sending the request upstream.
    pub fn parse(headers: &HeaderMap) -> Result<Self, Code> {
        if !valid_fields(headers) {
            return Err(Code::H3_MESSAGE_ERROR);
        }
        let mut values = headers.get_all(header::CONTENT_LENGTH).iter();
        let Some(value) = values.next() else { return Ok(Self(None)) };
        if values.next().is_some() || !value.as_bytes().iter().all(u8::is_ascii_digit) {
            return Err(Code::H3_MESSAGE_ERROR);
        }
        let decimal = value.to_str().map_err(|_| Code::H3_MESSAGE_ERROR)?;
        let length = decimal.parse().map_err(|_| Code::H3_MESSAGE_ERROR)?;
        Ok(Self(Some(length)))
    }

    /// account for each chunk before making it available to the backend.
    pub fn consume(&mut self, size: usize) -> io::Result<()> {
        if let Some(left) = &mut self.0 {
            *left = left.checked_sub(size as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "excess HTTP/3 request data")
            })?;
        }
        Ok(())
    }

    /// require all declared bytes before accepting trailers or EOF.
    pub fn finish(self) -> io::Result<()> {
        if self.0.is_some_and(|left| left != 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete HTTP/3 request body",
            ));
        }
        Ok(())
    }
}

/// reject connection, framing and routing fields in the trailing section.
pub fn trailers(headers: HeaderMap) -> io::Result<HeaderMap> {
    if !valid_fields(&headers)
        || headers.contains_key(header::CONTENT_LENGTH)
        || headers.contains_key(header::HOST)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid HTTP/3 trailers",
        ));
    }
    Ok(headers)
}
