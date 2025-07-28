use bytes::{Bytes, BytesMut};
use core::str;
use memchr;
use tokio_util::{self, codec::Decoder};
use tracing::{debug, error, info, trace, warn};

#[derive(Debug, Clone, PartialEq)]
pub struct BufSplit(usize, usize);

impl BufSplit {
    pub fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        trace!(start = self.0, end = self.1, "Extracting slice from buffer");
        &buf[self.0..self.1]
    }

    pub fn as_bytes(&self, buf: &Bytes) -> Bytes {
        trace!(start = self.0, end = self.1, "Converting to Bytes");
        buf.slice(self.0..self.1)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum Resp {
    String(BufSplit),
    BufString(BufSplit),
    Error(BufSplit),
    Array(Vec<Resp>),
    Int(i64),
    NullArray,
    NullBulkString,
}

impl Resp {
    #[tracing::instrument(level = "debug", skip(buf))]
    fn redis_value(self, buf: &Bytes) -> RespOrig {
        debug!("Converting Resp to RespOrig");
        match self {
            Resp::String(bfs) => {
                let result = RespOrig::String(bfs.as_bytes(buf));
                debug!("Converted to String");
                result
            },
            Resp::BufString(bfs) => {
                let result = RespOrig::BulkString(bfs.as_bytes(buf));
                debug!("Converted to BulkString");
                result
            },
            Resp::Error(bfs) => {
                let result = RespOrig::Error(bfs.as_bytes(buf));
                debug!("Converted to Error");
                result
            },
            Resp::Array(arr) => {
                debug!(elements = arr.len(), "Converting array");
                let result = RespOrig::Array(arr.into_iter().map(|bfs| bfs.redis_value(buf)).collect());
                debug!("Converted to Array");
                result
            },
            Resp::NullArray => {
                debug!("Converted to NullArray");
                RespOrig::NullArray
            },
            Resp::NullBulkString => {
                debug!("Converted to NullBulkString");
                RespOrig::NullBulkString
            },
            Resp::Int(i) => {
                let result = RespOrig::Int(i);
                debug!(value = i, "Converted to Int");
                result
            },
        }
    }
}

/// original look of resp type for values flowing thorugh the system. inputs and ouputs converts into 'Resp'
#[derive(Debug)]
pub enum RespOrig {
    String(Bytes),
    BulkString(Bytes),
    Error(Bytes),
    Int(i64),
    Array(Vec<RespOrig>),
    NullArray,
    NullBulkString,
}

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
}

impl From<std::io::Error> for RESPError {
    fn from(err: std::io::Error) -> Self {
        error!(error = ?err, "IO Error occurred");
        RESPError::IOError(err)
    }
}

#[derive(Default)]
pub struct RespParser;
type RedisResult = Result<Option<(usize, Resp)>, RESPError>;

impl RespParser {
    #[tracing::instrument(level = "debug", skip(buf), fields(buf_len = buf.len()))]
    fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
        // nowhere to continue. end of packet
        if buf.len() <= pos {
            debug!("Buffer too short, can't read word");
            return None;
        }
        
        //start looking for for "\r" after word - end of word
        trace!("Searching for \\r in buffer");
        memchr::memchr(b'\r', &buf[pos..]).and_then(|end| {
            if buf.len() <= pos {
                return None;
            }
            if end + 1 < buf.len() {
                // pos + end == end of word
                // pos + end + 2 == \r\n<HERE>
                let new_pos = pos + end + 2;
                let word = BufSplit(pos, pos + end);
                trace!(new_pos, "Found word with \\r\\n termination");
                Some((new_pos, word))
            } else {
                // edge
                debug!("Found \\r but no \\n - incomplete word");
                None
            }
        })
    }

    #[tracing::instrument(level = "debug", skip(buf), fields(buf_len = buf.len()))]
    fn parse(buf: &BytesMut, pos: usize) -> RedisResult {
        if buf.is_empty() {
            debug!("Empty buffer, nothing to parse");
            return Ok(None);
        }

        println!("buffer: \n\n{buf:?}\n\n");

        
        match buf[pos] {
            b'+' => {
                debug!("Detected simple string");
                simple_string(buf, pos + 1)
            },
            b'-' => {
                debug!("Detected error");
                error(buf, pos + 1)
            },
            b'$' => {
                debug!("Detected bulk string");
                bulk_string(buf, pos + 1)
            },
            b':' => {
                debug!("Detected integer");
                resp_int(buf, pos + 1)
            },
            b'*' => {
                debug!("Detected array");
                array(buf, pos + 1)
            },
            _ => {
                Err(RESPError::UnknownStartingByte)
            },
        }
    }
}

impl Decoder for RespParser {
    type Item = RespOrig;
    type Error = RESPError;

    #[tracing::instrument(level = "debug", skip(self, src), fields(src_len = src.len()))]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        info!("Decoding RESP data");
        
        if src.is_empty() {  // Исправлено с !src.is_empty() на src.is_empty()
            debug!("Empty source buffer");
            return Ok(None);
        }

        match RespParser::parse(src, 0)? {
            Some((pos, value)) => {
                debug!(pos, "Successfully parsed RESP value");
                let data = src.split_to(pos);
                let result = value.redis_value(&data.freeze());
                info!(type = ?result, "Decoded RESP value");
                Ok(Some(result))
            }
            None => {
                debug!("Incomplete RESP data, waiting for more");
                Ok(None)
            },
        }
    }
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
#[tracing::instrument(level = "debug", skip(buf))]
fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
    trace!("Parsing simple string");
    Ok(RespParser::word(buf, pos).map(|(pos, word)| {
        trace!(pos, "Found simple string");
        (pos, Resp::String(word))
    }))
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-errors
#[tracing::instrument(level = "debug", skip(buf))]
fn error(buf: &BytesMut, pos: usize) -> RedisResult {
    trace!("Parsing error string");
    Ok(RespParser::word(buf, pos).map(|(pos, word)| {
        trace!(pos, "Found error string");
        (pos, Resp::Error(word))
    }))
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#integers
#[tracing::instrument(level = "debug", skip(buf))]
fn resp_int(buf: &BytesMut, pos: usize) -> RedisResult {
    trace!("Parsing integer");
    Ok(int(buf, pos)?.map(|(pos, int_val)| {
        trace!(pos, int_val, "Found integer");
        (pos, Resp::Int(int_val))
    }))
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
#[tracing::instrument(level = "debug", skip(buf))]
fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
    trace!("Parsing bulk string");
    // recall that the `pos` returned by `int` is the first index of the string content.
    match int(buf, pos)? {
        // special case: redis defines a NullBulkString type, with length of -1.
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#null-bulk-strings
        Some((pos, -1)) => {
            debug!(pos, "Found null bulk string");
            Ok(Some((pos, Resp::NullBulkString)))
        },
        // We have a size >= 0
        Some((pos, size)) if size >= 0 => {
            debug!(pos, size, "Bulk string with size");
            // We trust the client here, and directly calculate the end index of string (absolute w.r.t pos)
            let total_size = pos + size as usize;
            // The client hasn't sent us enough bytes
            if buf.len() < total_size + 2 {
                debug!(required = total_size + 2, actual = buf.len(), "Incomplete bulk string");
                Ok(None)
            } else {
                // We have enough bytes, so we can generate the correct type.
                let bb = Resp::String(BufSplit(pos, total_size));
                // total_size + 2 == ...bulkstring\r\n<HERE> -- after CLRF
                trace!(pos = total_size + 2, "Complete bulk string parsed");
                Ok(Some((total_size + 2, bb)))
            }
        }
        // We recieved a garbage size (size < -1), so error out
        Some((_pos, bad_size)) => {
            error!(size = bad_size, "Invalid bulk string size");
            Err(RESPError::BadBulkStringSize(bad_size))
        },
        // Not enough bytes to parse an int (i.e. no CLRF to delimit the int)
        None => {
            debug!("Incomplete bulk string size");
            Ok(None)
        },
    }
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
#[tracing::instrument(level = "debug", skip(buf))]
fn array(buf: &BytesMut, pos: usize) -> RedisResult {
    trace!("Parsing array");
    match int(buf, pos)? {
        Some((pos, -1)) => {
            debug!(pos, "Found null array");
            Ok(Some((pos, Resp::NullArray)))
        },
        Some((pos, num_elements)) if num_elements >= 0 => {
            debug!(pos, num_elements, "Array with elements");
            let mut values = Vec::with_capacity(num_elements as usize);
            let mut curr_pos = pos;
            for i in 0..num_elements {
                trace!(index = i, position = curr_pos, "Parsing array element");
                match RespParser::parse(buf, curr_pos)? {
                    Some((pos, word)) => {
                        curr_pos = pos;
                        trace!(new_position = curr_pos, "Element parsed");
                        values.push(word);
                    }
                    None => {
                        debug!(elements_parsed = i, "Incomplete array");
                        return Ok(None);
                    }
                }
            }
            debug!(elements = values.len(), "Array fully parsed");
            Ok(Some((curr_pos, Resp::Array(values))))
        }
        Some((_pos, bad)) => {
            error!(size = bad, "Invalid array size");
            Err(RESPError::BadArraySize(bad))
        },
        None => {
            debug!("Incomplete array size");
            Ok(None)
        },
    }
}

#[tracing::instrument(level = "debug", skip(buf))]
pub fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    trace!("Parsing integer value");
    RespParser::word(buf, pos)
        .map(|(pos, bufsplit)| {
            let bytes = bufsplit.as_slice(buf);
            trace!(bytes = ?bytes, "Raw integer bytes");
            
            let s = match str::from_utf8(bytes) {
                Ok(s) => s,
                Err(e) => {
                    error!(error = ?e, "Failed to parse UTF-8 for integer");
                    return Err(RESPError::IntParseFailure);
                }
            };
            
            trace!(string = s, "Integer as string");
            
            match s.parse::<i64>() {
                Ok(i) => {
                    debug!(position = pos, value = i, "Integer parsed successfully");
                    Ok(Some((pos, i)))
                },
                Err(e) => {
                    error!(error = ?e, string = s, "Failed to parse integer");
                    Err(RESPError::IntParseFailure)
                }
            }
        })
        .unwrap_or_else(|| {
            debug!("Incomplete integer");
            Ok(None)
        })
}