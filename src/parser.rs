use bytes::{Bytes, BytesMut};
use core::str;
use memchr;
use tokio_util::{self, codec::Decoder};

#[derive(Clone, PartialEq)]
struct BufSplit(usize, usize);

impl BufSplit {
    pub fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    pub fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

#[derive(Clone, PartialEq)]
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
    fn redis_value(self, buf: &Bytes) -> RespOrig {
        match self {
            // bfs is BufSplit(start, end), which has the as_bytes method defined above
            Resp::String(bfs) => RespOrig::String(bfs.as_bytes(buf)),
            Resp::BufString(bfs) => RespOrig::BulkString(bfs.as_bytes(buf)),
            Resp::Error(bfs) => RespOrig::Error(bfs.as_bytes(buf)),
            Resp::Array(arr) => {
                RespOrig::Array(arr.into_iter().map(|bfs| bfs.redis_value(buf)).collect())
            }
            Resp::NullArray => RespOrig::NullArray,
            Resp::NullBulkString => RespOrig::NullBulkString,
            Resp::Int(i) => RespOrig::Int(i),
        }
    }
}
/// original look of resp type for values flowing thorugh the system. inputs and ouputs converts into 'Resp'
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
        RESPError::IOError(err)
    }
}

#[derive(Default)]
pub struct RespParser;
type RedisResult = Result<Option<(usize, Resp)>, RESPError>;

impl RespParser {
    fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
        // nowhere to continue. end of packet
        if buf.len() <= pos {
            return None;
        }
        //start looking for for "\r" after word - end of word
        memchr::memchr(b'\r', &buf[pos..]).and_then(|end| {
            if end + 1 < buf.len() {
                // pos + end == end of word
                // pos + end + 2 == \r\n<HERE>
                Some((pos + end + 2, BufSplit(pos, pos + end)))
            } else {
                // edge
                None
            }
        })
    }

    fn parse(buf: &BytesMut, pos: usize) -> RedisResult {
        if buf.is_empty() {
            return Ok(None);
        }

        match buf[pos] {
            b'+' => simple_string(buf, pos + 1),
            b'-' => error(buf, pos + 1),
            b'$' => bulk_string(buf, pos + 1),
            b':' => resp_int(buf, pos + 1),
            b'*' => array(buf, pos + 1),
            _ => Err(RESPError::UnknownStartingByte),
        }
    }
}

impl Decoder for RespParser {
    type Item = RespOrig;
    type Error = RESPError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !src.is_empty() {
            return Ok(None);
        }

        match RespParser::parse(src, 0)? {
            Some((pos, value)) => {
                let data = src.split_to(pos);
                Ok(Some(value.redis_value(&data.freeze())))
            }
            None => Ok(None),
        }
    }
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(RespParser::word(buf, pos).map(|(pos, word)| (pos, Resp::String(word))))
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-errors
fn error(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(RespParser::word(buf, pos).map(|(pos, word)| (pos, Resp::Error(word))))
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#integers
fn resp_int(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(int(buf, pos)?.map(|(pos, int)| (pos, Resp::Int(int))))
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
    // recall that the `pos` returned by `int` is the first index of the string content.
    match int(buf, pos)? {
        // special case: redis defines a NullBulkString type, with length of -1.
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#null-bulk-strings
        Some((pos, -1)) => Ok(Some((pos, Resp::NullBulkString))),
        // We have a size >= 0
        Some((pos, size)) if size >= 0 => {
            // We trust the client here, and directly calculate the end index of string (absolute w.r.t pos)
            let total_size = pos + size as usize;
            // The client hasn't sent us enough bytes
            if buf.len() < total_size + 2 {
                Ok(None)
            } else {
                // We have enough bytes, so we can generate the correct type.
                let bb = Resp::String(BufSplit(pos, total_size));
                // total_size + 2 == ...bulkstring\r\n<HERE> -- after CLRF
                Ok(Some((total_size + 2, bb)))
            }
        }
        // We recieved a garbage size (size < -1), so error out
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        // Not enough bytes to parse an int (i.e. no CLRF to delimit the int)
        None => Ok(None),
    }
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
fn array(buf: &BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, Resp::NullArray))),
        Some((pos, num_elements)) if num_elements >= 0 => {
            let mut values = Vec::with_capacity(num_elements as usize);
            let mut curr_pos = pos;
            for _ in 0..num_elements {
                match RespParser::parse(buf, curr_pos)? {
                    Some((pos, word)) => {
                        curr_pos += pos;
                        values.push(word);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((curr_pos, Resp::Array(values))))
        }
        Some((_pos, bad)) => Err(RESPError::BadArraySize(bad)),
        None => Ok(None),
    }
}

pub fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    RespParser::word(buf, pos)
        .map(|(pos, bufsplit)| {
            let s =
                str::from_utf8(bufsplit.as_slice(buf)).map_err(|_| RESPError::IntParseFailure)?;
            let i: i64 = s.parse().map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        })
        .unwrap()
}
