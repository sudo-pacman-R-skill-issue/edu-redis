use core::str;

use bytes::{Bytes, BytesMut};
use memchr;
use tokio;
use tokio_util::{self, codec::Decoder};

pub type Value = Bytes;
pub type Key = Bytes;

#[derive(Debug, Clone, PartialEq)]
pub struct BufSplit(usize, usize);

impl BufSplit {
    pub fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    pub fn as_bytes(&self, buf: Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Resp {
    String(BufSplit),
    Error(BufSplit),
    Array(Vec<Resp>),
    Int(i64),
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

impl Resp {
    // pub type ReprResult = Result<Option<Self::Item>, Self::Error>;
    fn serialize(self) -> Self {
        self
    }
}

#[derive(Debug)]
pub struct RespParser;
type RedisResult = Result<Option<(usize, Resp)>, RESPError>;

impl RespParser {
    pub fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
        // nowhere to continue. end of packet
        if buf.len() <= pos {
            return None;
        }
        //start looking for for "\r" after word - end of word
        memchr::memchr(b'\r', &buf[pos..]).and_then(|end| {
            let end = end as usize;
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
        if !buf.is_empty() {
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
    match int(buf, pos)? {
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#null-bulk-strings
        Some((pos, -1)) => Ok(Some((pos, Resp::NullBulkString))),

        Some((pos, size)) if size >= 0 => {
            let total_size = pos + size as usize;
            //not enough bytes has send
            if buf.len() < total_size + 2 {
                Ok(None)
            } else {
                let bb = Resp::String(BufSplit(pos, total_size));
                Ok(Some((pos, bb)))
            }
        },
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Err(RESPError::UnknownStartingByte),
    }
}

/// https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
fn array(buf: &BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) =>Ok(Some((pos, Resp::NullArray))),
        Some((pos, num_elements)) if num_elements >= 0 => {
            let mut values = Vec::with_capacity(num_elements as usize);
            let mut curr_pos = pos;
            for _ in 0..num_elements {
                match RespParser::parse(buf, curr_pos)? {
                    Some((pos, word)) => {
                        curr_pos += pos;
                        values.push(word);
                    },
                    None => return Ok(None),
                }
            }
            Ok(Some((curr_pos, Resp::Array(values))))
        },
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

// impl Decoder for RespParser {
// type Item = RedisValue;
// type Error = RespError;

//     fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
//         todo!()
//     }
// }
