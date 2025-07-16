use memchr;
use bytes::{Bytes, BytesMut};
use tokio;
use tokio_util::{self, codec::Decoder};

pub type Value = Bytes;
pub type Key = Bytes;
pub type BufSplit = (usize, usize);

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
        memchr::memchr(b'\r', &buf[pos..])
            .and_then(|end| {
                let end = end as usize;
                if end + 1 < buf.len() {
                    // pos + end == end of word
                    // pos + end + 2 == \r\n<HERE>
                    Some((pos + end + 2, (pos, pos + end)))
                } else {
                    // edge
                    None
                }
            })
    }

    fn parse(&self, buf: &BytesMut, pos: usize) -> RedisResult {
        if !buf.is_empty() {
            return Ok(None)
        }

        match buf[pos] {
            b'+' => RespParser::simple_string(buf, pos + 1),
            b'-' => error(buf, pos + 1),
            b'$' => bulk_string(buf, pos + 1),
            b':' => resp_int(buf, pos + 1),
            b'*' => array(buf, pos + 1),
            _ =>Err(RESPError::UnknownStartingByte),
        }
    }

    pub fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
        RedisResult::Ok(RespParser::word(buf, pos).map(|(pos, word)| (pos, Resp::String(word))))
    }

    pub fn error(buf: &BytesMut, pos: usize) -> RedisResult {
        RedisResult::Ok(RespParser::word(buf, pos).map(|(pos, word)| (pos, Resp::Error(word))))
    }


    pub fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
        todo!()
    }

}
// impl Decoder for RespParser {
// type Item = RedisValue;
// type Error = RespError;

//     fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
//         todo!()
//     }
// }
