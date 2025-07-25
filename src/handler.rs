use crate::parser::*;
use bytes::{BufMut, Bytes, BytesMut};
use tracing::*;

pub trait ToResp {
    fn to_resp(self) -> Bytes;
}

impl RespOrig {
    #[tracing::instrument(level = "debug")]
    pub fn handle_command(self) -> Option<Bytes> {
        debug!("handling resp command");
        match self {
            RespOrig::String(bytes) => {
                debug!(data = ?bytes, "Handling string command");
                Some(bytes)},
            RespOrig::BulkString(bytes) => {
                debug!(data = ?bytes, "Handling bulk string command");
                if bytes.is_empty() {
                    return Some(Bytes::from("$0\r\n\r\n"))
                }
                
                let mut buffer = Vec::with_capacity(bytes.len() + 5);
                buffer.extend_from_slice("$".as_bytes());
                buffer.put_slice(format!("{}\r\n", bytes.len()).as_bytes());
                buffer.extend_from_slice(&bytes);
                buffer.put_slice("\r\n".as_bytes());
                Some(Bytes::from(buffer))
            }
            RespOrig::Error(bytes) => {
                error!("Error command encountered: {:?}", bytes);
                Some(bytes)
            },
            RespOrig::Int(int) => Some(Bytes::copy_from_slice(format!("{int}").as_bytes())),
            RespOrig::Array(items) => {
                if items.is_empty() {
                    return None;
                }

                let command = &items[0];
                
                let cmd_name = match command {
                    RespOrig::String(bytes) | RespOrig::BulkString(bytes) => {
                        std::str::from_utf8(bytes).ok().map(|s| s.to_uppercase())
                    },
                    _ => None,
                };
                
                match cmd_name.as_deref() {
                    Some("PING") => Some(Bytes::from("+PONG\r\n")),
                    Some("ECHO") => {
                        if items.len() > 1 {
                            match &items[1] {
                                RespOrig::String(bytes) | RespOrig::BulkString(bytes) => {
                                    let mut buffer = BytesMut::with_capacity(bytes.len() + 3);
                                    buffer.put_u8(b'+');
                                    buffer.extend_from_slice(bytes);
                                    buffer.put_slice(b"\r\n");
                                    Some(buffer.freeze())
                                },
                                _ => None,
                            }
                        } else {
                            None
                        }
                    },
                    _ => {
                        Some(Bytes::from("-ERR unknown command\r\n"))
                    }
                }
            },
            RespOrig::NullArray => None,
            RespOrig::NullBulkString => None,
        }
    }
}
impl ToResp for RespOrig {
    fn to_resp(self) -> Bytes {
        match self {
            RespOrig::String(bytes) => {
                let mut buffer = BytesMut::with_capacity(bytes.len() + 3);
                buffer.put_u8(b'+');
                buffer.extend_from_slice(&bytes);
                buffer.put_slice(b"\r\n");
                buffer.freeze()
            },
            RespOrig::BulkString(bytes) => {
                let prefix = format!("${}\r\n", bytes.len());
                let mut buffer = BytesMut::with_capacity(prefix.len() + bytes.len() + 2);
                buffer.extend_from_slice(prefix.as_bytes());
                buffer.extend_from_slice(&bytes);
                buffer.put_slice(b"\r\n");
                buffer.freeze()
            },
            RespOrig::Error(bytes) => {
                let mut buffer = BytesMut::with_capacity(bytes.len() + 3);
                buffer.put_u8(b'-');
                buffer.extend_from_slice(&bytes);
                buffer.put_slice(b"\r\n");
                buffer.freeze()
            },
            RespOrig::Int(val) => {
                let num_str = val.to_string();
                let mut buffer = BytesMut::with_capacity(num_str.len() + 3);
                buffer.put_u8(b':');
                buffer.extend_from_slice(num_str.as_bytes());
                buffer.put_slice(b"\r\n");
                buffer.freeze()
            },
            RespOrig::Array(items) => {
                items.to_resp()
            },
            RespOrig::NullArray => {
                Bytes::from("*-1\r\n")
            },
            RespOrig::NullBulkString => {
                Bytes::from("$-1\r\n")
            }
        }
    }
}

// Реализуем для Vec<RespOrig>
impl ToResp for Vec<RespOrig> {
    fn to_resp(self) -> Bytes {
        let array_header = format!("*{}\r\n", self.len());
        let mut buffer = BytesMut::with_capacity(array_header.len() + 100);
        buffer.extend_from_slice(array_header.as_bytes());
        
        for item in self {
            let item_bytes = item.to_resp();
            buffer.extend_from_slice(&item_bytes);
        }
        
        buffer.freeze()
    }
}
