use crate::parser::*;
use bytes::{BufMut, Bytes, BytesMut};

pub trait ToResp {
    fn to_resp(self) -> Bytes;
}

impl RespOrig {
    /// TODO лишняя функция, лучше сразу обрабатывать входящие команды
    /// TODO лишняя обёртка
    pub fn extract_value/*handle_command*/(self) -> Option<Bytes> {
        match self {
            RespOrig::String(bytes) => Some(bytes),
            RespOrig::BulkString(bytes) => {
                Some(bytes)
            }
            RespOrig::Error(bytes) => Some(bytes),
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

    ///TODO: функция энкодер в resp протокол
    pub fn to_resp(value: Bytes) -> Resp {
        todo!()
    }
}
