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
            RespOrig::Error(bytes) => Some(bytes),
            RespOrig::Int(int) => Some(Bytes::copy_from_slice(format!("{}", int).as_bytes())),
            RespOrig::Array(resp_origs) => {
                let mut result = Vec::with_capacity(resp_origs.len());
                resp_origs.into_iter().for_each(|element| 
                    match Self::extract_value(element) {
                        Some(value) => result.push(append_crlf(&value)),
                        None => todo!(),
                    }
                );
                let res = combine_bytes_vector(result);
                Some(res)
            }
            RespOrig::NullArray => None,
            RespOrig::NullBulkString => None,
        }
    }

    ///TODO: функция энкодер в resp протокол
    pub fn to_resp(value: Bytes) -> Resp {
        todo!()
    }
}
