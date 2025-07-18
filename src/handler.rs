use crate::parser::*;
use bytes::{BufMut, Bytes, BytesMut};

fn append_crlf(bytes: &Bytes) -> Bytes {
    let mut buffer = BytesMut::with_capacity(bytes.len() + 2);
    buffer.extend_from_slice(&bytes[..]);
    buffer.put_slice(b"\r\n");
    buffer.freeze()
}

fn combine_bytes_vector(vec_bytes: Vec<Bytes>) -> Bytes {
    let total_size = vec_bytes.iter().map(|x| x.len()).sum();
    let mut buffer = BytesMut::with_capacity(total_size);
    vec_bytes.into_iter().for_each(|b| buffer.extend_from_slice(&b));
    buffer.freeze()
}

impl RespOrig {
    /// TODO лишняя функция, лучше сразу обрабатывать входящие команды(назвать функцию handle_command)
    /// TODO а то получается лишняя обёртка
    pub fn extract_value(self) -> Option<Bytes> {
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
    pub fn to_resp(value: Bytes) {
        todo!()
    }
}
