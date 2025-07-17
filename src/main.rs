#![allow(unused_imports)]
use bytes::BytesMut;
use codecrafters_redis::parser::RespParser;
use std::{
    io::{Error, Read, Write},
    thread,
};
use tokio::io::BufReader;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::Decoder;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let (mut stream, _) = listener.accept().await?;
    tokio::spawn(async move {
        loop {
            let mut buf = BytesMut::with_capacity(512);
            if &stream.read_buf(&mut buf).await.unwrap() >= &(0 as usize) {
                let mut resp: RespParser = Default::default();
                let resp_value = resp.decode(&mut buf).unwrap();

                stream.write_all(todo!()).await?
            } else {
                break;
            }
        }
        Ok::<(), Error>(())
    })
    .await??;
    Ok(())
}
