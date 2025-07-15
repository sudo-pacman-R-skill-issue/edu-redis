#![allow(unused_imports)]
use std::{io::{BufReader, Error, Read, Write}, net::TcpListener};

fn main() -> Result<(), Error> {
    println!("Logs from your program will appear here!");

    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let mut buf = [0u8;512];
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut buf_reader = BufReader::new(stream);
                loop {
                    if buf_reader.read(&mut buf).unwrap() > 0 {
                        let streamchik = buf_reader.get_mut();
                        streamchik.write_all(b"+PONG\r\n")?;
                        streamchik.flush()?;
                    } else {
                        break
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
