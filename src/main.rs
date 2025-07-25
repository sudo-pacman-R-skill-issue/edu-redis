#![allow(unused_imports)]
use bytes::BytesMut;
use codecrafters_redis::parser::{RespParser, RespOrig};
use codecrafters_redis::handler::ToResp;
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
use tracing::{debug, error, info, span, trace, warn, Level, Instrument};
use tracing_subscriber::{EnvFilter, prelude::*};

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            EnvFilter::new("debug")
                .add_directive("tokio=info".parse().unwrap())
                .add_directive("runtime=info".parse().unwrap())
        });

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_forest::ForestLayer::default())
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_tracing();
    
    info!("Starting Redis server...");
    
    let addr = "127.0.0.1:6379";
    info!(address = %addr, "Binding TCP listener");
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!(address = %addr, "Successfully bound to address");
            l
        },
        Err(e) => {
            error!(error = ?e, address = %addr, "Failed to bind to address");
            return Err(e);
        }
    };
    
    info!("Waiting for client connections");
    
    loop {
        let accept_span = span!(Level::INFO, "accept_connection");
        let connection = listener.accept().instrument(accept_span).await;
        
        match connection {
            Ok((stream, addr)) => {
                info!(client = %addr, "New client connected");
                
                tokio::spawn(
                    async move {
                        debug!(client = %addr, "Starting client handler task");
                        if let Err(e) = handle_client(stream).await {
                            error!(client = %addr, error = ?e, "Error handling client");
                        }
                        info!(client = %addr, "Client disconnected");
                    }
                    .instrument(span!(Level::INFO, "client", %addr))
                );
            },
            Err(e) => {
                error!(error = ?e, "Failed to accept connection");
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream) -> Result<(), Error> {
    info!("Client handler started");
    
    let addr = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => "unknown".to_string(),
    };
    
    loop {
        let mut buf = BytesMut::with_capacity(512);
        
        let read_span = span!(Level::DEBUG, "read_from_socket");
        let bytes_read = stream
            .read_buf(&mut buf)
            .instrument(read_span)
            .await;
        
        match bytes_read {
            Ok(0) => {
                debug!("Client closed connection (read 0 bytes)");
                break;
            },
            Ok(n) => {
                debug!(bytes = n, "Read data from client");
                trace!(data = ?buf, "Raw input data");
                
                let parse_span = span!(Level::DEBUG, "parse_command");
                let result = async {
                    let mut resp: RespParser = Default::default();
                    match resp.decode(&mut buf) {
                        Ok(Some(resp_value)) => {
                            debug!(command = ?resp_value, "Successfully parsed command");
                            
                            let handle_span = span!(Level::DEBUG, "handle_command");
                            let response = resp_value
                                .handle_command()
                                .instrument(handle_span)
                                .inner().clone();
                            
                            match response {
                                Some(bytes) => {
                                    debug!(response_size = bytes.len(), "Command produced response");
                                    trace!(response = ?bytes, "Response data");
                                    
                                    let write_span = span!(Level::DEBUG, "write_response");
                                    match stream.write_all(&bytes).instrument(write_span).await {
                                        Ok(_) => {
                                            debug!("Response sent successfully");
                                            Ok(())
                                        },
                                        Err(e) => {
                                            error!(error = ?e, "Failed to send response");
                                            Err(e)
                                        }
                                    }
                                },
                                None => {
                                    debug!("Command produced no response");
                                    Ok(())
                                }
                            }
                        },
                        Ok(None) => {
                            debug!("Incomplete command, waiting for more data");
                            Ok(())
                        },
                        Err(e) => {
                            error!(error = ?e, "Failed to parse command");
                            let error_msg = format!("-ERR parsing error: {:?}\r\n", e);
                            stream.write_all(error_msg.as_bytes()).await?;
                            Ok(())
                        }
                    }
                }
                .instrument(parse_span)
                .await;
                
                if let Err(e) = result {
                    error!(error = ?e, "Error in command processing loop");
                    return Err(e);
                }
            },
            Err(e) => {
                error!(error = ?e, "Failed to read from socket");
                return Err(e);
            }
        }
    }
    
    info!("Client handler completed");
    Ok(())
}
