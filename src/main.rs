use dotenv::dotenv;
use std::net::SocketAddr;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpSocket, TcpStream, UnixStream};

pub fn tcp_listener(addr: SocketAddr, buffer_size: Option<u32>) -> TcpListener {
    let buffer_size = buffer_size.unwrap_or(1152);
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_keepalive(true).unwrap();
    socket.set_reuseaddr(true).unwrap();
    socket.set_reuseport(true).unwrap();
    socket.set_recv_buffer_size(buffer_size).unwrap();
    socket.set_send_buffer_size(buffer_size).unwrap();

    socket.bind(addr).unwrap();

    socket.listen(2048).unwrap()
}

pub fn unix_socket_backends() -> Vec<String> {
    std::env::var("BACKENDS")
        .unwrap()
        .split(",")
        .map(|s| s.to_owned())
        .collect::<Vec<String>>()
}

pub fn round_robin(backends: &mut Vec<String>) -> String {
    let backend = backends[0].clone();

    backends.rotate_left(1);

    backend
}

pub async fn handle(mut tcp_stream: TcpStream, backend: &str) {
    let mut unix_stream = UnixStream::connect(backend).await.unwrap();

    copy_bidirectional(&mut tcp_stream, &mut unix_stream)
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let port = std::env::var("PORT").unwrap_or("9999".to_string());
    let addr = format!("0.0.0.0:{port}");
    let listener = tcp_listener(addr.parse().unwrap(), Some(1152));

    let mut backends = unix_socket_backends();

    loop {
        let (tcp_stream, _) = listener.accept().await.unwrap();

        let backend = round_robin(&mut backends);

        tokio::spawn(async move {
            handle(tcp_stream, &backend).await;
        });
    }
}
