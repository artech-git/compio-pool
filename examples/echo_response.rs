
use compio_pool::Builder; 
use compio_pool::ManageConnection;
use compio_pool::Pool; 


struct UdpConnection{
    addr: std::net::SocketAddr,
    connections: Vec<std::net::UdpSocket>,
}; 

type UdpConnectionError = std::io::Error;

impl ManageConnection for UdpConnection {
    type Connection = std::net::UdpSocket;
    type Error = UdpConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // let socket = std::net::UdpSocket::bind("
        todo!(); 
    }
    
    fn is_valid(&self, conn: &mut Self::Connection)
    -> impl Future<Output = Result<(), Self::Error>> {
        todo!()
    }
    
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        todo!()
    }

}


#[compio::main]
async fn main() { 
    let file_pool = Pool::builder().max_size(4).build(compio_pool::FileManager);
}