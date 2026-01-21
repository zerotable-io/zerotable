use tonic::{Request, Response, Status, transport::Server};

use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
        };

        Ok(Response::new(reply))
    }
}

fn hello_fjall() -> fjall::Result<bool> {
    let db = fjall::Database::builder(".fjall_data").open()?;

    let items = db.keyspace("items", fjall::KeyspaceCreateOptions::default)?;

    items.is_empty()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match hello_fjall()? {
        true => println!("Empty"),
        false => println!("Has items"),
    }

    let addr = "[::1]:50051".parse()?;
    let greeter = MyGreeter::default();

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
