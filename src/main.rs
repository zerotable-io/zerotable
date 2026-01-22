// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

use tonic::{Request, Response, Status, transport::Server};
use zerotable::Engine;

pub mod api {
    pub mod v1alpha1 {
        tonic::include_proto!("api.v1alpha1");
    }
}

use api::v1alpha1::zerotable_server::{Zerotable, ZerotableServer};
use api::v1alpha1::{
    CreateDocumentRequest, DeleteDocumentRequest, Document, GetDocumentRequest,
    UpdateDocumentRequest,
};

#[derive(Clone)]
pub struct ZerotableService {
    engine: Engine,
}

impl ZerotableService {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl Zerotable for ZerotableService {
    async fn get_document(
        &self,
        _request: Request<GetDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn create_document(
        &self,
        _request: Request<CreateDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn update_document(
        &self,
        _request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn delete_document(
        &self,
        _request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let engine = Engine::open(".zerotable_data")?;
    let service = ZerotableService::new(engine);

    println!("Zerotable listening on {}", addr);

    Server::builder()
        .add_service(ZerotableServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
