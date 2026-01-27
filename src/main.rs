// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

use prost::Message;
use prost_types::Timestamp;
use tonic::{Request, Response, Status, transport::Server};
use zerotable::{Engine, EngineError, generate_uuid_v7, now_millis};

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

/// Convert EngineError to tonic Status.
fn engine_err_to_status(err: EngineError) -> Status {
    match err {
        EngineError::AlreadyExists => Status::already_exists(err.to_string()),
        EngineError::NotFound => Status::not_found(err.to_string()),
        EngineError::InvalidKey(_) => Status::invalid_argument(err.to_string()),
        EngineError::Storage(_) => Status::internal(err.to_string()),
        EngineError::TransactionConflict => Status::aborted(err.to_string()),
    }
}

/// Parse a resource name "collection_id/document_id" into parts.
fn parse_name(name: &str) -> Result<(&str, &str), Status> {
    let parts: Vec<&str> = name.splitn(2, '/').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err(Status::invalid_argument(
            "name must be in format 'collection_id/document_id'",
        ));
    }
    Ok((parts[0], parts[1]))
}

#[tonic::async_trait]
impl Zerotable for ZerotableService {
    async fn get_document(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        let req = request.into_inner();
        let (collection_id, doc_id) = parse_name(&req.name)?;

        let engine = self.engine.clone();
        let collection_id = collection_id.to_string();
        let doc_id = doc_id.to_string();

        let data = tokio::task::spawn_blocking(move || {
            engine.get_document(&collection_id, &doc_id)
        })
        .await
        .map_err(|e| Status::internal(format!("task failed: {e}")))?
        .map_err(engine_err_to_status)?;

        let doc = Document::decode(data.as_slice())
            .map_err(|e| Status::internal(format!("failed to decode document: {e}")))?;

        Ok(Response::new(doc))
    }

    async fn create_document(
        &self,
        request: Request<CreateDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        let req = request.into_inner();

        if req.collection_id.is_empty() {
            return Err(Status::invalid_argument("collection_id is required"));
        }

        let mut doc = req.document.ok_or_else(|| {
            Status::invalid_argument("document is required")
        })?;

        let (doc_id, now) = if req.document_id.is_empty() {
            let (uuid, ts) = generate_uuid_v7();
            (uuid.to_string(), ts)
        } else {
            (req.document_id, now_millis())
        };

        let prost_now: Timestamp = now.into();
        doc.name = format!("{}/{}", req.collection_id, doc_id);
        doc.create_time = Some(prost_now.clone());
        doc.update_time = Some(prost_now);

        let data = doc.encode_to_vec();
        let engine = self.engine.clone();
        let collection_id = req.collection_id;
        let doc_id_clone = doc_id.clone();

        tokio::task::spawn_blocking(move || {
            engine.create_document(&collection_id, &doc_id_clone, &data)
        })
        .await
        .map_err(|e| Status::internal(format!("task failed: {e}")))?
        .map_err(engine_err_to_status)?;

        Ok(Response::new(doc))
    }

    async fn update_document(
        &self,
        _request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn delete_document(
        &self,
        request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let (collection, doc_id) = parse_name(&req.name)?;

        let engine = self.engine.clone();
        let collection = collection.to_string();
        let doc_id = doc_id.to_string();

        tokio::task::spawn_blocking(move || {
            engine.delete_document(&collection, &doc_id)
        })
        .await
        .map_err(|e| Status::internal(format!("task failed: {e}")))?
        .map_err(engine_err_to_status)?;

        Ok(Response::new(()))
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
