// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

use std::fmt;
use std::path::Path;

use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

use crate::keys::{self, KeyError};

/// Errors returned by Engine operations.
#[derive(Debug)]
pub enum EngineError {
    /// The document already exists.
    AlreadyExists,
    /// The document was not found.
    NotFound,
    /// Invalid key (empty, null byte, forward slash, too long).
    InvalidKey(KeyError),
    /// Storage-level error from fjall.
    Storage(fjall::Error),
    /// Transaction conflict.
    /// At commit time there might be a conflict, the user in this case needs to retry the transaction!
    TransactionConflict,
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::AlreadyExists => write!(f, "document already exists"),
            EngineError::NotFound => write!(f, "document not found"),
            EngineError::InvalidKey(e) => write!(f, "invalid key: {e}"),
            EngineError::Storage(e) => write!(f, "storage error: {e}"),
            EngineError::TransactionConflict => write!(f, "transaction conflict"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<KeyError> for EngineError {
    fn from(e: KeyError) -> Self {
        EngineError::InvalidKey(e)
    }
}

impl From<fjall::Error> for EngineError {
    fn from(e: fjall::Error) -> Self {
        EngineError::Storage(e)
    }
}

#[derive(Clone)]
pub struct Engine {
    // NOTE: should we add a trait to abstract away fjall?
    db: OptimisticTxDatabase,
    primary: OptimisticTxKeyspace,
}

impl Engine {
    /// Open an optimistictx database, creating it if it does not exists.
    /// 
    /// Open also a 'primary' keyspace, creating it if it does not exists.
    pub fn open(path: impl AsRef<Path>) -> fjall::Result<Self> {
        let db = OptimisticTxDatabase::builder(path).open()?;

        // NOTE: For now we define a single keyspace where we insert all the things.
        // NOTE: Later maybe we can create another keyspace for indexes.
        let primary = db.keyspace("primary", KeyspaceCreateOptions::default)?;

        Ok(Engine { db, primary })
    }

    /// Create a document. Fails if the document already exists.
    pub fn create_document(
        &self,
        collection_id: &str,
        doc_id: &str,
        data: &[u8],
    ) -> Result<(), EngineError> {
        let key = keys::encode(collection_id, doc_id)?;

        let mut wtx = self.db.write_tx()?;

        // Check if document already exists (within a transaction)
        if wtx.get(&self.primary, &key)?.is_some() {
            return Err(EngineError::AlreadyExists);
        }

        wtx.insert(&self.primary, &key, data);

        wtx.commit()?
            .map_err(|_| EngineError::TransactionConflict)?; // we discard the Conflict error of fjall because it doesn't add something meaningful

        // TODO: Durability options to investigate:
        // - User configurable persist mode (like MongoDB write concern)
        // - Background worker for periodic fsync (configurable intervals?)
        // - Per-operation persist with PersistMode::SyncAll for strict durability
        Ok(())
    }

    /// Get a document by collection ID and document ID.
    pub fn get_document(&self, collection: &str, doc_id: &str) -> Result<Vec<u8>, EngineError> {
        let key = keys::encode(collection, doc_id)?;

        match self.primary.get(&key)? {
            Some(value) => Ok(value.to_vec()),
            None => Err(EngineError::NotFound),
        }
    }

    /// Delete a document. Fails if the document does not exist.
    pub fn delete_document(&self, collection: &str, doc_id: &str) -> Result<(), EngineError> {
        let key = keys::encode(collection, doc_id)?;

        let mut wtx = self.db.write_tx()?;

        // Check if document exists (within a transaction)
        if wtx.get(&self.primary, &key)?.is_none() {
            return Err(EngineError::NotFound);
        }

        wtx.remove(&self.primary, &key);

        wtx.commit()?
            .map_err(|_| EngineError::TransactionConflict)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> Engine {
        let dir = tempfile::tempdir().unwrap();
        Engine::open(dir.path()).unwrap()
    }

    #[test]
    fn test_open() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();
        let rtx = engine.db.read_tx();
        assert!(rtx.is_empty(&engine.primary).unwrap());
    }

    #[test]
    fn test_create_and_get() {
        let engine = test_engine();
        let data = b"hello world";

        engine.create_document("users", "doc1", data).unwrap();
        let result = engine.get_document("users", "doc1").unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_create_already_exists() {
        let engine = test_engine();

        engine.create_document("users", "doc1", b"data").unwrap();
        let err = engine
            .create_document("users", "doc1", b"other")
            .unwrap_err();

        assert!(matches!(err, EngineError::AlreadyExists));
    }

    #[test]
    fn test_get_not_found() {
        let engine = test_engine();
        let err = engine.get_document("users", "missing").unwrap_err();

        assert!(matches!(err, EngineError::NotFound));
    }

    #[test]
    fn test_delete() {
        let engine = test_engine();

        engine.create_document("users", "doc1", b"data").unwrap();
        engine.delete_document("users", "doc1").unwrap();

        let err = engine.get_document("users", "doc1").unwrap_err();
        assert!(matches!(err, EngineError::NotFound));
    }

    #[test]
    fn test_delete_not_found() {
        let engine = test_engine();
        let err = engine.delete_document("users", "missing").unwrap_err();

        assert!(matches!(err, EngineError::NotFound));
    }

    #[test]
    fn test_create_invalid_key() {
        let engine = test_engine();
        let err = engine.create_document("", "doc1", b"data").unwrap_err();

        assert!(matches!(err, EngineError::InvalidKey(KeyError::EmptyId)));
    }
}
