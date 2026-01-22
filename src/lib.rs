// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions};

#[derive(Clone)]
pub struct Engine {
    db: Database,
    primary: Keyspace,
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> fjall::Result<Self> {
        let db = Database::builder(path).open()?;

        //for now we define a single keyspace where we insert all the things. 
        // later maybe we can create another keyspace for indexes
        let primary = db.keyspace("primary", KeyspaceCreateOptions::default)?;
        
        Ok(Engine { db, primary })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();
        assert!(engine.primary.is_empty().unwrap());
    }
}
