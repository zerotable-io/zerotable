// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

pub fn hello_fjall() -> fjall::Result<bool> {
    let db = fjall::Database::builder(".fjall_data").open()?;

    let items = db.keyspace("items", fjall::KeyspaceCreateOptions::default)?;

    items.is_empty()
}
