// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::RangeBounds;

#[derive(Clone)]
pub enum RowOp {
    Insert(Vec<u8>),
    Delete(Vec<u8>),
    Update((Vec<u8>, Vec<u8>)),
}

/// `MemTable` is a buffer for modify operations without encoding
#[derive(Clone)]
pub struct MemTable {
    buffer: BTreeMap<Vec<u8>, RowOp>,
}

pub type MemTableIter<'a> = impl Iterator<Item = (&'a Vec<u8>, &'a RowOp)>;

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
        }
    }

    pub fn is_dirty(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// read methods
    pub fn get_row_op(&self, pk: &[u8]) -> Option<&RowOp> {
        self.buffer.get(pk)
    }

    /// write methods
    pub fn insert(&mut self, pk: Vec<u8>, value: Vec<u8>) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Insert(value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                x @ RowOp::Delete(_) => {
                    if let RowOp::Delete(ref mut old_value) = x {
                        let old_val = std::mem::take(old_value);
                        e.insert(RowOp::Update((old_val, value)));
                    } else {
                        unreachable!();
                    }
                }

                _ => {
                    panic!(
                        "invalid flush status: double insert {:?} -> {:?}",
                        e.key(),
                        value
                    );
                }
            },
        }
    }

    pub fn delete(&mut self, pk: Vec<u8>, old_value: Vec<u8>) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Delete(old_value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                RowOp::Insert(original_value) => {
                    debug_assert_eq!(original_value, &old_value);
                    e.remove();
                }
                RowOp::Delete(_) => {
                    panic!(
                        "invalid flush status: double delete {:?} -> {:?}",
                        e.key(),
                        old_value
                    );
                }
                RowOp::Update(value) => {
                    let (original_old_value, original_new_value) = std::mem::take(value);
                    debug_assert_eq!(original_new_value, old_value);
                    e.insert(RowOp::Delete(original_old_value));
                }
            },
        }
    }

    pub fn update(&mut self, pk: Vec<u8>, old_value: Vec<u8>, new_value: Vec<u8>) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Update((old_value, new_value)));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                RowOp::Insert(original_value) => {
                    debug_assert_eq!(original_value, &old_value);
                    e.insert(RowOp::Update((old_value, new_value)));
                }
                RowOp::Delete(_) => {
                    panic!(
                        "invalid flush status: double delete {:?} -> {:?}",
                        e.key(),
                        old_value
                    );
                }
                RowOp::Update(value) => {
                    let (original_old_value, original_new_value) = std::mem::take(value);
                    debug_assert_eq!(original_new_value, old_value);
                    e.insert(RowOp::Update((original_old_value, new_value)));
                }
            },
        }
    }

    pub fn into_parts(self) -> BTreeMap<Vec<u8>, RowOp> {
        self.buffer
    }

    pub fn iter<'a, R>(&'a self, key_range: R) -> MemTableIter<'a>
    where
        R: RangeBounds<Vec<u8>> + 'a,
    {
        self.buffer.range(key_range)
    }
}
