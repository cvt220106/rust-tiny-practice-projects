use crate::log::Log;
use std::{collections::btree_map, ops::Bound, path::PathBuf};
const MERGE_FILE_EXT: &str = "merge";

type KeyDir = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;
type Result<T> = std::result::Result<T, std::io::Error>;

/*
* log: the base storage file
* keydir: the memory struct of index map
* */
pub struct MiniBitcask {
    log: Log,
    keydir: KeyDir,
}

impl Drop for MiniBitcask {
    fn drop(&mut self) {
        if let Err(error) = self.flush() {
            log::error!("failed to flush: {:?}", error)
        }
    }
}

impl MiniBitcask {
    // create a new MiniBitcask from a storage file
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut log = Log::new(path)?;
        let keydir = log.load_index()?;

        Ok(Self { log, keydir })
    }

    // read: use key to get a value
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((value_pos, value_len)) = self.keydir.get(key) {
            let val = self.log.read_value(*value_pos, *value_len)?;

            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    // delete a key-value pair, logic delete, set a tombstone sign
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.keydir.remove(key);

        Ok(())
    }

    // write new key-value pair
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let (offset, len) = self.log.write_entry(key, Some(&value))?;
        let value_len = value.len() as u32;
        self.keydir.insert(
            key.to_vec(),
            (offset + len as u64 - value_len as u64, value_len),
        );

        Ok(())
    }

    // merge, because we append new entry all the time, but only the lastest one is we need
    // so we have many unuse data, so we need merge data file, clear invaild data
    pub fn merge(&mut self) -> Result<()> {
        // create a new temp file to write
        let mut merge_path = self.log.path.clone();
        merge_path.set_extension(MERGE_FILE_EXT);

        let mut new_log = Log::new(merge_path)?;
        let mut new_keydir = KeyDir::new();

        // traversal keydir(all useful data in there), write useful data to new one
        for (key, (value_pos, value_len)) in self.keydir.iter() {
            let value = self.log.read_value(*value_pos, *value_len)?;
            let (offset, len) = new_log.write_entry(key, Some(&value))?;
            new_keydir.insert(
                key.clone(),
                (offset + len as u64 - *value_len as u64, *value_len),
            );
        }

        // after rewrite, rename file
        std::fs::rename(new_log.path, self.log.path.clone())?;

        new_log.path = self.log.path.clone();
        self.log = new_log;
        self.keydir = new_keydir;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.log.file.sync_all()?)
    }

    pub fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> ScanIterator<'_> {
        ScanIterator {
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }

    // prefix scan, find key in the prefix pattern
    pub fn scan_prefix(&mut self, prefix: &[u8]) -> ScanIterator<'_> {
        let start = Bound::Included(prefix.to_vec());

        // make the end sign
        // the last bytes add 1, example "aaaa" -> "aaab"
        let mut bound_prefix = prefix.to_vec().clone();
        if let Some(last) = bound_prefix.iter_mut().last() {
            *last += 1;
        }
        let end = Bound::Excluded(bound_prefix.to_vec());

        self.scan((start, end))
    }
}

// impl iter for minibitcask, easy to scan all data
pub struct ScanIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> ScanIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (value_pos, value_len)) = item;
        let value = self.log.read_value(*value_pos, *value_len)?;

        Ok((key.clone(), value))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    // key-value pair
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

// front to end iter or end to front iter
impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}
