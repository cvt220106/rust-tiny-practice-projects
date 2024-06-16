use fs4::FileExt;
use std::{
    collections::btree_map,
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, Write},
    ops::Bound,
    path::PathBuf,
};

const KEY_VAL_HEADER_LEN: u32 = 4;
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

// the log structure in bitcask
// it contains a cretain file in disk
// every entry will append-write to this log file
struct Log {
    path: PathBuf,
    file: File,
}

impl Log {
    fn new(path: PathBuf) -> Result<Self> {
        // check the file path validation,
        // if not, recursively create all directory until it's valid
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        // add open options to open the log file
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // add exclusive lock, block the concurrency update
        file.try_lock_exclusive();

        Ok(Self { path, file })
    }

    // create the memory index for log
    // entry struct
    // | key size(4B) | value size(4B) | key | value |
    fn load_index(&mut self) -> Result<KeyDir> {
        let mut len_buf = [0u8; KEY_VAL_HEADER_LEN as usize];
        let mut keydir = KeyDir::new();
        let file_len = self.file.metadata()?.len();
        let mut r = BufReader::new(&mut self.file);
        let mut pos: u64 = r.seek(std::io::SeekFrom::Start(0))?;

        // read all key-value from disk file to keydir in memorty
        while pos < file_len {
            // define a closure to read a {key ,value_pos, value_len} from file
            let read_one = || -> Result<(Vec<u8>, u64, Option<u32>)> {
                // read the key len
                r.read_exact(&mut len_buf);
                let key_len = u32::from_be_bytes(len_buf);
                // read the value len
                r.read_exact(&mut len_buf);
                let value_lent_or_tombstone = match i32::from_be_bytes(len_buf) {
                    l if l >= 0 => Some(l as u32),
                    _ => None,
                };

                // the pos of value
                let value_pos = pos + KEY_VAL_HEADER_LEN as u64 * 2 + key_len as u64;

                // read key content
                let mut key = vec![0; key_len as usize];
                r.read_exact(&mut key);

                // jump the value len
                if let Some(value_len) = value_lent_or_tombstone {
                    r.seek_relative(value_len as i64)?;
                }

                // return {key, value_pos, value_len}, will be used by get value content
                Ok((key, value_pos, value_lent_or_tombstone))
            }();

            match read_one {
                Ok((key, value_pos, Some(value_len))) => {
                    // correctly get the existing key and value info
                    // add this to the buf key-value map
                    keydir.insert(key, (value_pos, value_len));
                    pos = value_pos + value_len as u64;
                }
                Ok((key, value_pos, None)) => {
                    // find a delete sign(tomb), remove the key
                    keydir.remove(&key);
                    pos = value_pos;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(keydir)
    }

    // read value content based on value_pos and value_len in keydir
    fn read_value(&mut self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        let mut value = vec![0; value_len as usize];
        self.file.seek(std::io::SeekFrom::Start(value_pos));
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    // entry strcut(the key-value struct writen in log file)
    // | key size(4B) | value size(4B) | key | value |
    // this function is used to write entry to log file, as append mode
    // return (insert_pos, entry_len)
    fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);
        let value_len_or_tombstone = value.map_or(-1, |v| v.len() as i32);

        // the entry total len
        let len = KEY_VAL_HEADER_LEN * 2 + key_len + value_len;

        let offset = self.file.seek(std::io::SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);
        w.write_all(&key_len.to_be_bytes())?;
        w.write_all(&value_len_or_tombstone.to_be_bytes())?;
        w.write_all(key)?;
        if let Some(value) = value {
            w.write_all(value)?;
        }
        w.flush()?;

        Ok((offset, len))
    }
}

#[cfg(test)]
mod tests {
    use super::{Log, MiniBitcask, Result};
    use std::ops::Bound;

    #[test]
    fn test_log_read_write() -> Result<()> {
        let path = std::env::temp_dir()
            .join("sqldb-disk-engine-log-test1")
            .join("log");

        let mut log = Log::new(path.clone())?;
        log.write_entry(b"a", Some(b"val1"))?;
        log.write_entry(b"b", Some(b"val2"))?;
        log.write_entry(b"c", Some(b"val3"))?;

        // rewrite
        log.write_entry(b"a", Some(b"val5"))?;
        // delete
        log.write_entry(b"c", None)?;

        let keydir = log.load_index()?;
        assert_eq!(2, keydir.len());

        // path.parent().map(|p| std::fs::remove_dir_all(p));

        Ok(())
    }

    #[test]
    fn test_log_reopen() -> Result<()> {
        let path = std::env::temp_dir()
            .join("sqldb-disk-engine-log-test2")
            .join("log");

        let mut log = Log::new(path.clone())?;
        log.write_entry(b"a", Some(b"val1"))?;
        log.write_entry(b"b", Some(b"val2"))?;
        log.write_entry(b"c", Some(b"val3"))?;
        log.write_entry(b"d", Some(b"val4"))?;
        log.write_entry(b"d", None)?;

        drop(log);

        let mut log = Log::new(path.clone())?;
        let keydir = log.load_index()?;
        assert_eq!(3, keydir.len());

        path.parent().map(|p| std::fs::remove_dir_all(p));

        Ok(())
    }

    // 测试点读的情况
    #[test]
    fn test_point_opt() -> Result<()> {
        let path = std::env::temp_dir().join("minibitcask-test").join("log");
        let mut eng = MiniBitcask::new(path.clone())?;

        // 测试获取一个不存在的 key
        assert_eq!(eng.get(b"not exist")?, None);

        // 获取一个存在的 key
        eng.set(b"aa", vec![1, 2, 3, 4])?;
        assert_eq!(eng.get(b"aa")?, Some(vec![1, 2, 3, 4]));

        // 重复 put，将会覆盖前一个值
        eng.set(b"aa", vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"aa")?, Some(vec![5, 6, 7, 8]));

        // 删除之后再读取
        eng.delete(b"aa")?;
        assert_eq!(eng.get(b"aa")?, None);

        // key、value 为空的情况
        assert_eq!(eng.get(b"")?, None);
        eng.set(b"", vec![])?;
        assert_eq!(eng.get(b"")?, Some(vec![]));

        eng.set(b"cc", vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"cc")?, Some(vec![5, 6, 7, 8]));

        path.parent().map(|p| std::fs::remove_dir_all(p));
        Ok(())
    }

    // 测试扫描
    #[test]
    fn test_scan() -> Result<()> {
        let path = std::env::temp_dir()
            .join("minibitcask-scan-test")
            .join("log");
        let mut eng = MiniBitcask::new(path.clone())?;

        eng.set(b"nnaes", b"value1".to_vec())?;
        eng.set(b"amhue", b"value2".to_vec())?;
        eng.set(b"meeae", b"value3".to_vec())?;
        eng.set(b"uujeh", b"value4".to_vec())?;
        eng.set(b"anehe", b"value5".to_vec())?;

        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"e".to_vec());

        let mut iter = eng.scan((start.clone(), end.clone()));
        let (key1, _) = iter.next().expect("no value founded")?;
        assert_eq!(key1, b"amhue".to_vec());

        let (key2, _) = iter.next().expect("no value founded")?;
        assert_eq!(key2, b"anehe".to_vec());
        drop(iter);

        let start = Bound::Included(b"b".to_vec());
        let end = Bound::Excluded(b"z".to_vec());
        let mut iter2 = eng.scan((start, end));

        let (key3, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key3, b"uujeh".to_vec());

        let (key4, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key4, b"nnaes".to_vec());

        let (key5, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key5, b"meeae".to_vec());

        path.parent().map(|p| std::fs::remove_dir_all(p));
        Ok(())
    }

    // 测试前缀扫描
    #[test]
    fn test_scan_prefix() -> Result<()> {
        let path = std::env::temp_dir()
            .join("minibitcask-scan-prefix-test")
            .join("log");
        let mut eng = MiniBitcask::new(path.clone())?;

        eng.set(b"ccnaes", b"value1".to_vec())?;
        eng.set(b"camhue", b"value2".to_vec())?;
        eng.set(b"deeae", b"value3".to_vec())?;
        eng.set(b"eeujeh", b"value4".to_vec())?;
        eng.set(b"canehe", b"value5".to_vec())?;
        eng.set(b"aanehe", b"value6".to_vec())?;

        let prefix = b"ca";
        let mut iter = eng.scan_prefix(prefix);
        let (key1, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key1, b"camhue".to_vec());
        let (key2, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key2, b"canehe".to_vec());

        println!("{:?}", path.clone());
        path.parent().map(|p| std::fs::remove_dir_all(p));
        Ok(())
    }

    #[test]
    fn test_merge() -> Result<()> {
        let path = std::env::temp_dir()
            .join("minibitcask-merge-test")
            .join("log");

        let mut eng = MiniBitcask::new(path.clone())?;

        eng.set(b"a", b"value1".to_vec())?;
        eng.set(b"b", b"value2".to_vec())?;
        eng.set(b"c", b"value3".to_vec())?;
        eng.delete(b"a")?;
        eng.delete(b"b")?;
        eng.delete(b"c")?;

        eng.merge()?;

        eng.set(b"a", b"value1".to_vec())?;
        eng.set(b"b", b"value2".to_vec())?;
        eng.set(b"c", b"value3".to_vec())?;

        let val = eng.get(b"a")?;
        assert_eq!(b"value1".to_vec(), val.unwrap());

        let val = eng.get(b"b")?;
        assert_eq!(b"value2".to_vec(), val.unwrap());

        let val = eng.get(b"c")?;
        assert_eq!(b"value3".to_vec(), val.unwrap());

        path.parent().map(|p| std::fs::remove_dir_all(p));
        Ok(())
    }
}
