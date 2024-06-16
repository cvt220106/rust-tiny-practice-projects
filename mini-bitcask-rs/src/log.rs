use fs4::FileExt;
use std::{
    collections::btree_map,
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

const KEY_VAL_HEADER_LEN: u32 = 4;

type KeyDir = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;
type Result<T> = std::result::Result<T, std::io::Error>;

// the log structure in bitcask
// it contains a cretain file in disk
// every entry will append-write to this log file
pub(crate) struct Log {
    pub(crate) path: PathBuf,
    pub(crate) file: File,
}

impl Log {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
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
    pub(crate) fn load_index(&mut self) -> Result<KeyDir> {
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
    pub(crate) fn read_value(&mut self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        let mut value = vec![0; value_len as usize];
        self.file.seek(std::io::SeekFrom::Start(value_pos));
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    // entry strcut(the key-value struct writen in log file)
    // | key size(4B) | value size(4B) | key | value |
    // this function is used to write entry to log file, as append mode
    // return (insert_pos, entry_len)
    pub(crate) fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
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
