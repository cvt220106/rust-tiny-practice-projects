use crate::bitcask::MiniBitcask;
use crate::log::Log;

type Result<T> = std::result::Result<T, std::io::Error>;

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
