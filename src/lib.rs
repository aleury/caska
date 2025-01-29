#![allow(dead_code, clippy::cast_possible_truncation)]
use anyhow::Result;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    time::{SystemTime, UNIX_EPOCH},
};

trait ToBytes {
    fn to_bytes(self) -> Vec<u8>;
}

impl ToBytes for u32 {
    fn to_bytes(self) -> Vec<u8> {
        let mut bytes = vec![0; 4];
        BigEndian::write_u32(&mut bytes, self);
        bytes
    }
}

impl ToBytes for &str {
    fn to_bytes(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(self) -> Vec<u8> {
        self
    }
}

#[derive(Debug, PartialEq)]
struct KeyValueEntry {
    tstamp: u32,
    ksz: u32,
    value_sz: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KeyValueEntry {
    fn new<K, V>(tstamp: u32, key: K, value: V) -> Self
    where
        K: ToBytes,
        V: ToBytes,
    {
        let key = key.to_bytes();
        let value = value.to_bytes();
        Self {
            tstamp,
            ksz: key.len() as u32,
            value_sz: value.len() as u32,
            key,
            value,
        }
    }

    fn value_offset(&self) -> usize {
        3 * size_of::<u32>() + self.key.len()
    }
}

impl TryFrom<KeyValueEntry> for Vec<u8> {
    type Error = anyhow::Error;

    fn try_from(entry: KeyValueEntry) -> Result<Self> {
        let mut buf: Vec<u8> = vec![];
        buf.write_u32::<BigEndian>(entry.tstamp)?;
        buf.write_u32::<BigEndian>(entry.ksz)?;
        buf.write_u32::<BigEndian>(entry.value_sz)?;
        buf.write_all(&entry.key)?;
        buf.write_all(&entry.value)?;
        Ok(buf)
    }
}

impl TryFrom<Vec<u8>> for KeyValueEntry {
    type Error = anyhow::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let tstamp = cursor.read_u32::<BigEndian>()?;
        let ksz = cursor.read_u32::<BigEndian>()?;
        let value_sz = cursor.read_u32::<BigEndian>()?;

        let mut key = vec![0; ksz as usize];
        cursor.read_exact(&mut key)?;

        let mut value = vec![0; value_sz as usize];
        cursor.read_exact(&mut value)?;

        Ok(Self {
            tstamp,
            ksz,
            value_sz,
            key,
            value,
        })
    }
}

#[derive(Debug)]
struct KeyDirEntry {
    file_id: u32,
    value_sz: u32,
    value_pos: u32,
    tstamp: u32,
}

#[derive(Debug)]
struct File {
    id: u32,
    data: Vec<u8>,
}

impl File {
    fn new(id: u32) -> Self {
        Self { id, data: vec![] }
    }

    fn size(&self) -> usize {
        self.data.len()
    }

    fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }
}

#[derive(Debug)]
struct MemoryStore {
    file: File,
    keydir: HashMap<Vec<u8>, KeyDirEntry>,
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            file: File::new(now()),
            keydir: HashMap::new(),
        }
    }

    fn get<K: ToBytes>(&self, key: K) -> Option<Vec<u8>> {
        let entry = self.keydir.get(&key.to_bytes())?;
        let offset = entry.value_pos as usize;
        let size = entry.value_sz as usize;
        self.file
            .data
            .get(offset..offset + size)
            .map(<[u8]>::to_vec)
    }

    fn put<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: ToBytes,
        V: ToBytes,
    {
        let entry = KeyValueEntry::new(now(), key, value);
        let value_pos = self.file.size() + entry.value_offset();

        let keydir_entry = KeyDirEntry {
            file_id: self.file.id,
            value_sz: entry.value_sz,
            value_pos: value_pos as u32,
            tstamp: entry.tstamp,
        };
        self.keydir.insert(entry.key.clone(), keydir_entry);

        let entry_data: Vec<u8> = entry.try_into()?;
        self.file.append(&entry_data);

        Ok(())
    }
}

fn now() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_a_key_value_entry() {
        let entry = KeyValueEntry::new(42, 42, 42);
        let got: Vec<u8> = entry.try_into().unwrap();
        let want = vec![
            0, 0, 0, 42, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 42, 0, 0, 0, 42,
        ];
        assert_eq!(want, got);
    }

    #[test]
    fn decode_a_key_value_entry() {
        let bytes = vec![
            0, 0, 0, 42, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 42, 0, 0, 0, 42,
        ];
        let want = KeyValueEntry::new(42, 42, 42);
        let got = bytes.try_into().unwrap();
        assert_eq!(want, got);
    }

    #[test]
    fn memory_store_can_set_and_get_a_key_value_pair() {
        let mut store = MemoryStore::new();

        let key_value_pairs = vec![
            ("hello", "world"),
            ("first_name", "john"),
            ("last_name", "smith"),
        ];
        for (key, value) in key_value_pairs {
            store.put(key, value).unwrap();

            let want = value;
            let got = store.get(key).unwrap();

            assert_eq!(want, String::from_utf8_lossy(&got));
        }
    }
}
