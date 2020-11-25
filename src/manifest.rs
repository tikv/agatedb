use std::fs::{self, File};
use std::io::{Read, Seek, Write};
use std::sync::Mutex;
use std::{
    collections::{HashMap, HashSet},
    io::{BufReader, SeekFrom},
    path::{Path, PathBuf},
};

use crc::crc32;
use prost::Message;
use proto::meta::{
    manifest_change::Operation as ManifestChangeOp, ManifestChange, ManifestChangeSet,
};

use crate::{AgateOptions, Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct Manifest {
    pub levels: Vec<LevelManifest>,
    pub tables: HashMap<u64, TableManifest>,
    pub creations: usize,
    pub deletions: usize,
}

#[derive(Default)]
pub struct LevelManifest {
    pub tables: HashSet<u64>,
}

pub struct TableManifest {
    pub level: u8,
    pub key_id: u64,
    // TODO: compression
}

struct ManifestFile {
    file: Option<File>,
    directory: PathBuf,
    deletions_rewrite_threshold: usize,
    manifest: Mutex<Manifest>,
}

impl ManifestFile {
    fn open_or_create_manifest_file(opt: &AgateOptions) -> Result<Self> {
        if opt.in_memory {
            Ok(Self {
                file: None,
                directory: PathBuf::new(),
                deletions_rewrite_threshold: 0,
                manifest: Mutex::new(Manifest::new()),
            })
        } else {
            // TODO: read-only mode
            Ok(Self::helper_open_or_create_manifest_file(
                &opt.path,
                MANIFEST_DELETION_REWRITE_THRESHOLD,
            )?)
        }
    }

    fn helper_open_or_create_manifest_file(
        path: impl AsRef<Path>,
        deletions_threshold: usize,
    ) -> Result<Self> {
        let path = path.as_ref().join(MANIFEST_FILENAME);

        todo!();
    }
}

pub const MANIFEST_FILENAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILENAME: &str = "MANIFEST_REWRITE";
const MANIFEST_DELETION_REWRITE_THRESHOLD: usize = 10000;
const MANIFEST_DELETIONS_RATIO: usize = 10;

const MAGIC_TEXT: &[u8] = b"Agat";
const MAGIC_VERSION: u32 = 8;

impl Manifest {
    pub fn new() -> Self {
        Self {
            levels: vec![],
            tables: HashMap::new(),
            creations: 0,
            deletions: 0,
        }
    }

    fn as_changes(&self) -> Vec<ManifestChange> {
        let mut changes = Vec::with_capacity(self.tables.len());
        for (id, tm) in &self.tables {
            changes.push(new_create_change(*id, tm.level as usize, tm.key_id));
        }
        changes
    }

    pub fn replay(file: &mut File) -> Result<(Manifest, u32)> {
        file.seek(SeekFrom::Start(0))?;
        let mut magic_buf = vec![0; 8];
        file.read_exact(&mut magic_buf)?;
        if &magic_buf[..4] != MAGIC_TEXT {
            return Err(Error::CustomError("bad magic text".to_string()));
        }
        let version = (&magic_buf[4..]).get_u32();
        if version != MAGIC_VERSION {
            return Err(Error::CustomError("bad magic version".to_string()));
        }
        let stat = file.metadata()?;
        let mut build = Manifest::new();
        let mut buf = vec![];
        let mut len_crc_buf = vec![0; 8];

        let mut offset = 8;

        loop {
            if file.read(&mut len_crc_buf)? != 8 {
                break;
            }
            offset += 8;

            let length = (&len_crc_buf[..4]).get_u32();
            if length as u64 > stat.len() {
                return Err(Error::CustomError(
                    "buffer length greater than file size".to_string(),
                ));
            }
            buf.resize(length as usize, 0);
            if file.read(&mut buf)? != length as usize {
                break;
            }
            offset += length;

            if crc32::checksum_castagnoli(&buf) != (&len_crc_buf[4..]).get_u32() {
                return Err(Error::CustomError("bad checksum".to_string()));
            }

            let change_set: ManifestChangeSet = Message::decode(&buf[..])?;

            apply_change_set(&mut build, &change_set)?;
        }

        Ok((build, offset))
    }
}

impl Clone for Manifest {
    fn clone(&self) -> Self {
        let change_set = ManifestChangeSet {
            changes: self.as_changes(),
        };
        let mut ret = Manifest::new();
        apply_change_set(&mut ret, &change_set).unwrap();
        ret
    }
}

fn apply_change_set(build: &mut Manifest, change_set: &ManifestChangeSet) -> Result<()> {
    for change in &change_set.changes {
        apply_manifest_change(build, change)?;
    }
    Ok(())
}

fn apply_manifest_change(build: &mut Manifest, tc: &ManifestChange) -> Result<()> {
    match ManifestChangeOp::from_i32(tc.op).unwrap() {
        ManifestChangeOp::Create => {
            if build.tables.contains_key(&tc.id) {
                return Err(Error::CustomError(format!(
                    "manifest invalid, {} exists",
                    tc.id
                )));
            }
            build.tables.insert(
                tc.id,
                TableManifest {
                    level: tc.level as u8,
                    key_id: tc.key_id,
                },
            );
            while build.levels.len() <= tc.level as usize {
                build.levels.push(LevelManifest::default());
            }
            build.levels[tc.level as usize].tables.insert(tc.id);
            build.creations += 1;
        }
        ManifestChangeOp::Delete => {
            if let Some(tm) = build.tables.get(&tc.id) {
                build.levels[tm.level as usize].tables.remove(&tc.id);
                build.tables.remove(&tc.id);
                build.deletions += 1;
            } else {
                return Err(Error::CustomError(format!(
                    "manifest invalid, removing non-existing table {}",
                    tc.id
                )));
            }
        }
    }
    Ok(())
}

fn new_create_change(id: u64, level: usize, key_id: u64) -> ManifestChange {
    ManifestChange {
        id,
        op: ManifestChangeOp::Create as i32,
        level: level as u32,
        key_id,
        // unused fields
        encryption_algo: 0,
        compression: 0,
    }
}

fn new_delete_change(id: u64) -> ManifestChange {
    ManifestChange {
        id,
        op: ManifestChangeOp::Delete as i32,
        // unused fields
        level: 0,
        key_id: 0,
        encryption_algo: 0,
        compression: 0,
    }
}
