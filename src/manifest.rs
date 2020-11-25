use std::fs::{self, File, OpenOptions};
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

struct Core {
    file: Option<File>,
    manifest: Manifest,
}

impl Core {
    fn rewrite(&mut self, dir: &Path) -> Result<()> {
        self.file.take();
        let (file, net_creations) = ManifestFile::help_rewrite(dir, &self.manifest)?;
        self.file = Some(file);
        self.manifest.creations = net_creations;
        self.manifest.deletions = 0;

        Ok(())
    }
}

struct ManifestFile {
    directory: PathBuf,
    deletions_rewrite_threshold: usize,
    core: Mutex<Core>,
}

impl ManifestFile {
    fn open_or_create_manifest_file(opt: &AgateOptions) -> Result<Self> {
        if opt.in_memory {
            Ok(Self {
                directory: PathBuf::new(),
                deletions_rewrite_threshold: 0,
                core: Mutex::new(Core {
                    manifest: Manifest::new(),
                    file: None,
                }),
            })
        } else {
            // TODO: read-only mode
            Ok(Self::help_open_or_create_manifest_file(
                &opt.path,
                MANIFEST_DELETION_REWRITE_THRESHOLD,
            )?)
        }
    }

    fn help_open_or_create_manifest_file(
        dir: impl AsRef<Path>,
        deletions_threshold: usize,
    ) -> Result<Self> {
        let path = dir.as_ref().join(MANIFEST_FILENAME);

        // TODO: read-only

        if path.exists() {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&path)?;
            let (manifest, trunc_offset) = Manifest::replay(&mut file)?;
            file.set_len(trunc_offset as u64)?;
            file.seek(SeekFrom::End(0))?;
            Ok(ManifestFile {
                directory: dir.as_ref().to_path_buf(),
                core: Mutex::new(Core {
                    file: Some(file),
                    manifest: manifest,
                }),
                deletions_rewrite_threshold: deletions_threshold,
            })
        } else {
            let manifest = Manifest::new();
            let (file, net_creations) = Self::help_rewrite(dir.as_ref(), &manifest)?;
            assert_eq!(net_creations, 0);
            // TODO: assert net creations = 0
            Ok(ManifestFile {
                directory: dir.as_ref().to_path_buf(),
                core: Mutex::new(Core {
                    file: Some(file),
                    manifest: manifest,
                }),
                deletions_rewrite_threshold: deletions_threshold,
            })
        }
    }

    fn help_rewrite(dir: impl AsRef<Path>, manifest: &Manifest) -> Result<(File, usize)> {
        let rewrite_path = dir.as_ref().join(MANIFEST_REWRITE_FILENAME);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&rewrite_path)?;

        let mut buf = vec![0; 8];

        buf[..4].clone_from_slice(MAGIC_TEXT);
        (&mut buf[4..]).put_u32(MAGIC_VERSION);

        let net_creations = manifest.tables.len();
        let changes = manifest.as_changes();
        let set = ManifestChangeSet { changes };
        let mut change_buf = BytesMut::new();
        set.encode(&mut change_buf)?;

        let mut len_crc_buf = vec![0; 8];
        (&mut len_crc_buf[..4]).put_u32(change_buf.len() as u32);
        (&mut len_crc_buf[4..]).put_u32(crc32::checksum_castagnoli(&change_buf));

        buf.extend_from_slice(&len_crc_buf);
        buf.extend_from_slice(&change_buf);

        file.write(&buf)?;
        file.sync_all()?;
        drop(file);

        let manifest_path = dir.as_ref().join(MANIFEST_FILENAME);
        fs::rename(&rewrite_path, &manifest_path)?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&manifest_path)?;

        file.seek(SeekFrom::End(0))?;

        // TODO: sync dir

        Ok((file, net_creations))
    }

    fn add_changes(&self, changes_param: Vec<ManifestChange>) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        if core.file.is_none() {
            return Ok(());
        }

        let changes = ManifestChangeSet {
            changes: changes_param,
        };
        let mut buf = BytesMut::new();
        changes.encode(&mut buf)?;

        apply_change_set(&mut core.manifest, &changes)?;

        if core.manifest.deletions > self.deletions_rewrite_threshold
            && core.manifest.deletions
                > MANIFEST_DELETIONS_RATIO * (core.manifest.creations - core.manifest.deletions)
        {
            core.rewrite(&self.directory)?;
        } else {
            let mut len_crc_buf = vec![0; 8];
            (&mut len_crc_buf[..4]).put_u32(buf.len() as u32);
            (&mut len_crc_buf[4..]).put_u32(crc32::checksum_castagnoli(&buf));
            len_crc_buf.extend_from_slice(&buf);
            core.file.as_mut().unwrap().write(&len_crc_buf)?;
        }

        core.file.as_mut().unwrap().sync_all()?;
        Ok(())
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
