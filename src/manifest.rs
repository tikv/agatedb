use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use bytes::{Buf, BufMut, BytesMut};
use crc::crc32;
use prost::Message;
use proto::meta::{
    manifest_change::Operation as ManifestChangeOp, ManifestChange, ManifestChangeSet,
};

use crate::{util, AgateOptions, Error, Result};

pub const MANIFEST_FILENAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILENAME: &str = "MANIFEST_REWRITE";
const MANIFEST_DELETION_REWRITE_THRESHOLD: usize = 10000;
const MANIFEST_DELETIONS_RATIO: usize = 10;

const MAGIC_TEXT: &[u8] = b"Agat";
const MAGIC_VERSION: u32 = 8;

/// Contains information about LSM tree levels in the MANIFEST file.
#[derive(Default, Clone, Debug)]
pub struct LevelManifest {
    pub tables: HashSet<u64>,
}

/// Contains information about a specific table in the LSM tree.
#[derive(Clone, Debug)]
pub struct TableManifest {
    pub level: u8,
    pub key_id: u64,
    // TODO: compression
}

/// Represents the contents of the MANIFEST file.
#[derive(Clone, Debug, Default)]
pub struct Manifest {
    pub levels: Vec<LevelManifest>,
    pub tables: HashMap<u64, TableManifest>,
    pub creations: usize,
    pub deletions: usize,
}

struct ManifestFileInner {
    file: Option<File>,
    manifest: Manifest,
}

impl ManifestFileInner {
    fn rewrite(&mut self, dir: &Path) -> Result<()> {
        self.file.take();
        let (file, net_creations) = ManifestFile::help_rewrite(dir, &self.manifest)?;
        self.file = Some(file);
        self.manifest.creations = net_creations;
        self.manifest.deletions = 0;

        Ok(())
    }
}

/// Holds the file pointer (and other info) about the MANIFEST file, which is a log
/// file we append to.
pub struct ManifestFile {
    directory: PathBuf,
    deletions_rewrite_threshold: usize,
    inner: Mutex<ManifestFileInner>,
}

impl Manifest {
    /// Returns a sequence of changes that could be used to recreate the manifest in its present state.
    fn as_changes(&self) -> Vec<ManifestChange> {
        let mut changes = Vec::with_capacity(self.tables.len());
        for (id, tm) in &self.tables {
            changes.push(new_create_change(*id, tm.level as usize, tm.key_id));
        }
        changes
    }

    /// Reads the manifest file and constructs manifest object. Returns the object
    /// and last offset after a completely read manifest entry.
    pub fn replay(file: &mut File) -> Result<(Manifest, u32)> {
        let file_len = file.metadata()?.len();

        let mut file = BufReader::new(file);
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

        let mut build = Manifest::default();
        let mut buf = vec![];
        let mut len_crc_buf = vec![0; 8];
        let mut offset = 8;

        loop {
            if let Err(e) = file.read_exact(&mut len_crc_buf) {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(e.into()),
                }
            }

            offset += 8;

            let length = (&len_crc_buf[..4]).get_u32();
            if length as u64 > file_len {
                return Err(Error::CustomError(
                    "buffer length greater than file size".to_string(),
                ));
            }

            buf.resize(length as usize, 0);
            if let Err(e) = file.read_exact(&mut buf) {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(e.into()),
                }
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

impl ManifestFile {
    /// Opens a Agate manifest file if it exists, or creates one if doesn’t exists.
    pub fn open_or_create_manifest_file(opt: &AgateOptions) -> Result<Self> {
        if opt.in_memory {
            Ok(Self {
                directory: PathBuf::new(),
                deletions_rewrite_threshold: 0,
                inner: Mutex::new(ManifestFileInner {
                    file: None,
                    manifest: Manifest::default(),
                }),
            })
        } else {
            Self::help_open_or_create_manifest_file(
                &opt.dir,
                opt.read_only,
                MANIFEST_DELETION_REWRITE_THRESHOLD,
            )
        }
    }

    fn help_open_or_create_manifest_file(
        dir: impl AsRef<Path>,
        read_only: bool,
        deletions_threshold: usize,
    ) -> Result<Self> {
        let path = dir.as_ref().join(MANIFEST_FILENAME);

        if path.exists() {
            let mut file = OpenOptions::new()
                .read(true)
                .write(!read_only)
                .create(false)
                .open(&path)?;

            let (manifest, trunc_offset) = Manifest::replay(&mut file)?;

            if !read_only {
                file.set_len(trunc_offset as u64)?;
            }
            file.seek(SeekFrom::End(0))?;

            Ok(ManifestFile {
                directory: dir.as_ref().to_path_buf(),
                deletions_rewrite_threshold: deletions_threshold,
                inner: Mutex::new(ManifestFileInner {
                    file: Some(file),
                    manifest,
                }),
            })
        } else {
            if read_only {
                return Err(Error::ReadOnlyError(path.as_path().display().to_string()));
            }

            let manifest = Manifest::default();
            let (file, net_creations) = Self::help_rewrite(dir.as_ref(), &manifest)?;
            assert_eq!(net_creations, 0);

            Ok(ManifestFile {
                directory: dir.as_ref().to_path_buf(),
                deletions_rewrite_threshold: deletions_threshold,
                inner: Mutex::new(ManifestFileInner {
                    file: Some(file),
                    manifest,
                }),
            })
        }
    }

    /// Writes `manifest` to MANIFEST file **atomically**.
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

        file.write_all(&buf)?;
        file.sync_data()?;
        drop(file);

        let manifest_path = dir.as_ref().join(MANIFEST_FILENAME);
        fs::rename(&rewrite_path, &manifest_path)?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&manifest_path)?;
        file.seek(SeekFrom::End(0))?;

        util::sync_dir(&dir)?;

        Ok((file, net_creations))
    }

    /// Writes a batch of changes, **atomically**, to the file. When we replay the
    /// MANIFEST file, we'll either replay all the changes or none of them.
    pub fn add_changes(&self, changes_param: Vec<ManifestChange>) -> Result<()> {
        {
            let inner = self.inner.lock().unwrap();
            if inner.file.is_none() {
                return Ok(());
            }
        }

        // We could drop the lock during encoding.
        let changes = ManifestChangeSet {
            changes: changes_param,
        };
        let mut buf = BytesMut::new();
        changes.encode(&mut buf)?;

        let mut inner = self.inner.lock().unwrap();

        apply_change_set(&mut inner.manifest, &changes)?;

        if inner.manifest.deletions > self.deletions_rewrite_threshold
            && inner.manifest.deletions
                > MANIFEST_DELETIONS_RATIO * (inner.manifest.creations - inner.manifest.deletions)
        {
            inner.rewrite(&self.directory)?;
        } else {
            let mut len_crc_buf = vec![0; 8];
            (&mut len_crc_buf[..4]).put_u32(buf.len() as u32);
            (&mut len_crc_buf[4..]).put_u32(crc32::checksum_castagnoli(&buf));
            len_crc_buf.extend_from_slice(&buf);

            inner.file.as_mut().unwrap().write_all(&len_crc_buf)?;
        }

        inner.file.as_mut().unwrap().sync_data()?;
        Ok(())
    }

    pub fn manifest_cloned(&self) -> Manifest {
        self.inner.lock().unwrap().manifest.clone()
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
                    "manifest invalid, table {} exists",
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
                assert!(build.levels[tm.level as usize].tables.remove(&tc.id));
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

pub fn new_create_change(id: u64, level: usize, key_id: u64) -> ManifestChange {
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

pub fn new_delete_change(id: u64) -> ManifestChange {
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

#[cfg(test)]
mod tests {
    use std::os::unix::prelude::FileExt;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_manifest_basic() {
        let mut opts = AgateOptions::default();
        let tmp_dir = tempdir().unwrap();
        opts.dir = tmp_dir.path().to_path_buf();

        let manifestfile = ManifestFile::open_or_create_manifest_file(&opts).unwrap();

        let changes_param1 = vec![new_create_change(1, 1, 1)];
        let changes_param2 = vec![new_create_change(2, 2, 2)];

        manifestfile.add_changes(changes_param1.clone()).unwrap();
        manifestfile.add_changes(changes_param2.clone()).unwrap();

        drop(manifestfile);

        let manifestfile = ManifestFile::open_or_create_manifest_file(&opts).unwrap();

        let mut changes = manifestfile.manifest_cloned().as_changes();
        changes.sort_by(|i, j| i.id.cmp(&j.id));

        assert_eq!([changes_param1, changes_param2].concat(), changes);
    }

    fn help_test_manifest_corruption(offset: u64, err: String) {
        let mut opts = AgateOptions::default();
        let tmp_dir = tempdir().unwrap();
        opts.dir = tmp_dir.path().to_path_buf();

        {
            ManifestFile::open_or_create_manifest_file(&opts).unwrap();
            let path = opts.dir.join(MANIFEST_FILENAME);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            file.write_at(&[b'G'], offset).unwrap();
        }

        let res = ManifestFile::open_or_create_manifest_file(&opts);

        match res.err().unwrap() {
            Error::CustomError(e) => {
                assert_eq!(e, err);
            }
            _ => {
                panic!("mismatch");
            }
        }
    }

    #[test]
    fn test_test_manifest_magic() {
        help_test_manifest_corruption(3, "bad magic text".to_string());
    }

    #[test]
    fn test_test_manifest_version() {
        help_test_manifest_corruption(4, "bad magic version".to_string());
    }

    #[test]
    fn test_test_manifest_checksum() {
        help_test_manifest_corruption(15, "bad checksum".to_string());
    }
}
