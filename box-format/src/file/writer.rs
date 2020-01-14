use std::collections::HashMap;
use std::default::Default;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader, BufWriter, Result, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use memmap::MmapOptions;

use crate::{
    compression::Compression,
    header::BoxHeader,
    path::BoxPath,
    record::{DirectoryRecord, FileRecord, LinkRecord, Record},
    ser::Serialize,
};

use super::{
    reader::{read_header, read_trailer},
    BoxMetadata,
};
use crate::file::Inode;

pub struct BoxFileWriter {
    pub(crate) file: BufWriter<File>,
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
}

impl Drop for BoxFileWriter {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl BoxFileWriter {
    #[inline(always)]
    fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.header.write(&mut self.file)
    }

    #[inline(always)]
    fn finish_inner(&mut self) -> std::io::Result<u64> {
        let pos = self.next_write_addr().get();
        self.header.trailer = NonZeroU64::new(pos);
        self.write_header()?;
        self.file.seek(SeekFrom::Start(pos))?;
        self.meta.write(&mut self.file)?;

        let new_pos = self.file.seek(SeekFrom::Current(0))?;
        let file = self.file.get_mut();
        file.set_len(new_pos)?;
        Ok(new_pos)
    }

    pub fn finish(mut self) -> std::io::Result<u64> {
        self.finish_inner()
    }

    #[inline(always)]
    fn next_write_addr(&self) -> NonZeroU64 {
        // TODO: this is probably slow as hell
        let offset = self
            .meta
            .inodes
            .iter()
            .rev()
            .find_map(|r| r.as_file())
            .map(|r| r.data.get() + r.length)
            .unwrap_or(std::mem::size_of::<BoxHeader>() as u64);

        let v = match self.header.alignment {
            None => offset,
            Some(alignment) => {
                let alignment = alignment.get();
                let diff = offset % alignment;
                if diff == 0 {
                    offset
                } else {
                    offset + (alignment - diff)
                }
            }
        };

        NonZeroU64::new(v).unwrap()
    }

    /// This will open an existing `.box` file for writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .map(|mut file| {
                // Try to load the header so we can easily rewrite it when saving.
                // If header is invalid, we're not even loading a .box file.
                let (header, meta) = {
                    let mut reader = BufReader::new(&mut file);
                    let header = read_header(&mut reader)?;
                    let ptr = header.trailer.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "no trailer found")
                    })?;
                    let meta = read_trailer(&mut reader, ptr)?;
                    (header, meta)
                };

                let f = BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header,
                    meta,
                };

                Ok(f)
            })?
    }

    /// This will create a new `.box` file for writing, and error if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .and_then(|file| {
                Ok(BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header: BoxHeader::default(),
                    meta: BoxMetadata::default(),
                })
            })?;

        boxfile.write_header()?;

        Ok(boxfile)
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    /// Will insert byte-aligned values based on provided `alignment` value. For best results, consider a power of 2.
    pub fn create_with_alignment<P: AsRef<Path>>(
        path: P,
        alignment: NonZeroU64,
    ) -> std::io::Result<BoxFileWriter> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .and_then(|file| {
                Ok(BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header: BoxHeader::with_alignment(alignment),
                    meta: BoxMetadata::default(),
                })
            })?;

        boxfile.write_header()?;

        Ok(boxfile)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn alignment(&self) -> Option<NonZeroU64> {
        self.header.alignment
    }

    pub fn version(&self) -> u32 {
        self.header.version
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    #[inline(always)]
    fn iter(&self) -> super::reader::Records {
        super::reader::Records::new(self.metadata(), &*self.metadata().root, None)
    }

    pub fn mkdir(&mut self, path: BoxPath, attrs: HashMap<String, Vec<u8>>) -> std::io::Result<()> {
        // let attrs = attrs
        //     .into_iter()
        //     .map(|(k, v)| {
        //         let k = self.meta.attr_key_or_create(&k);
        //         (k, v)
        //     })
        //     .collect::<HashMap<_, _>>();

        // self.meta
        //     .records
        //     .push(Record::Directory(DirectoryRecord::new(name)));
        // Ok(())

        todo!("This needs to be recursive now :D")
    }

    pub fn link(
        &mut self,
        path: BoxPath,
        target: BoxPath,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<()> {
        todo!()
        // let attrs = attrs
        //     .into_iter()
        //     .map(|(k, v)| {
        //         let k = self.meta.attr_key_or_create(&k);
        //         (k, v)
        //     })
        //     .collect::<HashMap<_, _>>();

        // self.meta.records.push(Record::Link(LinkRecord {
        //     path,
        //     target,
        //     attrs,
        // }));
        // Ok(())
    }

    // #[inline(always)]

    // fn insert_into_inode(
    //     &mut self,
    //     compression: Compression,
    //     inode: Option<Inode>,
    //     value: &mut R,
    //     attrs: HashMap<String, Vec<u8>>,
    // ) -> std::io::Result<&FileRecord> {

    // }

    pub fn insert<R: Read>(
        &mut self,
        compression: Compression,
        path: BoxPath,
        value: &mut R,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord> {
        // Find the parent inode to add this to
        let inode = match path.parent() {
            None => {
                // This means we're at the root
                None // return self.insert_into_root(compression, path, value, attrs);
            }
            Some(path) => {
                // Find the record for this path
                let result = self.iter().find(|x| x.path == path);
                let result = match result {
                    Some(v) => v,
                    None => todo!(),
                };

                if result.record.as_directory().is_none() {
                    todo!()
                }

                // Check if other inodes are this path
                // TODO
                Some(result.inode)
            }
        };

        let next_addr = self.next_write_addr();
        let byte_count = self.write_data::<R>(compression, next_addr.get(), value)?;
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| {
                let k = self.meta.attr_key_or_create(&k);
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        let record = FileRecord {
            compression,
            length: byte_count.write,
            decompressed_length: byte_count.read,
            name: path.filename(),
            data: next_addr,
            attrs,
        };

        self.meta.inodes.push(Record::File(record));
        let file_inode = Inode::new(self.meta.inodes.len() as u64).unwrap();

        if let Some(inode) = inode {
            let record = self
                .meta
                .record_mut(inode)
                .unwrap()
                .as_directory_mut()
                .unwrap();
            record.inodes.push(file_inode);
        } else {
            self.meta.root.push(file_inode);
        }

        Ok(&self.meta.inodes.last().unwrap().as_file().unwrap())
    }

    pub unsafe fn data(&self, record: &FileRecord) -> std::io::Result<memmap::Mmap> {
        self.read_data(record)
    }

    #[inline(always)]
    fn write_data<R: Read>(
        &mut self,
        compression: Compression,
        pos: u64,
        reader: &mut R,
    ) -> std::io::Result<comde::com::ByteCount> {
        self.file.seek(SeekFrom::Start(pos))?;
        compression.compress(&mut self.file, reader)
    }

    pub fn set_attr<S: AsRef<str>>(
        &mut self,
        path: &BoxPath,
        key: S,
        value: Vec<u8>,
    ) -> Result<()> {
        let inode = match self.iter().find(|r| &r.path == path) {
            Some(v) => v.inode,
            None => todo!()
        };

        let key = self.meta.attr_key_or_create(key.as_ref());
        let record = self.meta.record_mut(inode).unwrap();
        record.attrs_mut().insert(key, value);

        Ok(())
    }

    pub fn set_file_attr<S: AsRef<str>>(&mut self, key: S, value: Vec<u8>) -> Result<()> {
        let key = self.meta.attr_key_or_create(key.as_ref());

        self.meta.attrs.insert(key, value);

        Ok(())
    }

    #[inline(always)]
    unsafe fn read_data(&self, header: &FileRecord) -> std::io::Result<memmap::Mmap> {
        MmapOptions::new()
            .offset(header.data.get())
            .len(header.length as usize)
            .map(&self.file.get_ref())
    }
}
