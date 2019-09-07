use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::fs::OpenOptions;
use std::io::{prelude::*, Result, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

mod de;
mod ser;

use de::DeserializeOwned;
use ser::Serialize;

use comde::{
    deflate::{DeflateCompressor, DeflateDecompressor},
    snappy::{SnappyCompressor, SnappyDecompressor},
    stored::{StoredCompressor, StoredDecompressor},
    xz::{XzCompressor, XzDecompressor},
    zstd::{ZstdCompressor, ZstdDecompressor},
    ByteCount, Compress, Compressor, Decompress, Decompressor,
};
use memmap::MmapOptions;

#[cfg(not(windows))]
/// The platform-specific separator as a string, used for splitting
/// platform-supplied paths and printing `BoxPath`s in the platform-preferred
/// manner.
pub const PATH_PLATFORM_SEP: &str = "/";

#[cfg(windows)]
/// The platform-specific separator as a string, used for splitting
/// platform-supplied paths and printing `BoxPath`s in the platform-preferred
/// manner.
pub const PATH_PLATFORM_SEP: &str = "\\";

/// The separator used in `BoxPath` type paths, used primarily in
/// `FileRecord` and `DirectoryRecord` fields.
pub const PATH_BOX_SEP: &str = "\x1f";

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Compression {
    Stored,
    Deflate,
    Zstd,
    Xz,
    Snappy,
    Unknown(u8),
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Compression::*;

        let s = match self {
            Stored => "stored",
            Deflate => "DEFLATE",
            Zstd => "Zstandard",
            Xz => "xz",
            Snappy => "Snappy",
            Unknown(id) => return write!(f, "Unknown(id: {:x})", id),
        };

        write!(f, "{}", s)
    }
}

impl fmt::Debug for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

const COMPRESSION_STORED: u8 = 0x00;
const COMPRESSION_DEFLATE: u8 = 0x10;
const COMPRESSION_ZSTD: u8 = 0x20;
const COMPRESSION_XZ: u8 = 0x30;
const COMPRESSION_SNAPPY: u8 = 0x40;

impl Compression {
    pub fn id(self) -> u8 {
        use Compression::*;

        match self {
            Stored => COMPRESSION_STORED,
            Deflate => COMPRESSION_DEFLATE,
            Zstd => COMPRESSION_ZSTD,
            Xz => COMPRESSION_XZ,
            Snappy => COMPRESSION_SNAPPY,
            Unknown(id) => id,
        }
    }

    fn compress<W: Write + Seek, V: Compress>(self, writer: W, data: V) -> Result<ByteCount> {
        use Compression::*;

        match self {
            Stored => StoredCompressor.compress(writer, data),
            Deflate => DeflateCompressor.compress(writer, data),
            Zstd => ZstdCompressor.compress(writer, data),
            Xz => XzCompressor.compress(writer, data),
            Snappy => SnappyCompressor.compress(writer, data),
            Unknown(id) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Cannot handle compression with id {}", id),
            )),
        }
    }

    fn decompress<R: Read, V: Decompress>(self, reader: R) -> Result<V> {
        use Compression::*;

        match self {
            Stored => StoredDecompressor.from_reader(reader),
            Deflate => DeflateDecompressor.from_reader(reader),
            Zstd => ZstdDecompressor.from_reader(reader),
            Xz => XzDecompressor.from_reader(reader),
            Snappy => SnappyDecompressor.from_reader(reader),
            Unknown(id) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Cannot handle decompression with id {}", id),
            )),
        }
    }

    fn decompress_write<R: Read, W: Write>(self, reader: R, writer: W) -> Result<()> {
        use Compression::*;

        match self {
            Stored => StoredDecompressor.copy(reader, writer),
            Deflate => DeflateDecompressor.copy(reader, writer),
            Zstd => ZstdDecompressor.copy(reader, writer),
            Xz => XzDecompressor.copy(reader, writer),
            Snappy => SnappyDecompressor.copy(reader, writer),
            Unknown(id) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Cannot handle decompression with id {}", id),
            )),
        }?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct BoxHeader {
    magic_bytes: [u8; 4],
    version: u32,
    alignment: Option<NonZeroU64>,
    trailer: Option<NonZeroU64>,
}

impl BoxHeader {
    fn new(trailer: Option<NonZeroU64>) -> BoxHeader {
        BoxHeader {
            magic_bytes: *b"BOX\0",
            version: 0x0,
            alignment: None,
            trailer,
        }
    }

    fn with_alignment(alignment: NonZeroU64) -> BoxHeader {
        let mut header = BoxHeader::default();
        header.alignment = Some(alignment);
        header
    }

    pub fn alignment(&self) -> Option<NonZeroU64> {
        self.alignment
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

impl Default for BoxHeader {
    fn default() -> Self {
        BoxHeader::new(None)
    }
}

pub type AttrMap = HashMap<u32, Vec<u8>>;

#[derive(Debug, Default)]
pub struct BoxMetadata {
    records: Vec<Record>,
    attr_keys: Vec<String>,
    attrs: AttrMap,
}

impl BoxMetadata {
    pub fn records(&self) -> &[Record] {
        &*self.records
    }
}

#[derive(Debug)]
pub enum Record {
    File(FileRecord),
    Directory(DirectoryRecord),
}

impl Record {
    #[inline(always)]
    pub fn as_file(&self) -> Option<&FileRecord> {
        match self {
            Record::File(file) => Some(file),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_directory(&self) -> Option<&DirectoryRecord> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn path(&self) -> &BoxPath {
        match self {
            Record::File(file) => &file.path,
            Record::Directory(dir) => &dir.path,
        }
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, boxfile: &BoxFile, key: S) -> Option<&Vec<u8>> {
        let key = boxfile.attr_key_for(key.as_ref())?;
        self.attrs().get(&key)
    }

    #[inline(always)]
    fn attrs(&self) -> &AttrMap {
        match self {
            Record::Directory(dir) => &dir.attrs,
            Record::File(file) => &file.attrs,
        }
    }

    #[inline(always)]
    fn attrs_mut(&mut self) -> &mut AttrMap {
        match self {
            Record::Directory(dir) => &mut dir.attrs,
            Record::File(file) => &mut file.attrs,
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BoxPath(String);

#[derive(Debug, Clone)]
pub enum IntoBoxPathError {
    UnrepresentableStr,
    NonCanonical,
    EmptyPath,
}

impl std::error::Error for IntoBoxPathError {}

impl fmt::Display for IntoBoxPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl IntoBoxPathError {
    pub fn as_str(&self) -> &str {
        match self {
            IntoBoxPathError::NonCanonical => "non-canonical path received as input",
            IntoBoxPathError::UnrepresentableStr => "unrepresentable string found in path",
            IntoBoxPathError::EmptyPath => "no path provided",
        }
    }

    pub fn as_io_error(&self) -> std::io::Error {
        use std::io::{Error, ErrorKind};
        Error::new(ErrorKind::InvalidInput, self.as_str())
    }
}

impl<'a> PartialEq<&'a str> for BoxPath {
    fn eq(&self, other: &&'a str) -> bool {
        self.0 == *other
    }
}

impl BoxPath {
    pub fn new<P: AsRef<Path>>(path: P) -> std::result::Result<BoxPath, IntoBoxPathError> {
        use std::path::Component;
        use unic_normal::StrNormalForm;
        use unic_ucd::GeneralCategory;

        let mut out = vec![];

        for component in path.as_ref().components() {
            match component {
                Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
                Component::ParentDir => {
                    out.pop();
                }
                Component::Normal(os_str) => out.push(
                    os_str
                        .to_str()
                        .map(|x| x.trim())
                        .filter(|x| x.len() > 0)
                        .filter(|x| {
                            !x.chars().any(|c| {
                                let cat = GeneralCategory::of(c);
                                c == '\\' || cat == GeneralCategory::Control || (cat.is_separator() && c != ' ')
                            })
                        })
                        .map(|x| x.nfc().collect::<String>())
                        .ok_or(IntoBoxPathError::UnrepresentableStr)?,
                ),
            }
        }

        if out.len() == 0 {
            return Err(IntoBoxPathError::EmptyPath);
        }

        Ok(BoxPath(out.join(PATH_BOX_SEP)))
    }

    pub fn to_string(&self) -> String {
        let mut s = String::with_capacity(self.0.len());
        let mut iter = self.0.split(PATH_BOX_SEP);
        if let Some(v) = iter.next() {
            s.push_str(v);
        }
        iter.for_each(|v| {
            s.push_str(PATH_PLATFORM_SEP);
            s.push_str(v);
        });
        s
    }

    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(self.to_string())
    }
}

#[derive(Debug)]
pub struct DirectoryRecord {
    /// The path of the directory. A path is always relative (no leading separator),
    /// always delimited by a `UNIT SEPARATOR U+001F` (`"\x1f"`), and may not contain
    /// any `.` or `..` path chunks.
    pub path: BoxPath,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

#[derive(Debug)]
pub struct FileRecord {
    /// a bytestring representing the type of compression being used, always 8 bytes.
    pub compression: Compression,

    /// The exact length of the data as written, ignoring any padding.
    pub length: u64,

    /// A hint for the size of the content when decompressed. Do not trust in absolute terms.
    pub decompressed_length: u64,

    /// The position of the data in the file
    pub data: NonZeroU64,

    /// The path of the file. A path is always relative (no leading separator),
    /// always delimited by a `UNIT SEPARATOR U+001F` (`"\x1f"`), and may not contain
    /// any `.` or `..` path chunks.
    pub path: BoxPath,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl FileRecord {
    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, boxfile: &BoxFile, key: S) -> Option<&Vec<u8>> {
        let key = boxfile.attr_key_for(key.as_ref())?;
        self.attrs.get(&key)
    }
}

#[derive(Debug)]
pub struct BoxFile {
    file: std::fs::File,
    header: BoxHeader,
    meta: BoxMetadata,
}

impl BoxFile {
    /// This will open an existing `.box` file for reading and writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFile> {
        OpenOptions::new()
            .write(true)
            .read(true)
            .open(path.as_ref())
            .map(|file| {
                let mut f = BoxFile {
                    file,
                    header: BoxHeader::default(),
                    meta: BoxMetadata::default(),
                };

                // Try to load the header so we can easily rewrite it when saving.
                // If header is invalid, we're not even loading a .box file.
                f.header = f.read_header()?;
                f.meta = f.read_trailer()?;

                Ok(f)
            })?
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFile> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .map(|file| BoxFile {
                file,
                header: BoxHeader::default(),
                meta: BoxMetadata::default(),
            })?;

        boxfile.write_header_and_trailer()?;

        Ok(boxfile)
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    /// Will insert byte-aligned values based on provided `alignment` value. For best results, consider a power of 2.
    pub fn create_with_alignment<P: AsRef<Path>>(
        path: P,
        alignment: NonZeroU64,
    ) -> std::io::Result<BoxFile> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .map(|file| BoxFile {
                file,
                header: BoxHeader::with_alignment(alignment),
                meta: BoxMetadata::default(),
            })?;

        boxfile.write_header_and_trailer()?;

        Ok(boxfile)
    }

    #[inline(always)]
    fn write_header_and_trailer(&mut self) -> std::io::Result<()> {
        self.write_header()?;
        let pos = self.write_trailer()?;
        self.header.trailer = Some(NonZeroU64::new(pos).unwrap());
        self.write_header()
    }

    pub fn header(&self) -> &BoxHeader {
        &self.header
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    #[inline(always)]
    fn next_write_addr(&self) -> NonZeroU64 {
        let offset = self
            .meta
            .records
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

    pub fn data(&self, record: &FileRecord) -> std::io::Result<memmap::Mmap> {
        self.read_data(record)
    }

    pub fn decompress_value<V: Decompress>(&self, record: &FileRecord) -> std::io::Result<V> {
        let mmap = self.read_data(record)?;
        record.compression.decompress(std::io::Cursor::new(mmap))
    }

    pub fn decompress<W: Write>(&self, record: &FileRecord, dest: W) -> std::io::Result<()> {
        let mmap = self.read_data(record)?;
        record
            .compression
            .decompress_write(std::io::Cursor::new(mmap), dest)
    }

    #[inline(always)]
    fn attr_key_for_mut(&mut self, key: &str) -> u32 {
        match self.meta.attr_keys.iter().position(|r| r == key) {
            Some(v) => v as u32,
            None => {
                self.meta.attr_keys.push(key.to_string());
                (self.meta.attr_keys.len() - 1) as u32
            }
        }
    }

    #[inline(always)]
    fn attr_key_for(&self, key: &str) -> Option<u32> {
        self.meta
            .attr_keys
            .iter()
            .position(|r| r == key)
            .map(|v| v as u32)
    }

    pub fn set_attr<S: AsRef<str>>(
        &mut self,
        path: &BoxPath,
        key: S,
        value: Vec<u8>,
    ) -> Result<()> {
        let key = self.attr_key_for_mut(key.as_ref());

        if let Some(record) = self.meta.records.iter_mut().find(|r| r.path() == path) {
            record.attrs_mut().insert(key, value);
        }

        self.write_header_and_trailer()
    }

    pub fn attr<S: AsRef<str>>(&self, path: &BoxPath, key: S) -> Option<&Vec<u8>> {
        let key = self.attr_key_for(key.as_ref())?;

        if let Some(record) = self.meta.records.iter().find(|r| r.path() == path) {
            record.attrs().get(&key)
        } else {
            None
        }
    }

    pub fn mkdir(&mut self, path: BoxPath, attrs: HashMap<String, Vec<u8>>) -> std::io::Result<()> {
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| {
                let k = self.attr_key_for_mut(&k);
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        self.meta
            .records
            .push(Record::Directory(DirectoryRecord { path, attrs }));
        self.write_trailer()?;
        Ok(())
    }

    pub fn insert<V: Compress>(
        &mut self,
        compression: Compression,
        path: BoxPath,
        value: V,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord> {
        let data = self.next_write_addr();
        let bytes = self.write_data::<V>(compression, data.get(), value)?;
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| {
                let k = self.attr_key_for_mut(&k);
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        // Check there isn't already a record for this path
        if self.meta.records.iter().any(|x| x.path() == &path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "path already found",
            ));
        }

        let record = FileRecord {
            compression,
            length: bytes.write,
            decompressed_length: bytes.read,
            path,
            data,
            attrs,
        };

        self.meta.records.push(Record::File(record));
        let pos = self.write_trailer()?;
        self.header.trailer = Some(NonZeroU64::new(pos).unwrap());
        self.write_header()?;

        Ok(&self.meta.records.last().unwrap().as_file().unwrap())
    }

    #[inline(always)]
    fn read_header(&mut self) -> std::io::Result<BoxHeader> {
        self.file.seek(SeekFrom::Start(0))?;
        BoxHeader::deserialize_owned(&mut self.file)
    }

    #[inline(always)]
    fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.header.write(&mut self.file)
    }

    #[inline(always)]
    fn write_trailer(&mut self) -> std::io::Result<u64> {
        let pos = self.next_write_addr().get();
        self.file.set_len(pos)?;
        self.file.seek(SeekFrom::Start(pos))?;
        self.meta.write(&mut self.file)?;
        Ok(pos)
    }

    #[inline(always)]
    fn write_data<V: Compress>(
        &mut self,
        compression: Compression,
        pos: u64,
        reader: V,
    ) -> std::io::Result<comde::com::ByteCount> {
        self.file.seek(SeekFrom::Start(pos))?;
        compression.compress(&mut self.file, reader)
    }

    #[inline(always)]
    fn read_trailer(&mut self) -> std::io::Result<BoxMetadata> {
        let header = self.read_header()?;
        let ptr = header
            .trailer
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no trailer found"))?;
        self.file.seek(SeekFrom::Start(ptr.get()))?;
        BoxMetadata::deserialize_owned(&mut self.file)
    }

    #[inline(always)]
    fn read_data(&self, header: &FileRecord) -> std::io::Result<memmap::Mmap> {
        unsafe {
            MmapOptions::new()
                .offset(header.data.get())
                .len(header.length as usize)
                .map(&self.file)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_box<F: AsRef<Path>>(filename: F) {
        let _ = std::fs::remove_file(filename.as_ref());

        let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let data = b"hello\0\0\0";

        let mut header = BoxHeader::default();
        let mut trailer = BoxMetadata::default();
        trailer.records.push(Record::File(FileRecord {
            compression: Compression::Stored,
            length: data.len() as u64,
            decompressed_length: data.len() as u64,
            data: NonZeroU64::new(std::mem::size_of::<BoxHeader>() as u64).unwrap(),
            path: BoxPath::new("hello.txt").unwrap(),
            attrs: HashMap::new(),
        }));

        header.trailer = NonZeroU64::new(std::mem::size_of::<BoxHeader>() as u64 + 8);

        header.write(&mut cursor).unwrap();
        cursor.write_all(data).unwrap();
        trailer.write(&mut cursor).unwrap();

        let mut f = std::fs::File::create(filename.as_ref()).unwrap();
        f.write_all(&*cursor.get_ref()).unwrap();
    }

    #[test]
    fn create_box_file() {
        create_test_box("./smoketest.box");
    }

    #[test]
    fn read_garbage() {
        let filename = "./read_garbage.box";
        create_test_box(&filename);

        let mut bf = BoxFile::open(&filename).unwrap();
        let trailer = bf.read_trailer().unwrap();
        println!("{:?}", bf.read_header());
        println!("{:?}", &trailer);
        let file_data = bf
            .read_data(&trailer.records[0].as_file().unwrap())
            .unwrap();
        println!("{:?}", &*file_data);
        assert_eq!(&*file_data, b"hello\0\0\0")
    }

    #[test]
    fn create_garbage() {
        let filename = "./create_garbage.box";
        let _ = std::fs::remove_file(&filename);
        let mut bf = BoxFile::create(&filename).expect("Mah box");
        assert!(bf.read_header().is_ok());
        assert!(bf.read_trailer().is_ok());
    }

    #[test]
    fn box_path_sanitisation() {
        let box_path = BoxPath::new("/something/../somethingelse/./foo.txt").unwrap();
        assert_eq!(box_path, "somethingelse\x1ffoo.txt");
        let box_path = BoxPath::new("../something/../somethingelse/./foo.txt/.").unwrap();
        assert_eq!(box_path, "somethingelse\x1ffoo.txt");

        // This one will do different things on Windows and Unix, because Unix loves a good backslash
        let box_path = BoxPath::new(r"..\something\..\somethingelse\.\foo.txt\.");

        #[cfg(not(windows))]
        assert!(box_path.is_err());
        #[cfg(windows)]
        assert_eq!(box_path.unwrap().0, "somethingelse\x1ffoo.txt");

        let box_path = BoxPath::new(r"..\something/..\somethingelse\./foodir\");
        #[cfg(not(windows))]
        assert!(box_path.is_err());
        #[cfg(windows)]
        assert_eq!(box_path.unwrap().0, "somethingelse\x1ffoodir");
    }

    #[test]
    fn box_path_sanitisation2() {
        // Null is a sassy fellow
        let box_path = BoxPath::new("\0");
        assert!(box_path.is_err());
    }

    #[test]
    fn box_path_sanitisation3() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("");
        assert!(box_path.is_err());
    }

    #[test]
    fn box_path_sanitisation4() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("/cant/hate//the/path");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "cant\x1fhate\x1fthe\x1fpath");
    }

    #[test]
    fn box_path_sanitisation_bidi() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("this is now العَرَبِيَّة.txt");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "this is now العَرَبِيَّة.txt");
    }

    #[test]
    fn box_path_sanitisation_basmala() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("this is now ﷽.txt");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "this is now ﷽.txt");
    }

    #[test]
    fn box_path_sanitisation_icecube_emoji() {
        let box_path = BoxPath::new("///🧊/🧊");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "🧊\x1f🧊");
    }
    

    fn insert_impl<F>(filename: &str, f: F)
    where
        F: Fn(&str) -> BoxFile,
    {
        let _ = std::fs::remove_file(&filename);
        let v =
            "This, this, this, this, this is a compressable string string string string string.\n"
                .to_string();

        {
            use std::time::SystemTime;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_le_bytes();

            let mut bf = f(filename);

            let mut dir_attrs = HashMap::new();
            dir_attrs.insert("created".into(), now.to_vec());
            dir_attrs.insert("unix.acl".into(), 0o755u16.to_le_bytes().to_vec());

            let mut attrs = HashMap::new();
            attrs.insert("created".into(), now.to_vec());
            attrs.insert("unix.acl".into(), 0o644u16.to_le_bytes().to_vec());

            bf.mkdir(BoxPath::new("test").unwrap(), dir_attrs).unwrap();

            bf.insert(
                Compression::Zstd,
                BoxPath::new("test/string.txt").unwrap(),
                v.clone(),
                attrs.clone(),
            )
            .unwrap();
            bf.insert(
                Compression::Deflate,
                BoxPath::new("test/string2.txt").unwrap(),
                v.clone(),
                attrs.clone(),
            )
            .unwrap();
            println!("{:?}", &bf);
        }

        let bf = BoxFile::open(&filename).expect("Mah box");
        println!("{:#?}", &bf);

        assert_eq!(
            v,
            bf.decompress_value::<String>(&bf.meta.records[1].as_file().unwrap())
                .unwrap()
        );
        assert_eq!(
            v,
            bf.decompress_value::<String>(&bf.meta.records[2].as_file().unwrap())
                .unwrap()
        );
    }

    #[test]
    fn insert() {
        insert_impl("./insert_garbage.box", |n| BoxFile::create(n).unwrap());
        insert_impl("./insert_garbage_align8.box", |n| {
            BoxFile::create_with_alignment(n, NonZeroU64::new(8).unwrap()).unwrap()
        });
        insert_impl("./insert_garbage_align7.box", |n| {
            BoxFile::create_with_alignment(n, NonZeroU64::new(7).unwrap()).unwrap()
        });
    }
}
