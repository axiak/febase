use super::error::Error;
use crate::error::ErrorKind;
use byteorder::{BigEndian, ReadBytesExt};
use std::cell::Cell;
use std::cmp::min;
use std::io::{Cursor, Read};
use std::io::SeekFrom;
use std::pin::Pin;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use prost::{DecodeError, Message, encoding};


use crate::error::ErrorKind::StorageError;

// Include the `items` module, which is generated from items.proto.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/hbase.pb.rs"));
}

const MIN_FORMAT_VERSION: u8 = 2;
const MAX_FORMAT_VERSION: u8 = 3;
const TRAILER_MAGIC: &'static [u8] = "TRABLK\"$".as_bytes();

const fn trailer_size_for_version(version: u8) -> usize {
    if version == 2 {
        212
    } else {
        4 * 1024
    }
}

const fn max_trailer_size() -> usize {
    trailer_size_for_version(MAX_FORMAT_VERSION)
}

pub const MAX_TRAILER_SIZE: usize = max_trailer_size();
pub const PROTOBUF_TRAILER_MINOR_VERSION: u8 = 2;

#[derive(Debug, Clone, Default)]
pub struct HFileTrailer {
    major_version: u8,
    minor_version: u8,
    num_data_index_levels: u32,
    last_data_block_offset: u64,
    first_data_block_offset: u64,
    load_on_open_data_offset: u64,
    uncompressed_data_index_size: u64,
    total_uncompressed_bytes: u64,
    entry_count: u64,
    data_index_count: u32,
    meta_index_count: u32,
    file_info_offset: u64,
    cell_comparator: CellComparator,
    compression_codec: CompressionCodec,
    encryption_key: Box<[u8]>,
}

impl HFileTrailer {
    // pub async fn from_file(mut file: tokio::fs::File) -> Result<HFileTrailer, Error> {
    //
    // }

    pub async fn from_fname(fname: &str) -> Result<HFileTrailer, Error> {
        // HFileTrailer::from_file(tokio::fs::File::open(fname).await?).await
        let mut file = tokio::fs::File::open(fname).await?;
        let length = file.metadata().await?.len();

        let mut buffer_size = MAX_TRAILER_SIZE as u64;
        let seek_point = if buffer_size > length {
            buffer_size = length;
            0
        } else {
            length - buffer_size
        };

        let pos = file.seek(SeekFrom::Start(seek_point)).await?;

        let mut buf: Vec<u8> = vec![0; buffer_size as usize];
        file.read_exact(&mut buf).await?;

        let mut reader = Cursor::new(buf);
        let _ = reader.seek(SeekFrom::End(-4)).await?;
        let encoded_version = ReadBytesExt::read_u32::<BigEndian>(&mut reader).unwrap();

        let (major_version, minor_version) = Self::decode_version(encoded_version);

        Self::check_version(major_version)?;

        let trailer_size = trailer_size_for_version(major_version);
        let _ = reader.seek(SeekFrom::End(-(trailer_size as i64))).await?;

        let mut magic: [u8; TRAILER_MAGIC.len()] = [0; TRAILER_MAGIC.len()];
        AsyncReadExt::read_exact(&mut reader, &mut magic).await?;

        Self::from_reader(major_version, minor_version, &mut reader).await
    }

    async fn from_reader(
        major_version: u8,
        minor_version: u8,
        reader: &mut Cursor<Vec<u8>>,
    ) -> Result<HFileTrailer, Error> {
        let mut hfile_trailer = HFileTrailer::default();
        hfile_trailer.major_version = major_version;
        hfile_trailer.minor_version = minor_version;

        if !hfile_trailer.use_protobuf() {
            todo!();
            return Err(Error::new(ErrorKind::StorageError(
                "did not implement old stuff".into(),
            )));
        }

        let proto_length = prost::encoding::decode_varint(reader)
            .map_err(|e| Error::new(ErrorKind::ProtoDecodeError(e)))?
        as usize;

        let pos = reader.position() as usize;
        let proto_slice = &reader.get_ref()[pos..pos + proto_length];

        println!("proto {:x?}", proto_slice);

        println!("proto length: {}", proto_length);
        let file_trailer_proto = pb::FileTrailerProto::decode(proto_slice)
            .map_err(|e| Error::new(ErrorKind::ProtoDecodeError(e)))?;

        if let Some(file_info_offset) = file_trailer_proto.file_info_offset {
            hfile_trailer.file_info_offset = file_info_offset;
        }
        if let Some(load_on_open_data_offset) = file_trailer_proto.load_on_open_data_offset {
            hfile_trailer.load_on_open_data_offset = load_on_open_data_offset;
        }
        if let Some(uncompressed_data_index_size) = file_trailer_proto.uncompressed_data_index_size {
            hfile_trailer.uncompressed_data_index_size = uncompressed_data_index_size;
        }
        if let Some(total_uncompressed_bytes) = file_trailer_proto.total_uncompressed_bytes {
            hfile_trailer.total_uncompressed_bytes = total_uncompressed_bytes;
        }
        if let Some(data_index_count) = file_trailer_proto.data_index_count {
            hfile_trailer.data_index_count = data_index_count;
        }
        if let Some(meta_index_count) = file_trailer_proto.meta_index_count {
            hfile_trailer.meta_index_count = meta_index_count;
        }
        if let Some(entry_count) = file_trailer_proto.entry_count {
            hfile_trailer.entry_count = entry_count;
        }
        if let Some(num_data_index_levels) = file_trailer_proto.num_data_index_levels {
            hfile_trailer.num_data_index_levels = num_data_index_levels;
        }
        if let Some(first_data_block_offset) = file_trailer_proto.first_data_block_offset {
            hfile_trailer.first_data_block_offset = first_data_block_offset;
        }
        if let Some(last_data_block_offset) = file_trailer_proto.last_data_block_offset {
            hfile_trailer.last_data_block_offset = last_data_block_offset;
        }
        if let Some(comparator_class_name) = file_trailer_proto.comparator_class_name {
            println!("Got comparator {}", comparator_class_name);
        }

        if let Some(compression_codec) = file_trailer_proto.compression_codec {
            println!("Got compression codec {}", compression_codec);
        }
        if let Some(encryption_key) = file_trailer_proto.encryption_key {
            return Err(Error::new(ErrorKind::UnsupportedFile("Encryption unsupported.")));
        }

        println!("trailer: {:?}", hfile_trailer);

        Ok(hfile_trailer)
    }

    fn use_protobuf(&self) -> bool {
        self.major_version > 2
            || (self.major_version == 2 && self.minor_version >= PROTOBUF_TRAILER_MINOR_VERSION)
    }

    fn check_version(major_version: u8) -> Result<(), Error> {
        if major_version < MIN_FORMAT_VERSION || major_version > MAX_FORMAT_VERSION {
            Err(Error::new(ErrorKind::InvalidMajorVersion(major_version)))
        } else {
            Ok(())
        }
    }

    fn decode_version(encoded: u32) -> (u8, u8) {
        ((encoded & 0x00ffffff) as u8, (encoded >> 24) as u8)
    }
}

#[derive(Debug, Clone, Default)]
pub enum CellComparator {
    #[default]
    KvComparator,
    MetaComparator,
}

#[derive(Debug, Clone, Default)]
pub enum CompressionCodec {
    Lzo,
    Gz,
    #[default]
    None,
    Snappy,
    Lz4,
    Bzip2,
    Zstd,
}
