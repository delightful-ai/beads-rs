#![allow(dead_code)]

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use beads_rs::core::NamespaceId;
use beads_rs::daemon::wal::{EventWalError, EventWalResult, RecordHeader};

use super::wal::{SegmentFixture, TempWalDir, valid_segment};

const FRAME_CRC_OFFSET: u64 = 8;
const FRAME_HEADER_LEN: u64 = 12;

pub fn bad_crc_segment(
    temp: &TempWalDir,
    namespace: &NamespaceId,
    now_ms: u64,
) -> EventWalResult<SegmentFixture> {
    let segment = valid_segment(temp, namespace, now_ms)?;
    corrupt_frame_body(&segment, 0).map_err(|source| io_err(&segment.path, source))?;
    Ok(segment)
}

pub fn truncated_segment(
    temp: &TempWalDir,
    namespace: &NamespaceId,
    now_ms: u64,
) -> EventWalResult<SegmentFixture> {
    let segment = valid_segment(temp, namespace, now_ms)?;
    truncate_frame_mid_body(&segment, 0).map_err(|source| io_err(&segment.path, source))?;
    Ok(segment)
}

pub fn corrupt_frame_crc(segment: &SegmentFixture, frame_index: usize) -> std::io::Result<()> {
    let offset = segment.frame_offset(frame_index);
    flip_byte_at(&segment.path, offset + FRAME_CRC_OFFSET)
}

pub fn corrupt_frame_body(segment: &SegmentFixture, frame_index: usize) -> std::io::Result<()> {
    let offset = segment.frame_body_offset(frame_index);
    flip_byte_at(&segment.path, offset)
}

pub fn corrupt_record_header_event_time(
    segment: &SegmentFixture,
    frame_index: usize,
) -> EventWalResult<()> {
    let offset = segment.frame_body_offset(frame_index);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&segment.path)
        .map_err(|source| io_err(&segment.path, source))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| io_err(&segment.path, source))?;
    let mut header_prefix = [0u8; 4];
    file.read_exact(&mut header_prefix)
        .map_err(|source| io_err(&segment.path, source))?;
    let header_len = u16::from_le_bytes([header_prefix[2], header_prefix[3]]) as usize;
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| io_err(&segment.path, source))?;
    let mut header_bytes = vec![0u8; header_len];
    file.read_exact(&mut header_bytes)
        .map_err(|source| io_err(&segment.path, source))?;
    let (mut header, _) = RecordHeader::decode(&header_bytes)?;
    header.event_time_ms = header.event_time_ms.saturating_add(1);
    let encoded = header.encode()?;
    if encoded.len() != header_len {
        return Err(EventWalError::RecordHeaderInvalid {
            reason: "record header length changed during corruption".to_string(),
        });
    }
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| io_err(&segment.path, source))?;
    file.write_all(&encoded)
        .map_err(|source| io_err(&segment.path, source))?;
    Ok(())
}

pub fn truncate_frame_mid_body(
    segment: &SegmentFixture,
    frame_index: usize,
) -> std::io::Result<()> {
    let offset = segment.frame_offset(frame_index);
    let frame_len = segment.frame_len(frame_index) as u64;
    let truncate_at = offset.saturating_add(frame_len.saturating_sub(1));
    truncate_file(&segment.path, truncate_at)
}

pub fn truncate_file(path: &Path, len: u64) -> std::io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    Ok(())
}

pub fn append_partial_frame(path: &Path, frame: &[u8], cut_at: usize) -> std::io::Result<usize> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    let end = cut_at.min(frame.len());
    file.write_all(&frame[..end])?;
    Ok(end)
}

fn flip_byte_at(path: &Path, offset: u64) -> std::io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&byte)?;
    Ok(())
}

fn io_err(path: &Path, source: std::io::Error) -> EventWalError {
    EventWalError::Io {
        path: Some(path.to_path_buf()),
        source,
    }
}
