#![allow(dead_code)]

pub mod repl {
    pub use beads_daemon::admission::{AdmissionController, AdmissionPermit};
    pub use beads_rs::daemon::repl::session::{
        Inbound, InboundConnecting, Outbound, OutboundConnecting, SessionState,
        handle_inbound_message, handle_outbound_message,
    };
    pub use beads_rs::daemon::repl::{
        ContiguousBatch, Events, IngestOutcome, ReplError, SessionAction, SessionConfig,
        SessionPhase, SessionStore, ValidatedAck, Want, WatermarkSnapshot,
    };
    pub use beads_rs::daemon::wal::{ReplicaDurabilityRole, WalIndexError};
}

pub mod wal {
    pub use beads_rs::daemon::wal::frame::encode_frame;
    pub use beads_rs::daemon::wal::{
        EventWalError, EventWalResult, FRAME_CRC_OFFSET, FRAME_HEADER_LEN, IndexDurabilityMode,
        RecordHeader, RequestProof, SEGMENT_HEADER_PREFIX_LEN, SegmentConfig, SegmentHeader,
        SegmentWriter, SqliteWalIndex, VerifiedRecord, WAL_FORMAT_VERSION, WalIndexError,
    };
}
