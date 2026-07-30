//! Bounded binary IPC framing between Electron and the trusted Rust sidecar.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: [u8; 4] = *b"VHIP";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 24;
/// Maximum MessagePack control metadata accepted by the sidecar.
pub const MAX_METADATA_BYTES: usize = 256 * 1024;
/// Maximum payload in one local IPC frame. Streams use multiple frames.
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// IPC frame semantic type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// Authenticate the newly connected trusted Electron process.
    Hello = 1,
    /// One request/control message.
    Request = 2,
    /// One response/control message.
    Response = 3,
    /// Incremental request or response bytes.
    StreamData = 4,
    /// End of an incremental stream.
    StreamEnd = 5,
    /// Cancel an active operation.
    Cancel = 6,
    /// Asynchronous sidecar event.
    Event = 7,
}

impl TryFrom<u8> for FrameKind {
    type Error = IpcError;

    fn try_from(value: u8) -> Result<Self, IpcError> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::Request),
            3 => Ok(Self::Response),
            4 => Ok(Self::StreamData),
            5 => Ok(Self::StreamEnd),
            6 => Ok(Self::Cancel),
            7 => Ok(Self::Event),
            other => Err(IpcError::UnknownKind(other)),
        }
    }
}

/// One multiplexed local IPC frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcFrame {
    /// Frame semantic type.
    pub kind: FrameKind,
    /// Bit flags reserved for compatible extensions.
    pub flags: u16,
    /// Caller-generated correlation identifier.
    pub request_id: u64,
    /// MessagePack-encoded control metadata.
    pub metadata: Bytes,
    /// Raw binary payload bytes.
    pub payload: Bytes,
}

impl IpcFrame {
    /// Construct an IPC frame from serializable MessagePack metadata and raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata serialization fails or either field exceeds its bound.
    pub fn from_metadata<T: Serialize>(
        kind: FrameKind,
        request_id: u64,
        metadata: &T,
        payload: Bytes,
    ) -> Result<Self, IpcError> {
        let encoded = rmp_serde::to_vec_named(metadata)
            .map_err(|error| IpcError::MetadataEncode(error.to_string()))?;
        let frame = Self {
            kind,
            flags: 0,
            request_id,
            metadata: Bytes::from(encoded),
            payload,
        };
        frame.validate()?;
        Ok(frame)
    }

    /// Decode the MessagePack metadata into a concrete type.
    ///
    /// # Errors
    ///
    /// Returns an error when the metadata is not valid for `T`.
    pub fn decode_metadata<T: DeserializeOwned>(&self) -> Result<T, IpcError> {
        rmp_serde::from_slice(&self.metadata)
            .map_err(|error| IpcError::MetadataDecode(error.to_string()))
    }

    /// Encode a complete frame.
    ///
    /// # Errors
    ///
    /// Returns an error when a length cannot be represented or a configured bound is exceeded.
    pub fn encode(&self) -> Result<Bytes, IpcError> {
        self.validate()?;
        let metadata_len = u32::try_from(self.metadata.len())
            .map_err(|_| IpcError::MetadataTooLarge(self.metadata.len()))?;
        let payload_len = u32::try_from(self.payload.len())
            .map_err(|_| IpcError::PayloadTooLarge(self.payload.len()))?;
        let total = HEADER_LEN + self.metadata.len() + self.payload.len();
        let mut output = BytesMut::with_capacity(total);
        output.put_slice(&MAGIC);
        output.put_u8(VERSION);
        output.put_u8(self.kind as u8);
        output.put_u16(self.flags);
        output.put_u64(self.request_id);
        output.put_u32(metadata_len);
        output.put_u32(payload_len);
        output.put_slice(&self.metadata);
        output.put_slice(&self.payload);
        Ok(output.freeze())
    }

    fn validate(&self) -> Result<(), IpcError> {
        if self.metadata.len() > MAX_METADATA_BYTES {
            return Err(IpcError::MetadataTooLarge(self.metadata.len()));
        }
        if self.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(IpcError::PayloadTooLarge(self.payload.len()));
        }
        Ok(())
    }
}

/// Read one bounded IPC frame from an async byte stream.
///
/// # Errors
///
/// Returns an error for I/O failure, malformed headers, unsupported versions, or oversized fields.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<IpcFrame, IpcError> {
    let mut header = [0_u8; HEADER_LEN];
    reader.read_exact(&mut header).await.map_err(IpcError::Io)?;
    let mut cursor = &header[..];
    let mut magic = [0_u8; 4];
    cursor.copy_to_slice(&mut magic);
    if magic != MAGIC {
        return Err(IpcError::InvalidMagic);
    }
    let version = cursor.get_u8();
    if version != VERSION {
        return Err(IpcError::UnsupportedVersion(version));
    }
    let kind = FrameKind::try_from(cursor.get_u8())?;
    let flags = cursor.get_u16();
    let request_id = cursor.get_u64();
    let metadata_len = cursor.get_u32() as usize;
    let payload_len = cursor.get_u32() as usize;
    if metadata_len > MAX_METADATA_BYTES {
        return Err(IpcError::MetadataTooLarge(metadata_len));
    }
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(IpcError::PayloadTooLarge(payload_len));
    }
    let mut metadata = vec![0_u8; metadata_len];
    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut metadata).await.map_err(IpcError::Io)?;
    reader.read_exact(&mut payload).await.map_err(IpcError::Io)?;
    Ok(IpcFrame {
        kind,
        flags,
        request_id,
        metadata: Bytes::from(metadata),
        payload: Bytes::from(payload),
    })
}

/// Write one complete bounded IPC frame to an async byte stream.
///
/// # Errors
///
/// Returns an error when encoding or writing fails.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &IpcFrame,
) -> Result<(), IpcError> {
    writer.write_all(&frame.encode()?).await.map_err(IpcError::Io)?;
    writer.flush().await.map_err(IpcError::Io)
}

/// Initial authentication metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// Random secret supplied only to the trusted parent and child processes.
    pub secret: String,
}

/// IPC framing failures.
#[derive(Debug, Error)]
pub enum IpcError {
    /// Underlying local stream I/O failed.
    #[error("IPC I/O failed: {0}")]
    Io(#[source] std::io::Error),
    /// Frame magic does not identify VeilidHttp IPC.
    #[error("invalid VeilidHttp IPC magic")]
    InvalidMagic,
    /// Protocol version is not supported.
    #[error("unsupported VeilidHttp IPC version {0}")]
    UnsupportedVersion(u8),
    /// Frame kind is not recognized.
    #[error("unknown VeilidHttp IPC frame kind {0}")]
    UnknownKind(u8),
    /// MessagePack metadata is too large.
    #[error("IPC metadata length {0} exceeds its bound")]
    MetadataTooLarge(usize),
    /// Raw frame payload is too large.
    #[error("IPC payload length {0} exceeds its bound")]
    PayloadTooLarge(usize),
    /// Metadata serialization failed.
    #[error("IPC metadata encoding failed: {0}")]
    MetadataEncode(String),
    /// Metadata decoding failed.
    #[error("IPC metadata decoding failed: {0}")]
    MetadataDecode(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_round_trip_over_duplex_stream() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let expected = IpcFrame::from_metadata(
            FrameKind::Request,
            42,
            &Hello { secret: "secret".to_owned() },
            Bytes::from_static(b"body"),
        )
        .unwrap();
        let sent = expected.clone();
        let writer = tokio::spawn(async move { write_frame(&mut client, &sent).await.unwrap() });
        let received = read_frame(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(received, expected);
        assert_eq!(received.decode_metadata::<Hello>().unwrap().secret, "secret");
    }
}
