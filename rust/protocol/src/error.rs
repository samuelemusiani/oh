use zerocopy::error::{AlignmentError, CastError, ConvertError, SizeError, ValidityError};

#[derive(Debug, thiserror::Error)]
pub enum OHProtocolError {
    #[error("Buffer too short or wrong size for type")]
    Size,
    #[error("Buffer not properly aligned")]
    Alignment,
    #[error("Invalid data for type")]
    Validity,
    #[error("Empty packet data")]
    EmptyPayload,
    #[error("Invalid version: {0}")]
    InvalidVersion(u8),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid input string for base16 decoding")]
    Base16Decode(#[from] base16::DecodeError),
    #[error("Invalid size, expected {expected:?}, got {actual:?}")]
    InvalidSize { expected: usize, actual: usize },
}

impl<AlignmentError, SizeError, ValidityError>
    From<ConvertError<AlignmentError, SizeError, ValidityError>> for OHProtocolError
{
    fn from(value: ConvertError<AlignmentError, SizeError, ValidityError>) -> Self {
        match value {
            ConvertError::Alignment(_) => OHProtocolError::Alignment,
            ConvertError::Size(_) => OHProtocolError::Size,
            ConvertError::Validity(_) => OHProtocolError::Validity,
        }
    }
}
