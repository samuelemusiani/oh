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
