#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Protocol error: {0}")]
    Protocol(#[from] protocol::error::OHProtocolError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Channel send error: {0}")]
    ChannelSend(#[from] tokio::sync::mpsc::error::SendError<protocol::packet_owned::PacketOwned>),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}
