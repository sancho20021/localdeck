use super::hash::TrackId;


/// Represent a music track
#[derive(Debug)]
pub struct Track {
    pub id: TrackId,
    pub metadata: TrackMetadata,
}

#[derive(Debug, Default)]
pub struct TrackMetadata {
    pub artist: Option<String>,
    pub title: Option<String>,
}

