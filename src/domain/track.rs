use super::hash::TrackId;

/// Represent a music track
#[derive(Debug)]
pub struct Track {
    pub id: TrackId,
    pub metadata: TrackMetadata,
}

#[derive(Debug, Clone)]
pub struct TrackMetadata {
    pub artist: String,
    pub title: String,
    pub year: Option<u32>,
    pub label: Option<String>,
    pub artwork: Option<ArtworkRef>,
}

#[derive(Debug, Clone)]
pub struct ArtworkRef(pub String);
