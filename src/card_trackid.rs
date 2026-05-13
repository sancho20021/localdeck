use url::Url;

use crate::domain::hash::TrackId;

/// Extracts track id from QR/card text.
///
/// Accepts:
/// - raw hash:
///     abc123
///
/// - full URL:
///     https://example.com/play?h=abc123
pub fn extract_trackid(text: &str) -> Result<TrackId, String> {
    let text = text.trim();

    // -----------------------------------------
    // Full URL:
    // https://example.com/play?h=abc123
    // -----------------------------------------
    if let Ok(url) = Url::parse(text) {
        if let Some(hash) = url
            .query_pairs()
            .find(|(k, _)| k == "h")
            .map(|(_, v)| v.to_string())
        {
            if let Ok(hash) = TrackId::from_hex(hash) {
                return Ok(hash);
            }
        }
    }

    // -----------------------------------------
    // raw hash:
    // abc123
    // -----------------------------------------
    TrackId::from_hex(text)
}
