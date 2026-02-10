use crate::{config::PublicEndpoint, domain::hash::TrackId};

/// returns url to be printed on QRs and NFCs
pub fn get_play_url(conf: &PublicEndpoint, track: TrackId, yt_part: Option<String>) -> String {
    let url = &conf.base_url.trim_end_matches('/');
    if let Some(yt) = yt_part {
        format!("{url}/play?h={track}&y={yt}")
    } else {
        format!("{url}/play?h={track}")
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::PublicEndpoint, domain::hash::TrackId, public_endpoint::get_play_url};

    fn endpoint() -> PublicEndpoint {
        PublicEndpoint {
            base_url: "http://main-deck:8080".to_string(),
        }
    }

    fn mock_trackid(x: i32) -> TrackId {
        let bytes = x.to_be_bytes();
        TrackId::from_bytes(&bytes)
    }

    fn mock_trackid_str(x: i32) -> String {
        mock_trackid(x).to_hex()
    }

    #[test]
    fn test_play_url_without_yt() {
        let conf = endpoint();
        let track = mock_trackid(123);

        let url = get_play_url(&conf, track, None);

        assert_eq!(
            url,
            format!("http://main-deck:8080/play?h={}", mock_trackid_str(123))
        );
    }

    #[test]
    fn test_play_url_with_yt() {
        let conf = endpoint();
        let track = mock_trackid(124);

        let url = get_play_url(&conf, track, Some("yt42".to_string()));

        assert_eq!(
            url,
            format!(
                "http://main-deck:8080/play?h={}&y=yt42",
                mock_trackid_str(124)
            )
        );
    }

    #[test]
    fn test_play_url_with_empty_yt() {
        let conf = endpoint();
        let track = mock_trackid(124);

        let url = get_play_url(&conf, track, Some(String::new()));

        assert_eq!(
            url,
            format!("http://main-deck:8080/play?h={}&y=", mock_trackid_str(124))
        );
    }

    #[test]
    fn test_play_url_trailing_slash() {
        let conf = PublicEndpoint {
            base_url: "http://main-deck:8080/".to_string(),
        };

        let track = mock_trackid(124);

        let url = get_play_url(&conf, track, None);

        assert_eq!(
            url,
            format!("http://main-deck:8080/play?h={}", mock_trackid_str(124))
        );
    }
}
