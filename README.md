# localdeck
Tactile crate digging for mp3s


## API documentation

**Most important route: /play**.

URL of this form is planned to be used on QR codes and NFC chips so it must never change:

```
http://main-deck:8080/play?h=<track_id>&y=<part of the YouTube link>
```
where &y=... is optional,

# Ideas for extension

1) automation of qr code printing:
   - automatic layout of multiple cards on A3 pdf
   - automatic generation of individual cards based on title, artist, url, graphics

2) Add metadata to the database
   - command to enrich information about the track

3) Add synchronization with Yandex Music so newly added to yandex music tracks are also downloaded to the server

4) add integration with discog for metadata retrieval https://www.discogs.com/developers?srsltid=AfmBOorolLjd1NRBhUYYU5b9XTzx5OHhwZ4k0VqPHiFez7RaOiV9MOO0


