# localdeck
Tactile crate digging for mp3s


## API documentation

**Most important route: /play**.

URL of this form is planned to be used on QR codes and NFC chips so it must never change:

```
http://main-deck:8080/play?h=<track_id>&y=<part of the YouTube link>
```
where &y=... is optional,
