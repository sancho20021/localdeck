# localdeck

## Comments

The default is **no comment**. A comment earns its place only by carrying
something a competent reader cannot get from the code in front of them.

Before keeping any comment, apply these tests. Each one is a hard fail.

- **Deletion test.** Delete it and reread the code. If nothing is lost, it stays
  deleted.
- **Novelty test.** Name the fact it carries that is not in the file: a hardware
  quirk, a spec or upstream bug, a measured number, a failure mode, an
  invariant another thread depends on. No such fact, no comment.
- **No comparatives.** `rather than`, `instead of`, `not X but Y`, `used to`,
  `now`, `previously`. These narrate a diff or restate the line below. Git holds
  the history; write in the timeless present, as if the code had always read
  this way.
- **No labels.** Do not summarize or announce a block. `// load the record` over
  code that loads a record is noise.
- **No restating.** If the comment is just the code beneath it written out in
  English, drop it.
- **No defending the choice.** Do not explain why some other approach was not
  taken, or what the code deliberately avoids doing. That belongs in the commit
  message. Document what the code guarantees and where that guarantee stops.
  `would` is the tell: a sentence about what *would* happen is about code that
  is not there. Delete it.
- **Plain words.** Say what happens, concretely. No jargon, no clever phrasing,
  no compressed noun stacks. If a sentence needs to be reread to parse, rewrite
  it. Domain terms the code itself uses are fine; literary ones are not.

What survives: intent behind a non-obvious choice, edge cases, performance
trade-offs, cross-thread or cross-module dependencies, and constraints imposed
from outside the file. Keep them short — fragments, not paragraphs. Prefer
linking a reference (spec, Wikipedia, issue) over deriving theory inline. Use
`///` and `//!`, matching the density of the surrounding module.

## Doc comments on functions

A `///` block is the bare minimum a caller needs: what the function does, what
it guarantees, and where that stops. One or two sentences is the norm.

**Errors.** Document only the errors this function's own body constructs. An
error that arrives through `?` from a callee belongs to that callee's contract
and is not listed here. A list that copies a callee's failure modes goes stale
the moment the callee changes, and nothing flags it. A caller that needs the
full set reads the callee. If it matters that the function stops at the first
failing item, say so in one line; do not enumerate what can fail.

```rust
// BAD - five of these six are check_deletable's and resolve_path's errors,
// copied by hand
/// Fails as soon as any file cannot be deleted:
/// - the track id does not exist: [`StorageError::TrackNotFound`]
/// - a usb drive is not mounted: [`StorageError::Internal`]
/// - the file does not exist: [`StorageError::FileMissing`]
/// - the path is a symlink or a directory: [`StorageError::NotARegularFile`]
/// - the size differs from the recorded one: [`StorageError::FileChangedSinceSync`]
/// - the file lies outside every configured root: [`StorageError::PathOutsideLibrary`]

// GOOD - only what this body raises, plus the one behaviour a caller cannot
// see from the signature
/// [`StorageError::TrackNotFound`] when the track id does not exist.
/// Stops at the first file that cannot be deleted.
```

`Cargo.toml` takes no comments at all. Change the dependency or feature line and
nothing else.

Measurements, before/after numbers, and test names belong in the chat reply or
the commit message, never in a comment.
