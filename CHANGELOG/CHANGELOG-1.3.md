Note that we start to track changes starting from v1.3.7.

<hr>

## v1.3.12(2025-08-19)

### BoltDB
- [Add protection on meta page when it's being written](https://github.com/etcd-io/witchbolt/pull/1006)
- Fix [potential data corruption in `(*Tx)WriteTo` if underlying db file is overwritten](https://github.com/etcd-io/witchbolt/pull/1059)

<hr>

## v1.3.11(2024-08-21)

### BoltDB
- Fix [the `freelist.allocs` isn't rollbacked when a tx is rollbacked](https://github.com/etcd-io/witchbolt/pull/823).

### CMD
- Add [`-gobench-output` option for bench command to adapt to benchstat](https://github.com/etcd-io/witchbolt/pull/802).

### Other
- [Bump go version to 1.22.x](https://github.com/etcd-io/witchbolt/pull/822).
- This patch also added `dmflakey` package, which can be reused by other projects. See https://github.com/etcd-io/witchbolt/pull/812.

<hr>

## v1.3.10(2024-05-06)

### BoltDB
- [Remove deprecated `UnsafeSlice` and use `unsafe.Slice`](https://github.com/etcd-io/witchbolt/pull/717)
- [Stabilize the behaviour of Prev when the cursor already points to the first element](https://github.com/etcd-io/witchbolt/pull/744)

### Other
- [Bump go version to 1.21.9](https://github.com/etcd-io/witchbolt/pull/713)

<hr>

## v1.3.9(2024-02-24)

### BoltDB
- [Clone the key before operating data in bucket against the key](https://github.com/etcd-io/witchbolt/pull/639)

### CMD
- [Fix `witchbolt keys` and `witchbolt get` to prevent them from panicking when no parameter provided](https://github.com/etcd-io/witchbolt/pull/683)

<hr>

## v1.3.8(2023-10-26)

### BoltDB
- Fix [db.close() doesn't unlock the db file if db.munnmap() fails](https://github.com/etcd-io/witchbolt/pull/439).
- [Avoid syscall.Syscall use on OpenBSD](https://github.com/etcd-io/witchbolt/pull/406).
- Fix [rollback panicking after mlock failed or both meta pages corrupted](https://github.com/etcd-io/witchbolt/pull/444).
- Fix [witchbolt panicking due to 64bit unaligned on arm32](https://github.com/etcd-io/witchbolt/pull/584).

### CMD
- [Update the usage of surgery command](https://github.com/etcd-io/witchbolt/pull/411).

<hr>

## v1.3.7(2023-01-31)

### BoltDB
- Add [recursive checker to confirm database consistency](https://github.com/etcd-io/witchbolt/pull/225).
- Add [support to get the page size from the second meta page if the first one is invalid](https://github.com/etcd-io/witchbolt/pull/294).
- Add [support for loong64 arch](https://github.com/etcd-io/witchbolt/pull/303).
- Add [internal iterator to Bucket that goes over buckets](https://github.com/etcd-io/witchbolt/pull/356).
- Add [validation on page read and write](https://github.com/etcd-io/witchbolt/pull/358).
- Add [PreLoadFreelist option to support loading free pages in readonly mode](https://github.com/etcd-io/witchbolt/pull/381).
- Add [(*Tx) CheckWithOption to support generating human-readable diagnostic messages](https://github.com/etcd-io/witchbolt/pull/395).
- Fix [Use `golang.org/x/sys/windows` for `FileLockEx`/`UnlockFileEx`](https://github.com/etcd-io/witchbolt/pull/283).
- Fix [readonly file mapping on windows](https://github.com/etcd-io/witchbolt/pull/307).
- Fix [the "Last" method might return no data due to not skipping the empty pages](https://github.com/etcd-io/witchbolt/pull/341).
- Fix [panic on db.meta when rollback](https://github.com/etcd-io/witchbolt/pull/362).

### CMD
- Add [support for get keys in sub buckets in `witchbolt get` command](https://github.com/etcd-io/witchbolt/pull/295).
- Add [support for `--format` flag for `witchbolt keys` command](https://github.com/etcd-io/witchbolt/pull/306).
- Add [safeguards to witchbolt CLI commands](https://github.com/etcd-io/witchbolt/pull/354).
- Add [`witchbolt page` supports --all and --value-format=redacted formats](https://github.com/etcd-io/witchbolt/pull/359).
- Add [`witchbolt surgery` commands](https://github.com/etcd-io/witchbolt/issues/370).
- Fix [open db file readonly mode for commands which shouldn't update the db file](https://github.com/etcd-io/witchbolt/pull/365), see also [pull/292](https://github.com/etcd-io/witchbolt/pull/292).

### Other
- [Build witchbolt CLI tool, test and format the source code using golang 1.17.13](https://github.com/etcd-io/witchbolt/pull/297).
- [Bump golang.org/x/sys to v0.4.0](https://github.com/etcd-io/witchbolt/pull/397).

### Summary
Release v1.3.7 contains following critical fixes:
- fix to problem that `Last` method might return incorrect value ([#341](https://github.com/etcd-io/witchbolt/pull/341))
- fix of potential panic when performing transaction's rollback ([#362](https://github.com/etcd-io/witchbolt/pull/362))

Other changes focused on defense-in-depth ([#358](https://github.com/etcd-io/witchbolt/pull/358), [#294](https://github.com/etcd-io/witchbolt/pull/294), [#225](https://github.com/etcd-io/witchbolt/pull/225), [#395](https://github.com/etcd-io/witchbolt/pull/395))

`witchbolt` command line tool was expanded to:
- allow fixing simple corruptions by `witchbolt surgery` ([#370](https://github.com/etcd-io/witchbolt/pull/370))
- be flexible about output formatting ([#306](https://github.com/etcd-io/witchbolt/pull/306), [#359](https://github.com/etcd-io/witchbolt/pull/359))
- allow accessing data in subbuckets ([#295](https://github.com/etcd-io/witchbolt/pull/295))
