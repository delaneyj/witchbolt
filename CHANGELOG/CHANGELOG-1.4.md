
<hr>

## v1.4.3(2025-08-19)

### BoltDB
- Fix [potential data corruption in `(*Tx)WriteTo` if underlying db file is overwritten](https://github.com/etcd-io/witchbolt/pull/1058)

<hr>

## v1.4.2(2025-06-27)

### BoltDB
- [Fix the compilation issue on aix, android and solaris due to wrong use of `maxMapSize`](https://github.com/etcd-io/witchbolt/pull/990)
- [Add protection on meta page when it's being written](https://github.com/etcd-io/witchbolt/pull/1005)

<hr>

## v1.4.1(2025-06-10)

### BoltDB
- [Correct the incorrect usage of debug method](https://github.com/etcd-io/witchbolt/pull/905)
- [Add clarification on the option `InitialMmapSize`](https://github.com/etcd-io/witchbolt/pull/943)
- [Fix the crash when writing huge values](https://github.com/etcd-io/witchbolt/pull/978)

<hr>

## v1.4.0(2025-02-05)
There isn't any production code change since v1.4.0-beta.0. Only some dependencies
are bumped, also updated some typos in comment and readme, and removed the legacy
build tag `// +build` in https://github.com/etcd-io/witchbolt/pull/879.

<hr>

## v1.4.0-beta.0(2024-11-04)

### BoltDB
- Reorganized the directory structure of freelist source code
  - [Move array related freelist source code into a separate file](https://github.com/etcd-io/witchbolt/pull/777)
  - [Move method `freePages` into freelist.go](https://github.com/etcd-io/witchbolt/pull/783)
  - [Add an interface for freelist](https://github.com/etcd-io/witchbolt/pull/775)
- [Rollback alloc map when a transaction is rollbacked](https://github.com/etcd-io/witchbolt/pull/819)
- [No handling freelist as a special case when freeing a page](https://github.com/etcd-io/witchbolt/pull/788)
- [Ensure hashmap init method clears the data structures](https://github.com/etcd-io/witchbolt/pull/794)
- [Panicking when a write transaction tries to free a page allocated by itself](https://github.com/etcd-io/witchbolt/pull/792)

### CMD
- [Add `-gobench-output` flag for `witchbolt bench` command](https://github.com/etcd-io/witchbolt/pull/765)

### Other
- [Bump go version to 1.23.x](https://github.com/etcd-io/witchbolt/pull/821)

<hr>

## v1.4.0-alpha.1(2024-05-06)

### BoltDB
- [Enhance check functionality to support checking starting from a pageId](https://github.com/etcd-io/witchbolt/pull/659)
- [Optimize the logger performance for frequent called methods](https://github.com/etcd-io/witchbolt/pull/741)
- [Stabilize the behaviour of Prev when the cursor already points to the first element](https://github.com/etcd-io/witchbolt/pull/734)

### CMD
- [Fix `witchbolt keys` and `witchbolt get` to prevent them from panicking when no parameter provided](https://github.com/etcd-io/witchbolt/pull/682)
- [Fix surgery freelist command in info logs](https://github.com/etcd-io/witchbolt/pull/700)
- [Remove txid references in surgery meta command's comment and description](https://github.com/etcd-io/witchbolt/pull/703)
- [Add rnd read capabilities to witchbolt bench](https://github.com/etcd-io/witchbolt/pull/711)
- [Use `cobra.ExactArgs` to simplify the argument number check](https://github.com/etcd-io/witchbolt/pull/728)
- [Migrate `witchbolt check` command to cobra style](https://github.com/etcd-io/witchbolt/pull/723)
- [Simplify the naming of cobra commands](https://github.com/etcd-io/witchbolt/pull/732)
- [Aggregate adding completed ops for read test of the `witchbolt bench` command](https://github.com/etcd-io/witchbolt/pull/721)
- [Add `--from-page` flag to `witchbolt check` command](https://github.com/etcd-io/witchbolt/pull/737)

### Document
- [Add document for a known issue on the writing a value with a length of 0](https://github.com/etcd-io/witchbolt/pull/730)

### Test
- [Enhance robustness test to cover XFS](https://github.com/etcd-io/witchbolt/pull/707)

### Other
- [Bump go toolchain version to 1.22.2](https://github.com/etcd-io/witchbolt/pull/712)

<hr>

## v1.4.0-alpha.0(2024-01-12)

### BoltDB
- [Improve the performance of hashmapGetFreePageIDs](https://github.com/etcd-io/witchbolt/pull/419)
- [Improve CreateBucketIfNotExists to avoid double searching the same key](https://github.com/etcd-io/witchbolt/pull/532)
- [Support Android platform](https://github.com/etcd-io/witchbolt/pull/571)
- [Record the count of free page to improve the performance of hashmapFreeCount](https://github.com/etcd-io/witchbolt/pull/585)
- [Add logger to witchbolt](https://github.com/etcd-io/witchbolt/issues/509)
- [Support moving bucket inside the same db](https://github.com/etcd-io/witchbolt/pull/635)
- [Support inspecting database structure](https://github.com/etcd-io/witchbolt/pull/674)

### CMD
- [Add `surgery clear-page-elements` command](https://github.com/etcd-io/witchbolt/pull/417)
- [Add `surgery abandon-freelist` command](https://github.com/etcd-io/witchbolt/pull/443)
- [Add `witchbolt version` command](https://github.com/etcd-io/witchbolt/pull/552)
- [Add `witchbolt inspect` command](https://github.com/etcd-io/witchbolt/pull/674)
- [Add `--no-sync` option to `witchbolt compact` command](https://github.com/etcd-io/witchbolt/pull/290)
