# AgateDB

[![Coverage Status](https://codecov.io/gh/tikv/agatedb/branch/master/graph/badge.svg)](https://codecov.io/gh/tikv/agatedb)

AgateDB is an embeddable, persistent and fast key-value (KV) database written
in pure Rust. It is designed as an experimental engine for the [TiKV][1]
project, and will bring aggressive optimizations for TiKV specifically.

## Project Status

AgateDB is still under early heavy development, you can check the development
progress at the [GitHub Project][2].

The whole plan is to port [badger][3] in Rust first and then port the
optimizations that have been made in [unistore][4].

[1]: https://github.com/tikv/tikv
[2]: https://github.com/tikv/agatedb/projects/1
[3]: https://github.com/outcaste-io/badger/tree/45bca18f24ef5cc04701a1e17448ddfce9372da0
[4]: https://github.com/ngaut/unistore

AgateDB is under active development on [develop](https://github.com/tikv/agatedb/tree/develop)
branch. Currently, it can be used as a key-value store with MVCC. It implements most of the
functionalities of badger managed mode.

## Why not X?

X is a great project! The motivation of this project is to ultimately land the
optimizations we have been made to unistore in TiKV. Unistore is based
on badger, so we start with badger.

We are using Rust because it can bring memory safety out of box, which is important
during rapid development. TiKV is also written in Rust, so it will be easier
to integrate with each other like supporting async/await, sharing global
thread pools, etc.
