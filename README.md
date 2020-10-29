# AgateDB

AgateDB is an embeddable, persistent and fast key-value (KV) database written
in pure Rust. It is designed as an experimental engine for The [TiKV][1]
project that will bring aggressive optimizations for TiKV specifically.

## Project Status

AgateDB is still under early heavily development, you can check the development
progress at the [Project][2].

The whole plan is to port [badger][3] in Rust first and then port the
optimizations that has been used in [unistore][4].

1: https://github.com/tikv/tikv
2: https://github.com/tikv/tikv/projects/1
3: https://github.com/dgraph-io/badger
4: https://github.com/ngaut/unistore

## Why not X?

X is a great project! The motivation of this project is to land the
optimizations we have been made to unistore in TiKV finally. Unistore is based
on badger, so we start with badger.

Using Rust because it can bring memory safety out of box, which is important
during rapid development. TiKV is also written in Rust, so it will be easier
to integrate with each other like supporting async/await, sharing global
thread pools, etc.
