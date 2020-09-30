run:
	cargo run

run_release:
	cargo run --release

fmt:
	cargo fmt

clippy:
	cargo clippy --all-targets --all-features

test:
	cargo test --all-features --workspace

bench:
	cargo bench --all-features --workspace

ci: fmt clippy test run_release

proto: proto/proto/meta.proto proto/proto/rustproto.proto
	protoc proto/proto/meta.proto -Iproto/proto --rust_out=proto/src/

clean:
	cargo clean

.PHONY: run clean fmt clippy test
