
SANITIZER_FLAGS=-Zsanitizer=address

pre-format:
    cargo --version
	cargo install --help
	@rustup component add rustfmt
	@cargo install -q cargo-sort 

format: pre-format
	@cargo fmt
	@cargo sort -w ./Cargo.toml ./*/Cargo.toml > /dev/null 

clippy:
	cargo clippy --all-targets --all-features --workspace -- -D "warnings"

test:
	cargo test --all-features --workspace

test_sanitizer:
	RUSTFLAGS="$(SANITIZER_FLAGS)" cargo test --all-features --workspace

bench:
	cargo bench --all-features --workspace

bench_sanitizer:
	RUSTFLAGS="$(SANITIZER_FLAGS)" cargo bench --all-features --workspace

dev: format clippy test

clean:
	cargo clean

.PHONY: run clean format clippy test dev
