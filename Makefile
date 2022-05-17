
SANITIZER_FLAGS=-Zsanitizer=address

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi

pre-format: unset-override
	@rustup component add rustfmt
	@cargo install -q cargo-sort 

format: pre-format
	@cargo fmt
	@cargo sort -w ./Cargo.toml ./*/Cargo.toml > /dev/null 

clippy:
	cargo clippy --all-targets --all-features

test:
	cargo test --all-features --workspace

test_sanitizer:
	RUSTFLAGS="$(SANITIZER_FLAGS)" cargo test --all-features --workspace

bench:
	cargo bench --all-features --workspace

bench_sanitizer:
	RUSTFLAGS="$(SANITIZER_FLAGS)" cargo bench --all-features --workspace

ci: fmt clippy test

clean:
	cargo clean

.PHONY: run clean fmt clippy test
