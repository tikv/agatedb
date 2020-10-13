
SANITIZER_FLAGS=-Zsanitizer=address

fmt:
	cargo fmt

clippy:
	cargo clippy --all-targets --all-features

test:
	cargo test --all-features --workspace

test_sanitizer:
	RUSTFLAGS="$(SANITIZER_FLAGS)" cargo test --all-features --workspace

bench:
	cargo bench --all-features --workspace

ci: fmt clippy test

clean:
	cargo clean

.PHONY: run clean fmt clippy test
