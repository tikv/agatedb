fn main() {
    prost_build::compile_protos(&["src/proto/meta.proto"], &["src/proto"]).unwrap();
}
