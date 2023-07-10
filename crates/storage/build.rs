extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["src/HFile.proto", "src/HBase.proto"], &["src/"]).unwrap();
}
