fn main() {
    let proto_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto");
    let proto_file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../proto/rithmic_gateway/v1/session.proto"
    );
    println!("cargo:rerun-if-changed={proto_file}");
    prost_build::compile_protos(&[proto_file], &[proto_dir])
        .expect("compile rithmic_gateway proto");
}
