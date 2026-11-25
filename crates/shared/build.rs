fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/control_plane.proto");

    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.btree_map([".sys_analyzer.control.v1.RegisterRequest.metadata"]);
    tonic_prost_build::configure().compile_with_config(
        config,
        &["proto/control_plane.proto"],
        &["proto"],
    )?;
    Ok(())
}
