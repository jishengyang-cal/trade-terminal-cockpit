fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new().compile_protos(
        &[
            "../contracts/proto/trading/common.proto",
            "../contracts/proto/trading/events.proto",
            "../contracts/proto/trading/commands.proto",
            "../contracts/proto/trading/projections.proto",
        ],
        &["../contracts/proto"],
    )?;
    Ok(())
}
