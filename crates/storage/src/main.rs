use febase_storage::error::Error;
use febase_storage::trailer;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Error> {
    febase_storage::trailer::HFileTrailer::from_fname(
        "/home/axiak/Documents/projects/febase/crates/storage/test_1.hfile",
    )
    .await?;
    println!("Hello, world: {}", trailer::MAX_TRAILER_SIZE);
    Ok(())
}
