//! SigmaOS component binary: bigdata

use sigmaos_bigdata::Component;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Component::new().operate()?;
    println!("sigmaos-bigdata: ok");
    Ok(())
}
