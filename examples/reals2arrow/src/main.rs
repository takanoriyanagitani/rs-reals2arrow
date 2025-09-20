use std::io;
use std::process::ExitCode;

use arrow::array::Float32Array;
use rs_reals2arrow::arrow;
use rs_reals2arrow::sync::fs::filename2floats32le;

fn env2filename() -> Result<String, io::Error> {
    std::env::var("ENV_INPUT_RAW_REALS_32_LE").map_err(io::Error::other)
}

fn sub() -> Result<(), io::Error> {
    let file_name: String = env2filename()?;

    let array: Float32Array = filename2floats32le(file_name)?;

    println!("{array:#?}");

    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
