use std::io;
use std::io::BufReader;
use std::path::Path;

use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowPrimitiveType;

use arrow::datatypes::Float16Type;
use arrow::datatypes::Float32Type;
use arrow::datatypes::Float64Type;

pub fn filename2bufrdr<P>(filename: P) -> Result<BufReader<std::fs::File>, io::Error>
where
    P: AsRef<Path>,
{
    let f = std::fs::File::open(filename)?;
    Ok(BufReader::new(f))
}

pub fn filename2array<P, F, T>(filename: P, rdr2arr: F) -> Result<PrimitiveArray<T>, io::Error>
where
    P: AsRef<Path>,
    T: ArrowPrimitiveType,
    F: Fn(BufReader<std::fs::File>) -> Result<PrimitiveArray<T>, io::Error>,
{
    let rdr = filename2bufrdr(filename)?;
    rdr2arr(rdr)
}

macro_rules! filename2floats {
    ($fname:ident, $rdr2arr:ident, $aty:ty) => {
        /// Reads the file and converts it to an array of real numbers.
        pub fn $fname<P>(filename: P) -> Result<PrimitiveArray<$aty>, io::Error>
        where
            P: AsRef<Path>,
        {
            filename2array(filename, $rdr2arr)
        }
    };
}

use super::raw2floats2arrow16be;
use super::raw2floats2arrow16le;
use super::raw2floats2arrow32be;
use super::raw2floats2arrow32le;
use super::raw2floats2arrow64be;
use super::raw2floats2arrow64le;

filename2floats!(filename2floats16le, raw2floats2arrow16le, Float16Type);
filename2floats!(filename2floats16be, raw2floats2arrow16be, Float16Type);

filename2floats!(filename2floats32le, raw2floats2arrow32le, Float32Type);
filename2floats!(filename2floats32be, raw2floats2arrow32be, Float32Type);

filename2floats!(filename2floats64le, raw2floats2arrow64le, Float64Type);
filename2floats!(filename2floats64be, raw2floats2arrow64be, Float64Type);
