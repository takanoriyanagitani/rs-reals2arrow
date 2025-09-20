#[cfg(feature = "fs")]
pub mod fs;

use half::f16;

use std::io;

use arrow::array::PrimitiveArray;
use arrow::array::PrimitiveBuilder;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::{Float16Type, Float32Type, Float64Type};

use std::io::Read;

macro_rules! floats2arrow {
    ($fname:ident, $ty:ty) => {
        pub fn $fname<I>(ints: I) -> Result<PrimitiveArray<$ty>, io::Error>
        where
            I: Iterator<Item = Result<<$ty as ArrowPrimitiveType>::Native, io::Error>>,
        {
            let mut bldr = PrimitiveBuilder::new();
            for ri in ints {
                let i = ri?;
                bldr.append_value(i);
            }
            Ok(bldr.finish())
        }
    };
}

floats2arrow!(floats2arrow16, Float16Type);
floats2arrow!(floats2arrow32, Float32Type);
floats2arrow!(floats2arrow64, Float64Type);

pub fn raw2floats_rtfn<R, T, F, const N: usize>(
    mut rdr: R,
    buf2t: F,
) -> impl Iterator<Item = Result<T, io::Error>>
where
    R: Read,
    F: Fn([u8; N]) -> T,
{
    let mut buf: [u8; N] = [0; N];
    std::iter::from_fn(move || {
        let rslt = rdr.read_exact(&mut buf);
        match rslt {
            Ok(_) => Some(Ok(buf2t(buf))),
            Err(e) => match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            },
        }
    })
}

macro_rules! raw2floats {
    ($fname:ident, $ty:ty, $buf2t:expr, $bufsz:literal) => {
        pub fn $fname<R>(rdr: R) -> impl Iterator<Item = Result<$ty, io::Error>>
        where
            R: Read,
        {
            raw2floats_rtfn(rdr, $buf2t)
        }
    };
}

raw2floats!(raw2floats16le, f16, f16::from_le_bytes, 4);
raw2floats!(raw2floats16be, f16, f16::from_be_bytes, 4);

raw2floats!(raw2floats32le, f32, f32::from_le_bytes, 4);
raw2floats!(raw2floats32be, f32, f32::from_be_bytes, 4);
raw2floats!(raw2floats64le, f64, f64::from_le_bytes, 8);
raw2floats!(raw2floats64be, f64, f64::from_be_bytes, 8);

macro_rules! raw2floats2arrow {
    ($fname:ident, $rdr2f:ident, $f2arr:ident, $aty:ty) => {
        pub fn $fname<R>(rdr: R) -> Result<PrimitiveArray<$aty>, io::Error>
        where
            R: Read,
        {
            let f = $rdr2f(rdr);
            $f2arr(f)
        }
    };
}

raw2floats2arrow!(
    raw2floats2arrow16le,
    raw2floats16le,
    floats2arrow16,
    Float16Type
);
raw2floats2arrow!(
    raw2floats2arrow16be,
    raw2floats16be,
    floats2arrow16,
    Float16Type
);

raw2floats2arrow!(
    raw2floats2arrow32le,
    raw2floats32le,
    floats2arrow32,
    Float32Type
);
raw2floats2arrow!(
    raw2floats2arrow32be,
    raw2floats32be,
    floats2arrow32,
    Float32Type
);

raw2floats2arrow!(
    raw2floats2arrow64le,
    raw2floats64le,
    floats2arrow64,
    Float64Type
);
raw2floats2arrow!(
    raw2floats2arrow64be,
    raw2floats64be,
    floats2arrow64,
    Float64Type
);
