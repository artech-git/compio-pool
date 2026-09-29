//! Creates the dataset the three arms share, and nothing else.
//!
//! Separate from the arms so the files are written once, by one process, with
//! one allocation pattern. If each arm created its own, the third would be
//! reading blocks laid out by a filesystem that had already been churned by the
//! first two, and the comparison would be measuring allocation history.
//!
//! Run with: `FSB_DIR=/var/tmp/fsbench cargo run --release --bin fs_setup`

use std::io;

use fs_bench::Dataset;

fn main() -> io::Result<()> {
    let ds = Dataset::from_env();
    let read_total = ds.read_files as u64 * ds.read_file_size;
    let write_total = ds.write_files as u64 * ds.write_region;
    eprintln!(
        "dataset at {}: {} read files x {} MiB = {} MiB, {} write files x {} MiB = {} MiB",
        ds.dir.display(),
        ds.read_files,
        ds.read_file_size >> 20,
        read_total >> 20,
        ds.write_files,
        ds.write_region >> 20,
        write_total >> 20,
    );
    ds.ensure()?;
    eprintln!("ready");
    Ok(())
}
