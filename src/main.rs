extern crate clap;
extern crate fuse;
extern crate libc;
extern crate postgres;
extern crate time;

mod fs;
mod sql;

use clap::{App, Arg};
use fs::CockroachFS;
use fuse::mount;
use postgres::{Connection, TlsMode};
use std::io;
use std::path::Path;

fn main() -> io::Result<()> {
    let matches = App::new("CockroachFS")
        .version("0.1.0")
        .about("Filesystem backed by CockroachDB")
        .arg(
            Arg::with_name("mountpoint")
                .short("m")
                .long("mountpoint")
                .takes_value(true)
                .help("The location to mount the filesystem"),
        )
        .get_matches();

    let conn = Connection::connect("postgres://root@localhost:26257/cockroachfs", TlsMode::None)?;

    let path_str = matches.value_of("mountpoint").unwrap_or("./mountpoint");
    let path = Path::new(path_str);

    let crfs = CockroachFS::new(conn);
    return mount(crfs, &path, &[]);
}
