# CockroachFUSE

CockroachFUSE is a [FUSE
filesystem](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) backed by
[CockroachDB](https://github.com/cockroachdb/cockroach).

The project is intended to provide the highest quality development environment
possible for developing CockroachDB. Why develop CockroachDB on a normal
filesystem when you can develop CockroachDB on a filesystem that stores
everything in CockroachDB? This ensures that your source files remain consistent
and highly-available at all times, so that you as a developer never need to stop
improving the database. In the future we expect that all CockroachDB binaries
will be compiled on CockroachFUSE, promising a level of polish and reliability
unheard of in modern day executables.

## Details

A working FUSE filesystem consists of three parts:

1. The kernel driver that registers as a filesystem and forwards operations into a communication channel to a userspace process that handles them.
1. The userspace library (libfuse) that helps the userspace process to establish and run communication with the kernel driver.
1. The userspace implementation that actually processes the filesystem operations.

The kernel driver is provided by the FUSE project. The libfuse userspace library used by CockroachFUSE is [`rust-fuse`](https://github.com/zargony/rust-fuse).

## Dependencies

To run mount to a FUSE filesystem, the target system needs FUSE (OSXFUSE on macOS) to be properly installed (i.e. kernel driver and libraries. Some platforms may also require userland utils like `fusermount`). A default installation of package `fuse` on Linux, `fusefs-libs` on FreeBSD, or [`OSXFUSE`](https://osxfuse.github.io/) on macOS is usually sufficient.

To build, the host system needs FUSE libraries and headers installed. On Linux, the header package is usually called `libfuse-dev`. On FreeBSD and macOS, `fusefs-libs`/`OSXFUSE` installs everything that's needed. The build process also requires `pkg-config` to locate headers and libraries.

## Usage

Once a FUSE kernel driver is installed, follow these steps to run a CockroachFUSE filesystem.

```
# Clone the cockroach-fuse git repository
git clone https://github.com/nvanbenschoten/cockroach-fuse.git
cd cockroach_fuse

# Start a CockroachDB instance
cockroach start --insecure --background
cockroach sql --insecure -e 'CREATE DATABASE cockroachfs'

# Start the filesystem
mkdir mount
cargo run -- --mountpoint=mount
```

Getting a CockroachDB development environment working on this filesystem is easy. Just follow these steps.
```
# Clone CockroachDB
cd mount
git clone https://github.com/cockroachdb/cockroach.git

# Build CockroachDB
cd cockroach
make build
```
