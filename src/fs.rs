use super::sql;
use fuse::{
    FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite,
    Request,
};
use libc::{c_int, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFREG, S_IFSOCK};
use libc::{ECONNREFUSED, EEXIST, ENOENT, ENOTDIR};
use postgres::error;
use std::ffi::OsStr;
use time::Timespec;

/// Cache timeout for name and attribute replies.
const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

pub struct CockroachFS {
    /// Database connection
    conn: postgres::Connection,
}

impl CockroachFS {
    pub fn new(conn: postgres::Connection) -> CockroachFS {
        CockroachFS { conn: conn }
    }
}

impl Filesystem for CockroachFS {
    /// Initialize filesystem.
    /// Called before any other filesystem method.
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        // Initialize the databse schema.
        sql::create_schema(&self.conn).map_err(|e| {
            eprintln!("{}", e);
            ECONNREFUSED
        })?;

        // Create the root directory.
        sql::create_inode(&self.conn, 0, &"", FileType::Directory, 0).map_err(|e| {
            eprintln!("{}", e);
            ECONNREFUSED
        })?;

        Ok(())
    }

    /// Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup {} {}", parent, name.to_str().unwrap());
        match sql::lookup_dir_ent(&self.conn, parent, name.to_str().unwrap()) {
            Err(err) => {
                eprintln!("lookup {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(attr)) => {
                println!("lookup found {}", name.to_str().unwrap());
                reply.entry(&TTL, &attr, 0)
            }
        };
    }

    /// Get file attributes.
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr {}", ino);
        match sql::lookup_inode(&self.conn, ino) {
            Err(err) => {
                eprintln!("getattr {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(attr)) => reply.attr(&TTL, &attr),
        };
    }

    /// Set file attributes.
    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        _fh: Option<u64>,
        crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        println!("setattr {}", ino);
        let (kind, perm) = optional_kind_and_perm_from_mode(mode);
        match sql::update_inode(
            &self.conn, ino, size, atime, mtime, chgtime, crtime, kind, perm, uid, gid, flags,
        ) {
            Err(err) => {
                eprintln!("setattr {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(attr)) => reply.attr(&TTL, &attr),
        };
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.
    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32, // TODO: what is this supposed to be?
        rdev: u32,
        reply: ReplyEntry,
    ) {
        match sql::create_inode(
            &self.conn,
            parent,
            name.to_str().unwrap(),
            FileType::RegularFile,
            rdev,
        ) {
            Err(err) => {
                eprintln!("mknod {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(attr) => reply.entry(&TTL, &attr, 0),
        };
    }

    /// Create a directory.
    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        match sql::create_inode(
            &self.conn,
            parent,
            name.to_str().unwrap(),
            FileType::Directory,
            0,
        ) {
            Err(err) => {
                eprintln!("mkdir {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(attr) => reply.entry(&TTL, &attr, 0),
        };
    }

    /// Remove a file.
    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match sql::unlink(&self.conn, parent, name.to_str().unwrap()) {
            Err(err) => {
                eprintln!("unlink {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(_)) => reply.ok(),
        };
    }

    /// Remove a directory.
    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match sql::unlink(&self.conn, parent, name.to_str().unwrap()) {
            Err(err) => {
                eprintln!("rmdir {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(_)) => reply.ok(),
        };
    }

    /// Rename a file.
    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        match sql::rename_dir_ent(
            &self.conn,
            parent,
            name.to_str().unwrap(),
            newparent,
            newname.to_str().unwrap(),
        ) {
            Err(ref err) if err.code() == Some(&error::UNIQUE_VIOLATION) => reply.error(EEXIST),
            Err(err) => {
                eprintln!("rename {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(false) => reply.error(ENOENT),
            Ok(true) => reply.ok(),
        };
    }

    /// Create a hard link.
    fn link(
        &mut self,
        _req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        match sql::link(&self.conn, ino, newparent, newname.to_str().unwrap()) {
            Err(err) => {
                eprintln!("link {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(attr)) => reply.entry(&TTL, &attr, 0),
        };
    }

    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to
    /// this is when the file has been opened in 'direct_io' mode, in which case the
    /// return value of the read system call will reflect the return value of this
    /// operation. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        println!("read");
        match sql::read_data(&self.conn, ino, offset, size as usize) {
            Err(err) => {
                eprintln!("read {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(data)) => reply.data(data.as_slice()),
        };
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on error. An
    /// exception to this is when the file has been opened in 'direct_io' mode, in
    /// which case the return value of the write system call will reflect the return
    /// value of this operation. fh will contain the value set by the open method, or
    /// will be undefined if the open method didn't set any value.
    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        println!("write {} bytes to {}", data.len(), ino);
        match sql::write_data(&self.conn, ino, offset, data) {
            Err(err) => {
                eprintln!("write {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(size)) => reply.written(size as u32),
        };
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        reply.ok()
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        println!("readdir {} {}", ino, offset);
        let errno = match sql::lookup_inode_kind(&self.conn, ino) {
            Err(err) => {
                eprintln!("readdir {}", err);
                ECONNREFUSED
            }
            Ok(None) => ENOENT,
            Ok(Some(FileType::Directory)) => 0,
            Ok(Some(_)) => ENOTDIR,
        };
        if errno != 0 {
            reply.error(errno);
            return;
        }
        match sql::read_dir(&self.conn, ino, offset) {
            Err(err) => {
                eprintln!("readdir {}", err);
                reply.error(ECONNREFUSED)
            }
            Ok(ents) => {
                for (i, ent) in ents.iter().enumerate() {
                    reply.add(
                        ent.child_ino,
                        offset + 1 + (i as i64),
                        ent.child_kind,
                        &ent.child_name,
                    );
                }
                reply.ok();
            }
        };
    }
}

fn kind_and_perm_from_mode(mode: u32) -> (FileType, u16) {
    let perm = mode as u16;
    let kind = match ((mode as u16) >> 12) << 12 {
        S_IFIFO => Some(FileType::NamedPipe),
        S_IFCHR => Some(FileType::CharDevice),
        S_IFBLK => Some(FileType::BlockDevice),
        S_IFDIR => Some(FileType::Directory),
        S_IFREG => Some(FileType::RegularFile),
        S_IFLNK => Some(FileType::Symlink),
        S_IFSOCK => Some(FileType::Socket),
        _ => None,
    }
    .unwrap();
    (kind, perm)
}

fn optional_kind_and_perm_from_mode(mode: Option<u32>) -> (Option<FileType>, Option<u16>) {
    match mode {
        None => (None, None),
        Some(mode) => {
            let (kind, perm) = kind_and_perm_from_mode(mode);
            (Some(kind), Some(perm))
        }
    }
}
