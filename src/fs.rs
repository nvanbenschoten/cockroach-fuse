use super::sql;
use fuse::{
    FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, Request,
};
use libc::{c_int, ECONNREFUSED, ENOENT, ENOTDIR};
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
            Err(_) => reply.error(ECONNREFUSED),
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
            Err(_) => reply.error(ECONNREFUSED),
            Ok(None) => reply.error(ENOENT),
            Ok(Some(attr)) => reply.attr(&TTL, &attr),
        };
    }

    // /// Set file attributes.
    // fn setattr(
    //     &mut self,
    //     _req: &Request,
    //     ino: u64,
    //     mode: Option<u32>,
    //     uid: Option<u32>,
    //     gid: Option<u32>,
    //     size: Option<u64>,
    //     atime: Option<Timespec>,
    //     mtime: Option<Timespec>,
    //     _fh: Option<u64>,
    //     crtime: Option<Timespec>,
    //     chgtime: Option<Timespec>,
    //     _bkuptime: Option<Timespec>,
    //     flags: Option<u32>,
    //     reply: ReplyAttr,
    // ) {
    //     println!("setattr {}", ino);
    //     match sql::update_inode(&self.conn, ino) {
    //         Err(_) => reply.error(ECONNREFUSED),
    //         Ok(None) => reply.error(ENOENT),
    //         Ok(Some(attr)) => reply.attr(&TTL, &attr),
    //     };
    //     reply.error(ENOSYS);
    // }

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
            Err(_) => reply.error(ECONNREFUSED),
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
            Err(_) => reply.error(ECONNREFUSED),
            Ok(attr) => reply.entry(&TTL, &attr, 0),
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
            Err(_) => reply.error(ECONNREFUSED),
            Ok(false) => reply.error(ENOENT),
            Ok(true) => reply.ok(),
        };
    }

    /// Create a hard link.
    fn link(&mut self, _req: &Request, ino: u64, newparent: u64, newname: &OsStr, reply: ReplyEntry) {
        match sql::link(
            &self.conn,
            ino,
            newparent,
            newname.to_str().unwrap(),
        ) {
            Err(err) => {
                println!("link: {}", err);
                reply.error(ECONNREFUSED)
            },
            Ok(None) => reply.error(ENOENT),
            Ok(Some(attr)) => reply.entry(&TTL, &attr, 0),
        };
    }

    /// Remove a file.
    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match sql::unlink(
            &self.conn,
            parent,
            name.to_str().unwrap(),
        ) {
            Err(_) => reply.error(ECONNREFUSED),
            Ok(None) => reply.error(ENOENT),
            Ok(Some(_)) => reply.ok(),
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
        _size: u32,
        reply: ReplyData,
    ) {
        println!("read");
        if ino == 2 {
            reply.data(&"Hello World!\n".as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
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
            Err(_) => ECONNREFUSED,
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
                println!("failed to read_dir: {}", err);
                reply.error(ECONNREFUSED)
            },
            Ok(ents) => {
                for (i, ent) in ents.iter().enumerate() {
                    reply.add(
                        ent.child_ino,
                        offset+1+(i as i64),
                        ent.child_kind,
                        &ent.child_name,
                    );
                }
                reply.ok();
            }
        };
    }
}
