use fuse::{FileAttr, FileType};
use postgres::rows::Row;
use postgres::{GenericConnection, Result};
use std::cmp;
use time::Timespec;

const SCHEMAS: &[&str] = &[
    "CREATE SEQUENCE IF NOT EXISTS inode_alloc",
    "CREATE TABLE IF NOT EXISTS inodes (
        -- Inode number
        ino    INT8      NOT NULL PRIMARY KEY DEFAULT nextval('inode_alloc'),
        -- Size in bytes
        size   INT8      NOT NULL DEFAULT 0,
        -- Size in blocks
        blocks INT8      NOT NULL DEFAULT 0,
        -- Time of last access
        atime  TIMESTAMP NOT NULL DEFAULT now(),
        -- Time of last modification
        mtime  TIMESTAMP NOT NULL DEFAULT now(),
        -- Time of last change
        ctime  TIMESTAMP NOT NULL DEFAULT now(),
        -- Time of creation (macOS only)
        crtime TIMESTAMP NOT NULL DEFAULT now(),
        -- Kind of file (directory, file, pipe, etc)
        kind   STRING    NOT NULL,
        -- Permissions
        perm   INT2      NOT NULL DEFAULT 493,
        -- Number of hard links
        nlink  INT4      NOT NULL DEFAULT 1,
        -- User id
        uid    INT4      NOT NULL DEFAULT 501,
        -- Group id
        gid    INT4      NOT NULL DEFAULT 20,
        -- Rdev
        rdev   INT4      NOT NULL DEFAULT 0,
        -- Flags (macOS only, see chflags(2))
        flags  INT4      NOT NULL DEFAULT 0
    )",
    "CREATE TABLE IF NOT EXISTS dir_entries (
        dir_ino    INT8   NOT NULL REFERENCES inodes (ino),
        child_name STRING NOT NULL,
        child_kind STRING NOT NULL,
        child_ino  INT8   NOT NULL, -- REFERENCES inodes (ino)
        PRIMARY KEY (dir_ino, child_name)
    )",
    "CREATE TABLE IF NOT EXISTS blocks (
        file_ino  INT8 NOT NULL REFERENCES inodes (ino) ON DELETE CASCADE,
        block_idx INT8 NOT NULL,
        bytes     BYTES NOT NULL DEFAULT repeat(x'00'::string, 1024)::bytes,
        PRIMARY KEY (file_ino, block_idx)
    )",
];

const DATA_BLOCK_SIZE: i64 = 1 << 10;

#[derive(Debug)]
pub struct DirEntry {
    pub dir_ino: u64,
    pub child_ino: u64,
    pub child_kind: FileType,
    pub child_name: String,
}

pub fn create_schema<C: GenericConnection>(conn: &C) -> Result<()> {
    for table in SCHEMAS {
        conn.execute(table, &[]).map(|_| ())?;
    }
    Ok(())
}

pub fn create_inode<C: GenericConnection>(
    conn: &C,
    parent: u64,
    name: &str,
    ft: FileType,
    rdev: u32,
) -> Result<FileAttr> {
    let kind_str = file_type_to_str(ft);
    let txn = conn.transaction()?;
    let attr = txn
        .query(
            "INSERT INTO inodes (kind, rdev)
             VALUES ($1, $2)
             RETURNING *",
            &[&kind_str, &(rdev as i32)],
        )
        .map(|rows| row_to_file_attr(rows.get(0)))?;
    if parent != 0 {
        txn.execute(
            "INSERT INTO dir_entries
             VALUES ($1, $2, $3, $4)",
            &[&(parent as i64), &name, &kind_str, &(attr.ino as i64)],
        )?;
    }
    txn.commit()?;
    Ok(attr)
}

pub fn unlink<C: GenericConnection>(conn: &C, parent: u64, name: &str) -> Result<Option<()>> {
    println!("unlink: {} in {}", name, parent);
    let txn = conn.transaction()?;
    let mut inode = match lookup_dir_ent(&txn, parent, name)? {
        Some(dir_ent) => dir_ent,
        None => return Ok(None),
    };
    txn.execute(
        "DELETE FROM dir_entries
         WHERE (dir_ino, child_name, child_ino) = ($1, $2, $3)",
        &[&(parent as i64), &name, &(inode.ino as i64)],
    )?;
    inode.nlink -= 1;
    if inode.nlink == 0 {
        txn.execute("DELETE FROM inodes WHERE ino = $1", &[&(inode.ino as i64)])?;
    } else {
        update_nlink(&txn, inode.ino, inode.nlink)?;
    }
    txn.commit()?;
    return Ok(Some(()));
}

pub fn link<C: GenericConnection>(
    conn: &C,
    ino: u64,
    parent: u64,
    newname: &str,
) -> Result<Option<FileAttr>> {
    println!("link: {} as {} in {}", ino, newname, parent);
    let txn = conn.transaction()?;
    let inode_opt = lookup_inode(&txn, ino)?;
    let mut inode = match inode_opt {
        Some(inode) => inode,
        None => return Ok(None),
    };
    // TODO(ajwerner): return a better error if inode is a dir.
    if inode.kind != FileType::RegularFile {
        return Ok(None);
    }
    let kind_str = file_type_to_str(inode.kind);
    txn.execute(
        "INSERT INTO dir_entries
         VALUES ($1, $2, $3, $4)",
        &[&(parent as i64), &newname, &kind_str, &(ino as i64)],
    )?;
    inode.nlink += 1;
    update_nlink(&txn, inode.ino, inode.nlink)?;
    txn.commit()?;
    Ok(Some(inode))
}

pub fn lookup_inode_kind<C: GenericConnection>(conn: &C, ino: u64) -> Result<Option<FileType>> {
    conn.query("SELECT kind FROM inodes WHERE ino = $1", &[&(ino as i64)])
        .map(|rows| {
            if rows.len() == 0 {
                None
            } else {
                str_to_file_type(rows.get(0).get(0))
            }
        })
}

pub fn lookup_inode<C: GenericConnection>(conn: &C, ino: u64) -> Result<Option<FileAttr>> {
    conn.query("SELECT * FROM inodes WHERE ino = $1", &[&(ino as i64)])
        .map(|rows| {
            if rows.len() == 0 {
                None
            } else {
                Some(row_to_file_attr(rows.get(0)))
            }
        })
}

pub fn update_inode<C: GenericConnection>(
    conn: &C,
    ino: u64,
    size: Option<u64>,
    atime: Option<Timespec>,
    mtime: Option<Timespec>,
    chgtime: Option<Timespec>,
    crtime: Option<Timespec>,
    kind: Option<FileType>,
    perm: Option<u16>,
    uid: Option<u32>,
    gid: Option<u32>,
    flags: Option<u32>,
) -> Result<Option<FileAttr>> {
    let file_type = kind.map(file_type_to_str);
    conn.query(
        "UPDATE inodes SET
           size   = IFNULL($1, size),
           atime  = IFNULL($2, atime),
           mtime  = IFNULL($3, mtime),
           ctime  = IFNULL($4, ctime),
           crtime = IFNULL($5, crtime),
           kind   = IFNULL($6, kind),
           perm   = IFNULL($7, perm),
           uid    = IFNULL($8, uid),
           gid    = IFNULL($9, gid),
           flags  = IFNULL($10, flags)
         WHERE ino = $11
         RETURNING *",
        &[
            &size.map(|s| s as i64),
            &atime,
            &mtime,
            &chgtime,
            &crtime,
            &file_type,
            &perm.map(|p| p as i16),
            &uid.map(|p| p as i32),
            &gid.map(|p| p as i32),
            &flags.map(|p| p as i32),
            &(ino as i64),
        ],
    )
    .map(|rows| {
        if rows.len() == 0 {
            None
        } else {
            Some(row_to_file_attr(rows.get(0)))
        }
    })
}

pub fn read_dir<C: GenericConnection>(conn: &C, ino: u64, offset: i64) -> Result<Vec<DirEntry>> {
    conn.query(
        "SELECT * FROM dir_entries WHERE dir_ino = $1 ORDER BY child_name OFFSET $2 ROWS",
        &[&(ino as i64), &(offset)],
    )
    .map(|rows| {
        rows.iter()
            .map(|row| DirEntry {
                dir_ino: row.get::<_, i64>(0) as u64,
                child_name: row.get(1),
                child_kind: str_to_file_type(row.get(2)).unwrap(),
                child_ino: row.get::<_, i64>(3) as u64,
            })
            .collect()
    })
}

pub fn lookup_dir_ent<C: GenericConnection>(
    conn: &C,
    parent: u64,
    name: &str,
) -> Result<Option<FileAttr>> {
    conn.query(
        "SELECT i.* FROM inodes i 
         JOIN dir_entries d 
         ON i.ino = d.child_ino 
         WHERE d.dir_ino = $1 AND d.child_name = $2",
        &[&(parent as i64), &name],
    )
    .map(|rows| {
        if rows.len() == 0 {
            None
        } else {
            Some(row_to_file_attr(rows.get(0)))
        }
    })
}

pub fn update_nlink<C: GenericConnection>(conn: &C, ino: u64, nlink: u32) -> Result<()> {
    conn.execute(
        "UPDATE inodes
         SET (nlink) = ($1)
         WHERE (ino) = ($2)",
        &[&(nlink as i32), &(ino as i64)],
    )?;
    return Ok(());
}

pub fn rename_dir_ent<C: GenericConnection>(
    conn: &C,
    parent: u64,
    name: &str,
    new_parent: u64,
    new_name: &str,
) -> Result<bool> {
    let txn = conn.transaction()?;
    txn.execute(
        "DELETE FROM dir_entries
         WHERE (dir_ino, child_name) = ($1, $2)",
        &[&(new_parent as i64), &new_name],
    )?;
    let num = txn.execute(
        "UPDATE dir_entries
         SET   (dir_ino, child_name) = ($1, $2)
         WHERE (dir_ino, child_name) = ($3, $4)",
        &[&(new_parent as i64), &new_name, &(parent as i64), &name],
    )?;
    if num == 0 {
        txn.set_rollback();
        txn.finish()?;
        return Ok(false);
    }
    txn.commit()?;
    Ok(true)
}

pub fn read_data<C: GenericConnection>(
    conn: &C,
    ino: u64,
    offset: i64,
    size: usize,
) -> Result<Option<Vec<u8>>> {
    let txn = conn.transaction()?;
    let cur_inode: Option<i64> = txn
        .query("SELECT size FROM inodes WHERE ino = $1", &[&(ino as i64)])
        .map(|rows| {
            if rows.len() == 0 {
                None
            } else {
                Some(rows.get(0).get(0))
            }
        })?;
    match cur_inode {
        Some(cur_size) => {
            if cur_size < offset + size as i64 {
                return Ok(None);
            }
        }
        None => return Ok(None),
    };

    let start_block = offset / DATA_BLOCK_SIZE;
    let end_block = (offset + size as i64) / DATA_BLOCK_SIZE;
    let mut data = txn
        .query(
            "SELECT bytes FROM blocks 
            WHERE file_ino = $1 AND block_idx BETWEEN $2 AND $3",
            &[&(ino as i64), &(start_block as i64), &(end_block as i64)],
        )?
        .into_iter()
        .map(|row| row.get::<_, Vec<u8>>(0))
        .fold(Vec::with_capacity(size), |mut data, mut bytes| {
            data.append(&mut bytes);
            data
        });
    data.truncate(size);

    txn.commit()?;
    Ok(Some(data))
}

pub fn write_data<C: GenericConnection>(
    conn: &C,
    ino: u64,
    offset: i64,
    data: &[u8],
) -> Result<Option<usize>> {
    let txn = conn.transaction()?;
    let cur_inode: Option<(i64, i64)> = txn
        .query(
            "SELECT size, blocks FROM inodes WHERE ino = $1",
            &[&(ino as i64)],
        )
        .map(|rows| {
            if rows.len() == 0 {
                None
            } else {
                let row = rows.get(0);
                Some((row.get(0), row.get(1)))
            }
        })?;
    let (cur_size, cur_blocks) = match cur_inode {
        Some(v) => v,
        None => return Ok(None),
    };

    // Pad out to the offset.
    let before = offset / DATA_BLOCK_SIZE;
    for i in cur_blocks..before {
        txn.execute(
            "INSERT INTO blocks
             VALUES ($1, $2, DEFAULT)",
            &[&(ino as i64), &(i as i64)],
        )?;
    }

    let mut cur_block = before;
    let mut cur_offset = offset % DATA_BLOCK_SIZE;
    let mut created_blocks = 0;
    let mut data_left = data;
    while data_left.len() > 0 {
        let avail = (DATA_BLOCK_SIZE - cur_offset) as usize;
        let left = data_left.len();
        let chunk_size = if left >= avail { avail } else { left };
        let chunk = &data_left[0..chunk_size];
        let after = avail - chunk_size;
        if cur_blocks <= cur_block {
            // Create new block.
            txn.execute(
                "INSERT INTO blocks
                 VALUES ($1, $2, repeat(x'00'::string, $3)::bytes || $4 || repeat(x'00'::string, $5)::bytes)",
                &[
                    &(ino as i64),
                    &(cur_block as i64),
                    &(cur_offset as i64),
                    &chunk,
                    &(after as i64),
                ],
            )?;
            created_blocks = created_blocks + 1;
        } else {
            // Modify cur block.
            txn.execute(
                "UPDATE blocks
                 SET bytes = substr(convert_from(bytes, 'utf8'), 1, $1)::bytes || 
                             $2 || 
                             substr(convert_from(bytes, 'utf8'), $3+1)::bytes
                 WHERE file_ino = $4 AND block_idx = $5",
                &[
                    &(cur_offset as i64),
                    &chunk,
                    &(cur_offset + chunk_size as i64),
                    &(ino as i64),
                    &(cur_block as i64),
                ],
            )?;
        }
        cur_block += 1;
        cur_offset = 0;
        data_left = &data_left[chunk_size..];
    }

    // Update the inode with the new size and block count.
    let touched_size = offset + data.len() as i64;
    let new_size = cmp::max(cur_size, touched_size);
    let new_blocks = cur_blocks + created_blocks as i64;
    let num_updated = txn.execute(
        "UPDATE inodes SET size = $1, blocks = $2 WHERE ino = $3",
        &[&new_size, &new_blocks, &(ino as i64)],
    )?;
    if num_updated != 1 {
        return Ok(None);
    }

    txn.commit()?;
    Ok(Some(data.len()))
}

fn row_to_file_attr(row: Row) -> FileAttr {
    FileAttr {
        ino: row.get::<_, i64>(0) as u64,
        size: row.get::<_, i64>(1) as u64,
        blocks: row.get::<_, i64>(2) as u64,
        atime: row.get(3),
        mtime: row.get(4),
        ctime: row.get(5),
        crtime: row.get(6),
        kind: str_to_file_type(row.get(7)).unwrap(),
        perm: row.get::<_, i16>(8) as u16,
        nlink: row.get::<_, i32>(9) as u32,
        uid: row.get::<_, i32>(10) as u32,
        gid: row.get::<_, i32>(11) as u32,
        rdev: row.get::<_, i32>(12) as u32,
        flags: row.get::<_, i32>(13) as u32,
    }
}

fn file_type_to_str(ft: FileType) -> &'static str {
    match ft {
        FileType::NamedPipe => "S_IFIFO",
        FileType::CharDevice => "S_IFCHR",
        FileType::BlockDevice => "S_IFBLK",
        FileType::Directory => "S_IFDIR",
        FileType::RegularFile => "S_IFREG",
        FileType::Symlink => "S_IFLNK",
        FileType::Socket => "S_IFSOCK",
    }
}

fn str_to_file_type(s: String) -> Option<FileType> {
    match s.as_ref() {
        "S_IFIFO" => Some(FileType::NamedPipe),
        "S_IFCHR" => Some(FileType::CharDevice),
        "S_IFBLK" => Some(FileType::BlockDevice),
        "S_IFDIR" => Some(FileType::Directory),
        "S_IFREG" => Some(FileType::RegularFile),
        "S_IFLNK" => Some(FileType::Symlink),
        "S_IFSOCK" => Some(FileType::Socket),
        _ => None,
    }
}
