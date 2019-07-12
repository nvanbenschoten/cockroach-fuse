use fuse::{FileAttr, FileType};
use postgres::rows::Row;
use postgres::{Connection, Result};

const SCHEMAS: &[&str] = &[
    "CREATE SEQUENCE IF NOT EXISTS inode_alloc",
    "CREATE TABLE IF NOT EXISTS inodes (
        ino    INT8      NOT NULL PRIMARY KEY DEFAULT nextval('inode_alloc'),
        size   INT8      NOT NULL DEFAULT 0,
        blocks INT8      NOT NULL DEFAULT 0,
        atime  TIMESTAMP NOT NULL DEFAULT now(),
        mtime  TIMESTAMP NOT NULL DEFAULT now(),
        ctime  TIMESTAMP NOT NULL DEFAULT now(),
        crtime TIMESTAMP NOT NULL DEFAULT now(),
        kind   STRING    NOT NULL,
        perm   INT2      NOT NULL DEFAULT 493,
        nlink  INT4      NOT NULL DEFAULT 1,
        uid    INT4      NOT NULL DEFAULT 501,
        gid    INT4      NOT NULL DEFAULT 20,
        rdev   INT4      NOT NULL DEFAULT 0,
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
        file_ino  INT8 NOT NULL REFERENCES inodes (ino),
        block_idx INT8 NOT NULL,
        PRIMARY KEY (file_ino, block_idx)
    )",
];

pub struct DirEntry {
    pub dir_ino: u64,
    pub child_ino: u64,
    pub child_kind: FileType,
    pub child_name: String,
}

pub fn create_schema(conn: &Connection) -> Result<()> {
    for table in SCHEMAS {
        conn.execute(table, &[]).map(|_| ())?;
    }
    Ok(())
}

pub fn create_inode(
    conn: &Connection,
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

pub fn lookup_inode_kind(conn: &Connection, ino: u64) -> Result<Option<FileType>> {
    conn.query("SELECT kind FROM inodes WHERE ino = $1", &[&(ino as i64)])
        .map(|rows| {
            if rows.len() == 0 {
                None
            } else {
                str_to_file_type(rows.get(0).get(0))
            }
        })
}

pub fn lookup_inode(conn: &Connection, ino: u64) -> Result<Option<FileAttr>> {
    conn.query("SELECT * FROM inodes WHERE ino = $1", &[&(ino as i64)])
        .map(|rows| {
            if rows.len() == 0 {
                None
            } else {
                Some(row_to_file_attr(rows.get(0)))
            }
        })
}

//pub fn update_inode(conn: &Connection, attr: FileAttr) -> Result<Option<FileAttr>> {

// }

pub fn read_dir(conn: &Connection, ino: u64, offset: i64) -> Result<Vec<DirEntry>> {
    if offset != 0 {
        let offset_name: String = conn
            .query(
                "SELECT child_name FROM dir_entries WHERE dir_ino = $1 AND child_ino = $2",
                &[&(ino as i64), &offset],
            )
            .map(|rows| {
                if rows.len() == 0 {
                    None
                } else {
                    Some(rows.get(0).get(0))
                }
            })?
            .unwrap();

        conn.query(
            "SELECT * FROM dir_entries WHERE dir_ino = $1 AND child_name > $2",
            &[&(ino as i64), &offset_name],
        )
    } else {
        conn.query(
            "SELECT * FROM dir_entries WHERE dir_ino = $1",
            &[&(ino as i64)],
        )
    }
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

pub fn lookup_dir_ent(conn: &Connection, parent: u64, name: &str) -> Result<Option<FileAttr>> {
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

pub fn rename_dir_ent(
    conn: &Connection,
    parent: u64,
    name: &str,
    new_parent: u64,
    new_name: &str,
) -> Result<bool> {
    conn.execute(
        "UPDATE dir_entries
         SET   (dir_ino, child_name) = ($1, $2)
         WHERE (dir_ino, child_name) = ($3, $4)",
        &[&(new_parent as i64), &new_name, &(parent as i64), &name],
    )
    .map(|num| num == 1)
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
