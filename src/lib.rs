//! A crate providing access to the Postgres large object API.
//!
//! # Example
//!
//! ```rust,no_run
//! extern crate postgres;
//! extern crate postgres_large_object;
//!
//! use std::fs::File;
//! use std::io;
//!
//! use postgres::{Client, NoTls};
//! use postgres_large_object::{LargeObjectExt, LargeObjectTransactionExt, Mode};
//!
//! fn main() {
//!     let mut conn = Client::connect("postgres://postgres:postgres@localhost", NoTls).unwrap();
//!
//!     let mut file = File::open("vacation_photos.tar.gz").unwrap();
//!     let mut trans = conn.transaction().unwrap();
//!     let oid = trans.create_large_object().unwrap();
//!     {
//!         let mut large_object = trans.open_large_object(oid, Mode::Write).unwrap();
//!         io::copy(&mut file, &mut large_object).unwrap();
//!     }
//!     trans.commit().unwrap();
//!
//!     let mut file = File::create("vacation_photos_copy.tar.gz").unwrap();
//!     let mut trans = conn.transaction().unwrap();
//!     let mut large_object = trans.open_large_object(oid, Mode::Read).unwrap();
//!     io::copy(&mut large_object, &mut file).unwrap();
//! }
//! ```
#![doc(html_root_url = "https://docs.rs/postgres_large_object/0.7")]

extern crate postgres;

use postgres::types::Oid;
use postgres::Error;
use postgres::GenericClient;
use postgres::Transaction;
use std::cmp;
use std::fmt;
use std::i32;
use std::io::{self, Write};

/// An extension trait adding functionality to create and delete large objects.
pub trait LargeObjectExt {
    /// Creates a new large object, returning its `Oid`.
    fn create_large_object(&mut self) -> Result<Oid, Error>;

    /// Deletes the large object with the specified `Oid`.
    fn delete_large_object(&mut self, oid: Oid) -> Result<(), Error>;
}

impl<T: GenericClient> LargeObjectExt for T {
    fn create_large_object(&mut self) -> Result<Oid, Error> {
        let stmt = self.prepare("SELECT pg_catalog.lo_create(0)")?;
        let r = self.query_one(&stmt, &[]).map(|r| r.get(0));
        r
    }

    fn delete_large_object(&mut self, oid: Oid) -> Result<(), Error> {
        let stmt = self.prepare("SELECT pg_catalog.lo_unlink($1)")?;
        self.execute(&stmt, &[&oid]).map(|_| ())
    }
}

/// Large object access modes.
///
/// Note that Postgres currently does not make any distinction between the
/// `Write` and `ReadWrite` modes.
#[derive(Debug)]
pub enum Mode {
    /// An object opened in this mode may only be read from.
    Read,
    /// An object opened in this mode may be written to.
    Write,
    /// An object opened in this mode may be read from or written to.
    ReadWrite,
}

impl Mode {
    fn to_i32(&self) -> i32 {
        match *self {
            Mode::Read => 0x00040000,
            Mode::Write => 0x00020000,
            Mode::ReadWrite => 0x00040000 | 0x00020000,
        }
    }
}

/// An extension trait adding functionality to open large objects.
pub trait LargeObjectTransactionExt<'conn> {
    /// Opens the large object with the specified `Oid` in the specified `Mode`.
    fn open_large_object<'b>(
        &'b mut self,
        oid: Oid,
        mode: Mode,
    ) -> Result<LargeObject<'conn, 'b>, Error>;
}

impl<'conn> LargeObjectTransactionExt<'conn> for Transaction<'conn> {
    fn open_large_object<'b>(
        &'b mut self,
        oid: Oid,
        mode: Mode,
    ) -> Result<LargeObject<'conn, 'b>, Error> {
        let stmt = self.prepare("SELECT pg_catalog.lo_open($1, $2)")?;
        let fd = self
            .query(&stmt, &[&oid, &mode.to_i32()])?
            .iter()
            .next()
            .unwrap()
            .get(0);
        Ok(LargeObject {
            trans: self,
            fd: fd,
            finished: false,
        })
    }
}

/// Represents an open large object.
pub struct LargeObject<'conn, 'b> {
    trans: &'b mut Transaction<'conn>,
    fd: i32,
    finished: bool,
}

impl<'a, 'b> fmt::Debug for LargeObject<'a, 'b> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("LargeObject")
            .field("fd", &self.fd)
            .field("finished", &self.finished)
            .finish()
    }
}

impl<'a, 'b> Drop for LargeObject<'a, 'b> {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl<'a, 'b> LargeObject<'a, 'b> {
    /// Returns the file descriptor of the opened object.
    pub fn fd(&self) -> i32 {
        self.fd
    }

    /// Truncates the object to the specified size.
    ///
    /// If `len` is larger than the size of the object, it will be padded with
    /// null bytes to the specified size.
    pub fn truncate(&mut self, len: i64) -> Result<(), Error> {
        let stmt = self
            .trans
            .prepare("SELECT pg_catalog.lo_truncate64($1, $2)")?;

        self.trans.execute(&stmt, &[&self.fd, &len]).map(|_| ())
    }

    fn finish_inner(&mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }

        self.finished = true;
        let stmt = self.trans.prepare("SELECT pg_catalog.lo_close($1)")?;
        self.trans.execute(&stmt, &[&self.fd]).map(|_| ())
    }

    /// Consumes the `LargeObject`, cleaning up server side state.
    ///
    /// Functionally identical to the `Drop` implementation on `LargeObject`
    /// except that it returns any errors to the caller.
    pub fn finish(mut self) -> Result<(), Error> {
        self.finish_inner()
    }
}

impl<'a, 'b> io::Read for LargeObject<'a, 'b> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let stmt = self
            .trans
            .prepare("SELECT pg_catalog.loread($1, $2)")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let cap = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let rows = self
            .trans
            .query(&stmt, &[&self.fd, &cap])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        buf.write(rows.get(0).unwrap().get(0))
    }
}

impl<'a, 'b> io::Write for LargeObject<'a, 'b> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let stmt = self
            .trans
            .prepare("SELECT pg_catalog.lowrite($1, $2)")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let cap = cmp::min(buf.len(), i32::MAX as usize);
        self.trans
            .execute(&stmt, &[&self.fd, &&buf[..cap]])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(cap)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a, 'b> io::Seek for LargeObject<'a, 'b> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let (kind, pos) = match pos {
            io::SeekFrom::Start(pos) => {
                let pos = if pos <= i64::max_value as u64 {
                    pos as i64
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot seek more than 2^63 bytes",
                    ));
                };
                (0, pos)
            }
            io::SeekFrom::Current(pos) => (1, pos),
            io::SeekFrom::End(pos) => (2, pos),
        };

        let stmt = self
            .trans
            .prepare("SELECT pg_catalog.lo_lseek64($1, $2, $3)")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let rows = self
            .trans
            .query(&stmt, &[&self.fd, &pos, &kind.to_owned()])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let pos: i64 = rows.iter().next().unwrap().get(0);
        Ok(pos as u64)
    }
}

#[cfg(test)]
mod test {
    use postgres::{error::SqlState, Client, NoTls};

    use crate::{LargeObjectExt, LargeObjectTransactionExt, Mode};

    const DB_URL: &str = "postgres://postgres:postgres@localhost";

    #[test]
    fn test_create_delete() {
        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        let oid = conn.create_large_object().unwrap();
        conn.delete_large_object(oid).unwrap();
    }

    #[test]
    fn test_delete_bogus() {
        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        match conn.delete_large_object(0) {
            Ok(()) => panic!("unexpected success"),
            Err(ref e) if e.code() == Some(&SqlState::UNDEFINED_OBJECT) => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_open_bogus() {
        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        let mut trans = conn.transaction().unwrap();
        match trans.open_large_object(0, Mode::Read) {
            Ok(_) => panic!("unexpected success"),
            Err(ref e) if e.code() == Some(&SqlState::UNDEFINED_OBJECT) => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        };
    }

    #[test]
    fn test_open_finish() {
        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        let mut trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let lo = trans.open_large_object(oid, Mode::Read).unwrap();
        lo.finish().unwrap();
    }

    #[test]
    fn test_write_read() {
        use std::io::{Read, Write};

        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        let mut trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        {
            let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
            lo.write_all(b"hello world!!!").unwrap();
        }
        let mut lo = trans.open_large_object(oid, Mode::Read).unwrap();
        let mut out = vec![];
        lo.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello world!!!");
    }

    #[test]
    fn test_seek_tell() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        let mut trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();

        assert_eq!(14, lo.seek(SeekFrom::Current(0)).unwrap());
        assert_eq!(1, lo.seek(SeekFrom::Start(1)).unwrap());
        let mut buf = [0];
        assert_eq!(1, lo.read(&mut buf).unwrap());
        assert_eq!(b'e', buf[0]);
        assert_eq!(2, lo.seek(SeekFrom::Current(0)).unwrap());
        assert_eq!(10, lo.seek(SeekFrom::End(-4)).unwrap());
        assert_eq!(1, lo.read(&mut buf).unwrap());
        assert_eq!(b'd', buf[0]);
        assert_eq!(8, lo.seek(SeekFrom::Current(-3)).unwrap());
        assert_eq!(1, lo.read(&mut buf).unwrap());
        assert_eq!(b'r', buf[0]);
    }

    #[test]
    fn test_write_with_read_fd() {
        use std::io::Write;

        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        let mut trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Read).unwrap();
        assert!(lo.write_all(b"hello world!!!").is_err());
    }

    #[test]
    fn test_truncate() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let mut conn = Client::connect(DB_URL, NoTls).unwrap();
        let mut trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();

        lo.truncate(5).unwrap();
        lo.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![];
        lo.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello");
        lo.truncate(10).unwrap();
        lo.seek(SeekFrom::Start(0)).unwrap();
        buf.clear();
        lo.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello\0\0\0\0\0");
    }
}
