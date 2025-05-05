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

extern crate tokio_postgres;

use std::cmp;
use std::fmt;
use std::i32;
use std::io::{self, Write};
use tokio_postgres::Error;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;
use tokio_postgres::types::Oid;

/// An extension trait adding functionality to create and delete large objects.
pub trait LargeObjectExt {
    /// Creates a new large object, returning its `Oid`.
    async fn create_large_object(&mut self) -> Result<Oid, Error>;

    /// Deletes the large object with the specified `Oid`.
    async fn delete_large_object(&mut self, oid: Oid) -> Result<(), Error>;
}

impl<T: GenericClient> LargeObjectExt for T {
    async fn create_large_object(&mut self) -> Result<Oid, Error> {
        let stmt = self.prepare("SELECT pg_catalog.lo_create(0)").await?;
        let r = self.query_one(&stmt, &[]).await.map(|r| r.get(0));
        r
    }

    async fn delete_large_object(&mut self, oid: Oid) -> Result<(), Error> {
        let stmt = self.prepare("SELECT pg_catalog.lo_unlink($1)").await?;
        self.execute(&stmt, &[&oid]).await.map(|_| ())
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
    async fn open_large_object<'b>(
        &'b mut self,
        oid: Oid,
        mode: Mode,
    ) -> Result<LargeObject<'conn, 'b>, Error>
    where
        'conn: 'b;
}

impl<'conn> LargeObjectTransactionExt<'conn> for Transaction<'conn> {
    async fn open_large_object<'b>(
        &'b mut self,
        oid: Oid,
        mode: Mode,
    ) -> Result<LargeObject<'conn, 'b>, Error>
    where
        'conn: 'b,
    {
        let stmt = self.prepare("SELECT pg_catalog.lo_open($1, $2)").await?;
        let fd = self
            .query(&stmt, &[&oid, &mode.to_i32()])
            .await?
            .iter()
            .next()
            .unwrap()
            .get(0);
        Ok(LargeObject {
            trans: self,
            fd,
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
    pub async fn truncate(&mut self, len: i64) -> Result<(), Error> {
        let stmt = self
            .trans
            .prepare("SELECT pg_catalog.lo_truncate64($1, $2)")
            .await?;

        self.trans
            .execute(&stmt, &[&self.fd, &len])
            .await
            .map(|_| ())
    }

    async fn finish_inner(&mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }

        self.finished = true;
        let stmt = self.trans.prepare("SELECT pg_catalog.lo_close($1)").await?;
        self.trans.execute(&stmt, &[&self.fd]).await.map(|_| ())
    }

    /// Consumes the `LargeObject`, cleaning up server side state.
    ///
    /// Functionally identical to the `Drop` implementation on `LargeObject`
    /// except that it returns any errors to the caller.
    pub async fn finish(mut self) -> Result<(), Error> {
        self.finish_inner().await
    }
}

pub trait AsyncReadLargeObject {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
}

impl<'a, 'b> AsyncReadLargeObject for LargeObject<'a, 'b> {
    async fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let stmt = self
            .trans
            .prepare("SELECT pg_catalog.loread($1, $2)")
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let cap = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let rows = self
            .trans
            .query(&stmt, &[&self.fd, &cap])
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        buf.write(rows.get(0).unwrap().get(0))
    }
}

pub trait AsyncWriteLargeObject {
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize>;
}

impl<'a, 'b> AsyncWriteLargeObject for LargeObject<'a, 'b> {
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let stmt = self
            .trans
            .prepare("SELECT pg_catalog.lowrite($1, $2)")
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let cap = cmp::min(buf.len(), i32::MAX as usize);
        self.trans
            .execute(&stmt, &[&self.fd, &&buf[..cap]])
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(cap)
    }
}

pub trait AsyncSeekLargeObject {
    async fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64>;
}

impl<'a, 'b> AsyncSeekLargeObject for LargeObject<'a, 'b> {
    async fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
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
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let rows = self
            .trans
            .query(&stmt, &[&self.fd, &pos, &kind.to_owned()])
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let pos: i64 = rows.first().unwrap().get(0);
        Ok(pos as u64)
    }
}
