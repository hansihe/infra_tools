use anyhow::Context;
use deku::prelude::*;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big", magic = b"N")]
pub struct SendName {
    pub flags: u64,
    pub creation: u32,
    pub name: PString,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big", magic = b"sok")]
pub struct RecvStatus {}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big", magic = b"snamed:")]
pub struct RecvStatusNamed {
    pub name: PString,
    pub creation: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "big", magic = b"N")]
pub struct RecvChallenge {
    pub flags: u64,
    pub challenge: u32,
    pub creation: u32,
    pub name: PString,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big", magic = b"r")]
pub struct SendChallengeReply {
    pub challenge: u32,
    pub digest: [u8; 16],
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "big", magic = b"a")]
pub struct RecvChallengeAck {
    pub digest: [u8; 16],
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")]
pub struct PString {
    pub len: u16,
    #[deku(count = "len")]
    pub data: Vec<u8>,
}

impl PString {
    pub fn new(string: &str) -> Self {
        PString {
            len: string.bytes().len() as u16,
            data: string.to_owned().into_bytes(),
        }
    }
    pub fn as_str(&self) -> anyhow::Result<&str> {
        Ok(std::str::from_utf8(&self.data).context("was not valid string data")?)
    }
}

pub async fn read_framed(mut socket: impl AsyncRead + Unpin) -> anyhow::Result<Vec<u8>> {
    let len = socket.read_u16().await?;
    let mut data = vec![0; len as usize];
    socket.read_exact(&mut data).await?;
    Ok(data)
}

pub async fn write_framed(mut socket: impl AsyncWrite + Unpin, data: &[u8]) -> anyhow::Result<()> {
    socket.write_u16(data.len() as u16).await?;
    socket.write_all(data).await?;
    Ok(())
}
