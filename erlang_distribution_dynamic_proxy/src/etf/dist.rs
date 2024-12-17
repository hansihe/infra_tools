use std::io::Write;

use byteorder::{BigEndian, ReadBytesExt};
use nom::{
    bytes,
    combinator::map,
    multi::length_data,
    number::complete::{be_u16, be_u8},
    IResult, Parser as _,
};
use tokio_util::bytes::{Bytes, BytesMut};

use super::{consts::tag, p_version_tag};

#[derive(Debug)]
pub enum DistributionHeader {
    Normal {
        atom_cache_refs: Vec<AtomCacheRef>,
        /// If true, 2 bytes are used for atom lengths in messages.
        /// If false, 1 byte is used.
        long_atoms: bool,
        /// If present, subsequent continuation messages will be
        /// expected.
        fragment_state: Option<FragmentState>,
    },
    Continuation {
        fragment_state: FragmentState,
    },
}

#[derive(Debug)]
pub struct FragmentState {
    sequence_id: u64,
    fragment_id: u64,
}

impl DistributionHeader {
    pub fn p_dist_header(input: &[u8]) -> IResult<&[u8], DistributionHeader> {
        let (input, _) = p_version_tag(input)?;

        let (input, tag) = be_u8(input)?;

        match tag {
            // Distribution
            tag::DISTRIBUTION_HEADER => map(p_atom_cache_refs, |(long_atoms, refs)| {
                DistributionHeader::Normal {
                    atom_cache_refs: refs,
                    long_atoms,
                    fragment_state: None,
                }
            })
            .parse(input),
            tag::DISTRIBUTION_FRAGMENT_HEADER => todo!(),
            tag::DISTRIBUTION_FRAGMENT_CONT => todo!(),

            _ => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct AtomCacheRef {
    /// If Some, this is a cache slot update.
    pub value: Option<AtomData>,
    /// 10 bits segment index.
    pub segment_index: u16,
}

type AtomData = Bytes;

fn p_atom_cache_refs(input: &[u8]) -> IResult<&[u8], (bool, Vec<AtomCacheRef>)> {
    let (input, num_cache_refs) = be_u8(input)?;
    let num_cache_refs = num_cache_refs as usize;

    if num_cache_refs == 0 {
        Ok((input, (false, Vec::new())))
    } else {
        let (input, flags_buf) = bytes::complete::take((num_cache_refs / 2) + 1)(input)?;
        let get_flags = |idx: usize| {
            let val = flags_buf[idx >> 1];
            if idx & 1 == 0 {
                val & 0b1111
            } else {
                val >> 4
            }
        };

        let global_flags = get_flags(num_cache_refs);
        let long_atoms = global_flags & 0b1 == 1;

        let read_len = |i| {
            if long_atoms {
                map(be_u16, |v| v as usize)(i)
            } else {
                map(be_u8, |v| v as usize)(i)
            }
        };

        let mut refs = Vec::with_capacity(num_cache_refs);
        let mut input = input;
        for idx in 0..num_cache_refs {
            let flags = get_flags(idx);
            let new = flags & 0b1000 != 0;
            let mut seg = ((flags & 0b111) as u16) << 8;

            let (mut i, seg_lower) = be_u8(input)?;
            seg |= seg_lower as u16;

            let value = if new {
                let (ii, data) = length_data(read_len)(i)?;
                i = ii;
                let bytes: BytesMut = data.into();
                Some(bytes.into())
            } else {
                None
            };

            input = i;

            refs.push(AtomCacheRef {
                value,
                segment_index: seg,
            })
        }

        Ok((input, (long_atoms, refs)))
    }
}
