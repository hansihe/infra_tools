use byteorder::WriteBytesExt as _;
use std::io::Write;

pub use atom_cache::AtomCache;
use dist::{AtomCacheRef, DistributionHeader};
use nom::{
    bytes::{self, streaming::tag},
    combinator::map,
    multi::{count, length_count, length_data},
    number::{
        complete::{be_i32, be_u16, be_u64, be_u8},
        streaming::{be_f64, be_u32},
    },
    sequence::tuple,
    AsBytes, IResult, Parser,
};

mod atom_cache;
pub mod consts;
pub mod dist;
mod mapper;

pub use consts::ctrl::ControlMessage;
use consts::tag;

pub trait TermBackend {
    type Term;
    fn atom_cache_ref(&self, num: u8) -> Self::Term;
}

pub struct StandaloneEtfTerm;
impl TermBackend for StandaloneEtfTerm {
    type Term = EtfTerm;
    fn atom_cache_ref(&self, _num: u8) -> Self::Term {
        panic!("atom cache ref not supported in standalone term")
    }
}

pub struct DistMessageEtfTerm<'a> {
    cache: &'a AtomCache,
    refs: &'a [AtomCacheRef],
}
impl<'a> TermBackend for DistMessageEtfTerm<'a> {
    type Term = EtfTerm;
    fn atom_cache_ref(&self, num: u8) -> Self::Term {
        let term_ref = &self.refs[num as usize];
        EtfTerm::Atom(
            Atom(self.cache[term_ref.segment_index].clone().into()),
            Some(num),
        )
    }
}

pub struct Atom(pub Vec<u8>);
impl std::fmt::Debug for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(string) = std::str::from_utf8(self.0.as_bytes()).ok() {
            write!(f, ":{:?}", string)
        } else {
            write!(f, "Atom({:?})", self.0)
        }
    }
}

#[derive(Debug)]
pub struct BigInt {
    sign: u8,
    data: Vec<u8>,
}

pub enum EtfTerm {
    // Terminals
    Nil,
    Binary(Vec<u8>),
    String(Vec<u8>),
    Atom(Atom, Option<u8>),
    Integer(i32),
    Float(f64),
    BigInt(BigInt),

    // Reference types
    Pid {
        node: Box<EtfTerm>,
        id: u32,
        serial: u32,
        creation: u32,
    },
    Port {
        node: Box<EtfTerm>,
        id: u64,
        creation: u32,
    },
    Reference {
        node: Box<EtfTerm>,
        creation: u32,
        id: Vec<u32>,
    },
    FunExt {
        arity: u8,
        uniq: u128,
        index: u32,
        module: Box<EtfTerm>,
        old_index: Box<EtfTerm>,
        old_uniq: Box<EtfTerm>,
        pid: Box<EtfTerm>,
        free_vars: Vec<EtfTerm>,
    },
    ExportExt {
        module: Box<EtfTerm>,
        function: Box<EtfTerm>,
        arity: Box<EtfTerm>,
    },

    // Compounds
    Tuple(Vec<EtfTerm>),
    Map(Vec<(EtfTerm, EtfTerm)>),
    List(Vec<EtfTerm>, Box<EtfTerm>),
}

impl std::fmt::Debug for EtfTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EtfTerm::Nil => write!(f, "[]"),
            EtfTerm::Binary(vec) => write!(f, "{:?}", vec),
            EtfTerm::String(vec) => write!(f, "{:?}", vec),
            EtfTerm::Atom(atom, _) => write!(f, "{:?}", atom),
            EtfTerm::Integer(int) => write!(f, "{}", int),
            EtfTerm::Float(flt) => write!(f, "{}", flt),
            EtfTerm::BigInt(big_int) => write!(f, "{:?}", big_int),
            EtfTerm::Pid {
                node,
                id,
                serial,
                creation,
            } => write!(f, "#PID<{:?}.{}.{}.{}>", node, creation, id, serial),
            EtfTerm::Port { node, id, creation } => {
                write!(f, "#Port<{:?}.{}.{}>", node, id, creation)
            }
            EtfTerm::Reference { node, creation, id } => {
                write!(f, "#Reference<{:?}.{:?}.{}>", node, id, creation)
            }
            EtfTerm::FunExt {
                arity,
                uniq,
                index,
                module,
                pid,
                free_vars,
                ..
            } => write!(
                f,
                "Fun(&{:?}.{}/{}, {}, {:?}, {:?})",
                module, index, arity, uniq, pid, free_vars
            ),
            EtfTerm::ExportExt {
                module,
                function,
                arity,
            } => write!(f, "&{:?}.{:?}/{:?}", module, function, arity),
            EtfTerm::Tuple(vec) => f.debug_set().entries(vec).finish(),
            EtfTerm::Map(vec) => f
                .debug_map()
                .entries(vec.iter().map(|(k, v)| (k, v)))
                .finish(),
            EtfTerm::List(vec, etf_term) => f
                .debug_list()
                .entries(vec.iter())
                .entry(&**etf_term)
                .finish(),
        }
    }
}

pub struct DistMessage<'a> {
    pub header: DistributionHeader,
    pub header_slice: &'a [u8],
    pub control_typ: ControlMessage,
    pub control: EtfTerm,
    pub control_slice: &'a [u8],
    pub arg: Option<EtfTerm>,
    pub arg_slice: &'a [u8],
}

impl<'a> std::fmt::Debug for DistMessage<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistMessage")
            .field("header", &self.header)
            .field("control_typ", &self.control_typ)
            .field("control", &self.control)
            .field("arg", &self.arg)
            .finish()
    }
}

pub fn p_dist_message<'a>(
    i1: &'a [u8],
    atom_cache: &mut AtomCache,
) -> IResult<&'a [u8], DistMessage<'a>> {
    let (i2, dist_header) = DistributionHeader::p_dist_header(i1)?;
    let dist_header_slice = &i1[0..(i1.len() - i2.len())];

    let refs = match &dist_header {
        DistributionHeader::Normal {
            atom_cache_refs,
            fragment_state,
            ..
        } => {
            atom_cache.update(&*atom_cache_refs);
            assert!(fragment_state.is_none());
            atom_cache_refs
        }
        DistributionHeader::Continuation { fragment_state } => {
            todo!()
        }
    };

    let proto_state = DistMessageEtfTerm {
        cache: &*atom_cache,
        refs,
    };
    let (i3, cmd) = EtfTerm::p_term(i2, &proto_state)?;
    let cmd_slice = &i2[0..(i2.len() - i3.len())];

    let cmd_tup = cmd.tuple().unwrap();
    let ctrl_int = cmd_tup[0].integer().unwrap();
    let ctrl = ControlMessage::from_u8(ctrl_int as u8).unwrap();

    let (i5, term, term_slice) = if i3.len() > 0 {
        let (i4, term) = EtfTerm::p_term(i3, &proto_state)?;
        let term_slice = &i3[0..(i3.len() - i4.len())];
        (i4, Some(term), term_slice)
    } else {
        (i3, None, i3)
    };

    let message = DistMessage {
        header: dist_header,
        header_slice: dist_header_slice,
        control_typ: ctrl,
        control: cmd,
        control_slice: cmd_slice,
        arg: term,
        arg_slice: term_slice,
    };

    Ok((i5, message))
}

impl EtfTerm {
    pub fn tuple(&self) -> Option<&[EtfTerm]> {
        match self {
            EtfTerm::Tuple(tup) => Some(tup),
            _ => None,
        }
    }

    pub fn integer(&self) -> Option<i32> {
        match self {
            EtfTerm::Integer(val) => Some(*val),
            _ => None,
        }
    }

    pub fn p_versioned_term<'a>(
        i: &'a [u8],
        state: &impl TermBackend<Term = Self>,
    ) -> IResult<&'a [u8], Self> {
        let (i, _) = p_version_tag(i)?;
        Self::p_term(i, state)
    }

    pub fn p_term<'a>(
        input: &'a [u8],
        state: &impl TermBackend<Term = Self>,
    ) -> IResult<&'a [u8], EtfTerm> {
        let p_term = |i| Self::p_term(i, state);

        let (input, tag) = be_u8(input)?;
        match tag {
            tag::NIL_EXT => Ok((input, Self::Nil)),

            // Atoms
            tag::ATOM_CACHE_REF => be_u8.map(|v| state.atom_cache_ref(v)).parse(input),
            tag::SMALL_ATOM_UTF8_EXT => length_data(be_u8)
                .map(|v: &[u8]| Self::Atom(Atom(v.to_vec()), None))
                .parse(input),
            tag::ATOM_UTF8_EXT => length_data(be_u8)
                .map(|v: &[u8]| Self::Atom(Atom(v.to_vec()), None))
                .parse(input),

            // Terminal sequences
            tag::BINARY_EXT => length_data(be_u32)
                .map(|v: &[u8]| Self::Binary(v.to_vec()))
                .parse(input),
            tag::STRING_EXT => length_data(be_u16)
                .map(|v: &[u8]| Self::String(v.to_vec()))
                .parse(input),

            // Numbers
            tag::SMALL_INTEGER_EXT => be_u8.map(|v| Self::Integer(v as i32)).parse(input),
            tag::INTEGER_EXT => be_i32.map(Self::Integer).parse(input),
            tag::FLOAT_EXT => todo!(),
            tag::NEW_FLOAT_EXT => be_f64.map(Self::Float).parse(input),
            tag::SMALL_BIG_EXT => {
                let (i, (n, sign)) = tuple((be_u8, be_u8)).parse(input)?;
                let (i, data) = bytes::complete::take(n as usize)(i)?;
                Ok((
                    i,
                    EtfTerm::BigInt(BigInt {
                        sign,
                        data: data.to_vec(),
                    }),
                ))
            }
            tag::LARGE_BIG_EXT => {
                let (i, (n, sign)) = tuple((be_u32, be_u8)).parse(input)?;
                let (i, data) = bytes::complete::take(n as usize)(i)?;
                Ok((
                    i,
                    EtfTerm::BigInt(BigInt {
                        sign,
                        data: data.to_vec(),
                    }),
                ))
            }
            tag::BIT_BINARY_EXT => todo!(),

            // Reference types
            tag::PID_EXT => todo!(),
            tag::NEW_PID_EXT => tuple((p_term, be_u32, be_u32, be_u32))
                .map(|(node, id, serial, creation)| EtfTerm::Pid {
                    node: Box::new(node),
                    id,
                    serial,
                    creation,
                })
                .parse(input),
            tag::REFERENCE_EXT => todo!(),
            tag::NEW_REFERENCE_EXT => todo!(),
            tag::NEWER_REFERENCE_EXT => {
                let (input, (len, node, creation)) =
                    tuple((be_u16, p_term, be_u32)).parse(input)?;
                let (input, id) = count(be_u32, len as usize)(input)?;
                Ok((
                    input,
                    EtfTerm::Reference {
                        node: Box::new(node),
                        creation,
                        id,
                    },
                ))
            }
            tag::PORT_EXT => todo!(),
            tag::NEW_PORT_EXT => tuple((p_term, be_u32, be_u32))
                .map(|(node, id, creation)| EtfTerm::Port {
                    node: Box::new(node),
                    id: id.into(),
                    creation,
                })
                .parse(input),
            tag::V4_PORT_EXT => tuple((p_term, be_u64, be_u32))
                .map(|(node, id, creation)| EtfTerm::Port {
                    node: Box::new(node),
                    id,
                    creation,
                })
                .parse(input),

            tag::NEW_FUN_EXT => {
                let (i, (_size, arity, uniq, index, num_free, module, old_index, old_uniq, pid)) =
                    tuple((
                        be_u32,
                        be_u8,
                        bytes::complete::take(16usize),
                        be_u32,
                        be_u32,
                        p_term,
                        p_term,
                        p_term,
                        p_term,
                    ))(input)?;
                let (i, free_vars) = count(p_term, num_free as usize)(i)?;
                Ok((
                    i,
                    EtfTerm::FunExt {
                        arity,
                        uniq: u128::from_be_bytes(uniq.try_into().unwrap()),
                        index,
                        module: Box::new(module),
                        old_index: Box::new(old_index),
                        old_uniq: Box::new(old_uniq),
                        pid: Box::new(pid),
                        free_vars,
                    },
                ))
            }
            tag::EXPORT_EXT => tuple((p_term, p_term, p_term))
                .map(|(module, function, arity)| EtfTerm::ExportExt {
                    module: Box::new(module),
                    function: Box::new(function),
                    arity: Box::new(arity),
                })
                .parse(input),
            tag::LOCAL_EXT => todo!(),

            // Compound data structures
            tag::SMALL_TUPLE_EXT => length_count(be_u8, p_term).map(Self::Tuple).parse(input),
            tag::LARGE_TUPLE_EXT => length_count(be_u32, p_term).map(Self::Tuple).parse(input),
            tag::MAP_EXT => length_count(be_u32, tuple((p_term, p_term)))
                .map(Self::Map)
                .parse(input),
            tag::LIST_EXT => tuple((length_count(be_u32, p_term), p_term))
                .map(|(elems, tail)| Self::List(elems, Box::new(tail)))
                .parse(input),
            _ => todo!(),
        }
    }

    pub fn write(&self, w: &mut impl Write) -> std::io::Result<()> {
        match self {
            EtfTerm::Nil => w.write_u8(consts::tag::NIL_EXT)?,
            EtfTerm::Binary(vec) => {
                w.write_u8(consts::tag::BINARY_EXT)?;
                w.write_u32::<byteorder::BigEndian>(vec.len() as u32)?;
                w.write_all(vec)?;
            }
            EtfTerm::String(vec) => {
                w.write_u8(consts::tag::STRING_EXT)?;
                w.write_u16::<byteorder::BigEndian>(vec.len() as u16)?;
                w.write_all(vec)?;
            }
            EtfTerm::Atom(_atom, Some(num)) => {
                w.write_u8(consts::tag::ATOM_CACHE_REF)?;
                w.write_u8(*num)?;
            }
            EtfTerm::Atom(atom, None) => {
                if atom.0.len() <= 255 {
                    w.write_u8(consts::tag::SMALL_ATOM_UTF8_EXT)?;
                    w.write_u8(atom.0.len() as u8)?;
                } else {
                    w.write_u8(consts::tag::ATOM_UTF8_EXT)?;
                    w.write_u16::<byteorder::BigEndian>(atom.0.len() as u16)?;
                }
                w.write_all(&atom.0)?;
            }
            //EtfTerm::AtomCacheRef(num) => {
            //    w.write_u8(consts::tag::ATOM_CACHE_REF)?;
            //    w.write_u8(*num)?;
            //}
            EtfTerm::Integer(num) => {
                if *num >= 0 && *num <= 255 {
                    w.write_u8(consts::tag::SMALL_INTEGER_EXT)?;
                    w.write_u8(*num as u8)?;
                } else {
                    w.write_u8(consts::tag::INTEGER_EXT)?;
                    w.write_i32::<byteorder::BigEndian>(*num)?;
                }
            }
            EtfTerm::Float(num) => {
                w.write_u8(consts::tag::NEW_FLOAT_EXT)?;
                w.write_f64::<byteorder::BigEndian>(*num)?;
            }
            EtfTerm::BigInt(big_int) => {
                if big_int.data.len() <= 255 {
                    w.write_u8(consts::tag::SMALL_BIG_EXT)?;
                    w.write_u8(big_int.data.len() as u8)?;
                } else {
                    w.write_u8(consts::tag::LARGE_BIG_EXT)?;
                    w.write_u32::<byteorder::BigEndian>(big_int.data.len() as u32)?;
                }
                w.write_u8(big_int.sign)?;
                w.write_all(&big_int.data)?;
            }
            EtfTerm::Pid {
                node,
                id,
                serial,
                creation,
            } => {
                w.write_u8(consts::tag::NEW_PID_EXT)?;
                node.write(w)?;
                w.write_u32::<byteorder::BigEndian>(*id)?;
                w.write_u32::<byteorder::BigEndian>(*serial)?;
                w.write_u32::<byteorder::BigEndian>(*creation)?;
            }
            EtfTerm::Port { node, id, creation } => {
                // https://github.com/erlang/otp/blob/cbe831300ee8300fe6c0280ff3f0eb6ef6e37eb7/lib/erl_interface/src/encode/encode_port.c#L38
                // 28 bits
                if *id > 0x0fffffff {
                    w.write_u8(consts::tag::V4_PORT_EXT)?;
                    node.write(w)?;
                    w.write_u64::<byteorder::BigEndian>(*id)?;
                    w.write_u32::<byteorder::BigEndian>(*creation)?;
                } else {
                    w.write_u8(consts::tag::NEW_PORT_EXT)?;
                    node.write(w)?;
                    w.write_u32::<byteorder::BigEndian>(*id as u32)?;
                    w.write_u32::<byteorder::BigEndian>(*creation)?;
                }
            }
            EtfTerm::Reference { node, creation, id } => {
                w.write_u8(consts::tag::NEWER_REFERENCE_EXT)?;
                w.write_u16::<byteorder::BigEndian>(id.len() as u16)?;
                node.write(w)?;
                w.write_u32::<byteorder::BigEndian>(*creation)?;
                for id_part in id {
                    w.write_u32::<byteorder::BigEndian>(*id_part)?;
                }
            }
            EtfTerm::FunExt {
                arity,
                uniq,
                index,
                module,
                old_index,
                old_uniq,
                pid,
                free_vars,
            } => {
                let mut subbuf = Vec::new();
                subbuf.write_u8(*arity)?;
                subbuf.write_all(&uniq.to_be_bytes())?;
                subbuf.write_u32::<byteorder::BigEndian>(*index)?;
                subbuf.write_u32::<byteorder::BigEndian>(free_vars.len() as u32)?;
                module.write(&mut subbuf)?;
                old_index.write(&mut subbuf)?;
                old_uniq.write(&mut subbuf)?;
                pid.write(&mut subbuf)?;
                for var in free_vars {
                    var.write(&mut subbuf)?;
                }

                w.write_u8(consts::tag::NEW_FUN_EXT)?;
                w.write_u32::<byteorder::BigEndian>(subbuf.len() as u32)?;
                w.write_all(&subbuf)?;
            }
            EtfTerm::ExportExt {
                module,
                function,
                arity,
            } => {
                w.write_u8(consts::tag::EXPORT_EXT)?;
                module.write(w)?;
                function.write(w)?;
                arity.write(w)?;
            }
            EtfTerm::Tuple(vec) => {
                if vec.len() <= 255 {
                    w.write_u8(consts::tag::SMALL_TUPLE_EXT)?;
                    w.write_u8(vec.len() as u8)?;
                } else {
                    w.write_u8(consts::tag::LARGE_TUPLE_EXT)?;
                    w.write_u32::<byteorder::BigEndian>(vec.len() as u32)?;
                }
                for elem in vec {
                    elem.write(w)?;
                }
            }
            EtfTerm::Map(vec) => {
                w.write_u8(consts::tag::MAP_EXT)?;
                w.write_u32::<byteorder::BigEndian>(vec.len() as u32)?;
                for (key, value) in vec {
                    key.write(w)?;
                    value.write(w)?;
                }
            }
            EtfTerm::List(vec, etf_term) => {
                w.write_u8(consts::tag::LIST_EXT)?;
                w.write_u32::<byteorder::BigEndian>(vec.len() as u32)?;
                for elem in vec {
                    elem.write(w)?;
                }
                etf_term.write(w)?;
            }
        }
        Ok(())
    }
}

pub fn p_version_tag(input: &[u8]) -> IResult<&[u8], ()> {
    map(tag([consts::VERSION_TAG]), |_| ())(input)
}
