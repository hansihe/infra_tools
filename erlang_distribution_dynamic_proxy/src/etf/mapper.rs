use super::EtfTerm;

impl EtfTerm {
    pub fn map_term(&mut self, mapper: &mut impl FnMut(&mut EtfTerm)) {
        match self {
            EtfTerm::Nil => mapper(self),
            EtfTerm::Binary(_vec) => mapper(self),
            EtfTerm::String(_vec) => mapper(self),
            EtfTerm::Atom(_atom, _cache_ref) => mapper(self),
            EtfTerm::Integer(_) => mapper(self),
            EtfTerm::Float(_) => mapper(self),
            EtfTerm::BigInt(_big_int) => mapper(self),
            EtfTerm::Pid { node, .. } => {
                node.map_term(mapper);
                mapper(self);
            }
            EtfTerm::Reference { node, .. } => {
                node.map_term(mapper);
                mapper(self);
            }
            EtfTerm::FunExt {
                module,
                old_index,
                old_uniq,
                pid,
                free_vars,
                ..
            } => {
                module.map_term(mapper);
                old_index.map_term(mapper);
                old_uniq.map_term(mapper);
                pid.map_term(mapper);
                free_vars.iter_mut().for_each(|v| v.map_term(mapper));
                mapper(self);
            }
            EtfTerm::ExportExt {
                module,
                function,
                arity,
            } => {
                module.map_term(mapper);
                function.map_term(mapper);
                arity.map_term(mapper);
                mapper(self);
            }
            EtfTerm::Tuple(vec) => {
                vec.iter_mut().for_each(|v| v.map_term(mapper));
                mapper(self);
            }
            EtfTerm::Map(vec) => {
                vec.iter_mut().for_each(|(k, v)| {
                    k.map_term(mapper);
                    v.map_term(mapper);
                });
                mapper(self);
            }
            EtfTerm::List(vec, tail) => {
                vec.iter_mut().for_each(|v| v.map_term(mapper));
                tail.map_term(mapper);
                mapper(self);
            }
            EtfTerm::Port { node, .. } => {
                node.map_term(mapper);
                mapper(self);
            }
        }
    }
}
