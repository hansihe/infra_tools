use std::ops::Index;

use tokio_util::bytes::Bytes;

use super::dist::AtomCacheRef;

pub struct AtomCache {
    atoms: Vec<Option<Bytes>>,
}

impl AtomCache {
    pub fn new() -> Self {
        AtomCache {
            atoms: vec![None; 2048],
        }
    }

    pub fn update(&mut self, refs: &[AtomCacheRef]) {
        for acr in refs.iter() {
            if let Some(value) = &acr.value {
                self.atoms[acr.segment_index as usize] = Some(value.clone());
            }
        }
    }
}

impl Index<u16> for AtomCache {
    type Output = Bytes;
    fn index(&self, index: u16) -> &Self::Output {
        self.atoms[index as usize].as_ref().unwrap()
    }
}
