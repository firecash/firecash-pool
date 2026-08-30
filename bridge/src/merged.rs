//! Merged-mining (AuxPoW) support for the stratum bridge — the pool-side engine that
//! lets ASICs merge-mine ZKas.
//!
//! It plugs in as a decorator around [`crate::kaspaapi::KaspaApi`] so the entire
//! stratum/job/share-validation core stays untouched:
//!
//! * **get_block_template** — fetch a ZKas template, then hand the ASIC a
//!   *parent* (Kaspa-shaped) block whose single coinbase commits to the ZKas
//!   block hash `H_fc` and whose target (`bits`) is the ZKas target. The ASIC
//!   grinds the parent's kHeavyHash exactly as it would a normal job.
//! * **submit_block** — when a share clears the target, the "block" the bridge hands
//!   back is that solved parent. We rebuild the [`AuxPow`] from it and submit the
//!   *ZKas* block carrying the aux proof (the node accepts it via Option-2 dual
//!   acceptance). Because the aux rides `RpcRawHeader.aux_pow`, the existing
//!   `(&block).into()` submit path transmits it unchanged.
//!
//! In this first version the parent is synthetic (self-generated), which proves the
//! ASIC→aux path end to end. Swapping the synthetic parent for a live Kaspa
//! `getBlockTemplate` (and also submitting winning parents to Kaspa) is what adds the
//! second-chain reward — the struct is identical, only the parent's source changes.

use std::collections::{HashMap, HashSet, VecDeque};

use kaspa_consensus_core::{
    auxpow::AuxPow, block::Block, hashing, header::Header, merkle::calc_hash_merkle_root, subnets::SUBNETWORK_ID_COINBASE,
    tx::Transaction,
};
use kaspa_hashes::{Hash, ZERO_HASH};

/// Build the coinbase (leaf 0) Merkle inclusion branch for a *real* multi-tx parent,
/// i.e. the sequence of right-sibling hashes from the coinbase up to the parent's
/// `hash_merkle_root`. Reproduces Kaspa's tx Merkle tree
/// ([`kaspa_consensus_core::merkle::calc_hash_merkle_root`]): a full binary tree padded
/// to the next power of two, where a wholly-absent right subtree folds against
/// `ZERO_HASH` (matching `calc_merkle_root_with_hasher`'s `unwrap_or(ZERO_HASH)`).
///
/// The coinbase is always leaf 0, hence the left child at every level, so the branch is
/// exactly what [`AuxPow::verify_coinbase_inclusion`] folds (`acc = merkle_hash(acc,
/// sibling)`). Returns an empty branch for a single-tx parent (coinbase is the root).
pub fn coinbase_merkle_branch(txs: &[Transaction]) -> Vec<Hash> {
    if txs.len() <= 1 {
        return vec![];
    }
    // Leaves = per-tx hashes, padded to a power of two with `None` (absent leaves).
    let mut level: Vec<Option<Hash>> = txs.iter().map(|t| Some(hashing::tx::hash(t))).collect();
    level.resize(level.len().next_power_of_two(), None);

    let mut branch = Vec::new();
    let mut idx = 0usize; // coinbase index; stays 0 (leftmost) all the way up
    while level.len() > 1 {
        // Right sibling of the coinbase path. An absent subtree contributes ZERO_HASH,
        // exactly as consensus folds it.
        branch.push(level[idx ^ 1].unwrap_or(ZERO_HASH));
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks(2) {
            let combined = pair[0].map(|l| kaspa_merkle::merkle_hash(l, pair.get(1).copied().flatten().unwrap_or(ZERO_HASH)));
            next.push(combined);
        }
        idx /= 2;
        level = next;
    }
    branch
}

/// Build the parent block an ASIC hashes in merged mode: one coinbase committing to
/// `H_fc`, with the ZKas target. Returns `(parent_block, h_fc)`.
pub fn build_parent_block(fc_block: &Block) -> (Block, Hash) {
    let h_fc = fc_block.header.hash;
    let coinbase = Transaction::new(0, vec![], vec![], 0, SUBNETWORK_ID_COINBASE, 0, AuxPow::embed_commitment(&[], h_fc, &[]));
    let hash_merkle_root = calc_hash_merkle_root(std::iter::once(&coinbase));

    let mut parent = Header::from_precomputed_hash(ZERO_HASH, vec![Hash::from_u64_word(0xF12E_CA54)]);
    parent.hash_merkle_root = hash_merkle_root;
    parent.bits = fc_block.header.bits; // ASIC grinds against the ZKas target
    parent.timestamp = fc_block.header.timestamp;
    parent.finalize();

    (Block::new(parent, vec![coinbase]), h_fc)
}

/// The `H_fc` a parent block commits to (from its coinbase), or `None` if it carries
/// no valid commitment (i.e. this wasn't a merged-mining parent).
pub fn committed_h_fc(parent_block: &Block) -> Option<Hash> {
    let coinbase = parent_block.transactions.first()?.clone();
    AuxPow { parent_header: (*parent_block.header).clone(), parent_coinbase: coinbase, coinbase_merkle_branch: vec![] }
        .committed_hash()
}

/// Assemble the ZKas block carrying the AuxPoW proof, from the solved `parent_block`
/// (the bridge has set the winning nonce on its header) and the stashed `fc_block`.
///
/// Builds the *real* coinbase Merkle branch from the parent's transactions, so this is
/// correct for real multi-tx Kaspa parents (not just the single-tx synthetic case).
pub fn assemble_aux_block(parent_block: &Block, fc_block: &Block) -> Block {
    let coinbase = parent_block.transactions[0].clone();
    let branch = coinbase_merkle_branch(&parent_block.transactions);
    let aux = AuxPow { parent_header: (*parent_block.header).clone(), parent_coinbase: coinbase, coinbase_merkle_branch: branch };
    let fc_header = (*fc_block.header).clone().with_aux_pow(aux);
    Block::new(fc_header, (*fc_block.transactions).clone())
}

/// A small bounded map from `H_fc` to the ZKas block awaiting a solved parent.
/// Bounded FIFO so a busy pool that never solves a given template doesn't grow it
/// without limit.
pub struct MergedPending {
    map: HashMap<Hash, Block>,
    /// Lanes whose Kaspa parent was built paying the POOL rather than the miner,
    /// i.e. the lane was inside its pool-fee minute when the template was cut.
    ///
    /// Recorded at TEMPLATE-BUILD time and read back at submit time, deliberately.
    /// The alternative — asking "are we in a fee minute?" when the share arrives —
    /// is wrong at the boundary in both directions: a share solved against a
    /// miner-paying template but submitted a moment after the window opens would be
    /// reported as a pool block (miner robbed of the credit), and one solved inside
    /// the window but submitted after it closes would be credited to the miner even
    /// though the coinbase paid the pool (books wrong). Attribution has to follow the
    /// template the work was actually done against, so it rides with the template.
    fee_lane: HashSet<Hash>,
    order: VecDeque<Hash>,
    solved: HashSet<Hash>,
    cap: usize,
}

impl MergedPending {
    pub fn new(cap: usize) -> Self {
        Self { map: HashMap::new(), fee_lane: HashSet::new(), order: VecDeque::new(), solved: HashSet::new(), cap: cap.max(1) }
    }

    pub fn insert(&mut self, h_fc: Hash, fc_block: Block) {
        self.insert_with_payee(h_fc, fc_block, false)
    }

    /// Insert, recording whether this lane's Kaspa coinbase paid the pool
    /// (`paid_pool = true`, the fee minute) or the miner.
    pub fn insert_with_payee(&mut self, h_fc: Hash, fc_block: Block, paid_pool: bool) {
        if self.map.insert(h_fc, fc_block).is_none() {
            self.order.push_back(h_fc);
            while self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                    self.solved.remove(&old);
                    self.fee_lane.remove(&old);
                }
            }
        }
        // Set unconditionally: a re-insert for the same H_fc must not keep a stale
        // payee flag from an earlier template generation.
        if paid_pool {
            self.fee_lane.insert(h_fc);
        } else {
            self.fee_lane.remove(&h_fc);
        }
    }

    /// Did the Kaspa parent for this lane pay the pool instead of the miner?
    /// Unknown lanes answer `false` — a block we cannot attribute is credited to the
    /// miner, never quietly to the pool.
    pub fn paid_pool(&self, h_fc: &Hash) -> bool {
        self.fee_lane.contains(h_fc)
    }

    pub fn get(&self, h_fc: &Hash) -> Option<Block> {
        self.map.get(h_fc).cloned()
    }

    pub fn claim_solution(&mut self, h_fc: Hash) -> bool {
        self.map.contains_key(&h_fc) && self.solved.insert(h_fc)
    }

    pub fn is_unsolved(&self, h_fc: &Hash) -> bool {
        self.map.contains_key(h_fc) && !self.solved.contains(h_fc)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn consensus_uses_zkas_merge_mining_magic() {
        assert_eq!(kaspa_consensus_core::auxpow::MERGE_MINE_MAGIC, *b"ZKMM");
    }

    fn coinbase(h_fc: Hash) -> Transaction {
        Transaction::new(0, vec![], vec![], 0, SUBNETWORK_ID_COINBASE, 0, AuxPow::embed_commitment(&[1, 2, 3], h_fc, &[9]))
    }
    fn tx(tag: u8) -> Transaction {
        use kaspa_consensus_core::subnets::SUBNETWORK_ID_NATIVE;
        Transaction::new(0, vec![], vec![], 0, SUBNETWORK_ID_NATIVE, 0, vec![tag; 16])
    }

    #[test]
    fn h_fc_solution_can_be_claimed_only_once() {
        let block = Block::new(Header::from_precomputed_hash(Hash::from_bytes([7; 32]), vec![]), vec![]);
        let h_fc = block.header.hash;
        let mut pending = MergedPending::new(4);
        pending.insert(h_fc, block);

        assert!(pending.is_unsolved(&h_fc));
        assert!(pending.claim_solution(h_fc));
        assert!(!pending.is_unsolved(&h_fc));
        assert!(!pending.claim_solution(h_fc));
    }

    #[test]
    fn evicting_pending_work_also_evicts_solved_marker() {
        let first = Block::new(Header::from_precomputed_hash(Hash::from_bytes([1; 32]), vec![]), vec![]);
        let second = Block::new(Header::from_precomputed_hash(Hash::from_bytes([2; 32]), vec![]), vec![]);
        let first_hash = first.header.hash;
        let second_hash = second.header.hash;
        let mut pending = MergedPending::new(1);
        pending.insert(first_hash, first);
        assert!(pending.claim_solution(first_hash));
        pending.insert(second_hash, second);

        assert!(!pending.is_unsolved(&first_hash));
        assert!(!pending.claim_solution(first_hash));
        assert!(pending.is_unsolved(&second_hash));
    }

    /// For every parent size 1..=9, the branch we build must fold the coinbase back to
    /// the canonical `calc_hash_merkle_root`, i.e. `verify_coinbase_inclusion` passes —
    /// proving the branch matches Kaspa's real tx Merkle tree, not just the 1-tx case.
    #[test]
    fn coinbase_branch_matches_real_merkle_root_for_all_sizes() {
        let h_fc = Hash::from_bytes([0x5Au8; 32]);
        for n in 1..=9usize {
            let mut txs = vec![coinbase(h_fc)];
            for i in 1..n {
                txs.push(tx(i as u8));
            }
            let root = calc_hash_merkle_root(txs.iter());
            let mut header = Header::from_precomputed_hash(ZERO_HASH, vec![]);
            header.hash_merkle_root = root;
            let aux = AuxPow {
                parent_header: header,
                parent_coinbase: txs[0].clone(),
                coinbase_merkle_branch: coinbase_merkle_branch(&txs),
            };
            assert!(aux.verify_coinbase_inclusion(), "branch must reproduce the root for n={n} txs");
            assert!(aux.verify_binding(h_fc), "full binding (commitment + inclusion) must hold for n={n}");
        }
    }
}

/// How many minutes of each hour a lane's KAS rewards go to the pool instead of the
/// miner. One in sixty ≈ 1.67%.
pub const POOL_FEE_MINUTES_PER_HOUR: u64 = 1;

/// Whether this lane is inside its pool-fee minute right now.
///
/// # Why the window is staggered per lane, not global
///
/// The obvious implementation is "minute 0 of every hour, for everyone". That makes
/// the entire fleet switch its Kaspa coinbase at the same instant: every lane needs a
/// fresh parent template in the same second, the pool's KAS income arrives in one
/// lumpy burst per hour, and any bug in the switch fires fleet-wide simultaneously.
///
/// Hashing the lane identity spreads the fee minutes uniformly across the hour, so at
/// any moment roughly 1/60th of the fleet is in its window, template churn is flat,
/// and pool income is smooth. It is also stable: the same lane gets the same minute
/// every hour, so a miner watching closely sees a predictable pattern rather than
/// random dropouts.
///
/// `minute_of_hour` is passed in rather than read from a clock here so this stays a
/// pure function and can be tested across the whole hour without waiting for one.
pub fn is_pool_fee_minute(lane_id: u64, minute_of_hour: u64) -> bool {
    // Cheap integer mix (splitmix64 finalizer) — this is called on the job path, and a
    // hash here must never be a syscall or an allocation.
    let mut z = lane_id.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let start = z % 60;
    // Wrapping window so >1 fee minute still works if the constant is ever raised.
    (0..POOL_FEE_MINUTES_PER_HOUR).any(|k| (start + k) % 60 == minute_of_hour)
}

#[cfg(test)]
mod fee_window_tests {
    use super::*;

    /// Every lane must get exactly `POOL_FEE_MINUTES_PER_HOUR` fee minutes per hour —
    /// not zero (miner never pays) and not more (miner overpays).
    #[test]
    fn every_lane_pays_exactly_the_configured_minutes() {
        for lane in 0u64..500 {
            let hits = (0..60).filter(|m| is_pool_fee_minute(lane, *m)).count() as u64;
            assert_eq!(hits, POOL_FEE_MINUTES_PER_HOUR, "lane {lane} had {hits} fee minutes");
        }
    }

    /// The window must be STABLE for a lane — a miner whose fee minute moved around
    /// would see unpredictable KAS dropouts and no way to reconcile.
    #[test]
    fn a_lane_window_is_stable_across_calls() {
        for lane in 0u64..100 {
            let first: Vec<bool> = (0..60).map(|m| is_pool_fee_minute(lane, m)).collect();
            let again: Vec<bool> = (0..60).map(|m| is_pool_fee_minute(lane, m)).collect();
            assert_eq!(first, again, "lane {lane} window is not deterministic");
        }
    }

    /// The whole point of staggering: lanes must not share one minute. With 600 lanes
    /// over 60 slots a uniform spread puts ~10 per slot; assert no slot takes a
    /// pathological share (which would recreate the fleet-wide cliff).
    #[test]
    fn fee_minutes_are_spread_across_the_hour() {
        let mut per_minute = [0usize; 60];
        for lane in 0u64..600 {
            for m in 0..60 {
                if is_pool_fee_minute(lane, m) {
                    per_minute[m as usize] += 1;
                }
            }
        }
        let occupied = per_minute.iter().filter(|c| **c > 0).count();
        assert!(occupied >= 55, "fee minutes clustered into {occupied}/60 slots");
        let worst = per_minute.iter().copied().max().unwrap();
        assert!(worst < 40, "one minute took {worst} of 600 lanes — not a spread");
    }
}
