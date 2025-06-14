use solana_pubkey::Pubkey;
use {
    crate::{
        consensus::{tower_storage::{SavedTowerVersions, TowerStorage}, Tower, VOTE_THRESHOLD_DEPTH},
        next_leader::upcoming_leader_tpu_vote_sockets,
    },
    bincode::{deserialize, serialize},
    crossbeam_channel::Receiver,
    log::{error, info, trace, warn},
    serde::{Deserialize, Serialize},
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::measure::Measure,
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{
        clock::{Slot, FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET},
        hash::Hash,
        program_utils::limited_deserialize,
        transaction::Transaction,
        transport::TransportError,
    },
    solana_vote_program::vote_instruction::VoteInstruction,
    std::{
        collections::HashSet,
        error::Error,
        net::{SocketAddr, UdpSocket},
        sync::{Arc, Mutex, RwLock},
        thread::{self, Builder, JoinHandle},
    },
    thiserror::Error,
};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum VoteOp {
    PushVote {
        tx: Transaction,
        tower_slots: Vec<Slot>,
        saved_tower: Option<SavedTowerVersions>,
    },
    RefreshVote {
        tx: Transaction,
        last_voted_slot: Slot,
    },
}

impl VoteOp {
    fn tx(&self) -> &Transaction {
        match self {
            VoteOp::PushVote { tx, .. } => tx,
            VoteOp::RefreshVote { tx, .. } => tx,
        }
    }
}

#[derive(Debug, Error)]
enum SendVoteError {
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),
    #[error("Invalid TPU address")]
    InvalidTpuAddress,
    #[error(transparent)]
    TransportError(#[from] TransportError),
}

fn send_vote_transaction(
    cluster_info: &ClusterInfo,
    transaction: &Transaction,
    tpu: Option<SocketAddr>,
    connection_cache: &Arc<ConnectionCache>,
) -> Result<(), SendVoteError> {
    let tpu = tpu
        .or_else(|| {
            cluster_info
                .my_contact_info()
                .tpu(connection_cache.protocol())
        })
        .ok_or(SendVoteError::InvalidTpuAddress)?;
    let buf = serialize(transaction)?;
    let client = connection_cache.get_connection(&tpu);

    client.send_data_async(buf).map_err(|err| {
        trace!("Ran into an error when sending vote: {err:?} to {tpu:?}");
        SendVoteError::from(err)
    })
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VoteOpWithAncestors {
    pub(crate) vote_op: VoteOp,
    pub(crate) ancestors: HashSet<Slot>,
}

impl VoteOpWithAncestors {
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        serialize(self)
    }
}

pub struct VotingService {
    vote_processing_thread_hdl: JoinHandle<()>,
    udp_listener_thread_hdl: Option<JoinHandle<()>>,
}

enum VotingSource {
    Primary,
    Secondary,
}

impl VotingService {
    pub fn new(
        vote_receiver: Receiver<VoteOpWithAncestors>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        tower_storage: Arc<dyn TowerStorage>,
        connection_cache: Arc<ConnectionCache>,
        hsv_port: Option<u16>,
        vote_pubkey: Pubkey
    ) -> Self {
        let process_cluster_info = cluster_info.clone();
        let process_poh_recorder = poh_recorder.clone();
        let process_tower_storage = tower_storage.clone();
        let process_connection_cache = connection_cache.clone();

        let tower_vote_validator = Arc::new(Mutex::new(TowerVoteValidator::new()));
        let tower_vote_validator_clone_udp = Arc::clone(&tower_vote_validator);
        let tower_vote_validator_clone_inner = Arc::clone(&tower_vote_validator);

        let vote_processing_thread_hdl = Builder::new()
            .name("solVoteService".to_string())
            .spawn(move || {
                for vote_op_with_ancestors in vote_receiver.iter() {
                    let vote_op = vote_op_with_ancestors.vote_op;
                    let ancestors = vote_op_with_ancestors.ancestors;

                    if let Ok(proposed_vote) = Self::vote_op_to_proposed_vote(vote_op.clone()) {
                        match tower_vote_validator_clone_inner.lock().unwrap().try_add_vote(&proposed_vote, &ancestors, VotingSource::Primary) {
                            Ok(()) => {
                                Self::handle_vote(
                                    &process_cluster_info,
                                    &process_poh_recorder,
                                    process_tower_storage.as_ref(),
                                    vote_op,
                                    process_connection_cache.clone(),
                                );
                            }
                            Err(e) => {
                                info!("hot_spare_vote: Slot {} rejected (primary): {}.", &proposed_vote.slot, e);
                            }
                        }
                    } else {
                        error!("hot_spare_vote: could not extract proposed vote");
                    }
                }
            })
            .unwrap_or_else(|err| {
                error!("Failed to spawn vote processing thread: {:?}", err);
                panic!("Failed to spawn vote processing thread: {:?}", err);
            });

        let udp_listener_thread_hdl = Self::setup_udp_listener(
            tower_vote_validator_clone_udp,
            cluster_info,
            poh_recorder,
            tower_storage,
            connection_cache.clone(),
            hsv_port,
            vote_pubkey
        );

        Self {
            vote_processing_thread_hdl,
            udp_listener_thread_hdl,
        }
    }

    pub fn setup_udp_listener(
        tower_vote_validator: Arc<Mutex<TowerVoteValidator>>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        tower_storage: Arc<dyn TowerStorage>,
        connection_cache: Arc<ConnectionCache>,
        hsv_port: Option<u16>,
        vote_pubkey: Pubkey
    ) -> Option<JoinHandle<()>> {
        let udp_listener_thread_hdl = match hsv_port {
            Some(port) => {
                Some(Builder::new()
                    .name("solUdpListen".to_string())
                    .spawn(move || {
                        Self::udp_listen_loop(port, tower_vote_validator, cluster_info, poh_recorder, tower_storage, connection_cache, vote_pubkey);
                    })
                    .unwrap_or_else(|err| {
                        panic!("hot_spare_vote: Failed to spawn UDP listener thread: {:?}", err);
                    }))
            }
            None => {
                info!("hot_spare_vote: hsv_listen_port not set. UDP listener not started.");
                None
            }
        };

        udp_listener_thread_hdl
    }

    fn udp_listen_loop(
        port: u16,
        tower_vote_validator: Arc<Mutex<TowerVoteValidator>>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        tower_storage: Arc<dyn TowerStorage>,
        connection_cache: Arc<ConnectionCache>,
        vote_pubkey: Pubkey
    ) {
        let listen_addr = SocketAddr::from(([0, 0, 0, 0], port));
        let socket = match UdpSocket::bind(listen_addr) {
            Ok(s) => {
                info!("hot_spare_vote: UDP Listener started and bound to {}", listen_addr);
                s
            }
            Err(e) => {
                error!("hot_spare_vote: Failed to bind UDP listener to {}: {}. Thread terminating.", listen_addr, e);
                return;
            }
        };

        let mut buf = [0u8; 4096];
        loop {
            match socket.recv_from(&mut buf) {
                Ok((num_bytes, src_addr)) => {
                    let data = &buf[..num_bytes];
                    Self::handle_incoming_udp_request(
                        data,
                        src_addr,
                        tower_vote_validator.clone(),
                        cluster_info.clone(),
                        poh_recorder.clone(),
                        tower_storage.clone(),
                        connection_cache.clone(),
                        vote_pubkey.clone()
                    );
                }
                Err(e) => {
                    warn!("hot_spare_vote: UDP recv_from error on {}: {}. Continuing to listen.", listen_addr, e);
                }
            }
        }
    }

    fn handle_incoming_udp_request(
        data: &[u8],
        src_addr: SocketAddr,
        tower_vote_validator: Arc<Mutex<TowerVoteValidator>>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        tower_storage: Arc<dyn TowerStorage>,
        connection_cache: Arc<ConnectionCache>,
        vote_pubkey: Pubkey
    ) {
        match deserialize::<VoteOpWithAncestors>(data) {
            Ok(vote_op_with_ancestors) => {
                let VoteOpWithAncestors { vote_op, ancestors } = vote_op_with_ancestors;

                let required_signers = &vote_op.tx().message.account_keys[..vote_op.tx().message.header.num_required_signatures as usize];
                if !required_signers.contains(&vote_pubkey) || vote_op.tx().verify().is_err() {
                    warn!("hot_spare_vote: Received vote_tx is not signed by our vote pubkey");
                    return;
                }

                if let Ok(proposed_vote) = Self::vote_op_to_proposed_vote(vote_op.clone()) {
                    match tower_vote_validator.lock().unwrap().try_add_vote(&proposed_vote, &ancestors, VotingSource::Secondary) {
                        Ok(()) => {
                            info!("hot_spare_vote: Slot {} accepted (secondary {}).", &proposed_vote.slot, src_addr.to_string());
                            Self::handle_vote(
                                &cluster_info,
                                &poh_recorder,
                                tower_storage.as_ref(),
                                vote_op,
                                connection_cache.clone(),
                            );
                        }
                        Err(e) => {
                            info!("hot_spare_vote: Slot {} rejected (secondary {}): {}", &proposed_vote.slot, src_addr.to_string(), e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("hot_spare_vote: Failed to deserialize UDP packet from {} as VoteOpWithAncestors: {}", src_addr, e);
            }
        }
    }


    pub fn handle_vote(
        cluster_info: &ClusterInfo,
        poh_recorder: &RwLock<PohRecorder>,
        tower_storage: &dyn TowerStorage,
        vote_op: VoteOp,
        connection_cache: Arc<ConnectionCache>,
    ) {
        if let VoteOp::PushVote { saved_tower: saved_tower_opt, .. } = &vote_op {
            if let Some(saved_tower) = saved_tower_opt {
                let mut measure = Measure::start("tower storage save");
                if let Err(err) = tower_storage.store(saved_tower) {
                    error!("hot_spare_vote: Unable to save tower to storage: {:?}", err);
                    std::process::exit(1);
                }
                measure.stop();
                trace!("{measure}");
            }
        }

        // Attempt to send our vote transaction to the leaders for the next few slots
        const UPCOMING_LEADER_FANOUT_SLOTS: u64 = FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET;
        #[cfg(test)]
        static_assertions::const_assert_eq!(UPCOMING_LEADER_FANOUT_SLOTS, 2);
        let upcoming_leader_sockets = upcoming_leader_tpu_vote_sockets(
            cluster_info,
            poh_recorder,
            UPCOMING_LEADER_FANOUT_SLOTS,
            connection_cache.protocol(),
        );

        if !upcoming_leader_sockets.is_empty() {
            for tpu_vote_socket in upcoming_leader_sockets {
                let _ = send_vote_transaction(
                    cluster_info,
                    vote_op.tx(),
                    Some(tpu_vote_socket),
                    &connection_cache,
                );
            }
        } else {
            // Send to our own tpu vote socket if we cannot find a leader to send to
            let _ = send_vote_transaction(cluster_info, vote_op.tx(), None, &connection_cache);
        }

        match vote_op {
            VoteOp::PushVote {
                tx, tower_slots, ..
            } => {
                cluster_info.push_vote(&tower_slots, tx);
            }
            VoteOp::RefreshVote {
                tx,
                last_voted_slot,
            } => {
                cluster_info.refresh_vote(tx, last_voted_slot);
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        if let Some(hdl) = self.udp_listener_thread_hdl {
            match hdl.join() {
                Ok(_) => info!("hot_spare_vote: UDP listener thread joined successfully."),
                Err(e) => {
                    error!("hot_spare_vote: UDP listener thread panicked: {:?}", e);
                    std::panic::resume_unwind(e);
                }
            }
        }
        // Then join the main vote processing thread
        self.vote_processing_thread_hdl.join()
    }

    fn vote_op_to_proposed_vote(vote_op: VoteOp) -> Result<ProposedVote, Box<dyn Error>> {
        let vote_tx = vote_op.tx();
        let vote_ix_data: &Vec<u8> = &vote_tx
            .message
            .instructions
            .get(0)
            .expect("hot_spare_vote: Vote transaction must have at least one instruction")
            .data;

        let vote_ix = Self::deserialize_vote_ix(vote_ix_data).unwrap();

        match vote_ix {
            VoteInstruction::TowerSync(ref tower_sync) => {
                let slot = tower_sync.lockouts.back().unwrap().slot();
                let hash = tower_sync.hash;
                let block_id = tower_sync.block_id;

                Ok(ProposedVote {
                    slot,
                    hash,
                    block_id,
                })
            }
            _ => {
                // TODO: add support for other types of vote txs
                error!("hot_spare_vote: Not a TowerSync instruction");
                Err("hot_spare_vote: Not a TowerSync instruction".into())
            }
        }
    }


    pub fn deserialize_vote_ix(
        serialized: &[u8]
    ) -> Result<VoteInstruction, Box<dyn Error>> {
        if let Ok(vote_instruction) = limited_deserialize::<
            VoteInstruction,
        >(serialized)
        {
            return Ok(vote_instruction);
        }

        Err("hot_spare_vote: Failed to deserialize vote transaction".into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposedVote {
    pub slot: Slot,
    pub hash: Hash,
    pub block_id: Hash,
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum VoteValidatorError {
    #[error("hot_spare_vote: Vote for slot {attempted_slot} is too old. Last voted slot: {last_voted_slot:?}, Root slot: {root_slot}"
    )]
    VoteTooOld {
        attempted_slot: Slot,
        last_voted_slot: Option<Slot>,
        root_slot: Slot,
    },
    #[error("hot_spare_vote: Vote for slot {0} is locked out by previous votes in the tower")]
    LockedOut(Slot),
    #[error("hot_spare_vote: Bootstrap vote for slot {attempted_slot} is out of order or inconsistent. Last voted slot: {last_voted_slot:?}, Current tower root: {root_slot:?}"
    )]
    BootstrapVoteInconsistent {
        attempted_slot: Slot,
        last_voted_slot: Option<Slot>,
        root_slot: Option<Slot>,
    },
    #[error("hot_spare_vote: Root mismatch: Tower root {tower_root} not in vote ancestors for slot {vote_slot}. Cannot safely proceed with validation."
    )]
    RootMismatch {
        vote_slot: Slot,
        tower_root: Slot,
    },
    #[error("hot_spare_vote: Bootstrapping, not accepting non Primary votes.")]
    Bootstrapping,
}

pub struct TowerVoteValidator {
    tower: Tower,
}

impl TowerVoteValidator {
    pub fn new() -> Self {
        let tower = Tower::default();
        Self { tower }
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.tower.vote_state.votes.len() >= VOTE_THRESHOLD_DEPTH
    }


    fn try_add_vote(
        &mut self,
        proposed_vote: &ProposedVote,
        ancestors: &HashSet<Slot>,
        voting_source: VotingSource,
    ) -> Result<(), VoteValidatorError> {
        if let Some(last_voted_slot) = self.tower.vote_state.last_voted_slot() {
            if last_voted_slot >= proposed_vote.slot {
                return Err(VoteValidatorError::VoteTooOld {
                    attempted_slot: proposed_vote.slot,
                    last_voted_slot: self.tower.vote_state.last_voted_slot(),
                    root_slot: self.tower.root(),
                });
            }
        }

        if !self.is_bootstrapped()
            && !matches!(voting_source, VotingSource::Primary) {
            // Bootstrapping, don't accept non Primary votes
            return Err(VoteValidatorError::Bootstrapping);
        }

        if self.tower.root() == 0 {
            if !ancestors.contains(&0) {
                info!("hot_spare_vote: Tower root is 0, but vote for slot {} has ancestors not containing 0. Attempting root alignment.", proposed_vote.slot);
                let new_root_candidate = ancestors.iter()
                    .filter(|&&a| a < proposed_vote.slot)
                    .max()
                    .copied();
                if let Some(new_root) = new_root_candidate {
                    if ancestors.contains(&new_root) {
                        info!("hot_spare_vote: Aligning tower root from 0 to {} based on ancestors of slot {}.", new_root, proposed_vote.slot);
                        self.tower.initialize_root(new_root);
                    } else {
                        warn!("hot_spare_vote: Root alignment from 0 failed for slot {}: candidate new root {} not in its own ancestors.", proposed_vote.slot, new_root);
                        return Err(VoteValidatorError::RootMismatch {
                            vote_slot: proposed_vote.slot,
                            tower_root: 0,
                        });
                    }
                } else {
                    warn!("hot_spare_vote: Root alignment from 0 failed for slot {}: could not determine a new root from its ancestors (ancestors: {:?}).", proposed_vote.slot, ancestors);
                    if proposed_vote.slot != 0 && !ancestors.contains(&0) {
                        error!("hot_spare_vote: Root mismatch (failed alignment from root 0) for slot {}. Tower root is 0, which is not in vote ancestors. Aborting.", proposed_vote.slot);
                        return Err(VoteValidatorError::RootMismatch {
                            vote_slot: proposed_vote.slot,
                            tower_root: 0,
                        });
                    }
                }
            }
        }

        if self.tower.root() != 0 {
            let mut vote_state_copy_for_pre_check = self.tower.vote_state.clone();
            vote_state_copy_for_pre_check.process_next_vote_slot(proposed_vote.slot, true);

            if let Some(simulated_internal_root_in_copy) = vote_state_copy_for_pre_check.root_slot {
                let cond1_slot_neq_simulated_root = proposed_vote.slot != simulated_internal_root_in_copy;
                let cond2_ancestors_not_contains_simulated_root = !ancestors.contains(&simulated_internal_root_in_copy);

                if cond1_slot_neq_simulated_root && cond2_ancestors_not_contains_simulated_root {
                    if self.tower.root() == simulated_internal_root_in_copy {
                        warn!(
                            "hot_spare_vote: Stale Root Detected: Current tower root {} (simulated as {} in copy) not in ancestors for slot {}. Attempting re-alignment.",
                            self.tower.root(), simulated_internal_root_in_copy, proposed_vote.slot
                        );
                        let new_root_candidate = ancestors.iter()
                            .filter(|&&a| a < proposed_vote.slot)
                            .max()
                            .copied();
                        if let Some(new_actual_root) = new_root_candidate {
                            if ancestors.contains(&new_actual_root) {
                                info!("hot_spare_vote: Re-aligning stale tower root from {} to {}. Clearing existing tower votes.", self.tower.root(), new_actual_root);
                                self.tower.initialize_root(new_actual_root);
                                self.tower.vote_state.votes.clear();
                            } else {
                                error!("hot_spare_vote: Stale root re-alignment failed: new candidate root {} not in ancestors for slot {}. Vote rejected.", new_actual_root, proposed_vote.slot);
                                return Err(VoteValidatorError::RootMismatch { vote_slot: proposed_vote.slot, tower_root: self.tower.root() });
                            }
                        } else {
                            error!("hot_spare_vote: Stale root re-alignment failed: no new root candidate found from ancestors for slot {}. Vote rejected.", proposed_vote.slot);
                            return Err(VoteValidatorError::RootMismatch { vote_slot: proposed_vote.slot, tower_root: self.tower.root() });
                        }
                    } else {
                        error!("hot_spare_vote: Pre-check failed (complex root advancement in copy): Vote for slot {} implies internal root {} (original tower root {:?}), which is not in ancestors. Vote rejected.",
                        proposed_vote.slot, simulated_internal_root_in_copy, self.tower.root());
                        return Err(VoteValidatorError::RootMismatch {
                            vote_slot: proposed_vote.slot,
                            tower_root: simulated_internal_root_in_copy,
                        });
                    }
                }
            }
        } else {
            if proposed_vote.slot != 0 && !ancestors.contains(&0) {
                error!("hot_spare_vote: Safety check (tower root path == 0): Tower root is 0, proposed slot {} != 0, and ancestors do not contain 0. Aborting.", proposed_vote.slot);
                return Err(VoteValidatorError::RootMismatch {
                    vote_slot: proposed_vote.slot,
                    tower_root: 0,
                });
            }
        }

        if !self.is_bootstrapped() {
            if !self.tower.is_recent(proposed_vote.slot) {
                info!("hot_spare_vote: Bootstrap vote not recent. Slot: {}, Last Voted: {:?}, Tower Root: {:?}",
                  proposed_vote.slot, self.tower.vote_state.last_voted_slot(), self.tower.root());
                return Err(VoteValidatorError::BootstrapVoteInconsistent {
                    attempted_slot: proposed_vote.slot,
                    last_voted_slot: self.tower.vote_state.last_voted_slot(),
                    root_slot: Some(self.tower.root()),
                });
            }
            if self.tower.is_locked_out(proposed_vote.slot, ancestors) {
                info!("hot_spare_vote: Bootstrap vote {} locked out by existing tower votes.", proposed_vote.slot);
                return Err(VoteValidatorError::LockedOut(proposed_vote.slot));
            }
        } else {
            if !self.tower.is_recent(proposed_vote.slot) {
                return Err(VoteValidatorError::VoteTooOld {
                    attempted_slot: proposed_vote.slot,
                    last_voted_slot: self.tower.vote_state.last_voted_slot(),
                    root_slot: self.tower.root(),
                });
            }
            if self.tower.is_locked_out(proposed_vote.slot, ancestors) {
                info!("hot_spare_vote: Vote {} locked out.", proposed_vote.slot);
                return Err(VoteValidatorError::LockedOut(proposed_vote.slot));
            }
        }

        self.tower
            .vote_state
            .process_next_vote_slot(proposed_vote.slot, true);

        self.tower.update_last_vote_from_vote_state(
            proposed_vote.hash,
            true,
            proposed_vote.block_id,
        );
        Ok(())
    }

    pub fn tower(&self) -> &Tower {
        &self.tower
    }

    pub fn tower_mut(&mut self) -> &mut Tower {
        &mut self.tower
    }

    pub fn reset(&mut self) {
        self.tower = Tower::default();
    }
}