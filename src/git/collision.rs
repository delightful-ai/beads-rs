//! ID collision detection and resolution.
//!
//! Per SPEC §4.1.1: When two different beads share the same ID (created independently
//! on different replicas), we must resolve deterministically without manual intervention.
//!
//! Strategy:
//! - Winner: earlier `core.created`, tiebreak on `core.created.by` lexicographically
//! - Loser: gets remapped to a longer ID (+1 char, up to 8)
//! - Deps: all references to loser's old ID are updated
//! - Tombstone: added for loser's old ID to prevent resurrection

use crate::core::{Bead, BeadId, CanonicalState, CoreError, DepEdge, DepKey, Stamp, Tombstone};

/// Represents a detected ID collision between two beads.
#[derive(Debug, Clone)]
pub struct Collision {
    /// The colliding ID
    pub id: BeadId,
    /// The bead that keeps the ID (earlier created)
    pub winner: CollisionSide,
    /// The bead that must be remapped (later created)
    pub loser: CollisionSide,
    /// New ID for the loser bead
    pub loser_new_id: BeadId,
}

/// Which side of the merge a bead came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollisionSide {
    Local,
    Remote,
}

/// Detect ID collisions between local and remote state.
///
/// A collision occurs when both sides have a bead with the same ID but different
/// creation stamps (meaning they were created independently).
pub fn detect_collisions(local: &CanonicalState, remote: &CanonicalState) -> Vec<Collision> {
    let mut collisions = Vec::new();

    for (id, local_bead) in local.iter_live() {
        if let Some(remote_bead) = remote.get_live(id) {
            // Same ID - check if same bead or collision
            if local_bead.core.created() != remote_bead.core.created() {
                // Different creation stamp = independent creation = collision
                let (winner, loser) = determine_winner(local_bead, remote_bead);
                let loser_new_id = generate_remap_id(id, local, remote);

                collisions.push(Collision {
                    id: id.clone(),
                    winner,
                    loser,
                    loser_new_id,
                });
            }
        }
    }

    collisions
}

/// Determine winner/loser based on creation stamp.
///
/// Winner: earlier `core.created.at`, tiebreak on `core.created.by` lexicographically.
fn determine_winner(local: &Bead, remote: &Bead) -> (CollisionSide, CollisionSide) {
    let local_stamp = local.core.created();
    let remote_stamp = remote.core.created();

    // Compare creation timestamps
    let local_wins = match local_stamp.at.cmp(&remote_stamp.at) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => {
            // Tiebreak on actor ID (lexicographic)
            local_stamp.by.as_str() < remote_stamp.by.as_str()
        }
    };

    if local_wins {
        (CollisionSide::Local, CollisionSide::Remote)
    } else {
        (CollisionSide::Remote, CollisionSide::Local)
    }
}

/// Generate a new ID for the remapped bead.
///
/// Uses 6 characters and checks both states to ensure uniqueness.
fn generate_remap_id(original: &BeadId, local: &CanonicalState, remote: &CanonicalState) -> BeadId {
    // Generate a new ID longer than the original root suffix, capped to 8.
    let target_len = (original.root_len() + 1).clamp(3, 8);
    let slug = original.slug_value();
    loop {
        let new_id = BeadId::generate_with_slug(&slug, target_len);
        if new_id != *original && !local.contains(&new_id) && !remote.contains(&new_id) {
            return new_id;
        }
    }
}

/// Apply collision resolutions to the merged state.
///
/// For each collision:
/// 1. Remove loser bead from its current ID
/// 2. Insert loser bead under new ID (updating its core.id)
/// 3. Update all deps that reference the old ID
/// 4. Add tombstone for old ID (prevents resurrection)
pub fn resolve_collisions(
    local: &CanonicalState,
    remote: &CanonicalState,
    collisions: &[Collision],
    resolution_stamp: Stamp,
) -> Result<(CanonicalState, CanonicalState), CoreError> {
    let mut local_resolved = local.clone();
    let mut remote_resolved = remote.clone();

    for collision in collisions {
        match collision.loser {
            CollisionSide::Local => {
                resolve_single(
                    &mut local_resolved,
                    &collision.id,
                    &collision.loser_new_id,
                    &resolution_stamp,
                )?;
            }
            CollisionSide::Remote => {
                resolve_single(
                    &mut remote_resolved,
                    &collision.id,
                    &collision.loser_new_id,
                    &resolution_stamp,
                )?;
            }
        }
    }

    Ok((local_resolved, remote_resolved))
}

/// Resolve a single collision in a state.
fn resolve_single(
    state: &mut CanonicalState,
    old_id: &BeadId,
    new_id: &BeadId,
    resolution_stamp: &Stamp,
) -> Result<(), CoreError> {
    // 1. Remove bead from old ID, update its core.id, reinsert at new ID
    if let Some(mut bead) = state.remove_live(old_id) {
        let lineage = bead.core.created().clone();
        bead.core.id = new_id.clone();
        state.insert_live(bead);

        // 3. Add a lineage-scoped tombstone for old ID to prevent resurrection of the loser.
        let tombstone =
            Tombstone::new_collision(old_id.clone(), resolution_stamp.clone(), lineage, None);
        state.insert_tombstone(tombstone);
    }

    // 2. Update deps referencing old ID
    let deps_to_update: Vec<_> = state
        .iter_deps()
        .filter(|(key, _)| key.from() == old_id || key.to() == old_id)
        .map(|(key, edge)| (key.clone(), edge.clone()))
        .collect();

    for (old_key, edge) in deps_to_update {
        // Remove old dep
        state.remove_dep(&old_key);

        // Create new dep with updated ID
        let new_from = if old_key.from() == old_id {
            new_id.clone()
        } else {
            old_key.from().clone()
        };
        let new_to = if old_key.to() == old_id {
            new_id.clone()
        } else {
            old_key.to().clone()
        };

        // This should always succeed since we're remapping existing deps
        // (if old_key was valid, new_key will be valid unless from == to after remap,
        // which can't happen since old_id != new_id)
        let new_key = DepKey::new(new_from, new_to, old_key.kind())?;

        let new_edge = DepEdge::new(new_key, edge.created.clone());
        state.insert_dep(new_edge);
    }

    Ok(())
}

/// Check if a collision resolution is deterministic.
///
/// Both sides must resolve to the same winner given the same inputs.
#[cfg(test)]
fn verify_determinism(local: &Bead, remote: &Bead) -> bool {
    let (w1, _) = determine_winner(local, remote);
    let (w2, _) = determine_winner(remote, local);

    // If we flip inputs, winner should flip sides but refer to same bead
    match (w1, w2) {
        (CollisionSide::Local, CollisionSide::Remote) => true,
        (CollisionSide::Remote, CollisionSide::Local) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, BeadCore, BeadFields, BeadType, Claim, DepKind, Lww, Priority, Workflow,
        WriteStamp,
    };

    fn make_bead(id: &str, wall_ms: u64, actor: &str) -> Bead {
        let stamp = Stamp::new(WriteStamp::new(wall_ms, 0), ActorId::new(actor).unwrap());
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::new(2).unwrap(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Default::default(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn earlier_creation_wins() {
        let local = make_bead("bd-abc", 1000, "alice");
        let remote = make_bead("bd-abc", 2000, "bob");

        let (winner, loser) = determine_winner(&local, &remote);
        assert_eq!(winner, CollisionSide::Local);
        assert_eq!(loser, CollisionSide::Remote);
    }

    #[test]
    fn same_time_tiebreak_on_actor() {
        let local = make_bead("bd-abc", 1000, "bob");
        let remote = make_bead("bd-abc", 1000, "alice");

        // "alice" < "bob" lexicographically, so remote wins
        let (winner, loser) = determine_winner(&local, &remote);
        assert_eq!(winner, CollisionSide::Remote);
        assert_eq!(loser, CollisionSide::Local);
    }

    #[test]
    fn determinism() {
        let local = make_bead("bd-abc", 1000, "alice");
        let remote = make_bead("bd-abc", 2000, "bob");
        assert!(verify_determinism(&local, &remote));

        let local = make_bead("bd-abc", 1000, "bob");
        let remote = make_bead("bd-abc", 1000, "alice");
        assert!(verify_determinism(&local, &remote));
    }

    #[test]
    fn collision_resolution_tombstone_does_not_delete_winner() {
        let id = BeadId::parse("bd-abc").unwrap();

        // Winner is older.
        let winner = make_bead(id.as_str(), 1000, "alice");
        let loser = make_bead(id.as_str(), 2000, "bob");

        let mut local = CanonicalState::new();
        local.insert(winner.clone()).unwrap();

        let mut remote = CanonicalState::new();
        remote.insert(loser.clone()).unwrap();

        let collisions = detect_collisions(&local, &remote);
        assert_eq!(collisions.len(), 1);

        // Resolution happens later than either bead.
        let resolver = ActorId::new("resolver").unwrap();
        let resolution_stamp = Stamp::new(WriteStamp::new(5000, 0), resolver);

        let (local_resolved, remote_resolved) =
            resolve_collisions(&local, &remote, &collisions, resolution_stamp).unwrap();
        let merged = CanonicalState::join(&local_resolved, &remote_resolved).unwrap();

        // Winner retains the original ID.
        let merged_winner = merged.get_live(&id).expect("winner should remain live");
        assert_eq!(merged_winner.core.created(), winner.core.created());
        assert!(!merged.is_deleted(&id));

        // Loser is remapped.
        let new_id = &collisions[0].loser_new_id;
        let merged_loser = merged
            .get_live(new_id)
            .expect("loser should be present under remapped id");
        assert_eq!(merged_loser.core.created(), loser.core.created());

        // Collision tombstone is preserved and scoped to the losing lineage.
        let tomb = merged
            .iter_tombstones()
            .find_map(|(_, t)| {
                if t.id == id && t.lineage.as_ref() == Some(loser.core.created()) {
                    Some(t)
                } else {
                    None
                }
            })
            .expect("collision tombstone should exist");
        assert!(tomb.lineage.is_some());
    }

    #[test]
    fn collision_resolution_errors_on_self_dep_after_remap() {
        let id = BeadId::parse("bd-abc").unwrap();
        let new_id = BeadId::parse("bd-abcd").unwrap();

        let mut local = CanonicalState::new();
        local.insert(make_bead(id.as_str(), 1000, "alice")).unwrap();

        let dep_stamp = Stamp::new(WriteStamp::new(1500, 0), ActorId::new("alice").unwrap());
        let dep_key = DepKey::new(id.clone(), new_id.clone(), DepKind::Blocks).unwrap();
        local.insert_dep(DepEdge::new(dep_key, dep_stamp));

        let collision = Collision {
            id: id.clone(),
            winner: CollisionSide::Remote,
            loser: CollisionSide::Local,
            loser_new_id: new_id.clone(),
        };
        let resolution_stamp =
            Stamp::new(WriteStamp::new(2000, 0), ActorId::new("resolver").unwrap());

        let result = resolve_collisions(
            &local,
            &CanonicalState::new(),
            &[collision],
            resolution_stamp,
        );
        assert!(result.is_err());
    }
}
