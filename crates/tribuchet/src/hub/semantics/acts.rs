//! The acts a random history is made of, and how often each comes.

use super::*;

pub(super) fn act() -> impl Strategy<Value = Act> {
    let w = 0..3usize;
    prop_oneof![
        3 => (0..4u32, 0..3u32, any::<bool>()).prop_map(|(k, c, l)| Act::Submit(k, c, l)),
        1 => (0..4u32, 0..3u32).prop_map(|(k, c)| Act::Leave(k, c)),
        1 => (0..4u32, 0..3u32).prop_map(|(k, c)| Act::Bound(k, c)),
        1 => (0..4u32, any::<bool>()).prop_map(|(k, ok)| Act::Unread(k, ok)),
        2 => w.clone().prop_map(Act::Connect),
        1 => w.clone().prop_map(Act::Down),
        2 => w.clone().prop_map(Act::Crash),
        6 => w.clone().prop_map(Act::Deliver),
        5 => (w.clone(), 0..4usize).prop_map(|(w, i)| Act::Start(w, i)),
        4 => (w.clone(), 0..4usize, any::<bool>()).prop_map(|(w, i, ok)| Act::Finish(w, i, ok)),
        2 => (w.clone(), 0..4usize).prop_map(|(w, i)| Act::Reap(w, i)),
        3 => (0..64usize).prop_map(Act::Replay),
        2 => w.clone().prop_map(Act::Fetched),
        1 => (w.clone(), 0..4usize).prop_map(|(w, i)| Act::BogusFetched(w, i)),
        1 => (0..64usize).prop_map(Act::Contradict),
        1 => Just(Act::HubRestart),
        5 => (0..4u32, w.clone()).prop_map(|(k, w)| Act::Place(k, w)),
        2 => (0..8usize, 0..8u32).prop_map(|(i, k)| Act::Adopt(i, k)),
        2 => (0..8usize).prop_map(Act::Abort),
        1 => (0..4u32, 0..3u32).prop_map(|(k, c)| Act::Defer(k, c)),
        1 => (0..4u32).prop_map(Act::Refuse),
        1 => (0..4u32).prop_map(Act::Cannot),
        1 => w.clone().prop_map(Act::Suspect),
        1 => w.prop_map(Act::Decommission),
    ]
}
