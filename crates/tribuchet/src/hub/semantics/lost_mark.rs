//! A hand-written history of the lost mark (L10), beside the random ones.

use super::*;

/// L10: a clientless build whose run is lost ends, but its key stays lost,
/// so a resubmission runs alone; the next outcome, read by no one, lifts it.
#[test]
fn a_lost_mark_outlasts_its_build_until_an_outcome() {
    let mut w = World::default();
    let go = |w: &mut World, acts: &[Act]| acts.iter().for_each(|a| w.act(a.clone()));
    go(&mut w, &[Act::Connect(0)]);
    for client in 0..2 {
        go(&mut w, &[Act::Submit(0, client, false), Act::Place(0, 0)]);
        let lost = w.hub.lost.contains(&0);
        assert_eq!(lost, client == 1, "a resubmission after a loss starts lost");
        go(&mut w, &[Act::Deliver(0), Act::Start(0, 0)]);
        w.act(Act::Leave(0, client));
        let live = w.hub.builds.get(&0).and_then(|b| b.live);
        assert!(live.is_some(), "L9: a clientless run goes on");
        if client == 0 {
            // Its worker restarts without it: lost, with no known cause.
            go(&mut w, &[Act::Crash(0), Act::Connect(0)]);
            assert!(w.hub.builds.is_empty(), "C27: no one wants it");
            assert!(w.hub.lost.contains(&0), "L10: its key stays lost");
        }
    }
    go(&mut w, &[Act::Finish(0, 0, true), Act::Fetched(0)]);
    let (ended, lifted) = (w.hub.builds.is_empty(), w.hub.lost.is_empty());
    assert!(ended && lifted, "C27; L10: an unread outcome lifts it");
}
