//! Fine-grained reactive core, SolidJS-inspired per the Phase 13 spec.
//!
//! Deliberately has zero `web-sys`/DOM dependency, unlike every other module
//! in this crate — that's what makes it runnable under plain `cargo test` on
//! the host target instead of only inside a browser. `Signal<T>`/`create_effect`
//! are the only two primitives implemented (plus `create_memo`, a thin
//! wrapper over both); this is not a general-purpose reactivity library,
//! just enough to drive `Canvas.*` component redraws when a `KeystoneClient`
//! query result changes (`client.rs`).
//!
//! Scope cut vs. real SolidJS: no automatic cleanup/disposal graph (an effect
//! lives as long as its `Signal` subscriptions do — there's no `onCleanup`),
//! and no batching (setting N signals inside one callback re-runs each
//! dependent effect up to N times, not once at the end of the callback).

use std::cell::RefCell;
use std::rc::{Rc, Weak};

type EffectId = usize;

struct EffectBox {
    id: EffectId,
    run: RefCell<Box<dyn FnMut()>>,
}

thread_local! {
    /// The effect currently executing, if any — `Signal::get` reads this to
    /// know which effect to register itself as a dependency of. A stack
    /// (rather than a single slot) so a `Signal::get` inside a nested effect
    /// call still attributes correctly.
    static CURRENT_EFFECT: RefCell<Vec<Rc<EffectBox>>> = const { RefCell::new(Vec::new()) };
    static NEXT_EFFECT_ID: RefCell<EffectId> = const { RefCell::new(0) };
}

fn next_effect_id() -> EffectId {
    NEXT_EFFECT_ID.with(|n| {
        let mut n = n.borrow_mut();
        let id = *n;
        *n += 1;
        id
    })
}

/// A reactive cell. Cloning a `Signal` shares the same underlying value and
/// subscriber list (it's a handle, like `Rc`).
pub struct Signal<T> {
    value: Rc<RefCell<T>>,
    subscribers: Rc<RefCell<Vec<(EffectId, Weak<EffectBox>)>>>,
}

impl<T> Clone for Signal<T> {
    fn clone(&self) -> Self {
        Self { value: self.value.clone(), subscribers: self.subscribers.clone() }
    }
}

impl<T: Clone> Signal<T> {
    pub fn new(initial: T) -> Self {
        Self { value: Rc::new(RefCell::new(initial)), subscribers: Rc::new(RefCell::new(Vec::new())) }
    }

    /// Reads the current value, and — if called from inside a
    /// `create_effect`/`create_memo` closure — registers that effect to
    /// re-run whenever this signal next changes.
    pub fn get(&self) -> T {
        CURRENT_EFFECT.with(|ce| {
            if let Some(effect) = ce.borrow().last() {
                let mut subs = self.subscribers.borrow_mut();
                if !subs.iter().any(|(id, _)| *id == effect.id) {
                    subs.push((effect.id, Rc::downgrade(effect)));
                }
            }
        });
        self.value.borrow().clone()
    }

    /// Overwrites the value and synchronously re-runs every effect currently
    /// subscribed (no batching — see module docs).
    pub fn set(&self, new_value: T) {
        *self.value.borrow_mut() = new_value;
        self.notify();
    }

    pub fn update(&self, f: impl FnOnce(&T) -> T) {
        let next = f(&self.value.borrow());
        self.set(next);
    }

    fn notify(&self) {
        // Snapshot first: running an effect can re-subscribe (or drop) other
        // effects, and we don't want that mutating the list mid-iteration.
        let subs: Vec<_> = self.subscribers.borrow().clone();
        for (_, weak) in subs {
            if let Some(effect) = weak.upgrade() {
                run_effect(&effect);
            }
        }
        // Drop subscribers whose effect no longer exists.
        self.subscribers.borrow_mut().retain(|(_, w)| w.strong_count() > 0);
    }
}

fn run_effect(effect: &Rc<EffectBox>) {
    CURRENT_EFFECT.with(|ce| ce.borrow_mut().push(effect.clone()));
    (effect.run.borrow_mut())();
    CURRENT_EFFECT.with(|ce| {
        ce.borrow_mut().pop();
    });
}

/// Runs `f` once immediately, then re-runs it whenever any `Signal` it read
/// during its most recent run is `set`/`update`d. Returns a handle that must
/// be kept alive (dropping it lets the effect be garbage-collected on the
/// next `Signal::set` that would otherwise have re-run it) — there is no
/// separate `dispose()` call, matching the "no cleanup graph" scope cut.
#[must_use = "dropping the returned handle immediately allows the effect to stop running"]
pub fn create_effect(f: impl FnMut() + 'static) -> Rc<dyn std::any::Any> {
    let effect = Rc::new(EffectBox { id: next_effect_id(), run: RefCell::new(Box::new(f)) });
    run_effect(&effect);
    effect
}

/// A read-only `Signal` derived from other signals: recomputes `f` inside an
/// effect and writes the result into the returned signal. Not lazy (unlike a
/// real "memo") — it recomputes eagerly whenever a dependency changes, same
/// as any other `create_effect`.
pub fn create_memo<T: Clone + 'static>(f: impl Fn() -> T + 'static) -> (Signal<T>, Rc<dyn std::any::Any>) {
    let initial = f();
    let signal = Signal::new(initial);
    let write = signal.clone();
    let handle = create_effect(move || {
        let value = f();
        write.set(value);
    });
    (signal, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn effect_runs_immediately_and_on_change() {
        let count = Rc::new(Cell::new(0));
        let sig = Signal::new(1);
        let sig2 = sig.clone();
        let count2 = count.clone();
        let _handle = create_effect(move || {
            let _ = sig2.get();
            count2.set(count2.get() + 1);
        });
        assert_eq!(count.get(), 1);
        sig.set(2);
        assert_eq!(count.get(), 2);
        sig.set(3);
        assert_eq!(count.get(), 3);
    }

    #[test]
    fn effect_does_not_rerun_for_untracked_signal() {
        let count = Rc::new(Cell::new(0));
        let tracked = Signal::new(1);
        let untracked = Signal::new(1);
        let tracked2 = tracked.clone();
        let count2 = count.clone();
        let _handle = create_effect(move || {
            let _ = tracked2.get();
            count2.set(count2.get() + 1);
        });
        assert_eq!(count.get(), 1);
        untracked.set(99);
        assert_eq!(count.get(), 1, "effect must not re-run for a signal it never read");
    }

    #[test]
    fn memo_recomputes_from_dependency() {
        let base = Signal::new(2);
        let base2 = base.clone();
        let (doubled, _handle) = create_memo(move || base2.get() * 2);
        assert_eq!(doubled.get(), 4);
        base.set(5);
        assert_eq!(doubled.get(), 10);
    }

    #[test]
    fn dropping_effect_handle_stops_future_reruns() {
        let count = Rc::new(Cell::new(0));
        let sig = Signal::new(1);
        let sig2 = sig.clone();
        let count2 = count.clone();
        let handle = create_effect(move || {
            let _ = sig2.get();
            count2.set(count2.get() + 1);
        });
        assert_eq!(count.get(), 1);
        drop(handle);
        sig.set(2);
        assert_eq!(count.get(), 1, "effect should not run after its handle is dropped");
    }
}
