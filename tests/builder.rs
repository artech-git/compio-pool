//! The public constructors: `Pool::new`, `Pool::with_exchange` and `Builder`.

mod common;

use std::time::Duration;

use common::{LocalManager, MovableManager, cfg, field};
use compio_pool::{Config, NoExchange, Pool, Reservoir};

#[test]
fn pool_new_uses_the_config_it_is_given() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(3).min_idle(1));
    assert_eq!(field(pool.config(), "max_size"), "3");
    assert_eq!(field(pool.config(), "min_idle"), "1");
    assert!(!pool.is_closed());
}

#[test]
fn with_exchange_is_the_explicit_form_of_pool_new() {
    let pool = Pool::with_exchange(LocalManager::new(), cfg().max_size(3), NoExchange);
    assert_eq!(pool.metrics().parked, 0);
    assert_eq!(field(pool.config(), "max_size"), "3");
}

#[test]
fn the_builder_starts_from_the_defaults() {
    let pool = Pool::builder(LocalManager::new()).build();
    assert_eq!(field(pool.config(), "max_size"), "16");
    assert_eq!(field(pool.config(), "min_idle"), "0");
}

#[test]
fn every_builder_setter_reaches_the_config() {
    let pool = Pool::builder(LocalManager::new())
        .max_size(5)
        .min_idle(2)
        .acquire_timeout(Duration::from_millis(7))
        .max_lifetime(Duration::from_millis(8))
        .idle_timeout(Duration::from_millis(9))
        .max_uses(10u64)
        .build();

    let c = pool.config();
    assert_eq!(field(c, "max_size"), "5");
    assert_eq!(field(c, "min_idle"), "2");
    assert_eq!(field(c, "acquire_timeout"), "Some(7ms)");
    assert_eq!(field(c, "max_lifetime"), "Some(8ms)");
    assert_eq!(field(c, "idle_timeout"), "Some(9ms)");
    assert_eq!(field(c, "max_uses"), "Some(10)");
}

#[test]
fn the_builders_optional_setters_accept_none() {
    let pool = Pool::builder(LocalManager::new())
        .acquire_timeout(None)
        .max_lifetime(None)
        .idle_timeout(None)
        .max_uses(None)
        .build();

    let c = pool.config();
    for name in [
        "acquire_timeout",
        "max_lifetime",
        "idle_timeout",
        "max_uses",
    ] {
        assert_eq!(field(c, name), "None", "{name} should be cleared");
    }
}

/// `config` replaces the whole configuration, so anything set before it is lost.
#[test]
fn config_replaces_rather_than_merges() {
    let pool = Pool::builder(LocalManager::new())
        .min_idle(4)
        .config(Config::new().max_size(2).min_idle(1))
        .build();
    assert_eq!(field(pool.config(), "min_idle"), "1");
}

#[test]
fn setters_after_config_still_apply() {
    let pool = Pool::builder(LocalManager::new())
        .config(Config::new().max_size(8))
        .min_idle(3)
        .build();
    assert_eq!(field(pool.config(), "max_size"), "8");
    assert_eq!(field(pool.config(), "min_idle"), "3");
}

/// Installing an exchange changes the pool's type, so the manager and the config
/// have to survive the move.
#[test]
fn exchange_changes_the_type_and_keeps_the_config() {
    let pool: Pool<MovableManager, Reservoir<MovableManager>> =
        Pool::builder(MovableManager::new())
            .max_size(6)
            .min_idle(2)
            .exchange(Reservoir::new(4))
            .build();

    assert_eq!(field(pool.config(), "max_size"), "6");
    assert_eq!(field(pool.config(), "min_idle"), "2");
    assert_eq!(pool.metrics().parked, 0);
}

#[test]
fn a_manager_is_reachable_through_the_handle() {
    let pool = Pool::new(LocalManager::new(), cfg());
    // Managers usually carry the address or the credentials, so callers need this.
    pool.manager().set_fail_connect(true);
    assert_eq!(pool.manager().counts().connected(), 0);
}

#[test]
fn a_cloned_handle_is_the_same_pool() {
    let pool = Pool::new(LocalManager::new(), cfg());
    let clone = pool.clone();
    pool.close();
    assert!(clone.is_closed(), "state lives behind the shared Arc");
}

#[test]
#[should_panic(expected = "max_size must be greater than zero")]
fn a_zero_max_size_panics_through_the_builder_too() {
    let _ = Pool::builder(LocalManager::new()).max_size(0).build();
}
