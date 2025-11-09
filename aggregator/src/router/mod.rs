// Router module - main routing and execution plane
// This file implements the router that compiles strategies into PTBs,
// selects optimal routes, and executes through the Transaction Driver
//
// Numan Thabit 2025 Nov

pub mod execution;
pub mod routes;
pub mod selector;
pub mod validator;
pub mod validation;

#[allow(clippy::module_inception)]
pub mod router;

pub use execution::ExecutionEngine;
pub use routes::{Route, RoutePlan, RouteScore};
pub use selector::RouteSelector;
pub use validator::ValidatorSelector;
pub use router::Router;

