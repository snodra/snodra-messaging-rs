//! The transaction a store add-on hands out and its stores join.

use std::any::Any;

use futures::future::BoxFuture;

use super::StoreError;

/// A transaction of the service's own database, opaque to the core: the store add-on puts its own
/// transaction type in it ([`Tx::new`]) and gets it back in its stores ([`Tx::get_mut`]), e.g. a
/// MongoDB session or an SQL transaction. [`Tx::none`] holds nothing (for stores that ignore it).
pub struct Tx {
    inner: Option<Box<dyn Any + Send>>,
}

impl Tx {
    /// A transaction holding the add-on's own `inner`.
    pub fn new<T: Any + Send>(inner: T) -> Tx {
        Tx {
            inner: Some(Box::new(inner)),
        }
    }

    /// No transaction: for stores that ignore it (in memory).
    pub fn none() -> Tx {
        Tx { inner: None }
    }

    /// Whether it is [`Tx::none`]: it holds nothing.
    pub fn is_none(&self) -> bool {
        self.inner.is_none()
    }

    /// What [`Tx::new`] put in, when it is a `T`; `None` for [`Tx::none`] or another type.
    pub fn get_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.inner.as_mut().and_then(|inner| inner.downcast_mut())
    }
}

/// Starts, commits and aborts a [`Tx`] (Spring's `TransactionOperations`).
pub trait TxProvider: Send + Sync {
    /// A transaction.
    fn begin(&self) -> BoxFuture<'_, Result<Tx, StoreError>>;
    /// No transaction: what reads and writes outside one (the admin API) run in.
    fn plain(&self) -> BoxFuture<'_, Result<Tx, StoreError>>;
    fn commit<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, Result<(), StoreError>>;
    /// Abandons the transaction; never fails (there is nothing left to do about it).
    fn abort<'a>(&'a self, tx: &'a mut Tx) -> BoxFuture<'a, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_gives_back_what_it_holds_and_nothing_else() {
        let mut tx = Tx::new(String::from("session"));
        assert_eq!(tx.get_mut::<String>().map(|s| s.as_str()), Some("session"));
        tx.get_mut::<String>().unwrap().push('!');
        assert_eq!(tx.get_mut::<String>().map(|s| s.as_str()), Some("session!"));
        assert!(tx.get_mut::<u32>().is_none(), "another type");
        assert!(Tx::none().get_mut::<String>().is_none(), "nothing");
        assert!(Tx::none().is_none() && !tx.is_none());
    }
}
