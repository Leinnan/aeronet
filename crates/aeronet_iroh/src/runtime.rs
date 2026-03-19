use {
    bevy_ecs::prelude::*,
    core::{future::Future, time::Duration},
};

/// Runtime handle used to drive iroh tasks.
#[derive(Debug, Clone, Resource)]
pub struct IrohRuntime {
    handle: tokio::runtime::Handle,
}

impl Default for IrohRuntime {
    fn default() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        let runtime = Box::leak(Box::new(runtime));
        Self {
            handle: runtime.handle().clone(),
        }
    }
}

impl From<tokio::runtime::Handle> for IrohRuntime {
    fn from(value: tokio::runtime::Handle) -> Self {
        Self { handle: value }
    }
}

impl IrohRuntime {
    /// Spawns a future on this runtime.
    pub fn spawn_on_self<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handle.spawn(future);
    }

    /// Spawns a future on the current tokio runtime.
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }

    /// Sleeps for `duration`.
    pub async fn sleep(duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
