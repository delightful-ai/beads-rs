#![allow(dead_code)]

struct Response;

impl Response {
    fn ok() -> Self {
        Self
    }

    fn err_from<T>(_err: T) -> Self {
        Self
    }
}

struct Rotation;

struct Runtime;

impl Runtime {
    fn rotate_replica_id(&mut self) -> Result<Rotation, ()> {
        Ok(Rotation)
    }
}

struct Proof {
    runtime: Runtime,
}

impl Proof {
    fn store_id(&self) -> u64 {
        42
    }

    fn runtime_mut(&mut self) -> &mut Runtime {
        &mut self.runtime
    }
}

struct Daemon;

impl Daemon {
    fn ensure_repo_loaded_strict(&mut self) -> Result<Proof, ()> {
        Ok(Proof { runtime: Runtime })
    }

    fn reload_replication_runtime(&mut self, _store_id: u64) -> Result<(), ()> {
        Ok(())
    }

    fn admin_rotate_replica_id(&mut self) -> Response {
        let mut proof = match self.ensure_repo_loaded_strict() {
            Ok(proof) => proof,
            Err(err) => return Response::err_from(err),
        };
        let store_id = proof.store_id();
        let _rotation = match proof.runtime_mut().rotate_replica_id() {
            Ok(rotation) => rotation,
            Err(err) => return Response::err_from(err),
        };
        drop(proof);

        if let Err(err) = self.reload_replication_runtime(store_id) {
            return Response::err_from(err);
        }

        Response::ok()
    }
}

fn main() {
    let mut daemon = Daemon;
    let _ = daemon.admin_rotate_replica_id();
}
