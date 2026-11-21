//! forward opaque TCP connections, retaining half-close semantics in both directions.
use super::super::super::ir::StreamRoute;
use super::super::core::tcp::Admission;
use compio::{
    io::copy,
    net::{TcpListener, TcpStream},
    runtime::time::timeout,
};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{io, sync::Arc, time::Duration};
use tokio::sync::watch;

/// select a backend on accept. existing streams keep their peer across endpoint updates.
pub(super) async fn serve(
    socket: TcpListener,
    mut routes: watch::Receiver<Arc<StreamRoute>>,
    admission: Arc<Admission>,
    connect_timeout: Duration,
) -> io::Result<()> {
    let mut connections = FuturesUnordered::new();
    loop {
        tokio::select! {
            changed=routes.changed()=>if changed.is_err(){return Ok(())},
            _=connections.next(),if !connections.is_empty()=>{},
            accepted=socket.accept()=>{
                let (client,_)=accepted?;
                if let Some(address)=routes.borrow().select() && let Some(permit)=admission.acquire() {
                    connections.push(async move {
                        let _permit=permit;
                        let Ok(Ok(upstream))=timeout(connect_timeout,TcpStream::connect(address)).await else{return};
                        let (mut cr,mut cw)=client.split();
                        let (mut ur,mut uw)=upstream.split();
                        // copy shuts down the destination writer on EOF; either I/O error ends both directions.
                        let _=futures::try_join!(copy(&mut cr,&mut uw),copy(&mut ur,&mut cw));
                    });
                }
            }
        }
    }
}
