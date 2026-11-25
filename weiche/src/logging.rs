//! enqueue application and dependency logs on the calling thread's insomnilog queue.
use anyhow::bail;
use insomnilog::{ConsoleSink, LogLevel, Logger, PatternFormatter, ShutdownGuard};
use std::{
    env, io,
    sync::{Arc, OnceLock},
};
/// apply the existing target and span filters to native insomnilog events.
pub use tracing::{Level, event_enabled};
use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

#[doc(hidden)]
pub use insomnilog::{log_debug, log_error, log_info, log_warn};

mod field;
pub use field::{Argument, Timestamp};

/// process-wide output settings, installed before starting any runtime.
pub struct Output {
    /// logger shared by producers, each of which owns a separate queue.
    pub logger: Arc<Logger>,
    /// select the structured template for application events.
    pub json: bool,
}

/// absent when library callers have not installed a logging backend.
pub static OUTPUT: OnceLock<Output> = OnceLock::new();

/// enqueue a named event, evaluating fields once and only when its filter matches.
#[macro_export]
macro_rules! log {
    (debug, $($args:tt)*) => { $crate::log!(@emit DEBUG log_debug, $($args)*) };
    (info, $($args:tt)*) => { $crate::log!(@emit INFO log_info, $($args)*) };
    (warn, $($args:tt)*) => { $crate::log!(@emit WARN log_warn, $($args)*) };
    (error, $($args:tt)*) => { $crate::log!(@emit ERROR log_error, $($args)*) };
    (@emit $level:ident $emit:ident, $message:literal $($rest:tt)*) => {
        $crate::log!(@emit $level $emit, target: module_path!(), $message $($rest)*)
    };
    (@emit $level:ident $emit:ident, target: $target:expr, $message:literal $(, $key:ident = $value:expr)* $(,)?) => {{
        if $crate::logging::event_enabled!(target: $target, $crate::logging::Level::$level, message $(, $key)*)
            && let Some(output) = $crate::logging::OUTPUT.get()
        {
            use $crate::logging::Argument as _;
            let timestamp = $crate::logging::Timestamp::now();
            // bind arguments once since insomnilog sizes each value before copying it.
            let ($($key,)*) = ($($value,)*);
            $(let $key = $key.argument();)*
            if output.json {
                let message = $message.argument();
                let target = $target.argument();
                $crate::logging::$emit!(output.logger, concat!(
                    r#"{"timestamp":"{}","level":""#, stringify!($level),
                    r#"","target":{},"fields":{"message":{}"#,
                    $(r#",""#, stringify!($key), r#"":{}"#,)* "}}"
                ), timestamp, target, message $(, $key)*);
            } else {
                $crate::logging::$emit!(output.logger, concat!(
                    "{} ", stringify!($level), " ", $target, ": {}"
                    $(, " ", stringify!($key), "={}")*
                ), timestamp, $message $(, $key)*);
            }
        }
    }};
}

/// pass already formatted dependency events through the same producer queue and sink.
struct DependencyWriter(Arc<Logger>);

impl<'a> MakeWriter<'a> for DependencyWriter {
    type Writer = &'a Self;

    /// borrow the shared logger without updating its reference count on each event.
    fn make_writer(&'a self) -> Self::Writer {
        self
    }
}

impl io::Write for &DependencyWriter {
    /// the fmt layer submits a complete UTF-8 event with its trailing newline.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let line = str::from_utf8(bytes).map_err(io::Error::other)?;
        let line = line.strip_suffix('\n').unwrap_or(line);
        insomnilog::log_info!(self.0, "{}", line);
        Ok(bytes.len())
    }

    /// the insomnilog backend flushes stdout after draining each batch.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// register a supervised thread before it begins handling requests or watch events.
pub fn prepare_thread() {
    if OUTPUT.get().is_some() {
        insomnilog::preallocate_thread();
    }
}

/// start the backend and retain its shutdown guard until the process role returns.
pub fn init() -> anyhow::Result<ShutdownGuard> {
    let json = match env::var("WEICHE_LOG_FORMAT").as_deref() {
        Ok("json") => true,
        Ok("text" | "") | Err(env::VarError::NotPresent) => false,
        Ok(other) => bail!("unsupported WEICHE_LOG_FORMAT: {other}"),
        Err(env::VarError::NotUnicode(_)) => {
            bail!("WEICHE_LOG_FORMAT must be valid UTF-8")
        }
    };
    let guard = insomnilog::start(Default::default())?;
    let sink =
        Arc::new(ConsoleSink::new(PatternFormatter::new("{message}")?, LogLevel::Trace));
    let logger = insomnilog::create_logger("weiche", vec![sink], LogLevel::Trace)?;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(DependencyWriter(logger.clone()));
    if json {
        subscriber.json().try_init().map_err(anyhow::Error::msg)?;
    } else {
        subscriber.try_init().map_err(anyhow::Error::msg)?;
    }
    OUTPUT.get_or_init(|| Output { logger, json });
    insomnilog::preallocate_thread();
    Ok(guard)
}
