use std::fmt;

use anyhow::anyhow;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

pub fn init_tracing(log_level_server: &str) -> Result<(), anyhow::Error> {
    let level = match log_level_server {
        "ERROR" => Level::ERROR,
        "WARN" => Level::WARN,
        "INFO" => Level::INFO,
        "DEBUG" => Level::DEBUG,
        "TRACE" => Level::TRACE,
        _ => return Err(anyhow!("Log level '{log_level_server}' is unsupported")),
    };

    tracing_subscriber::fmt()
        .event_format(CustomFormatter)
        .with_max_level(level)
        .init();

    Ok(())
}

pub struct CustomFormatter;

impl<S, N> FormatEvent<S, N> for CustomFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();

        // Disable SQLx log
        let target = meta.target();
        let level = meta.level();
        if target.contains("sqlx") && *level != Level::ERROR {
            return Ok(());
        }

        write!(
            writer,
            "{} ",
            chrono::Local::now().format("%d-%m-%Y %H:%M:%S.%3f")
        )?;
        write!(writer, "[{level}] Server: ")?;

        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}
