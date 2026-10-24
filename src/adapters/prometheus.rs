use std::fmt::Write;
use std::time::Duration;

use crate::application::queries::bridge_metrics::BridgeMetrics;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Prometheus text exposition for the three fixed gauge families.
pub fn render(metrics: &[BridgeMetrics]) -> String {
    let mut output = String::from(
        "# HELP bridge_matrix_last_success_age_seconds Seconds since the last successful Matrix sync; +Inf until first success.\n\
         # TYPE bridge_matrix_last_success_age_seconds gauge\n",
    );
    for metric in metrics {
        writeln!(
            output,
            "bridge_matrix_last_success_age_seconds{{bridge=\"{}\"}} {}",
            escape_label(&metric.bridge.to_string()),
            health_age(metric.matrix_last_success_age)
        )
        .unwrap();
    }
    output.push_str(
        "# HELP bridge_irc_last_success_age_seconds Seconds since the last received IRC server message; +Inf until first success.\n\
         # TYPE bridge_irc_last_success_age_seconds gauge\n",
    );
    for metric in metrics {
        writeln!(
            output,
            "bridge_irc_last_success_age_seconds{{bridge=\"{}\"}} {}",
            escape_label(&metric.bridge.to_string()),
            health_age(metric.irc_last_success_age)
        )
        .unwrap();
    }
    output.push_str(
        "# HELP bridge_pending_message_age_seconds Age of the oldest pending message since enqueue, including retries; zero if empty.\n\
         # TYPE bridge_pending_message_age_seconds gauge\n",
    );
    for metric in metrics {
        for (destination, age) in [
            ("irc", metric.pending_to_irc_age),
            ("matrix", metric.pending_to_matrix_age),
        ] {
            writeln!(output, "bridge_pending_message_age_seconds{{bridge=\"{}\",destination=\"{destination}\"}} {}",
                escape_label(&metric.bridge.to_string()), age.as_secs_f64()).unwrap();
        }
    }
    output
}

fn health_age(age: Option<Duration>) -> String {
    age.map_or_else(|| "+Inf".into(), |age| age.as_secs_f64().to_string())
}

fn escape_label(label: &str) -> String {
    label
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::BridgeName;

    #[test]
    fn renders_gauges_with_escaped_labels_unknown_health_and_fractional_seconds() {
        let text = render(&[BridgeMetrics {
            bridge: BridgeName::new("a\"b\\c\nd").unwrap(),
            matrix_last_success_age: None,
            irc_last_success_age: Some(Duration::from_millis(1_250)),
            pending_to_matrix_age: Duration::ZERO,
            pending_to_irc_age: Duration::from_secs(9),
        }]);
        let label = "a\\\"b\\\\c\\nd";
        assert!(text.contains(&format!(
            "bridge_matrix_last_success_age_seconds{{bridge=\"{label}\"}} +Inf\n"
        )));
        assert!(text.contains(&format!(
            "bridge_irc_last_success_age_seconds{{bridge=\"{label}\"}} 1.25\n"
        )));
        assert!(text.contains(&format!(
            "bridge_pending_message_age_seconds{{bridge=\"{label}\",destination=\"irc\"}} 9\n"
        )));
        assert!(text.contains(&format!(
            "bridge_pending_message_age_seconds{{bridge=\"{label}\",destination=\"matrix\"}} 0\n"
        )));
        assert_eq!(text.matches("# TYPE ").count(), 3);
        assert_eq!(text.matches("# HELP ").count(), 3);
    }
}
