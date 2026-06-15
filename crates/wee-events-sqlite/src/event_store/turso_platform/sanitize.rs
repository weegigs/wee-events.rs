/// Sanitize a partition name into a valid Turso database name.
///
/// Turso database names allow only lowercase letters, digits, and dashes,
/// with a maximum length of 51 characters. The result is `{prefix}-{sanitized}`.
pub(crate) fn sanitize_database_name(partition_name: &str, prefix: &str) -> String {
    if partition_name.is_empty() {
        return sanitize_fragment(prefix, 51);
    }

    let prefix = named_database_prefix(prefix);
    let partition_hash = stable_hash_hex(partition_name);
    let remaining = 51 - prefix.len() - partition_hash.len() - 2;
    let partition = sanitize_fragment(partition_name, remaining);
    if partition.is_empty() {
        return format!("{prefix}-{partition_hash}");
    }

    format!("{prefix}-{partition}-{partition_hash}")
}

fn sanitize_fragment(raw: &str, limit: usize) -> String {
    let mut result = String::with_capacity(limit.min(51));
    let mut prev_dash = false;

    for c in raw.chars() {
        let out = match c {
            'a'..='z' | '0'..='9' => {
                prev_dash = false;
                c
            }
            'A'..='Z' => {
                prev_dash = false;
                c.to_ascii_lowercase()
            }
            _ => {
                if prev_dash || result.is_empty() {
                    continue;
                }
                prev_dash = true;
                '-'
            }
        };
        if result.len() >= limit {
            break;
        }
        result.push(out);
    }

    // Strip trailing dash
    while result.ends_with('-') {
        result.pop();
    }

    result
}

pub(crate) fn named_database_prefix(prefix: &str) -> String {
    const PREFIX_FRAGMENT_LIMIT: usize = 12;
    const PREFIX_HASH_LEN: usize = 8;

    let fragment = sanitize_fragment(prefix, PREFIX_FRAGMENT_LIMIT);
    let fragment = if fragment.is_empty() {
        "db".to_string()
    } else {
        fragment
    };
    let prefix_hash = stable_hash_hex(prefix);

    format!("{fragment}-{}", &prefix_hash[..PREFIX_HASH_LEN])
}

fn stable_hash_hex(raw: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for byte in raw.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_lowercase_name_gets_prefixed() {
        assert_eq!(
            sanitize_database_name("orders", "myapp"),
            format!(
                "{}-orders-{}",
                named_database_prefix("myapp"),
                stable_hash_hex("orders")
            )
        );
    }

    #[test]
    fn uppercase_is_lowercased() {
        assert_eq!(
            sanitize_database_name("Orders", "myapp"),
            format!(
                "{}-orders-{}",
                named_database_prefix("myapp"),
                stable_hash_hex("Orders")
            )
        );
    }

    #[test]
    fn special_chars_become_dashes() {
        assert_eq!(
            sanitize_database_name("tenant:acme/us-east", "myapp"),
            format!(
                "{}-tenant-acme-us-east-{}",
                named_database_prefix("myapp"),
                stable_hash_hex("tenant:acme/us-east")
            )
        );
    }

    #[test]
    fn underscores_become_dashes() {
        assert_eq!(
            sanitize_database_name("my_db", "app"),
            format!(
                "{}-my-db-{}",
                named_database_prefix("app"),
                stable_hash_hex("my_db")
            )
        );
    }

    #[test]
    fn consecutive_dashes_are_collapsed() {
        assert_eq!(
            sanitize_database_name("a--b", "myapp"),
            format!(
                "{}-a-b-{}",
                named_database_prefix("myapp"),
                stable_hash_hex("a--b")
            )
        );
    }

    #[test]
    fn trailing_special_chars_are_stripped() {
        assert_eq!(
            sanitize_database_name("trail:", "myapp"),
            format!(
                "{}-trail-{}",
                named_database_prefix("myapp"),
                stable_hash_hex("trail:")
            )
        );
    }

    #[test]
    fn long_names_are_truncated_to_51_chars() {
        let long_name = "a".repeat(100);
        let result = sanitize_database_name(&long_name, "myapp");
        assert!(result.len() <= 51);
        assert!(result.starts_with(&format!("{}-", named_database_prefix("myapp"))));
    }

    #[test]
    fn empty_partition_name_returns_prefix_only() {
        assert_eq!(sanitize_database_name("", "myapp"), "myapp");
    }

    #[test]
    fn mixed_case_complex_name() {
        assert_eq!(
            sanitize_database_name("Tenant:ACME/US_East", "ev"),
            format!(
                "{}-tenant-acme-us-east-{}",
                named_database_prefix("ev"),
                stable_hash_hex("Tenant:ACME/US_East")
            )
        );
    }

    #[test]
    fn default_partition_sanitizes_prefix() {
        assert_eq!(sanitize_database_name("", "My_App:"), "my-app");
    }

    #[test]
    fn partition_is_dropped_when_prefix_already_uses_remaining_budget() {
        let prefix = "a".repeat(50);
        let result = sanitize_database_name("b", &prefix);

        assert!(result.len() <= 51);
        assert!(result.ends_with(&stable_hash_hex("b")));
    }

    #[test]
    fn different_logical_names_do_not_collapse_after_sanitization() {
        assert_ne!(
            sanitize_database_name("tenant/acme", "myapp"),
            sanitize_database_name("tenant:acme", "myapp")
        );
    }

    #[test]
    fn different_long_logical_names_do_not_collapse_after_truncation() {
        let left = format!("tenant-{}", "a".repeat(80));
        let right = format!("tenant-{}b", "a".repeat(80));

        assert_ne!(
            sanitize_database_name(&left, "myapp"),
            sanitize_database_name(&right, "myapp")
        );
    }
}
