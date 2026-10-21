// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Hard placement pools. One node-session log has one follower ensemble, so
//! owners and followers belong to the same pool; this is not per-cell sharding.

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pool {
    name: [u8; 63],
    len: u8,
}

impl Pool {
    pub const DEFAULT: Self = Self {
        name: [0; 63],
        len: 0,
    };

    pub fn as_str(&self) -> &str {
        if self.len == 0 {
            "default"
        } else {
            std::str::from_utf8(&self.name[..usize::from(self.len)]).expect("validated ASCII pool")
        }
    }

    /// Named pools require a hot-capable follower. Legacy/default pools keep
    /// their ordinary durable-log replication policy.
    pub fn accepts_follower(self, pool: Self, hot_recovery: bool) -> bool {
        self == pool && (self == Self::DEFAULT || hot_recovery)
    }
}

impl Default for Pool {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl std::str::FromStr for Pool {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 63
            || !value.as_bytes()[0].is_ascii_lowercase()
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            })
        {
            return Err("placement pool must be 1–63 lowercase ASCII letters/digits/-/_, starting with a letter");
        }
        if value == "default" {
            return Ok(Self::DEFAULT);
        }
        let mut pool = Self::DEFAULT;
        pool.name[..value.len()].copy_from_slice(value.as_bytes());
        pool.len = value.len() as u8;
        Ok(pool)
    }
}

impl std::fmt::Display for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_names_are_bounded_canonical_and_path_safe() {
        assert_eq!("default".parse::<Pool>().unwrap(), Pool::DEFAULT);
        assert_eq!(
            "gpu_a100-80gb".parse::<Pool>().unwrap().as_str(),
            "gpu_a100-80gb"
        );
        for invalid in [
            "",
            "GPU",
            " gpu",
            "gpu/../cpu",
            "gpu.cpu",
            "1gpu",
            "gpu\n",
            "💻",
        ] {
            assert!(invalid.parse::<Pool>().is_err(), "{invalid:?}");
        }
        assert!("g".repeat(63).parse::<Pool>().is_ok());
        assert!("g".repeat(64).parse::<Pool>().is_err());
    }

    #[test]
    fn gpu_followers_must_match_the_pool_and_support_hot_recovery() {
        let gpu = "gpu".parse::<Pool>().unwrap();
        assert!(gpu.accepts_follower(gpu, true));
        assert!(!gpu.accepts_follower(gpu, false));
        assert!(!gpu.accepts_follower(Pool::DEFAULT, true));
        assert!(!Pool::DEFAULT.accepts_follower(gpu, true));
        assert!(Pool::DEFAULT.accepts_follower(Pool::DEFAULT, false));
    }
}
