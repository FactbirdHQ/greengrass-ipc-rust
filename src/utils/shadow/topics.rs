//! AWS IoT Device Shadow topic management
//!
//! This module handles the construction and management of MQTT topics used
//! for AWS IoT Device Shadow operations.

/// Shadow topic builder and manager
#[derive(Debug, Clone)]
pub struct ShadowTopics {
    /// Base topic prefix for this shadow
    #[allow(dead_code)]
    pub base: String,

    // Update operation topics
    pub update: String,
    pub update_accepted: String,
    pub update_rejected: String,

    // Get operation topics
    pub get: String,
    pub get_accepted: String,
    pub get_rejected: String,

    // Delete operation topics
    pub delete: String,
    pub delete_accepted: String,
    pub delete_rejected: String,

    // Delta topic for change notifications
    pub delta: String,
}

impl ShadowTopics {
    /// Create topics for a classic (unnamed) shadow
    pub fn new_classic(thing_name: &str) -> Self {
        let base = format!("$aws/things/{}/shadow", thing_name);
        Self::from_base(base)
    }

    /// Create topics for a named shadow
    pub fn new_named(thing_name: &str, shadow_name: &str) -> Self {
        let base = format!("$aws/things/{}/shadow/name/{}", thing_name, shadow_name);
        Self::from_base(base)
    }

    /// Create topics from a base topic path
    fn from_base(base: String) -> Self {
        Self {
            // Update topics
            update: format!("{}/update", base),
            update_accepted: format!("{}/update/accepted", base),
            update_rejected: format!("{}/update/rejected", base),

            // Get topics
            get: format!("{}/get", base),
            get_accepted: format!("{}/get/accepted", base),
            get_rejected: format!("{}/get/rejected", base),

            // Delete topics
            delete: format!("{}/delete", base),
            delete_accepted: format!("{}/delete/accepted", base),
            delete_rejected: format!("{}/delete/rejected", base),

            // Delta topic
            delta: format!("{}/update/delta", base),

            base,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classic_shadow_topics() {
        let topics = ShadowTopics::new_classic("my-device");

        assert_eq!(topics.base, "$aws/things/my-device/shadow");
        assert_eq!(topics.update, "$aws/things/my-device/shadow/update");
        assert_eq!(
            topics.update_accepted,
            "$aws/things/my-device/shadow/update/accepted"
        );
        assert_eq!(
            topics.update_rejected,
            "$aws/things/my-device/shadow/update/rejected"
        );
        assert_eq!(topics.get, "$aws/things/my-device/shadow/get");
        assert_eq!(
            topics.get_accepted,
            "$aws/things/my-device/shadow/get/accepted"
        );
        assert_eq!(
            topics.get_rejected,
            "$aws/things/my-device/shadow/get/rejected"
        );
        assert_eq!(topics.delete, "$aws/things/my-device/shadow/delete");
        assert_eq!(
            topics.delete_accepted,
            "$aws/things/my-device/shadow/delete/accepted"
        );
        assert_eq!(
            topics.delete_rejected,
            "$aws/things/my-device/shadow/delete/rejected"
        );
        assert_eq!(topics.delta, "$aws/things/my-device/shadow/update/delta");
    }

    #[test]
    fn test_named_shadow_topics() {
        let topics = ShadowTopics::new_named("my-device", "config");

        assert_eq!(topics.base, "$aws/things/my-device/shadow/name/config");
        assert_eq!(
            topics.update,
            "$aws/things/my-device/shadow/name/config/update"
        );
        assert_eq!(
            topics.update_accepted,
            "$aws/things/my-device/shadow/name/config/update/accepted"
        );
        assert_eq!(
            topics.delta,
            "$aws/things/my-device/shadow/name/config/update/delta"
        );
    }
}
