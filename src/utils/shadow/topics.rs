//! AWS IoT Device Shadow topic management
//!
//! This module handles the construction and management of MQTT topics used
//! for AWS IoT Device Shadow operations.

/// Shadow topic builder and manager
#[derive(Debug, Clone)]
pub struct ShadowTopics {
    /// Base topic prefix for this shadow
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

    /// Check if a topic is a shadow-related topic for this shadow
    pub fn is_shadow_topic(&self, topic: &str) -> bool {
        topic.starts_with(&self.base)
    }

    /// Determine the operation type from a response topic
    pub fn parse_response_topic(&self, topic: &str) -> Option<ShadowResponseType> {
        if topic == self.update_accepted {
            Some(ShadowResponseType::UpdateAccepted)
        } else if topic == self.update_rejected {
            Some(ShadowResponseType::UpdateRejected)
        } else if topic == self.get_accepted {
            Some(ShadowResponseType::GetAccepted)
        } else if topic == self.get_rejected {
            Some(ShadowResponseType::GetRejected)
        } else if topic == self.delete_accepted {
            Some(ShadowResponseType::DeleteAccepted)
        } else if topic == self.delete_rejected {
            Some(ShadowResponseType::DeleteRejected)
        } else if topic == self.delta {
            Some(ShadowResponseType::Delta)
        } else {
            None
        }
    }
}

/// Types of shadow response topics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowResponseType {
    UpdateAccepted,
    UpdateRejected,
    GetAccepted,
    GetRejected,
    DeleteAccepted,
    DeleteRejected,
    Delta,
}

impl ShadowResponseType {
    /// Check if this is an accepted response
    pub fn is_accepted(&self) -> bool {
        matches!(
            self,
            ShadowResponseType::UpdateAccepted
                | ShadowResponseType::GetAccepted
                | ShadowResponseType::DeleteAccepted
        )
    }

    /// Check if this is a rejected response
    pub fn is_rejected(&self) -> bool {
        matches!(
            self,
            ShadowResponseType::UpdateRejected
                | ShadowResponseType::GetRejected
                | ShadowResponseType::DeleteRejected
        )
    }

    /// Check if this is a delta notification
    pub fn is_delta(&self) -> bool {
        matches!(self, ShadowResponseType::Delta)
    }

    /// Get the operation type (update, get, delete, or delta)
    pub fn operation(&self) -> ShadowOperation {
        match self {
            ShadowResponseType::UpdateAccepted | ShadowResponseType::UpdateRejected => {
                ShadowOperation::Update
            }
            ShadowResponseType::GetAccepted | ShadowResponseType::GetRejected => {
                ShadowOperation::Get
            }
            ShadowResponseType::DeleteAccepted | ShadowResponseType::DeleteRejected => {
                ShadowOperation::Delete
            }
            ShadowResponseType::Delta => ShadowOperation::Delta,
        }
    }
}

/// Shadow operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowOperation {
    Update,
    Get,
    Delete,
    Delta,
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

    #[test]
    fn test_topic_parsing() {
        let topics = ShadowTopics::new_classic("test-device");

        assert_eq!(
            topics.parse_response_topic(&topics.update_accepted),
            Some(ShadowResponseType::UpdateAccepted)
        );
        assert_eq!(
            topics.parse_response_topic(&topics.update_rejected),
            Some(ShadowResponseType::UpdateRejected)
        );
        assert_eq!(
            topics.parse_response_topic(&topics.delta),
            Some(ShadowResponseType::Delta)
        );
        assert_eq!(
            topics.parse_response_topic("$aws/things/other-device/shadow/update/accepted"),
            None
        );
    }

    #[test]
    fn test_response_type_methods() {
        assert!(ShadowResponseType::UpdateAccepted.is_accepted());
        assert!(!ShadowResponseType::UpdateAccepted.is_rejected());
        assert!(!ShadowResponseType::UpdateAccepted.is_delta());

        assert!(ShadowResponseType::UpdateRejected.is_rejected());
        assert!(!ShadowResponseType::UpdateRejected.is_accepted());

        assert!(ShadowResponseType::Delta.is_delta());
        assert!(!ShadowResponseType::Delta.is_accepted());
        assert!(!ShadowResponseType::Delta.is_rejected());
    }

    #[test]
    fn test_is_shadow_topic() {
        let topics = ShadowTopics::new_classic("my-device");

        assert!(topics.is_shadow_topic("$aws/things/my-device/shadow/update"));
        assert!(topics.is_shadow_topic("$aws/things/my-device/shadow/update/accepted"));
        assert!(topics.is_shadow_topic("$aws/things/my-device/shadow/get/rejected"));
        assert!(!topics.is_shadow_topic("$aws/things/other-device/shadow/update"));
        assert!(!topics.is_shadow_topic("some/other/topic"));
    }
}
