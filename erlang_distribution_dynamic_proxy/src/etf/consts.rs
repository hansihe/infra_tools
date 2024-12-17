pub const VERSION_TAG: u8 = 131;

#[allow(dead_code)]
pub mod tag {
    // Distribution header tags
    pub const DISTRIBUTION_HEADER: u8 = 68;
    pub const DISTRIBUTION_FRAGMENT_HEADER: u8 = 69;
    pub const DISTRIBUTION_FRAGMENT_CONT: u8 = 70;

    // Compression
    pub const COMPRESSED: u8 = 80;

    // Atom related constants
    pub const ATOM_CACHE_REF: u8 = 82;
    pub const ATOM_UTF8_EXT: u8 = 118;
    pub const SMALL_ATOM_UTF8_EXT: u8 = 119;
    pub const ATOM_EXT: u8 = 100; // deprecated
    pub const SMALL_ATOM_EXT: u8 = 115; // deprecated

    // Integer related constants
    pub const SMALL_INTEGER_EXT: u8 = 97;
    pub const INTEGER_EXT: u8 = 98;

    // Float related constants
    pub const FLOAT_EXT: u8 = 99; // superseded by NEW_FLOAT_EXT
    pub const NEW_FLOAT_EXT: u8 = 70;

    // Port related constants
    pub const PORT_EXT: u8 = 102;
    pub const NEW_PORT_EXT: u8 = 89;
    pub const V4_PORT_EXT: u8 = 120;

    // PID related constants
    pub const PID_EXT: u8 = 103;
    pub const NEW_PID_EXT: u8 = 88;

    // Tuple related constants
    pub const SMALL_TUPLE_EXT: u8 = 104;
    pub const LARGE_TUPLE_EXT: u8 = 105;

    // Map constant
    pub const MAP_EXT: u8 = 116;

    // List related constants
    pub const NIL_EXT: u8 = 106;
    pub const STRING_EXT: u8 = 107;
    pub const LIST_EXT: u8 = 108;

    // Binary related constants
    pub const BINARY_EXT: u8 = 109;
    pub const BIT_BINARY_EXT: u8 = 77;

    // Big number related constants
    pub const SMALL_BIG_EXT: u8 = 110;
    pub const LARGE_BIG_EXT: u8 = 111;

    // Reference related constants
    pub const REFERENCE_EXT: u8 = 101; // deprecated
    pub const NEW_REFERENCE_EXT: u8 = 114;
    pub const NEWER_REFERENCE_EXT: u8 = 90;

    // Function related constants
    pub const NEW_FUN_EXT: u8 = 112;
    pub const EXPORT_EXT: u8 = 113;

    // Local format constant
    pub const LOCAL_EXT: u8 = 121;
}

#[allow(dead_code)]
pub mod ctrl {
    //! Control message types for the Erlang distribution protocol

    /// Creates a link between FromPid and ToPid
    /// Format: {1, FromPid, ToPid}
    pub const CTRL_LINK: u8 = 1;

    /// Sends a message to a process
    /// Format: {2, Unused, ToPid}
    /// Note: Followed by actual Message content
    pub const CTRL_SEND: u8 = 2;

    /// Signals that a link has been broken
    /// Format: {3, FromPid, ToPid, Reason}
    pub const CTRL_EXIT: u8 = 3;

    /// Removes a link between processes (Obsolete as of OTP 26)
    /// Format: {4, FromPid, ToPid}
    /// Note: Replaced by CTRL_UNLINK_ID in the new link protocol
    pub const CTRL_UNLINK: u8 = 4;

    /// Links nodes together
    /// Format: {5}
    pub const CTRL_NODE_LINK: u8 = 5;

    /// Sends a message to a named process
    /// Format: {6, FromPid, Unused, ToName}
    /// Note: Followed by actual Message content
    pub const CTRL_REG_SEND: u8 = 6;

    /// Sets the group leader for a process
    /// Format: {7, FromPid, ToPid}
    pub const CTRL_GROUP_LEADER: u8 = 7;

    /// Signals an exit triggered by exit/2 BIF
    /// Format: {8, FromPid, ToPid, Reason}
    pub const CTRL_EXIT2: u8 = 8;

    /// Sends a message with a trace token
    /// Format: {12, Unused, ToPid, TraceToken}
    /// Note: Followed by actual Message content
    pub const CTRL_SEND_TT: u8 = 12;

    /// Signals a link break with a trace token
    /// Format: {13, FromPid, ToPid, TraceToken, Reason}
    pub const CTRL_EXIT_TT: u8 = 13;

    /// Sends a message to a named process with a trace token
    /// Format: {16, FromPid, Unused, ToName, TraceToken}
    /// Note: Followed by actual Message content
    pub const CTRL_REG_SEND_TT: u8 = 16;

    /// Signals an exit/2 with a trace token
    /// Format: {18, FromPid, ToPid, TraceToken, Reason}
    pub const CTRL_EXIT2_TT: u8 = 18;

    /// Sets up process monitoring
    /// Format: {19, FromPid, ToProc, Ref}
    /// Note: ToProc can be either a pid or registered name (atom)
    pub const CTRL_MONITOR_P: u8 = 19;

    /// Removes process monitoring
    /// Format: {20, FromPid, ToProc, Ref}
    /// Note: ToProc can be either a pid or registered name (atom)
    pub const CTRL_DEMONITOR_P: u8 = 20;

    /// Signals that a monitored process has exited
    /// Format: {21, FromProc, ToPid, Ref, Reason}
    /// Note: FromProc can be either a pid or registered name (atom)
    pub const CTRL_MONITOR_P_EXIT: u8 = 21;

    // OTP 21 control messages

    /// Enhanced send operation that includes sender information
    /// Format: {22, FromPid, ToPid}
    /// Note: Followed by actual Message content. Requires DFLAG_SEND_SENDER
    pub const CTRL_SEND_SENDER: u8 = 22;

    /// Enhanced send with trace token that includes sender information
    /// Format: {23, FromPid, ToPid, TraceToken}
    /// Note: Followed by actual Message content. Requires DFLAG_SEND_SENDER
    pub const CTRL_SEND_SENDER_TT: u8 = 23;

    // OTP 22 control messages (PAYLOAD variants)

    /// Enhanced exit signal with separate payload
    /// Format: {24, FromPid, ToPid}
    /// Note: Followed by Reason as payload. Requires DFLAG_EXIT_PAYLOAD
    pub const CTRL_PAYLOAD_EXIT: u8 = 24;

    /// Enhanced exit signal with trace token and separate payload
    /// Format: {25, FromPid, ToPid, TraceToken}
    /// Note: Followed by Reason as payload. Requires DFLAG_EXIT_PAYLOAD
    pub const CTRL_PAYLOAD_EXIT_TT: u8 = 25;

    /// Enhanced exit/2 signal with separate payload
    /// Format: {26, FromPid, ToPid}
    /// Note: Followed by Reason as payload. Requires DFLAG_EXIT_PAYLOAD
    pub const CTRL_PAYLOAD_EXIT2: u8 = 26;

    /// Enhanced exit/2 signal with trace token and separate payload
    /// Format: {27, FromPid, ToPid, TraceToken}
    /// Note: Followed by Reason as payload. Requires DFLAG_EXIT_PAYLOAD
    pub const CTRL_PAYLOAD_EXIT2_TT: u8 = 27;

    /// Enhanced monitor exit signal with separate payload
    /// Format: {28, FromProc, ToPid, Ref}
    /// Note: Followed by Reason as payload. Requires DFLAG_EXIT_PAYLOAD
    pub const CTRL_PAYLOAD_MONITOR_P_EXIT: u8 = 28;

    // OTP 23 control messages

    /// Request to spawn a new process
    /// Format: {29, ReqId, From, GroupLeader, {Module, Function, Arity}, OptList}
    /// Note: Followed by ArgList. Requires DFLAG_SPAWN
    pub const CTRL_SPAWN_REQUEST: u8 = 29;

    /// Request to spawn a new process with trace token
    /// Format: {30, ReqId, From, GroupLeader, {Module, Function, Arity}, OptList, Token}
    /// Note: Followed by ArgList. Requires DFLAG_SPAWN
    pub const CTRL_SPAWN_REQUEST_TT: u8 = 30;

    /// Reply to a spawn request
    /// Format: {31, ReqId, To, Flags, Result}
    /// Note: Result is either Pid on success or error reason atom
    pub const CTRL_SPAWN_REPLY: u8 = 31;

    /// Reply to a spawn request with trace token
    /// Format: {32, ReqId, To, Flags, Result, Token}
    /// Note: Result is either Pid on success or error reason atom
    pub const CTRL_SPAWN_REPLY_TT: u8 = 32;

    // OTP 24 control messages

    /// Sends a message to a process alias
    /// Format: {33, FromPid, Alias}
    /// Note: Followed by Message. Requires DFLAG_ALIAS
    pub const CTRL_ALIAS_SEND: u8 = 33;

    /// Sends a message to a process alias with trace token
    /// Format: {34, FromPid, Alias, Token}
    /// Note: Followed by Message. Requires DFLAG_ALIAS
    pub const CTRL_ALIAS_SEND_TT: u8 = 34;

    // New link protocol messages (mandatory as of OTP 26)

    /// Removes a link between processes with guaranteed ordering
    /// Format: {35, Id, FromPid, ToPid}
    /// Note: Part of new link protocol. Must be acknowledged with UNLINK_ID_ACK
    pub const CTRL_UNLINK_ID: u8 = 35;

    /// Acknowledges an UNLINK_ID request
    /// Format: {36, Id, FromPid, ToPid}
    /// Note: Must be sent before any other signals to the UNLINK_ID sender
    pub const CTRL_UNLINK_ID_ACK: u8 = 36;

    // Spawn reply flags

    /// Flag indicating a link was set up between processes in spawn reply
    pub const SPAWN_REPLY_LINK: u8 = 1;

    /// Flag indicating a monitor was set up between processes in spawn reply
    pub const SPAWN_REPLY_MONITOR: u8 = 2;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u8)]
    pub enum ControlMessage {
        /// Creates a link between FromPid and ToPid
        Link = CTRL_LINK,
        /// Sends a message to a process
        Send = CTRL_SEND,
        /// Signals that a link has been broken
        Exit = CTRL_EXIT,
        /// Removes a link between processes (Obsolete as of OTP 26)
        #[deprecated(note = "Obsolete as of OTP 26, use UnlinkId instead")]
        Unlink = CTRL_UNLINK,
        /// Links nodes together
        NodeLink = CTRL_NODE_LINK,
        /// Sends a message to a named process
        RegSend = CTRL_REG_SEND,
        /// Sets the group leader for a process
        GroupLeader = CTRL_GROUP_LEADER,
        /// Signals an exit triggered by exit/2 BIF
        Exit2 = CTRL_EXIT2,
        /// Sends a message with a trace token
        SendTT = CTRL_SEND_TT,
        /// Signals a link break with a trace token
        ExitTT = CTRL_EXIT_TT,
        /// Sends a message to a named process with a trace token
        RegSendTT = CTRL_REG_SEND_TT,
        /// Signals an exit/2 with a trace token
        Exit2TT = CTRL_EXIT2_TT,
        /// Sets up process monitoring
        MonitorP = CTRL_MONITOR_P,
        /// Removes process monitoring
        DemonitorP = CTRL_DEMONITOR_P,
        /// Signals that a monitored process has exited
        MonitorPExit = CTRL_MONITOR_P_EXIT,
        /// Enhanced send operation that includes sender information
        SendSender = CTRL_SEND_SENDER,
        /// Enhanced send with trace token that includes sender information
        SendSenderTT = CTRL_SEND_SENDER_TT,
        /// Enhanced exit signal with separate payload
        PayloadExit = CTRL_PAYLOAD_EXIT,
        /// Enhanced exit signal with trace token and separate payload
        PayloadExitTT = CTRL_PAYLOAD_EXIT_TT,
        /// Enhanced exit/2 signal with separate payload
        PayloadExit2 = CTRL_PAYLOAD_EXIT2,
        /// Enhanced exit/2 signal with trace token and separate payload
        PayloadExit2TT = CTRL_PAYLOAD_EXIT2_TT,
        /// Enhanced monitor exit signal with separate payload
        PayloadMonitorPExit = CTRL_PAYLOAD_MONITOR_P_EXIT,
        /// Request to spawn a new process
        SpawnRequest = CTRL_SPAWN_REQUEST,
        /// Request to spawn a new process with trace token
        SpawnRequestTT = CTRL_SPAWN_REQUEST_TT,
        /// Reply to a spawn request
        SpawnReply = CTRL_SPAWN_REPLY,
        /// Reply to a spawn request with trace token
        SpawnReplyTT = CTRL_SPAWN_REPLY_TT,
        /// Sends a message to a process alias
        AliasSend = CTRL_ALIAS_SEND,
        /// Sends a message to a process alias with trace token
        AliasSendTT = CTRL_ALIAS_SEND_TT,
        /// Removes a link between processes with guaranteed ordering
        UnlinkId = CTRL_UNLINK_ID,
        /// Acknowledges an UNLINK_ID request
        UnlinkIdAck = CTRL_UNLINK_ID_ACK,
    }

    impl ControlMessage {
        /// Converts a u8 to a ControlMessage, returning None if the value
        /// doesn't correspond to a valid control message
        pub fn from_u8(value: u8) -> Option<Self> {
            use ControlMessage::*;
            match value {
                CTRL_LINK => Some(Link),
                CTRL_SEND => Some(Send),
                CTRL_EXIT => Some(Exit),
                #[allow(deprecated)]
                CTRL_UNLINK => Some(Unlink),
                CTRL_NODE_LINK => Some(NodeLink),
                CTRL_REG_SEND => Some(RegSend),
                CTRL_GROUP_LEADER => Some(GroupLeader),
                CTRL_EXIT2 => Some(Exit2),
                CTRL_SEND_TT => Some(SendTT),
                CTRL_EXIT_TT => Some(ExitTT),
                CTRL_REG_SEND_TT => Some(RegSendTT),
                CTRL_EXIT2_TT => Some(Exit2TT),
                CTRL_MONITOR_P => Some(MonitorP),
                CTRL_DEMONITOR_P => Some(DemonitorP),
                CTRL_MONITOR_P_EXIT => Some(MonitorPExit),
                CTRL_SEND_SENDER => Some(SendSender),
                CTRL_SEND_SENDER_TT => Some(SendSenderTT),
                CTRL_PAYLOAD_EXIT => Some(PayloadExit),
                CTRL_PAYLOAD_EXIT_TT => Some(PayloadExitTT),
                CTRL_PAYLOAD_EXIT2 => Some(PayloadExit2),
                CTRL_PAYLOAD_EXIT2_TT => Some(PayloadExit2TT),
                CTRL_PAYLOAD_MONITOR_P_EXIT => Some(PayloadMonitorPExit),
                CTRL_SPAWN_REQUEST => Some(SpawnRequest),
                CTRL_SPAWN_REQUEST_TT => Some(SpawnRequestTT),
                CTRL_SPAWN_REPLY => Some(SpawnReply),
                CTRL_SPAWN_REPLY_TT => Some(SpawnReplyTT),
                CTRL_ALIAS_SEND => Some(AliasSend),
                CTRL_ALIAS_SEND_TT => Some(AliasSendTT),
                CTRL_UNLINK_ID => Some(UnlinkId),
                CTRL_UNLINK_ID_ACK => Some(UnlinkIdAck),
                _ => None,
            }
        }

        /// Converts a ControlMessage to its corresponding u8 value
        pub fn as_u8(self) -> u8 {
            self as u8
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_control_message_conversion() {
            // Test valid conversions
            assert_eq!(
                ControlMessage::from_u8(CTRL_LINK),
                Some(ControlMessage::Link)
            );
            assert_eq!(
                ControlMessage::from_u8(CTRL_UNLINK_ID_ACK),
                Some(ControlMessage::UnlinkIdAck)
            );

            // Test invalid conversions
            assert_eq!(ControlMessage::from_u8(0), None); // Too low
            assert_eq!(ControlMessage::from_u8(37), None); // Too high
            assert_eq!(ControlMessage::from_u8(9), None); // Gap in valid values
            assert_eq!(ControlMessage::from_u8(14), None); // Gap in valid values
            assert_eq!(ControlMessage::from_u8(17), None); // Gap in valid values

            // Test roundtrip
            for i in 1..=36 {
                if let Some(msg) = ControlMessage::from_u8(i) {
                    assert_eq!(msg.as_u8(), i);
                }
            }
        }

        #[test]
        fn test_all_variants() {
            use ControlMessage::*;

            let variants = [
                Link,
                Send,
                Exit,
                #[allow(deprecated)]
                Unlink,
                NodeLink,
                RegSend,
                GroupLeader,
                Exit2,
                SendTT,
                ExitTT,
                RegSendTT,
                Exit2TT,
                MonitorP,
                DemonitorP,
                MonitorPExit,
                SendSender,
                SendSenderTT,
                PayloadExit,
                PayloadExitTT,
                PayloadExit2,
                PayloadExit2TT,
                PayloadMonitorPExit,
                SpawnRequest,
                SpawnRequestTT,
                SpawnReply,
                SpawnReplyTT,
                AliasSend,
                AliasSendTT,
                UnlinkId,
                UnlinkIdAck,
            ];

            // Test that each variant can be converted to u8 and back
            for &variant in &variants {
                assert_eq!(ControlMessage::from_u8(variant.as_u8()), Some(variant));
            }
        }

        #[test]
        fn test_discriminant_values() {
            // Verify specific discriminant values match the protocol constants
            assert_eq!(ControlMessage::Link as u8, CTRL_LINK);
            assert_eq!(ControlMessage::SendTT as u8, CTRL_SEND_TT);
            assert_eq!(ControlMessage::UnlinkIdAck as u8, CTRL_UNLINK_ID_ACK);
        }
    }
}
/// Distribution capability flags used in the Erlang distribution protocol handshake.
/// These flags determine how communication between nodes should be performed.
#[allow(dead_code)]
pub mod dist_flags {
    /// The node is to be published and part of the global namespace.
    pub const DFLAG_PUBLISHED: u64 = 0x1;

    /// The node implements an atom cache (obsolete).
    pub const DFLAG_ATOM_CACHE: u64 = 0x2;

    /// The node implements extended (3 × 32 bits) references.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_EXTENDED_REFERENCES: u64 = 0x4;

    /// The node implements distributed process monitoring.
    pub const DFLAG_DIST_MONITOR: u64 = 0x8;

    /// The node uses separate tags for funs (lambdas) in the distribution protocol.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_FUN_TAGS: u64 = 0x10;

    /// The node implements distributed named process monitoring.
    pub const DFLAG_DIST_MONITOR_NAME: u64 = 0x20;

    /// The (hidden) node implements atom cache (obsolete).
    pub const DFLAG_HIDDEN_ATOM_CACHE: u64 = 0x40;

    /// The node understands the NEW_FUN_EXT tag.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_NEW_FUN_TAGS: u64 = 0x80;

    /// The node can handle extended pids and ports.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_EXTENDED_PIDS_PORTS: u64 = 0x100;

    /// The node understands the EXPORT_EXT tag.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_EXPORT_PTR_TAG: u64 = 0x200;

    /// The node understands the BIT_BINARY_EXT tag.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_BIT_BINARIES: u64 = 0x400;

    /// The node understands the NEW_FLOAT_EXT tag.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_NEW_FLOATS: u64 = 0x800;

    /// Unicode IO support flag
    pub const DFLAG_UNICODE_IO: u64 = 0x1000;

    /// The node implements atom cache in distribution header.
    pub const DFLAG_DIST_HDR_ATOM_CACHE: u64 = 0x2000;

    /// The node understands the SMALL_ATOM_EXT tag.
    pub const DFLAG_SMALL_ATOM_TAGS: u64 = 0x4000;

    /// The node understands UTF-8 atoms encoded with ATOM_UTF8_EXT and SMALL_ATOM_UTF8_EXT.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_UTF8_ATOMS: u64 = 0x10000;

    /// The node understands the map tag MAP_EXT.
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_MAP_TAG: u64 = 0x20000;

    /// The node understands big node creation tags (NEW_PID_EXT, NEW_PORT_EXT, NEWER_REFERENCE_EXT).
    /// This flag is mandatory. If not present, the connection is refused.
    pub const DFLAG_BIG_CREATION: u64 = 0x40000;

    /// Use the SEND_SENDER control message instead of SEND and SEND_SENDER_TT instead of SEND_TT.
    pub const DFLAG_SEND_SENDER: u64 = 0x80000;

    /// The node understands any term as the seqtrace label.
    pub const DFLAG_BIG_SEQTRACE_LABELS: u64 = 0x100000;

    /// Use PAYLOAD variants of exit-related control messages instead of non-PAYLOAD variants.
    pub const DFLAG_EXIT_PAYLOAD: u64 = 0x400000;

    /// Use fragmented distribution messages to send large messages.
    pub const DFLAG_FRAGMENTS: u64 = 0x800000;

    /// The node supports the new connection setup handshake (version 6) introduced in OTP 23.
    /// This flag is mandatory from OTP 25. If not present, the connection is refused.
    pub const DFLAG_HANDSHAKE_23: u64 = 0x1000000;

    /// Use the new link protocol.
    /// This flag is mandatory as of OTP 26.
    pub const DFLAG_UNLINK_ID: u64 = 0x2000000;

    /// The node supports spawn request and spawn reply control messages.
    pub const DFLAG_SPAWN: u64 = 1 << 32;

    /// Dynamic node name. Request to receive node name from accepting node during handshake.
    pub const DFLAG_NAME_ME: u64 = 1 << 33;

    /// The node accepts larger amounts of data in pids, ports and references (node container v4).
    /// Introduced in OTP 24 and mandatory in OTP 26.
    pub const DFLAG_V4_NC: u64 = 1 << 34;

    /// The node supports process aliases and ALIAS_SEND/ALIAS_SEND_TT control messages.
    /// Introduced in OTP 24.
    pub const DFLAG_ALIAS: u64 = 1 << 35;

    /// The node supports all capabilities that are mandatory in OTP 25.
    /// Will become mandatory in OTP 27.
    pub const DFLAG_MANDATORY_25_DIGEST: u64 = 1 << 36;

    /// A convenience function that returns all mandatory flags for OTP 26 compatibility
    pub fn mandatory_flags() -> u64 {
        DFLAG_EXTENDED_REFERENCES
            | DFLAG_FUN_TAGS
            | DFLAG_NEW_FUN_TAGS
            | DFLAG_EXTENDED_PIDS_PORTS
            | DFLAG_EXPORT_PTR_TAG
            | DFLAG_BIT_BINARIES
            | DFLAG_NEW_FLOATS
            | DFLAG_UTF8_ATOMS
            | DFLAG_MAP_TAG
            | DFLAG_BIG_CREATION
            | DFLAG_HANDSHAKE_23
            | DFLAG_UNLINK_ID
            | DFLAG_V4_NC
    }
}
