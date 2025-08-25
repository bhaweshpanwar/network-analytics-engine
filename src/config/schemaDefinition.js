const schema = {
  a_party_id: {
    label: 'Subscriber ID',
    required: true,
    description: 'The unique ID for the user (e.g., MSISDN, UserID).',
  },
  start_time: {
    label: 'Session Start Time',
    required: true,
    description: 'When the data session began.',
  },
  end_time: {
    label: 'Session End Time',
    required: false,
    description: 'When the data session ended.',
  },
  duration_ms: {
    label: 'Duration (ms)',
    required: false,
    description: 'The length of the session in milliseconds.',
  },
  dst_ip: {
    label: 'Destination IP',
    required: true,
    description: "The B-Party's IP address.",
  },
  dst_port: {
    label: 'Destination Port',
    required: true,
    description: "The B-Party's port.",
  },
  nat_ip: {
    label: 'Public/NAT IP',
    required: false,
    description: "The user's public-facing IP address.",
  },
  src_ip: {
    label: 'Private/Source IP',
    required: false,
    description: "The user's internal device IP.",
  },
  bytes_up: {
    label: 'Data Uploaded (Bytes)',
    required: false,
    description: 'Bytes sent from the user.',
  },
  bytes_down: {
    label: 'Data Downloaded (Bytes)',
    required: false,
    description: 'Bytes received by the user.',
  },
  protocol: {
    label: 'Protocol',
    required: false,
    description: 'e.g., TCP, UDP.',
  },
};

module.exports = schema;
