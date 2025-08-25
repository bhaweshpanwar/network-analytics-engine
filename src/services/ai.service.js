const { GoogleGenerativeAI } = require('@google/generative-ai');
require('dotenv').config();
const schemaDefinition = require('../config/schemaDefinition');

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const FIELD_PATTERNS = {
  a_party_id: [
    'msisdn',
    'imsi',
    'subscriber_id',
    'user_id',
    'calling_party',
    'a_number',
    'mobile_number',
    'phone_number',
    'caller_id',
    'subscriber',
    'account_id',
    'customer_id',
  ],
  src_ip: [
    'a_party_ip',
    'source_ip',
    'private_ip',
    'client_ip',
    'user_ip',
    'internal_ip',
    'local_ip',
    'originating_ip',
  ],
  dst_ip: [
    'b_party_ip',
    'destination_ip',
    'dest_ip',
    'server_ip',
    'target_ip',
    'remote_ip',
    'external_ip',
    'called_ip',
  ],
  start_time: [
    'start_time',
    'session_start',
    'begin_time',
    'call_start',
    'timestamp',
    'start_timestamp',
    'session_begin',
    'connection_start',
  ],
  end_time: [
    'end_time',
    'session_end',
    'stop_time',
    'call_end',
    'finish_time',
    'end_timestamp',
    'session_stop',
    'connection_end',
  ],
  dst_port: [
    'b_party_port',
    'destination_port',
    'dest_port',
    'server_port',
    'target_port',
    'remote_port',
    'called_port',
  ],
  protocol: ['protocol', 'l4_protocol', 'transport_protocol', 'ip_protocol'],
  bytes_up: [
    'bytes_up',
    'uplink_volume',
    'upload_bytes',
    'tx_bytes',
    'sent_bytes',
    'outbound_bytes',
    'upstream_bytes',
  ],
  bytes_down: [
    'bytes_down',
    'downlink_volume',
    'download_bytes',
    'rx_bytes',
    'received_bytes',
    'inbound_bytes',
    'downstream_bytes',
    'bytes_transferred',
  ],
};

function fuzzyMatchField(header, patterns) {
  const headerLower = header.toLowerCase().replace(/[_\s-]/g, '');

  for (const pattern of patterns) {
    const patternLower = pattern.toLowerCase().replace(/[_\s-]/g, '');

    // Exact match
    if (headerLower === patternLower) return 1.0;

    // Contains match
    if (
      headerLower.includes(patternLower) ||
      patternLower.includes(headerLower)
    ) {
      return 0.8;
    }

    // Partial match (Levenshtein-like)
    const similarity = calculateSimilarity(headerLower, patternLower);
    if (similarity > 0.7) return similarity;
  }

  return 0;
}

function calculateSimilarity(str1, str2) {
  const longer = str1.length > str2.length ? str1 : str2;
  const shorter = str1.length > str2.length ? str2 : str1;

  if (longer.length === 0) return 1.0;

  const editDistance = levenshteinDistance(longer, shorter);
  return (longer.length - editDistance) / longer.length;
}

function levenshteinDistance(str1, str2) {
  const matrix = Array(str2.length + 1)
    .fill(null)
    .map(() => Array(str1.length + 1).fill(null));

  for (let i = 0; i <= str1.length; i++) matrix[0][i] = i;
  for (let j = 0; j <= str2.length; j++) matrix[j][0] = j;

  for (let j = 1; j <= str2.length; j++) {
    for (let i = 1; i <= str1.length; i++) {
      const indicator = str1[i - 1] === str2[j - 1] ? 0 : 1;
      matrix[j][i] = Math.min(
        matrix[j][i - 1] + 1, // insertion
        matrix[j - 1][i] + 1, // deletion
        matrix[j - 1][i - 1] + indicator // substitution
      );
    }
  }

  return matrix[str2.length][str1.length];
}

function createFallbackMapping(fileHeaders) {
  const mapping = {};

  // Initialize all fields to null
  Object.keys(schemaDefinition).forEach((field) => {
    mapping[field] = null;
  });

  // Try to find best matches using fuzzy matching
  Object.keys(FIELD_PATTERNS).forEach((schemaField) => {
    let bestMatch = null;
    let bestScore = 0;

    fileHeaders.forEach((header) => {
      const score = fuzzyMatchField(header, FIELD_PATTERNS[schemaField]);
      if (score > bestScore && score > 0.6) {
        // Minimum confidence threshold
        bestScore = score;
        bestMatch = header;
      }
    });

    if (bestMatch) {
      mapping[schemaField] = bestMatch;
      console.log(
        `Fallback mapping: ${schemaField} -> ${bestMatch} (confidence: ${(
          bestScore * 100
        ).toFixed(1)}%)`
      );
    }
  });

  return mapping;
}

async function getSuggestedMapping(fileHeaders) {
  const model = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });

  const schemaDescription = Object.entries(schemaDefinition)
    .map(([key, { label, description, required }]) => {
      const requiredText = required ? ' **[REQUIRED]**' : ' [OPTIONAL]';
      const examples = FIELD_PATTERNS[key]
        ? ` (Common names: ${FIELD_PATTERNS[key].slice(0, 3).join(', ')})`
        : '';
      return `- "${key}"${requiredText}: ${label} - ${description}${examples}`;
    })
    .join('\n');

  const prompt = `
You are an expert IPDR/CDR telecommunications data mapping specialist.

🎯 CRITICAL MISSION: Map file headers to our telecom database schema with HIGH ACCURACY.

📋 OUR SCHEMA (⚠️ = ABSOLUTELY REQUIRED):
${schemaDescription}

📄 FILE HEADERS: ${JSON.stringify(fileHeaders)}

🧠 MAPPING INTELLIGENCE RULES:

1. **SUBSCRIBER IDENTIFICATION (CRITICAL):**
   - a_party_id = ONLY subscriber identifiers (MSISDN like "919876543210", IMSI, UserID, subscriber_id)
   - a_party_id ≠ IP addresses, session IDs, or connection IDs
   - If no subscriber ID exists, map a_party_id to null

2. **IP ADDRESS MAPPING:**
   - src_ip = A-Party/source/client/user IP addresses (a_party_ip, source_ip, private_ip)
   - dst_ip = B-Party/destination/server IP addresses (b_party_ip, dest_ip, server_ip)

3. **PORT MAPPING:**
   - dst_port = destination/server/target port (NOT source port)

4. **DATA VOLUME:**
   - bytes_up = uplink/upload/sent/tx data
   - bytes_down = downlink/download/received/rx data
   - If only one volume field exists, map to bytes_down

5. **TIME FIELDS:**
   - start_time = session/call/connection start
   - duration_ms = can be in seconds (we'll convert) or milliseconds

6. **CONFIDENCE REQUIREMENTS:**
   - Only map if 80%+ confident
   - When uncertain, map to null (we have fallback logic)

🎯 RETURN: Raw JSON object only. No explanations. No markdown.

Example format: {"a_party_id": "subscriber_id", "src_ip": "a_party_ip", ...}
`;

  try {
    const result = await model.generateContent(prompt);
    const response = await result.response;
    let text = response.text();

    // Clean up the response
    text = text
      .replace(/```json/g, '')
      .replace(/```/g, '')
      .replace(/^\s*[\r\n]/gm, '') // Remove empty lines
      .trim();

    let aiMapping = JSON.parse(text);

    // Validate AI mapping quality
    const requiredFields = Object.keys(schemaDefinition).filter(
      (field) => schemaDefinition[field].required
    );

    const mappedRequiredFields = requiredFields.filter(
      (field) => aiMapping[field] && aiMapping[field] !== null
    );

    console.log(
      `🤖 AI mapped ${mappedRequiredFields.length}/${requiredFields.length} required fields`
    );

    // If AI mapping is poor, enhance with fallback
    if (mappedRequiredFields.length < requiredFields.length * 0.6) {
      console.log('🔄 AI mapping incomplete, enhancing with fallback logic...');
      const fallbackMapping = createFallbackMapping(fileHeaders);

      // Merge AI mapping with fallback (AI takes precedence)
      Object.keys(fallbackMapping).forEach((field) => {
        if (!aiMapping[field] && fallbackMapping[field]) {
          aiMapping[field] = fallbackMapping[field];
          console.log(`📈 Enhanced: ${field} -> ${fallbackMapping[field]}`);
        }
      });
    }

    return aiMapping;
  } catch (error) {
    console.error('🚨 Gemini API failed, using fallback mapping:', error);

    // Complete fallback to rule-based mapping
    return createFallbackMapping(fileHeaders);
  }
}

// Additional validation function
function validateMappingQuality(mapping, fileHeaders) {
  const issues = [];
  const suggestions = [];

  // Check if a_party_id looks like an IP address
  if (mapping.a_party_id && fileHeaders.includes(mapping.a_party_id)) {
    const headerName = mapping.a_party_id.toLowerCase();
    if (headerName.includes('ip') || headerName.includes('address')) {
      issues.push({
        field: 'a_party_id',
        issue: 'Mapped field appears to be an IP address, not a subscriber ID',
        suggestion:
          'Look for MSISDN, IMSI, subscriber_id, or user_id fields instead',
      });
    }
  }

  // Check if src_ip and dst_ip are swapped
  if (mapping.src_ip && mapping.dst_ip) {
    const srcHeader = mapping.src_ip.toLowerCase();
    const dstHeader = mapping.dst_ip.toLowerCase();

    if (
      srcHeader.includes('dest') ||
      srcHeader.includes('server') ||
      srcHeader.includes('b_party')
    ) {
      issues.push({
        field: 'src_ip',
        issue: 'Source IP appears to be mapped to a destination field',
        suggestion: 'Check if src_ip and dst_ip mappings are swapped',
      });
    }
  }

  return { issues, suggestions };
}

module.exports = {
  getSuggestedMapping,
  validateMappingQuality,
  createFallbackMapping,
  FIELD_PATTERNS,
};
