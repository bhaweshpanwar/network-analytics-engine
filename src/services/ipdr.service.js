const fs = require('fs');
const { Transform, pipeline } = require('stream');
const csv = require('csv-parser');
const pool = require('../config/db');
const { from } = require('pg-copy-streams');
const { getSuggestedMapping } = require('./ai.service');

// Define critical required fields
const REQUIRED_FIELDS = {
  a_party_id: 'Subscriber identifier is absolutely required for IPDR analysis',
  src_ip: "Source IP is required to identify the subscriber's network endpoint",
  dst_ip: 'Destination IP is required to identify what service was accessed',
  dst_port: 'Destination port is required to identify the service type',
  start_time: 'Session start time is required for temporal analysis',
};

async function analyzeFileHeaders(filePath) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('headers', (headerList) => {
        getSuggestedMapping(headerList)
          .then((suggestedMapping) => {
            // Validate required fields mapping
            const validation = validateRequiredFields(
              suggestedMapping,
              headerList
            );

            resolve({
              fileHeaders: headerList,
              suggestedMapping: suggestedMapping,
              validation: validation,
              isValid: validation.isValid,
              errors: validation.errors,
              warnings: validation.warnings,
            });
          })
          .catch(reject);
      })
      .on('error', reject)
      .on('data', () => {}); // We only need headers
  });
}

function validateRequiredFields(mapping, fileHeaders) {
  const errors = [];
  const warnings = [];

  // Check for critical required fields
  Object.keys(REQUIRED_FIELDS).forEach((requiredField) => {
    if (!mapping[requiredField] || mapping[requiredField] === null) {
      errors.push({
        field: requiredField,
        message: REQUIRED_FIELDS[requiredField],
        severity: 'error',
      });
    }
  });

  // Check for missing important optional fields
  const IMPORTANT_OPTIONAL = ['bytes_up', 'bytes_down', 'protocol'];
  IMPORTANT_OPTIONAL.forEach((field) => {
    if (!mapping[field] || mapping[field] === null) {
      warnings.push({
        field: field,
        message: `${field} is not mapped - data volume/protocol analysis will be limited`,
        severity: 'warning',
      });
    }
  });

  // Check if we have subscriber ID vs just session ID
  if (mapping.a_party_id) {
    const mappedHeader = mapping.a_party_id.toLowerCase();
    if (
      mappedHeader.includes('session') &&
      !mappedHeader.includes('subscriber') &&
      !mappedHeader.includes('msisdn') &&
      !mappedHeader.includes('imsi')
    ) {
      warnings.push({
        field: 'a_party_id',
        message:
          'Mapped field appears to be session ID rather than subscriber ID - this may limit subscriber analysis',
        severity: 'warning',
      });
    }
  }

  return {
    isValid: errors.length === 0,
    errors: errors,
    warnings: warnings,
    requiredFieldsCount: Object.keys(REQUIRED_FIELDS).length,
    mappedRequiredCount: Object.keys(REQUIRED_FIELDS).filter(
      (field) => mapping[field] && mapping[field] !== null
    ).length,
  };
}

function getServiceLabel(port) {
  const portNum = parseInt(port, 10);
  if (isNaN(portNum)) return 'UNKNOWN';

  const portMap = {
    80: 'HTTP/WEB',
    443: 'HTTPS/WEB',
    53: 'DNS',
    22: 'SSH',
    21: 'FTP',
    25: 'SMTP',
    110: 'POP3',
    143: 'IMAP',
    993: 'IMAPS',
    995: 'POP3S',
    3306: 'MySQL',
    5432: 'PostgreSQL',
    6379: 'Redis',
    27017: 'MongoDB',
    8080: 'HTTP-ALT',
    8443: 'HTTPS-ALT',
    1194: 'OpenVPN',
    1723: 'PPTP',
    5060: 'SIP',
  };

  return portMap[portNum] || 'OTHER';
}

// Generate session identifier when a_party_id is missing or suspicious
function generateSessionIdentifier(record) {
  const timestamp = record.start_time
    ? new Date(record.start_time).getTime()
    : Date.now();
  const srcInfo = record.src_ip || 'unknown';
  return `${srcInfo}_${timestamp}_${Math.random().toString(36).substr(2, 9)}`;
}

class IPDRProcessor extends Transform {
  constructor(confirmedMapping) {
    super({ objectMode: true });
    this.mapping = confirmedMapping;
    this.recordCount = 0;
    this.errorCount = 0;
    this.validationErrors = [];
  }

  _transform(rawRecord, encoding, callback) {
    try {
      // Extract mapped values
      const mappedValues = this._extractMappedValues(rawRecord);

      // Validate critical fields in actual data
      const validation = this._validateRecord(mappedValues, rawRecord);
      if (!validation.isValid) {
        this.errorCount++;
        this.validationErrors.push({
          record: this.recordCount + 1,
          errors: validation.errors,
          rawRecord: rawRecord,
        });

        // Skip invalid records
        callback();
        return;
      }

      // Clean and process the record
      const cleanRecord = this._processRecord(mappedValues, rawRecord);

      // Convert to CSV line
      const csvLine = this._recordToCsv(cleanRecord);

      this.recordCount++;
      this.push(csvLine);
      callback();
    } catch (error) {
      console.error(`Failed to process record ${this.recordCount + 1}:`, error);
      this.errorCount++;
      callback(); // Continue processing other records
    }
  }

  _extractMappedValues(rawRecord) {
    return {
      a_party_id: this.mapping.a_party_id
        ? rawRecord[this.mapping.a_party_id]
        : null,
      start_time: this.mapping.start_time
        ? rawRecord[this.mapping.start_time]
        : null,
      end_time: this.mapping.end_time ? rawRecord[this.mapping.end_time] : null,
      duration_ms: this.mapping.duration_ms
        ? rawRecord[this.mapping.duration_ms]
        : null,
      src_ip: this.mapping.src_ip ? rawRecord[this.mapping.src_ip] : null,
      src_port: this.mapping.src_port ? rawRecord[this.mapping.src_port] : null,
      nat_ip: this.mapping.nat_ip ? rawRecord[this.mapping.nat_ip] : null,
      nat_port: this.mapping.nat_port ? rawRecord[this.mapping.nat_port] : null,
      dst_ip: this.mapping.dst_ip ? rawRecord[this.mapping.dst_ip] : null,
      dst_port: this.mapping.dst_port ? rawRecord[this.mapping.dst_port] : null,
      protocol: this.mapping.protocol ? rawRecord[this.mapping.protocol] : null,
      bytes_up: this.mapping.bytes_up ? rawRecord[this.mapping.bytes_up] : null,
      bytes_down: this.mapping.bytes_down
        ? rawRecord[this.mapping.bytes_down]
        : null,
    };
  }

  _validateRecord(mappedValues, rawRecord) {
    const errors = [];

    // Check required fields have actual values
    Object.keys(REQUIRED_FIELDS).forEach((field) => {
      const value = mappedValues[field];
      if (!value || value === '' || value === 'null' || value === 'NULL') {
        errors.push(
          `Missing required field: ${field} (${REQUIRED_FIELDS[field]})`
        );
      }
    });

    // Validate data types and formats
    if (mappedValues.start_time && isNaN(Date.parse(mappedValues.start_time))) {
      errors.push('Invalid start_time format');
    }

    if (
      mappedValues.dst_port &&
      (isNaN(parseInt(mappedValues.dst_port)) ||
        parseInt(mappedValues.dst_port) < 1)
    ) {
      errors.push('Invalid dst_port - must be a positive number');
    }

    // Validate IP addresses (basic check)
    if (mappedValues.src_ip && !this._isValidIP(mappedValues.src_ip)) {
      errors.push('Invalid src_ip format');
    }

    if (mappedValues.dst_ip && !this._isValidIP(mappedValues.dst_ip)) {
      errors.push('Invalid dst_ip format');
    }

    return {
      isValid: errors.length === 0,
      errors: errors,
    };
  }

  _isValidIP(ip) {
    // Basic IP validation - IPv4 and IPv6
    const ipv4Regex =
      /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
    const ipv6Regex = /^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/;
    return ipv4Regex.test(ip) || ipv6Regex.test(ip) || ip.includes(':'); // Allow partial IPv6
  }

  _processRecord(mappedValues, rawRecord) {
    const startTime = mappedValues.start_time
      ? new Date(mappedValues.start_time)
      : new Date();
    const endTime = mappedValues.end_time
      ? new Date(mappedValues.end_time)
      : null;
    const dstPort = mappedValues.dst_port
      ? parseInt(mappedValues.dst_port, 10)
      : null;

    // Calculate duration if not provided
    let duration = null;
    if (mappedValues.duration_ms) {
      duration = parseInt(mappedValues.duration_ms, 10);
      // Convert seconds to milliseconds if needed
      if (duration < 1000000) {
        // Assume seconds if less than 1M milliseconds (16 minutes)
        duration = duration * 1000;
      }
    } else if (startTime && endTime) {
      duration = endTime.getTime() - startTime.getTime();
    }

    return {
      a_party_id:
        mappedValues.a_party_id ||
        generateSessionIdentifier({
          start_time: startTime,
          src_ip: mappedValues.src_ip,
        }),
      start_time: startTime,
      end_time: endTime,
      duration_ms: duration,
      src_ip: mappedValues.src_ip,
      src_port: mappedValues.src_port
        ? parseInt(mappedValues.src_port, 10)
        : null,
      nat_ip: mappedValues.nat_ip,
      nat_port: mappedValues.nat_port
        ? parseInt(mappedValues.nat_port, 10)
        : null,
      dst_ip: mappedValues.dst_ip,
      dst_port: dstPort,
      protocol: (mappedValues.protocol || 'TCP').toUpperCase(),
      service_label: getServiceLabel(dstPort),
      bytes_up: mappedValues.bytes_up ? parseInt(mappedValues.bytes_up, 10) : 0,
      bytes_down: mappedValues.bytes_down
        ? parseInt(mappedValues.bytes_down, 10)
        : 0,
    };
  }

  _recordToCsv(record) {
    const toCsv = (value) => {
      if (value === null || value === undefined || Number.isNaN(value)) {
        return '';
      }
      if (value instanceof Date) {
        return value.toISOString();
      }
      // Escape CSV values that contain commas or quotes
      const strValue = String(value);
      if (
        strValue.includes(',') ||
        strValue.includes('"') ||
        strValue.includes('\n')
      ) {
        return `"${strValue.replace(/"/g, '""')}"`;
      }
      return strValue;
    };

    return (
      [
        toCsv(record.a_party_id),
        toCsv(record.start_time),
        toCsv(record.end_time),
        toCsv(record.duration_ms),
        toCsv(record.nat_ip),
        toCsv(record.nat_port),
        toCsv(record.src_ip),
        toCsv(record.src_port),
        toCsv(record.dst_ip),
        toCsv(record.dst_port),
        toCsv(record.protocol),
        toCsv(record.service_label),
        toCsv(record.bytes_up),
        toCsv(record.bytes_down),
      ].join(',') + '\n'
    );
  }

  getProcessingStats() {
    return {
      totalProcessed: this.recordCount,
      errors: this.errorCount,
      successRate:
        this.recordCount > 0
          ? (
              (this.recordCount / (this.recordCount + this.errorCount)) *
              100
            ).toFixed(2)
          : 0,
      validationErrors: this.validationErrors,
    };
  }
}

async function processFile(filePath, confirmedMapping) {
  // Pre-validate the mapping
  const validation = validateRequiredFields(confirmedMapping, []);
  if (!validation.isValid) {
    throw new Error(
      `Cannot process file - required fields missing: ${validation.errors
        .map((e) => e.field)
        .join(', ')}`
    );
  }

  const client = await pool.connect();

  const copyCommand = from(`
    COPY ipdr_sessions (
      a_party_id, start_time, end_time, duration_ms, 
      nat_ip, nat_port, src_ip, src_port, 
      dst_ip, dst_port, protocol, service_label,
      bytes_up, bytes_down
    ) FROM STDIN WITH (FORMAT csv)
  `);

  const dbWriteStream = client.query(copyCommand);
  const fileReadStream = fs.createReadStream(filePath);
  const csvParser = csv();
  const ipdrProcessor = new IPDRProcessor(confirmedMapping);

  console.log('Starting IPDR processing pipeline...');
  console.log(
    'Required fields validated:',
    validation.mappedRequiredCount,
    'of',
    validation.requiredFieldsCount
  );

  return new Promise((resolve, reject) => {
    pipeline(fileReadStream, csvParser, ipdrProcessor, dbWriteStream, (err) => {
      const stats = ipdrProcessor.getProcessingStats();
      client.release();

      if (err) {
        console.error('Pipeline failed:', err);
        console.error('Processing stats:', stats);
        reject({
          error: err,
          stats: stats,
        });
      } else {
        console.log(`✅ Pipeline succeeded!`);
        console.log(`📊 Records processed: ${stats.totalProcessed}`);
        console.log(`❌ Errors: ${stats.errors}`);
        console.log(`📈 Success rate: ${stats.successRate}%`);

        if (stats.errors > 0) {
          console.log(
            '⚠️  First few validation errors:',
            stats.validationErrors.slice(0, 5)
          );
        }

        resolve({
          recordsProcessed: stats.totalProcessed,
          errors: stats.errors,
          successRate: stats.successRate,
          validationErrors: stats.validationErrors,
        });
      }
    });
  });
}

module.exports = {
  analyzeFileHeaders,
  processFile,
  validateRequiredFields,
  REQUIRED_FIELDS,
};
