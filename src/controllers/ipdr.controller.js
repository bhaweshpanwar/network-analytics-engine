const fs = require('fs').promises;
const path = require('path');

const {
  analyzeFileHeaders,
  processFile,
  validateRequiredFields,
  REQUIRED_FIELDS,
} = require('../services/ipdr.service');
const schemaDefinition = require('../config/schemaDefinition');

const analyzeUpload = async (req, res) => {
  if (!req.file) {
    return res.status(400).send({ message: 'Please upload a file.' });
  }

  try {
    const { fileHeaders, suggestedMapping } = await analyzeFileHeaders(
      req.file.path
    );
    res.status(200).send({
      message: 'File analyzed successfully. Please confirm the mapping.',
      filePath: req.file.path,
      schemaDefinition: schemaDefinition,
      fileHeaders: fileHeaders,
      suggestedMapping: suggestedMapping,
    });
  } catch (error) {
    res
      .status(500)
      .send({ message: 'Failed to analyze file.', error: error.message });
  }
};

const processUpload = async (req, res) => {
  const { filePath, confirmedMapping } = req.body;

  // 1a. Basic Input Validation
  if (!filePath || !confirmedMapping) {
    return res.status(400).json({
      success: false,
      message: 'File path and confirmed mapping are required.',
    });
  }

  try {
    // 1b. Check if the temp file actually exists
    try {
      await fs.access(filePath);
    } catch (error) {
      return res.status(404).json({
        success: false,
        message:
          'File not found on server. It may have expired or been cleaned up. Please re-upload.',
      });
    }

    // 1c. Validate that the mapping includes all required fields
    const mappingValidation = validateRequiredFields(confirmedMapping, []);
    if (!mappingValidation.isValid) {
      return res.status(400).json({
        success: false,
        message:
          'Invalid field mapping. The following required fields must be mapped to a column from your file.',
        errors: mappingValidation.errors,
      });
    }

    // This is a powerful check to ensure the user's mapping is valid for the file.
    const fileHeaders = (await analyzeFileHeaders(filePath)).fileHeaders;
    const validationErrors = [];
    Object.entries(confirmedMapping).forEach(([schemaField, fileField]) => {
      // If a mapping is provided (not null), we check if that column exists in the file.
      if (fileField && !fileHeaders.includes(fileField)) {
        validationErrors.push(
          `The column '${fileField}' (mapped to '${schemaField}') does not exist in the uploaded file.`
        );
      }
    });

    if (validationErrors.length > 0) {
      return res.status(400).json({
        success: false,
        message:
          'Mapping validation failed. One or more mapped columns were not found in the file.',
        errors: validationErrors,
        availableHeaders: fileHeaders,
      });
    }

    // Immediately respond that the job has been accepted.
    res.status(202).json({
      success: true,
      message:
        'File validation passed. Processing has started in the background.',
    });

    // --- Asynchronous "Fire and Forget" Processing ---
    // processFile but do NOT await it. The controller's job is done.
    console.log(`Starting background processing for file: ${filePath}`);
    processFile(filePath, confirmedMapping)
      .then((result) => {
        console.log(
          `[SUCCESS] Background processing finished for ${path.basename(
            filePath
          )}.`
        );
        console.log(
          `📊 Records processed: ${result.recordsProcessed}, Errors: ${result.errors}`
        );
      })
      .catch((error) => {
        console.error(
          `[FAILED] Background processing failed for ${path.basename(
            filePath
          )}`,
          error.error || error
        );
      })
      .finally(async () => {
        // delete the temporary file after processing is finished or failed.
        try {
          await fs.unlink(filePath);
          console.log(`🗑️ Cleaned up temporary file: ${filePath}`);
        } catch (cleanupError) {
          console.error(
            `⚠️ Failed to clean up temporary file: ${filePath}`,
            cleanupError
          );
        }
      });
  } catch (error) {
    console.error(
      'An unexpected error occurred in the processUpload controller:',
      error
    );
    return res.status(500).json({
      success: false,
      message: 'An internal server error occurred.',
    });
  }
};

module.exports = {
  analyzeUpload,
  processUpload,
};
