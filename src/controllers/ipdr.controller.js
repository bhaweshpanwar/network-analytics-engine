const { analyzeFileHeaders, processFile } = require('../services/ipdr.service');
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

  if (!filePath || !confirmedMapping) {
    return res
      .status(400)
      .send({ message: 'File path and a confirmed mapping are required.' });
  }

  res.status(202).send({ message: `Processing started for file: ${filePath}` });

  processFile(filePath, confirmedMapping)
    .then((recordCount) => {
      console.log(
        `[SUCCESS] Processing finished for ${filePath}. Records: ${recordCount}`
      );
      fs.unlinkSync(filePath);
    })
    .catch((error) => {
      console.error(`[FAILED] Processing failed for ${filePath}`, error);
    });
};

module.exports = {
  analyzeUpload,
  processUpload,
};
