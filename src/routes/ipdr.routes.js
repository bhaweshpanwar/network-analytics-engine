const express = require('express');
const multer = require('multer');
const ipdrController = require('../controllers/ipdr.controller');

const router = express.Router();
const upload = multer({ dest: 'uploads/' });

// Route for Step 1: Upload and get AI-suggested mapping
router.post(
  '/upload/analyze',
  upload.single('ipdrFile'),
  ipdrController.analyzeUpload
);

// Route for Step 2: Start the actual processing with a user-confirmed mapping
router.post('/upload/process', ipdrController.processUpload);

module.exports = router;
