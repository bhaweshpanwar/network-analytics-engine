const express = require('express');
const ipdrRoutes = require('./src/routes/ipdr.routes');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());

// Main route for our application
app.use('/api', ipdrRoutes);

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
