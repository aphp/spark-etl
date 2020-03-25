const express = require('express');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const stripBom = require('strip-bom-stream');

const app = express();
app.use(express.static(path.join(__dirname, 'build')));

let tableData = [];
app.get('/table', function (req, res) {
 return res.json(tableData);
});
let attributeData = [];
app.get('/attributes', function (req, res) {
  return res.json(attributeData);
 });
 
app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

function readCsv(filename, addLine) {
  fs.createReadStream(filename)
  .pipe(stripBom()).pipe(csv())
  .on('data', (data) => {
    addLine(data);
  })
  .on('end', () => {
    console.log('successfully read: ' + filename);
  });
}

if (process.argv.length < 4) {
  console.log('Usage: node ' + process.argv[1] + ' table.csv attributes.csv');
  process.exit(1);
}

readCsv(process.argv[2], function (line) {
  tableData.push(line);
});

readCsv(process.argv[3], function (line) {
  attributeData.push(line);
}); 

const port = process.env.PORT || 8080;
console.log('server running on port: ' + port);
app.listen(port); 