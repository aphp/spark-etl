const express = require('express');
// const bodyParser = require('body-parser')
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

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
  .pipe(csv())
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

app.listen(process.env.PORT || 8080); 