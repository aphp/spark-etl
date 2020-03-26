const express = require('express');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const stripBom = require('strip-bom-stream');
const pgStructure = require('pg-structure').default;

function splitComment(comment) {
  const res = {comment_omop: '', comment_aphp: ''};
  if (!comment) {
    return res;
  }
  const splitComment = comment.split('- APHP Annotation :');
  res.comment_omop = splitComment[0].replace('OMOP Comment : ', '');
  if (splitComment.length == 2) {
    res.comment_aphp = splitComment[1];
  }
  return res;
}

let tablesData = [];
async function loadDbSchema() {
  const db = await pgStructure({ database: 'postgres', user: 'postgres', password: 'password' }, {  });

  tablesData = db.tables.map(t => {
    return {
      name: t.name,
      ...splitComment(t.comment),
      columns: t.columns.map(c => {
        return {
          name: c.name,
          type: c.type.shortName,
          notNull: c.notNull,
          ...splitComment(c.comment)
        }
      }),
    }
  });
}
loadDbSchema();


const app = express();
app.use(express.static(path.join(__dirname, 'build')));

app.get('/tables', function (req, res) {
 return res.json(tablesData);
});
 
app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

const port = process.env.PORT || 8080;
console.log('server running on port: ' + port);
app.listen(port); 