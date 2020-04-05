const express = require('express');
const path = require('path');
const pgStructure = require('pg-structure').default;
const Pool = require('pg').Pool

// DB credentials
const database = process.env.POSTGRES_DB || 'docker';
const user = process.env.POSTGRES_USER || 'docker';
const password = process.env.POSTGRES_PASSWORD || 'docker';
const host = process.env.POSTGRES_HOST || 'localhost';
const port = process.env.POSTGRES_PORT || 5432

const pool = new Pool({
  user,
  host,
  database,
  password,
  port,
});

pool.on('connect', (client) => {
  client.query(`SET search_path TO "meta"`);
});

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

function nameSorter(a, b) {
  if (a.name < b.name) return -1;
  if (a.name > b.name) return 1;
  return 0;
}

const tablesData = [];
const links = [];
async function loadDbSchema() {

  const db = await pgStructure({ database, user, password, host, port }, {  });
  const tablesRef = {}

  let id = 0;
  for (let i = 0; i < db.tables.length; i++) {
    const t = db.tables[i];

    const rowKey = 'row-' + i;
    
    const columns = [];
    for (let j = 0; j < t.columns.length; j++) {
      const colKey = rowKey + '-col-' + j;
      const c = t.columns[j];
      columns.push({
        id: id.toString(),
        _key: colKey,
        name: c.name,
        type: c.type.shortName,
        ...splitComment(c.comment)
      });
      columns.sort(nameSorter);
      id += 1;
    }
    
    const table = {
      id: id.toString(),
      _key: rowKey,
      name: t.name,
      ...splitComment(t.comment),
      columns
    }
    tablesData.push(table);
    tablesRef[t.name] = table.id;
    id += 1;
  }
  tablesData.sort(nameSorter);

  function addTarget(l, relation) {
    const sourceTableId = tablesRef[relation.sourceTable.name];
    const targetTableId = tablesRef[relation.targetTable.name];
    // Do not add a link if it already exists in the other way
    if (l.hasOwnProperty(targetTableId) && l[targetTableId].has(sourceTableId)) {
      return;
    }

    if (!l.hasOwnProperty(sourceTableId)) {
      l[sourceTableId] = new Set();
    }
    l[sourceTableId].add(targetTableId);
  }

  const linkSet = {}
  for (const table of db.tables) {
    for (relation of table.o2mRelations) {
      addTarget(linkSet, relation);
    }
    // This is already taken into account with o2m and m2o relationships
    // for (relation of table.m2mRelations) {
    //   addTarget(linkSet, relation);
    // }
    for (relation of table.m2oRelations) {
      addTarget(linkSet, relation);
    }
  }
  for (const source in linkSet) {
    for (const target of linkSet[source]) {
      links.push({source, target});
    }
  }
}
loadDbSchema();


const app = express();
app.use(express.static(path.join(__dirname, 'build')));

app.get('/schema', function (req, res) {
 return res.json({tables: tablesData, links});
});


// New endpoints
app.get('/databases', (req, res) => {
  pool.query('SELECT ids_database AS id, lib_database AS name FROM meta_database WHERE COALESCE(is_active_m, is_active) ORDER BY lib_database ASC', (error, results) => {
    if (error) {
      throw error
    }
    res.json(results.rows);
  });
});

app.get('/schemas', function (req, res) {
  pool.query({
    text: 'SELECT ids_schema AS id, \
           lib_schema AS name \
           FROM meta_schema \
           WHERE ids_database = $1 \
           AND COALESCE(is_active_m, is_active) \
           ORDER BY lib_schema ASC',
    values: [req.query.ids_database]
  }, (error, results) => {
    if (error) {
      throw error
    }
    res.json(results.rows);
  });
});


app.get('/tables', (req, res) => {
  pool.query({
    text: 'SELECT ids_table AS id, \
           COALESCE(lib_table_m, lib_table) AS name, \
           COALESCE(comment_fonctionnel_m, comment_fonctionnel) AS comment_fonctionnel, \
           COALESCE(comment_technique_m, comment_technique) AS comment_technique, \
           COALESCE(typ_table_m, typ_table) AS typ_table \
           FROM meta_table \
           WHERE ids_schema = $1 \
           AND COALESCE(is_active_m, is_active) \
           ORDER BY name ASC',
      values: [req.query.ids_schema]
  }, (error, results) => {
    if (error) {
      throw error
    }
    res.json(results.rows);
  });
});
 
app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

const serverPort = process.env.PORT || 8080;
console.log('server running on port: ' + serverPort);
app.listen(serverPort); 