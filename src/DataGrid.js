import React from "react";

import MaterialTable from "material-table";

function getColumns(rows, skip) {
  if (!rows) {
    return [];
  }

  const columns = [];
  const colNames = Object.keys(rows[0]);
  
  for (let i = 0; i < colNames.length; i++ ) {
    const name = colNames[i];
    if (skip && skip.includes(name)) {
      continue;
    }

    columns.push({'field': name, 'title': name})
  }

  return columns;
}


function DataGrid({ rows, attributes, title }) { 
  const columns = getColumns(rows);
  
  const indexedAttributes = {};
  for (let attr of attributes) {
    const key = attr['lib_table'];
    if (!indexedAttributes.hasOwnProperty(key)) {
      indexedAttributes[key] = []
    }
    indexedAttributes[key].push(attr);
  }
  
  // Do not display lib_table data for attributes (redundant)
  const attributeCols = getColumns(attributes, ['lib_table']);

  return (
    <div style={{ maxWidth: "100%" }}>
      <MaterialTable
        options={{
          paging: false,
          rowStyle: x => {
            if (x.tableData.id % 2) {
              return { backgroundColor: "#efefef" }
            }
          },
        }} 
        detailPanel={[
          {
            tooltip: 'Attributs',
            render: rowData => {
              const tableName = rowData['lib_table'];
              return (
                <MaterialTable
                  options={{
                    paging: false
                  }} 
                  columns={attributeCols}
                  data={indexedAttributes[tableName]}
                  title={tableName + ': attributs'}
                />
              )
            },
          }
        ]}             
        columns={columns}
        data={rows}
        title={title}   
      />
    </div>
  );
}

export default DataGrid;
