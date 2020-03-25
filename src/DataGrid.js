import React from "react";

import MaterialTable from "material-table";

function getColumns(rows) {
  if (!rows) {
    return [];
  }

  const columns = [];
  const colNames = Object.keys(rows[0]);
  for (let i = 0; i < colNames.length; i++ ) {
    columns.push({'field': colNames[i], 'title': colNames[i]})
  }
  return columns;
}


function DataGrid({ rows, attributes, title }) { 
  const columns = getColumns(rows);
  if (rows && rows.length > 0) {
    const colNames = Object.keys(rows[0]);
    for (let i = 0; i < colNames.length; i++ ) {
      columns.push({'field': colNames[i], 'title': colNames[i]})
    }
  }
  
  const indexedAttributes = {};
  for (const attr of attributes) {
    const key = attr['lib_table'];
    if (!indexedAttributes.hasOwnProperty(key)) {
      indexedAttributes[key] = []
    }
    indexedAttributes[key].push(attr);
  }
  const attributeCols = getColumns(attributes);

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
