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


function DataGrid({ rows, title }) { 
  const columns = getColumns(rows, ['columns']);
  const attributeCols = getColumns(rows[0].columns);

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
              return (
                <MaterialTable
                  options={{
                    paging: false
                  }} 
                  columns={attributeCols}
                  data={rowData.columns}
                  title={rowData.name + ': attributs'}
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
