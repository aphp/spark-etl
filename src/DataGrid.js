import React from "react";

import MaterialTable from "material-table";



function DataGrid({ rows, title }) { 

  const columns = [];
  const displayedRows = []
  if (rows && rows.length > 0) {
    const colNames = Object.keys(rows[0]);
    for (let i = 0; i < colNames.length; i++ ) {
      columns.push({'field': colNames[i], 'title': colNames[i]})
    }

    for (let i = 0; i < rows.length; i++ ) {
      displayedRows.push({'key': 'row' + i, 'content': rows[i]})
    }
  }
  return (
    <div style={{ maxWidth: "100%" }}>
      <MaterialTable
        options={{
          paging: false
        }}      
        columns={columns}
        data={rows}
        title={title}
      />
    </div>
  );
}

export default DataGrid;
