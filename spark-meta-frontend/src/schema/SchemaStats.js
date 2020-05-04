import React from 'react';
import BoxLine from '../helpers/BoxLine.js';

function SchemaStats ({ tables, visibleTables, visibleColumns }) {
  if (!tables || tables.length === 0) {
      return null;
  }

  const columnsCount = tables.reduce((a, b) => a + b.columns.length, 0);

  let tableText = `${tables.length} tables`;
  let columnsText = `${columnsCount} columns`;

  if (visibleTables != null && visibleTables !== tables.length) {
    tableText = `${visibleTables}/${tables.length} tables shown`;
  }
  if (visibleColumns != null && visibleColumns !== columnsCount) {
    columnsText = `${visibleColumns}/${columnsCount} columns shown`;
  }

  return <BoxLine items={[tableText, columnsText]}/>;
}

export default SchemaStats;