import React, { useState } from "react";
import ReactDataGrid from "react-data-grid";
import { Toolbar, Data } from "react-data-grid-addons";

const defaultColumnProperties = {
  sortable: true,
  filterable: true,
  editable: true
};

const selectors = Data.Selectors;

const handleFilterChange = filter => filters => {
  const newFilters = { ...filters };
  if (filter.filterTerm) {
    newFilters[filter.column.key] = filter;
  } else {
    delete newFilters[filter.column.key];
  }
  return newFilters;
};

function getRows(rows, filters) {
  return selectors.getRows({ rows, filters });
}

const sortRows = (initialRows, sortColumn, sortDirection) => rows => {
  const comparer = (a, b) => {
    if (sortDirection === "ASC") {
      return a[sortColumn] > b[sortColumn] ? 1 : -1;
    } else if (sortDirection === "DESC") {
      return a[sortColumn] < b[sortColumn] ? 1 : -1;
    }
  };
  return sortDirection === "NONE" ? initialRows : [...rows].sort(comparer);
};


function DataGrid({ rows }) {  
  let columns = [];
    if (rows && rows.length > 0) {
    columns = Object.keys(rows[0]).map(
      key => ({'key': key.replace(' ', '_'), 'name': key, ...defaultColumnProperties})
    );
  }
  const [filters, setFilters] = useState({});
  const [sortedRows, setRows] = useState(rows);
  const filteredRows = getRows(sortedRows, filters);
  return (
    <ReactDataGrid
      columns={columns}
      rowGetter={i => filteredRows[i]}
      rowsCount={filteredRows.length}
      minHeight={500}
      toolbar={<Toolbar enableFilter={true} />}
      onAddFilter={filter => setFilters(handleFilterChange(filter))}
      onClearFilters={() => setFilters({})}
      onGridSort={(sortColumn, sortDirection) =>
        setRows(sortRows(rows, sortColumn, sortDirection))
      }      
    />
  );
}

export default DataGrid;
