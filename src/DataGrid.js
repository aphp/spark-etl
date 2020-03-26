import React from "react";
import { withStyles, makeStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';

const StyledHeader = withStyles(theme => ({
  head: {
    backgroundColor: '#1976d2',
    color: theme.palette.common.white,
    fontSize: 18,
  },
}))(TableCell);


const useStyles = makeStyles({
  tableHead: {
    backgroundColor: '#eeeeee',
    fontSize: 16,
  },
});

function DataRow({row, useName, cells, className}) {
  if (!row._display) {
    return null;
  }

  return (
    <TableRow className={className} hover={true}>
    {cells.map(col => (
      <TableCell key={'table-cell-' + row._key + '-' + col.name}>
        <React.Fragment>{useName ? row[col.name] : col.name}</React.Fragment>
      </TableCell>
    ))}
  </TableRow>
  );
}

function AttributeTable({row, columns, attributeCols}) {
  if (!row._hasColumnDisplay) {
    return null;
  }

  return (
    <TableRow hover={true}>
      <TableCell colSpan={columns.length}>
        <Table aria-label="table">
          <TableHead>
            <DataRow key={row._key + '-attributes-head'} row={row} useName={false} cells={attributeCols}></DataRow>
          </TableHead>
          <TableBody>
            {row.columns.map(tableColumn => (
              <DataRow row={tableColumn} useName={true} cells={attributeCols} key={row._key + '-attributes-' + tableColumn.name}></DataRow>
            ))}
          </TableBody>
        </Table>
      </TableCell>                  
    </TableRow>); 
}

function TableData({row, columns, attributeCols}) {
  const classes = useStyles();

  if (!row._display) {
    return null;
  }

  return (
    <React.Fragment>
      <DataRow className={classes.tableHead}  key={row._key + '-content'} row={row} useName={true} cells={columns}></DataRow>      
      <AttributeTable row={row} columns={columns} attributeCols={attributeCols}></AttributeTable>
    </React.Fragment>       
  )
}

function DataGrid({ tables }) { 
  const { rows, columns, attributeCols } = tables;

  return (
    <TableContainer component={Paper}>
      <Table aria-label="table">
        <TableHead>
          <TableRow>
            {columns.map(col => (
              <StyledHeader key={'col-' + col.name}>{col.name}</StyledHeader>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map(row => (
            <TableData key={'tabledata-' + row._key} row={row} columns={columns} attributeCols={attributeCols}></TableData>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}

export default DataGrid;
