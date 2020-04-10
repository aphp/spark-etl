import React from "react";
import { withStyles } from '@material-ui/core/styles';
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


class DataRow extends React.Component {
  shouldComponentUpdate(nextProps, nextState) {
    return (nextProps.row._forceUpdate);
  }
  render() {
    const {row, useName, cells, className} = this.props;
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
}

class AttributeTable extends React.Component {
  render() {
    const {table, columns, attributeCols} = this.props;
    if (!table._hasColumnDisplay) {
      return null;
    }

    return (
      <TableRow hover={true}>
        <TableCell colSpan={columns.length}>
          <Table aria-label="table">
            <TableHead>
              <DataRow key={table._key + '-attributes-head'} row={table} useName={false} cells={attributeCols}></DataRow>
            </TableHead>
            <TableBody>
              {table.columns.map(tableColumn => (
                <DataRow row={tableColumn} useName={true} cells={attributeCols} key={table._key + '-attributes-' + tableColumn.name}></DataRow>
              ))}
            </TableBody>
          </Table>
        </TableCell>
      </TableRow>);
  }
}

class TableData extends React.Component {
  shouldComponentUpdate(nextProps, nextState) {
    return (nextProps.table._forceUpdate);
  }

  render() {
    const {table, columns, attributeCols, classes} = this.props;
    if (!table._display) {
      return null;
    }

    return (
      <React.Fragment>
        <DataRow className={classes.tableHead} key={table._key + '-content'} row={table} useName={true} cells={columns}></DataRow>
        <AttributeTable table={table} columns={columns} attributeCols={attributeCols}></AttributeTable>
      </React.Fragment>
    );
  }
}


const StyledTableData = withStyles(theme => ({
  tableHead: {
    backgroundColor: '#eeeeee',
    fontSize: 16,
  },
}))(TableData);

class DataGrid extends React.Component {
  shouldComponentUpdate(nextProps, nextState) {
    if (nextProps.schema && this.props.schema) {
      return this.props.schema.id === nextProps.schema.id || nextProps.schema._forceUpdate;
    }
    return false;
  }

  render() {
    const { schema } = this.props;
    if (!schema || schema.tables.length === 0) {
      return null;
    }

    const { tables, tableHeaders, attributeCols } = schema;

    return (
      <TableContainer component={Paper}>
        <Table aria-label="table">
          <TableHead>
            <TableRow>
              {tableHeaders.map(col => (
                <StyledHeader key={'col-' + col.name}>{col.name}</StyledHeader>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {tables.map(table => (
              <StyledTableData key={'tabledata-' + table._key} table={table} columns={tableHeaders} attributeCols={attributeCols}></StyledTableData>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    );
  }
}

export default DataGrid;
