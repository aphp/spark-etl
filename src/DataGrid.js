import React from "react";
import { withStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import Editable from './helpers/Editable.js';

const StyledHeader = withStyles(theme => ({
  head: {
    backgroundColor: '#1976d2',
    color: theme.palette.common.white,
    fontSize: 18,
  },
}))(TableCell);


class TextHightlighter extends React.PureComponent {
  constructor(props) {
    super(props)
    this.contentEditable = React.createRef();
    this.state = {
      text: props.text || ''
    }
  };

  save = evt => {
    const value = evt.target.value;
    // This is still empty text
    if (this.props.text === null && value.trim().length === 0) {
      return;
    }
    if (value !== this.props.text) {
      this.props.updateText(value);
      this.setState({text: value});
    }
  };

  onChange = evt => {
    this.setState({text: evt.target.value});
  }

  render() {
    const {highlight, editable, text} = this.props;

    let displayText;
    if (!highlight || highlight === '' || typeof text !== 'string') {
      displayText = text;
    } else {
      const parts = text.split(new RegExp(`(${highlight})`, 'gi'));
      displayText = parts.map((part, i) => {
        if (part.toLowerCase() === highlight.toLowerCase()) {
          return `<span style="background-color: orange">${part}</span>`;
        }
        return part;
      }).join('');
    }

    if (!editable || !this.props.updateText) {
      return <span dangerouslySetInnerHTML={{ __html: displayText}}></span>;
    }

    return <Editable
      text={displayText}
      childRef={this.contentEditable}
      type="input">
      <textarea
        ref={this.contentEditable}
        type="text"
        rows="5"
        value={this.state.text}
        onChange={this.onChange}
        onBlur={this.save}/>
    </Editable>
  }
}

class DataRow extends React.Component {
  shouldComponentUpdate(nextProps, nextState) {
    return (nextProps.row._forceUpdate);
  }

  updateText = (col, text) => {
    this.props.updateText(this.props.row, col, text);
  }

  render() {
    const {row, useName, cells, className, searchText} = this.props;

    if (!row._display) {
      return null;
    }

    return (
      <TableRow className={className} hover={true}>
      {cells.map(col => (
        <TableCell key={'table-cell-' + row._key + '-' + col.name}>
          <TextHightlighter
            text={useName ? row[col.name] : col.name}
            highlight={searchText}
            updateText={this.props.updateText ? (text => this.updateText(col.name, text)) : null}
            editable={col.editable}/>
        </TableCell>
      ))}
    </TableRow>
    );
  }
}

class AttributeTable extends React.Component {
  render() {
    const {table, columns, attributeCols, searchText} = this.props;
    if (!table._hasColumnDisplay) {
      return null;
    }

    return (
      <TableRow hover={true}>
        <TableCell colSpan={columns.length}>
          <Table aria-label="table">
            <TableHead>
              <DataRow
                key={table._key + '-attributes-head'}
                row={table}
                useName={false}
                cells={attributeCols}
                searchText={searchText}
                />
            </TableHead>
            <TableBody>
              {table.columns.map(tableColumn => (
                <DataRow
                  row={tableColumn}
                  useName={true}
                  cells={attributeCols}
                  key={table._key + '-attributes-' + tableColumn.name}
                  searchText={searchText}
                  updateText={this.props.updateText}/>
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
    const {table, columns, attributeCols, classes, searchText} = this.props;
    if (!table._display) {
      return null;
    }

    return (
      <React.Fragment>
        <DataRow
          className={classes.tableHead}
          key={table._key + '-content'}
          row={table} useName={true}
          cells={columns}
          searchText={searchText}
          updateText={this.props.updateTables}/>
        <AttributeTable
          table={table}
          columns={columns}
          attributeCols={attributeCols}
          searchText={searchText}
          updateText={this.props.updateColumns}/>
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
    const { schema, searchText } = this.props;
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
              <StyledTableData
                key={'tabledata-' + table._key}
                table={table} columns={tableHeaders}
                attributeCols={attributeCols}
                searchText={searchText}
                updateColumns={this.props.updateColumns}
                updateTables={this.props.updateTables}/>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    );
  }
}

export default DataGrid;
