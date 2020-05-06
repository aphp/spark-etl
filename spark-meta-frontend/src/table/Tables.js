import React from "react";
import DataGrid from './DataGrid.js';
import SearchInput from '../helpers/SearchInput.js';
import PropTypes from 'prop-types';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import SimpleModal from '../helpers/Modal.js'
import { Diagram, defaultNodeColor } from '../diagram/Diagram.js';
import Select from '../helpers/Select.js'
import Error from '../helpers/Error.js';
import CircularIndeterminate from '../helpers/CircularIndeterminate.js';
import SchemaStats from '../schema/SchemaStats.js';
import ColumnCheckboxes from './ColumnCheckboxes.js'
import { Container } from "@material-ui/core";
import ImageDiagram from "./ImageDiagram.js";
import TableDrawer from "./TableDrawer.js";

function TabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <Typography
      component="div"
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {children}
    </Typography>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.any.isRequired,
  value: PropTypes.any.isRequired,
};

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    'aria-controls': `simple-tabpanel-${index}`,
  };
}


async function postData(url = '', data = {}, authToken) {
  const headers = {
    'Content-Type': 'application/json',
  };

  if (authToken) {
    headers['X-Authorization'] = 'Bearer ' + authToken;
  }

  // Default options are marked with *
  const response = await fetch(url, {
    method: 'POST', // *GET, POST, PUT, DELETE, etc.
    mode: 'cors', // no-cors, *cors, same-origin
    cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
    credentials: 'same-origin', // include, *same-origin, omit
    headers,
    redirect: 'follow', // manual, *follow, error
    referrerPolicy: 'no-referrer', // no-referrer, *client
    body: JSON.stringify(data) // body data type must match "Content-Type" header
  });
  return response; // parses JSON response into native JavaScript objects
}


function hasSearchText(col, attributeCols, filter) {
  if (!filter || filter.length === 0) {
    return true;
  }

  const filterLower = filter.toLowerCase();

  for (const colName of attributeCols) {
    if (typeof col[colName.name] ==='string' && col[colName.name].search(filterLower) >= 0) {
      return true;
    }
  }

  return false;
}


function applySearchFilter(table, tableHeaders, attributeCols, filter) {
  const visibleColumns = [];

  for (let j = 0; j < table.columns.length; j++) {
    const col = table.columns[j];
    if (hasSearchText(col, attributeCols, filter)) {
      visibleColumns.push(col);
    }
  }

  const tableHeadersHasText = hasSearchText(table, tableHeaders, filter);

  return {visibleColumns, tableHeadersHasText};
}

function selectNode(node, selected) {
  node.setSelected(selected);
  for (const port of node.getInPorts()) {
    for (const link of Object.values(port.getLinks())) {
      link.setSelected(selected);
    }
  }

  for (const port of node.getOutPorts()) {
    for (const link of Object.values(port.getLinks())) {
      link.setSelected(selected);
    }
  }
}

class Tables extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tables: [],
      tableHeaders: [],
      attributeCols: [],
      visibleColumnsCount: null,
      visibleTablesObj: {},
      visibleSelectedTableObj: {},
      tabIndex: 0,
      searchText: '',
      isLoading: true,
      updateId: 0,
      authToken: '',
      drawerOpened: false
    };
  }

  componentDidMount() {
    // Bind this function globally so that it can be called from svg
    window.selectByTableId = this.selectByTableId;

    if (!this.props.selectedSchema) {
      this.setState({
        tables: [],
        selectedTable: null,
        error: null
      });
      return;
    }

    fetch("/tables?ids_schema=" + this.props.selectedSchema.id)
    .then(res => res.json())
    .then(
      (results) => {
        this.setState({
          tables: results.tables,
          tableHeaders: results.tableHeaders,
          attributeCols: results.attributeCols,
          selectedTable: null,
          searchText: '',
          isLoading: false,
          error: null
        }, () => this.updateSearchText(''));
      },
      (error) => {
        this.setState({
          error,
          searchText: '',
          isLoading: false,
          selectedTable: null,
        });
      }
    );
  }

  updateTables = (row, colName, text) => {
    postData('/tables', { id: row.id, colName: colName, text: text }, this.state.authToken)
      .then((res) => {
        if (res.status !== 200) {
          res.json().then(err => {
            console.error(`error updating row table: ${row.id}, name: ${colName} with value: ${text}: ${err}`); // JSON data parsed by `response.json()` call
          }).catch(err => {
            console.error(`error (2) updating row column: ${row.id}, name: ${colName} with value: ${text}: ${err.message}`); // JSON data parsed by `response.json()` call
          });
          return;
        }
        const table = this.state.tables.find(e => e.id === row.id);
        table[colName] = text;
        this.setState({updateId: this.state.updateId + 1})
        console.log(`successfully updated table: ${row.id}, name: ${colName} with value: ${text}`); // JSON data parsed by `response.json()` call
      }).catch((err) => {
        console.error(`error updating row table: ${row.id}, name: ${colName} with value: ${text}: ${err.message}`); // JSON data parsed by `response.json()` call
      });
  }

  updateColumns = (row, colName, text) => {
    postData('/columns', { id: row.id, colName: colName, text: text }, this.state.authToken)
      .then((res) => {
        if (res.status !== 200) {
          res.json().then(err => {
            console.error(`error updating row column: ${row.id}, name: ${colName} with value: ${text}: ${err.message}`); // JSON data parsed by `response.json()` call
          }).catch(err => {
            console.error(`error (2) updating row column: ${row.id}, name: ${colName} with value: ${text}: ${err.message}`); // JSON data parsed by `response.json()` call
          });
          return;
        }
        row[colName] = text;
        this.setState({updateId: this.state.updateId + 1})
        console.log(`successfully updated column: ${row.id}, name: ${colName} with value: ${text}`); // JSON data parsed by `response.json()` call
      }).catch((err) => {
        console.error(`error updating row column: ${row.id}, name: ${colName} with value: ${text}: ${err}`); // JSON data parsed by `response.json()` call
      });
  }

  updateVisibleAttributeCols = attributeCols => {
    return attributeCols.filter(e => e.display);
  }

  changeColVisibility = col => {
    col.display = !col.display;
  }

  selectByTableId = (tableId) => {
    if (!tableId) {
      this.setState({
        selectedTable: null,
        visibleSelectedTableObj: {},
      });
    }

    const table = this.state.tables.find(e => e.id === tableId);
    if (!table) {
      return;
    }
    const visibleSelectedTableObj = {}
    visibleSelectedTableObj[table.id] = table.columns;

    if (table._node) {
      selectNode(table._node, true);
    }

    if (this.state.selectedTable && this.state.selectedTable._node) {
      selectNode(this.state.selectedTable._node, false);
    }

    this.setState({ selectedTable: table, visibleSelectedTableObj });
  }

  changeTab = (event, newTabIndex) => {
    this.setState({tabIndex: newTabIndex});
  };

  onSelectTable = (values) => {
    this.selectByTableId(values && values.id);
  }

  onSelectedDiagramTable = (e) => {
    if (!e.isSelected) {
      return;
    }
    this.selectByTableId(e.entity.id);
  };

  setAuthToken = (v) => {
    this.setState({authToken: v}, () => {
      console.log('Set auth token to: ' + this.state.authToken);
    });
  }

  changeColVisibility = (col, visible) => {
    col.display = visible;
    this.setState({updateId: this.state.updateId + 1});
  }

  updateSearchText = (searchText) => {
    const visibleSelectedTableObj = {};
    if (this.state.selectedTable) {
      const {visibleColumns} = applySearchFilter(this.state.selectedTable, this.state.tableHeaders, this.state.attributeCols, null);
      visibleSelectedTableObj[this.state.selectedTable.id] = visibleColumns;
    }

    const visibleAttributeCols = this.state.attributeCols.filter(e => e.display);
    const visibleTablesObj = {};
    let visibleColumnsCount = 0;
    for (const table of this.state.tables) {
      const {visibleColumns, tableHeadersHasText} = applySearchFilter(table, this.state.tableHeaders, visibleAttributeCols, searchText);
      if (tableHeadersHasText || visibleColumns.length > 0) {
        visibleTablesObj[table.id] = visibleColumns;
        visibleColumnsCount += visibleColumns.length;

        // For diagram
        if (table._node) {
          table._node.options.color = defaultNodeColor;
        }
      } else if (table._node) {
        table._node.options.color = '#cccccc';
      }
    }

    this.setState({ searchText, visibleSelectedTableObj, visibleTablesObj, visibleColumnsCount });
  }

  render() {
    const {selectedSchema, classes} = this.props;

    if (!selectedSchema) {
      return null;
    }

    if (this.state.error) {
      return <Error error={this.state.error}/>
    }

    if (this.state.isLoading) {
      return <CircularIndeterminate size="100px"/>;
    }

    const selectedTable = this.state.selectedTable;
    const visibleAttributeCols = this.state.attributeCols.filter(e => e.display);

    return (
      <React.Fragment>
        <Container maxWidth="md">
          <SchemaStats tables={this.state.tables} visibleTables={Object.keys(this.state.visibleTablesObj).length} visibleColumns={this.state.visibleColumnsCount} />
          <Select label="tables" options={this.state.tables} onChange={(values) => { this.onSelectTable(values, true) }} selectedValue={selectedTable}></Select>
          <ColumnCheckboxes columns={this.state.attributeCols} changeColVisibility={this.changeColVisibility}/>
        </Container>
        <TableDrawer
            selectedTable={selectedTable}
            selectByTableId={this.selectByTableId}
            visibleTables={this.state.visibleSelectedTableObj}
            tableHeaders={this.state.tableHeaders}
            attributeCols={visibleAttributeCols}
            searchText={this.state.searchText}
            updateColumns={this.updateColumns}
            updateTables={this.updateTables}
            expandedColumns={true}
            canEdit={this.state.authToken !== ''}/>
        <AppBar position="static" className={classes.bar}>
        <Toolbar>
          <Tabs value={this.state.tabIndex} onChange={this.changeTab} aria-label="Tabs" className={classes.grow}>
            <Tab label="Diagram" {...a11yProps(0)} />
            <Tab label="Static diagram" {...a11yProps(1)} />
            <Tab label="All tables" {...a11yProps(2)} />
          </Tabs>
            <SearchInput updateSearchText={this.updateSearchText} searchText={this.state.searchText}/>
            <SimpleModal authToken={this.state.authToken} setAuthToken={this.setAuthToken}/>
        </Toolbar>
        </AppBar>
        <TabPanel value={this.state.tabIndex} index={0}>
          { this.state.tables.length > 0 && <Diagram
            tables={this.state.tables}
            visibleTables={this.state.visibleTablesObj}
            selectedTable={selectedTable}
            onSelected={this.onSelectedDiagramTable}/>}
        </TabPanel>
        <TabPanel value={this.state.tabIndex} index={1}>
          {/* We need to use "object" instead of "img" to load images embedded in the svg. */}
          <ImageDiagram schema={selectedSchema}/>
        </TabPanel>
        <TabPanel value={this.state.tabIndex} index={2}>
          <DataGrid
            selectByTableId={this.selectByTableId}
            searchText={this.state.searchText}
            tables={this.state.tables}
            visibleTables={this.state.visibleTablesObj}
            tableHeaders={this.state.tableHeaders}
            attributeCols={visibleAttributeCols}
            updateColumns={this.updateColumns}
            updateTables={this.updateTables}
            canEdit={this.state.authToken !== ''}/>
        </TabPanel>
      </React.Fragment>
    );
  }
}

export default Tables;
