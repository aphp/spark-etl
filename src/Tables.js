import React from "react";
import DataGrid from './DataGrid.js';
import SearchInput from './helpers/SearchInput.js';
import PropTypes from 'prop-types';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Box from '@material-ui/core/Box';
import Diagram from './diagram/Diagram.js';
import Select from './helpers/Select.js'
import Error from './helpers/Error.js';
import CircularIndeterminate from './helpers/CircularIndeterminate.js';
import SchemaStats from './SchemaStats.js';


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
      <Box p={3}>{children}</Box>
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

function hasSearchText(col, attributeCols, filter) {
  if (!filter || filter.length === 0) {
    return true;
  }

  for (const colName of attributeCols) {
    if (typeof col[colName.name] ==='string' && col[colName.name].search(filter) >= 0) {
      return true;
    }
  }

  return false;
}

// This is the only place where a table or row content can change
// so this is also where we handle the '_forceUpdate' attribute to improve performance in DataGrid with shouldComponentUpdate()
function applySearchFilter(schema, filter) {
  const { tables, tableHeaders, attributeCols } = schema;
  let changed = false;
  let visibleColumns = 0;
  let visibleTable = 0;

  for (let i = 0; i < tables.length; i++) {
    const row = tables[i];

    row._hasColumnDisplay = false;
    let tableChanged = false;

    for (let j = 0; j < row.columns.length; j++) {
      const col = row.columns[j];
      const hasText = hasSearchText(col, attributeCols, filter);
      const colChanged = col._display !== hasText;
      col._forceUpdate = colChanged || hasText;
      tableChanged = tableChanged || col._forceUpdate;
      col._display = hasText;
      row._hasColumnDisplay = row._hasColumnDisplay || col._display;
      if (hasText) {
        visibleColumns++;
      }
    }


    if (row._hasColumnDisplay) {
      row._display = true;
    } else {
      const hasText = hasSearchText(row, tableHeaders, filter);
      tableChanged = tableChanged || (row._display !== hasText);
      row._display = hasText;
    }

    row._forceUpdate = tableChanged;
    changed = changed || tableChanged;

    if (row._display) {
      visibleTable++;
    }
  }

  schema._forceUpdate = changed;
  schema.visibleTables = visibleTable;
  schema.visibleColumns = visibleColumns;
}

class Tables extends React.PureComponent {
  constructor(props) {
    super(props);
    this.state = {tables: [], tabIndex: 0, searchText: '', isLoading: true};
  }

  componentDidMount() {
    if (!this.props.selectedSchema) {
      this.setState({
        tables: [],
        selectedTable: null,
        error: null
      });
      return;
    }

    fetch("/tables?ids_schema=" + this.props.selectedSchema.schema.id)
    .then(res => res.json())
    .then(
      (results) => {
        for (const table of results.tables) {
          table._display = true;
          table._hasColumnDisplay = true;
          table._forceUpdate = false;
          table._key = 'table-' + table.id;
          for (const column of table.columns) {
            column._display = true;
            column._forceUpdate = false;
            column._key = table._key + '-col-' + column.id;
          }
        }

        this.props.selectedSchema.tables = results.tables;
        this.props.selectedSchema.links = results.links;
        this.props.selectedSchema.tableHeaders = results.tableHeaders;
        this.props.selectedSchema.attributeCols = results.attributeCols;
        this.props.selectedSchema._forceUpdate = false;
        this.setState({
          selectedTable: null,
          searchText: '',
          isLoading: false,
          error: null
        });
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

  selectByTableId = (tableId) => {
    if (!tableId) {
      this.setState({
        selectedTable: null
      });
    }

    const table = this.props.selectedSchema.tables.filter(e => e.id === tableId);
    if (!table) {
      return;
    }

    const selectedTable = {};
    selectedTable.tables = table;
    selectedTable.tableHeaders = this.props.selectedSchema.tableHeaders;
    selectedTable.attributeCols = this.props.selectedSchema.attributeCols;
    this.setState({ selectedTable });
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


  updateSearchText = (e) => {
    const searchText = e.target.value;
    applySearchFilter(this.props.selectedSchema, searchText);
    this.setState({ searchText: searchText });
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
    const selectedTableValue = selectedTable ? selectedTable.tables[0] : null;
    return (
      <div className={classes.root}>
        <SchemaStats selectedSchema={selectedSchema}/>
        <Select label="tables" options={selectedSchema.tables} onChange={this.onSelectTable} selectedValue={selectedTableValue}></Select>
        <AppBar position="static">
        <Toolbar>
          <Tabs value={this.state.tabIndex} onChange={this.changeTab} aria-label="Tabs">
            <Tab label="Diagram" {...a11yProps(0)} />
            <Tab label="All tables" {...a11yProps(1)} />
            { selectedTable && <Tab
              label={<div>
                  <div>{'Table: ' + selectedTableValue.name}</div>
                  <div>{'(' + selectedTableValue.columns.length + ' columns)'}</div>
                </div> }
              {...a11yProps(2)} /> }
          </Tabs>
          <SearchInput updateSearchText={this.updateSearchText} searchText={this.state.searchText}/>
        </Toolbar>
        </AppBar>
        <TabPanel value={this.state.tabIndex} index={0}>
          { selectedSchema.tables.length > 0 && <Diagram
            tables={selectedSchema.tables}
            links={selectedSchema.links}
            selectedTable={selectedTable}
            onSelected={this.onSelectedDiagramTable}/>}
        </TabPanel>
        <TabPanel value={this.state.tabIndex} index={1}>
          <DataGrid className={classes.selectedTable} schema={selectedSchema} searchText={this.state.searchText}/>
        </TabPanel>
        <TabPanel value={this.state.tabIndex} index={2}>
          <DataGrid className={classes.selectedTable} schema={selectedTable} searchText={this.state.searchText}/>
        </TabPanel>
      </div>
    );
  }
}

export default Tables;
