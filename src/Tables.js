import React from "react";
import DataGrid from './DataGrid.js';
import SearchInput from './SearchInput.js';
import './App.css';
import PropTypes from 'prop-types';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Box from '@material-ui/core/Box';
import Diagram from './diagram/Diagram.js';
import Select from './Select.js'
import Error from './Error.js';


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

function applySearchFilter(schema, filter) {
  const { tables, columns, attributeCols } = schema;

  for (let i = 0; i < tables.length; i++) {
    const row = tables[i];

    row._hasColumnDisplay = false;
    for (let j = 0; j < row.columns.length; j++) {
      const col = row.columns[j];
      col._display = hasSearchText(col, attributeCols, filter);
      row._hasColumnDisplay = row._hasColumnDisplay || col._display;
    }

    if (row._hasColumnDisplay) {
      row._display = true;
    } else {
      row._display = hasSearchText(row, columns, filter);
    }
  }
}


function getColumns(rows, skip) {
  const columns = [];
  const colNames = Object.keys(rows[0]);

  for (let i = 0; i < colNames.length; i++ ) {
    const name = colNames[i];
    if (name.startsWith('_') || (skip && skip.includes(name))) {
      continue;
    }

    columns.push({name: name})
  }

  return columns;
}

class Tables extends React.Component {
  constructor(props) {
    super(props);
    this.state = {tables: [], tabIndex: 0};
    this.selectByTableId = this.selectByTableId.bind(this);
    this.changeTab = this.changeTab.bind(this);
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
          table._key = 'table-' + table.id;
          for (const column of table.columns) {
            column._display = true;
            column._key = table._key + '-col-' + column.id;
          }
        }

        this.props.selectedSchema.tables = results.tables;
        this.props.selectedSchema.links = results.links;
        this.setState({
          selectedTable: null,
          error: null
        });
      },
      (error) => {
        this.setState({
          error,
          selectedTable: null,
        });
      }
    );
  }

  selectByTableId(tableId) {
    if (!tableId) {
      this.setState({
        selectedTable: null
      });
    }

    for (const table of this.props.selectedSchema.tables) {
      if (table.id === tableId) {

        this.setState({
          selectedTable: {
            tables: [table],
            columns: getColumns([table], ['columns', 'id']),
            attributeCols: getColumns(table.columns, ['id', 'ids_table'])
          }
        });

        break;
      }
    }
  }

  changeTab (event, newTabIndex) {
    this.setState({tabIndex: newTabIndex});
  };

  render() {
    const {selectedSchema, classes} = this.props;

    if (!selectedSchema) {
      return null;
    }

    if (this.state.error) {
      return <Error error={this.state.error}/>
    }


    const onSelectTable = values => {
      this.selectByTableId(values && values.id);
    };

    const onSelectedDiagramTable = e => {
      if (!e.isSelected) {
        return;
      }
      this.selectByTableId(e.entity.id);
    };

    const searchText = '';

    // const updateSearchText = e => {
    //   const searchText = e.target.value;
    //   this.setState({ searchText: searchText });
    //   applySearchFilter(schema, searchText);
    // }

    const selectedTable = this.state.selectedTable;
    const selectedTableValue = selectedTable ? selectedTable.tables[0] : null;
    let columnsCount = selectedSchema.tables.reduce((a, b) => a + b.columns.length, 0);
    return (
      <div className={classes.root}>
        <Box display="flex" style={{ width: '100%' }}>
          <Box m="auto"><Typography component="div" display="inline" bgcolor="background.paper">{selectedSchema.tables.length} tables</Typography></Box>
          <Box m="auto"><Typography component="div" display="inline" bgcolor="background.paper">{columnsCount} columns</Typography></Box>
        </Box>
        <Select label="tables" options={selectedSchema.tables} onChange={onSelectTable} selectedValue={selectedTableValue}></Select>
        <AppBar position="static">
        <Toolbar>
          <Tabs value={this.state.tabIndex} onChange={this.changeTab} aria-label="Tabs">
            <Tab label="Diagram" {...a11yProps(0)} />
            { selectedTable && <Tab
              label={<div>
                  <div>{'Table: ' + selectedTable.tables[0].name}</div>
                  <div>{'(' + selectedTable.tables[0].columns.length + ' columns)'}</div>
                </div> }
              {...a11yProps(1)} /> }
          </Tabs>
          {/* <SearchInput updateSearchText={updateSearchText} searchText={searchText}></SearchInput> */}
        </Toolbar>
        </AppBar>
        <TabPanel value={this.state.tabIndex} index={0}>
          { selectedSchema.tables.length > 0 && <Diagram
            tables={selectedSchema.tables}
            links={selectedSchema.links}
            selectedTable={selectedTable}
            onSelected={onSelectedDiagramTable}>
          </Diagram>}
        </TabPanel>
        <TabPanel value={this.state.tabIndex} index={1}>
          <DataGrid className={classes.selectedTable} schema={selectedTable} filter={searchText}/>
        </TabPanel>
      </div>
    );
  }
}

export default Tables;
