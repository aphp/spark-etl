import React, { useState } from "react";
import DataGrid from './DataGrid.js';
import SearchInput from './SearchInput.js';
import './App.css';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Box from '@material-ui/core/Box';
import Diagram from './diagram/Diagram.js';
import Select from './Select.js'


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

const useStyles = makeStyles(theme => ({
  root: {
    flexGrow: 1,
    backgroundColor: theme.palette.background.paper,
  },
  selectedTable: {
    float: 'right'
  }
}));

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

function TabbedApp(props) {
  const {selectedSchema, selectedTable, classes, selectByTableId} = props;
  const [currentTabIndex, changeTabIndex] = useState(0);

  if (!selectedSchema) {
    return null;
  }
  
  const changeTab = (event, newTabIndex) => {
    changeTabIndex(newTabIndex);
  };      
  
  const onSelectTable = values => {
    selectByTableId(values && values.id);   
  };

  const onSelectedDiagramTable = e => {
    if (!e.isSelected) {
      return;
    }
    selectByTableId(e.entity.id);
  };

  const searchText = '';

  // const updateSearchText = e => {
  //   const searchText = e.target.value;
  //   this.setState({ searchText: searchText });
  //   applySearchFilter(schema, searchText);
  // }

  const selectedTableValue = selectedTable ? selectedTable.tables[0] : null;

  return (
    <div className={classes.root}>
      <Select label="tables" options={selectedSchema.tables} onChange={onSelectTable} selectedValue={selectedTableValue}></Select>
      <AppBar position="static">
      <Toolbar>
        <Tabs value={currentTabIndex} onChange={changeTab} aria-label="Tabs">
          <Tab label="Diagram" {...a11yProps(0)} />
          { selectedTable && <Tab label={'Table: ' + selectedTable.tables[0].name} {...a11yProps(1)} /> }
        </Tabs>
        {/* <SearchInput updateSearchText={updateSearchText} searchText={searchText}></SearchInput> */}
      </Toolbar>
      </AppBar>
      <TabPanel value={currentTabIndex} index={0}>
        <Diagram 
          tables={selectedSchema.tables} 
          links={selectedSchema.links} 
          selectedTable={selectedTable} 
          onSelected={onSelectedDiagramTable}>
        </Diagram>
      </TabPanel>
      <TabPanel value={currentTabIndex} index={1}>
        <DataGrid className={classes.selectedTable} schema={selectedTable} filter={searchText}/>
      </TabPanel>      
    </div>
  );
}

class SelectDatabases extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      databases: [],
      schemas: [],
      error: null,
    };
    this.onSelectDatabase = this.onSelectDatabase.bind(this);
    this.onSelectSchema = this.onSelectSchema.bind(this);
    this.selectByTableId = this.selectByTableId.bind(this);
  }

  componentDidMount() {
    fetch("/databases")
      .then(res => res.json())
      .then(
        (results) => {
          this.setState({
            databases: results,
            schemas: [],
            selectedSchema: null,
            selectedDatabase: null,
            selectedTable: null
          });
        },
        (error) => {
          this.setState({
            error
          });
        }
      );
  }

  onSelectDatabase(values) {
    if (!values) {
      this.setState({
        selectedSchema: null,
        selectedTable: null,
        selectedDatabase: null,
        schemas: []
      });
      return;
    }

    fetch("/schemas?ids_database=" + values.id)
    .then(res => res.json())
    .then(
      (results) => {
        this.setState({
          schemas: results,
          selectedDatabase: this.state.databases.find(e => e.id === values.id),
          selectedSchema: null,
          selectedTable: null
        });

        // Only one schema, select it by default
        if (results.length === 1) {
          this.onSelectSchema(results[0]);
        }
      },
      (error) => {
        this.setState({
          error,
          selectedSchema: null,
          selectedTable: null,
          selectedDatabase: null,
          schemas: []
        });
      }
    );    
  }

  onSelectSchema(values) {
    if (!values) {
      this.setState({
        selectedSchema: null,
        selectedTable: null,
      });
      return;
    }

    fetch("/tables?ids_schema=" + values.id)
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

        const schema = this.state.schemas.find(e => e.id === values.id);
        this.setState({
          selectedSchema: {...results, schema: schema},
          selectedTable: null,
        });
      },
      (error) => {
        this.setState({
          error,
          selectedSchema: null,
          selectedTable: null,
          schemas: []
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

    for (const table of this.state.selectedSchema.tables) {
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
  
  render() {
    const { databases, schemas, selectedDatabase, selectedSchema, selectedTable } = this.state;
    const classes = this.props.classes;
    const searchText = '';

    const selectedSchemaValue = selectedSchema ? selectedSchema.schema : null;
    const tabKey = 'tab-' + (selectedSchema ? selectedSchema.schema.id : 'none');

    return (
      <div>
        <Select label="databases" options={databases} onChange={this.onSelectDatabase} selectedValue={selectedDatabase}></Select>
        <Select label="schemas" options={schemas} onChange={this.onSelectSchema} selectedValue={selectedSchemaValue}></Select>
        <TabbedApp key={tabKey} classes={classes} selectedTable={selectedTable} selectedSchema={selectedSchema} selectByTableId={this.selectByTableId}></TabbedApp>
      </div>
    );
  }
}

function App() { 
    const classes = useStyles();
    return (<SelectDatabases classes={classes}></SelectDatabases>);
} 

export default App;
