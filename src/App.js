import React from "react";
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
      {value === index && <Box p={3}>{children}</Box>}
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

class TabbedApp extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      selectedTable: null,
      tables: null,
      error: null,
      currentTabIndex: 0,
      searchText: ''
    };
  }

  componentDidMount() {
    fetch("/schema")
      .then(res => res.json())
      .then(
        (results) => {
          const columns = getColumns(results.tables, ['columns', 'id']);
          const attributeCols = getColumns(results.tables[0].columns, ['id']);
          const schema = { tables: results.tables, columns, attributeCols };
          applySearchFilter(schema, '');
        
          this.setState({
            schema: schema,
            links: results.links
          });
        },
        (error) => {
          this.setState({
            error
          });
        }
      );
  }
  render() {  
    const { error, schema, links, currentTabIndex, searchText, selectedTable } = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!schema) {
      return <div>Loading data...</div>;
    } else {
    
      const handleChange = (event, newTabIndex) => {
        this.setState({currentTabIndex: newTabIndex});
      };      
      
      const updateSearchText = e => {
        const searchText = e.target.value;
        this.setState({ searchText: searchText });
        applySearchFilter(schema, searchText);
      }

      const onSelectedDiagramTable = e => {
        if (!e.isSelected) {
          return;
        }
        for (const table of schema.tables) {
          if (table.id === e.entity.id) {
            this.setState({selectedTable: table});
            break;
          }
        }
      }

      const classes = this.props.classes;
      return (
        <div className={classes.root}>
          <AppBar position="static">
          <Toolbar>
            <Tabs value={currentTabIndex} onChange={handleChange} aria-label="Tabs">
              <Tab label="Tables" {...a11yProps(0)} />
              <Tab label="Diagram" {...a11yProps(1)} />
            </Tabs>
            <SearchInput updateSearchText={updateSearchText} searchText={searchText}></SearchInput>
          </Toolbar>
          </AppBar>
          <TabPanel value={currentTabIndex} index={0}>
            <DataGrid schema={schema} filter={searchText}/>
          </TabPanel>
          <TabPanel value={currentTabIndex} index={1}>
            <Diagram tables={schema.tables} links={links} onSelected={onSelectedDiagramTable}></Diagram>
            { selectedTable && <DataGrid className={classes.selectedTable} schema={{tables: [selectedTable], columns: schema.columns, attributeCols: schema.attributeCols}} filter={searchText}/>}
          </TabPanel>      
        </div>
      );
    }
  }
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
    this.onSelectedDiagramTable = this.onSelectedDiagramTable.bind(this);
  }

  componentDidMount() {
    fetch("/databases")
      .then(res => res.json())
      .then(
        (results) => {
          this.setState({
            databases: results,
            schemas: [],
            tables: [],
            selectedDatabase: null
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
    fetch("/schemas?ids_database=" + values.id)
    .then(res => res.json())
    .then(
      (results) => {
        this.setState({
          schemas: results,
          selectedDatabase: values.id
        });
      },
      (error) => {
        this.setState({
          error,
          schemas: []
        });
      }
    );    
  }

  onSelectSchema (values) {
    fetch("/tables?ids_schema=" + values.id)
    .then(res => res.json())
    .then(
      (results) => {
        this.setState({
          tables: results,
        });
      },
      (error) => {
        this.setState({
          error,
          schemas: []
        });
      }
    );      
  }

  render() {
    const { error, databases, schemas, selectedDatabase } = this.state;

    return (
      <div>
        <Select label="databases" options={databases} error={error} onChange={this.onSelectDatabase}></Select>
        <Select key={selectedDatabase} label="schemas" options={schemas} error={error} onChange={this.onSelectSchema}></Select>
      </div>
    );
  }
}

function App() { 
    const classes = useStyles();
    return (<SelectDatabases></SelectDatabases>);
} 

export default App;
