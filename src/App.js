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

function applySearchFilter(tables, filter) {
  const { rows, columns, attributeCols } = tables;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

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

function buildKeyRows(rows) {
  const keyRows = [];
  
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    row._key = 'row-' + i;
    const keyColumns = [];
  
    for (let j = 0; j < row.columns.length; j++) {
      const col = row.columns[j];
      col._key = row._key + '-col-' + j;
      keyColumns.push(col);
    }
  
    row.columns = keyColumns;
    keyRows.push(row);
  }
  
  return keyRows;
}


class TabbedApp extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tables: null,
      error: null,
      currentTabIndex: 0,
      searchText: ''
    };
  }

  componentDidMount() {
    fetch("/tables")
      .then(res => res.json())
      .then(
        (result) => {
          const columns = getColumns(result, ['columns']);
          const attributeCols = getColumns(result[0].columns);
          const rows = buildKeyRows(result);
          const tables = { rows, columns, attributeCols };
          applySearchFilter(tables, '');
        
          this.setState({
            tables: tables
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
    const { error, tables, currentTabIndex, searchText } = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!tables) {
      return <div>Loading data...</div>;
    } else {
    
      const handleChange = (event, newTabIndex) => {
        this.setState({currentTabIndex: newTabIndex});
      };      
      
      const updateSearchText = (e) => {
        const searchText = e.target.value;
        this.setState({ searchText: searchText });
        applySearchFilter(tables, searchText);
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
            <DataGrid tables={tables} filter={searchText}/>
          </TabPanel>
          <TabPanel value={currentTabIndex} index={1}>
            Diagram
          </TabPanel>      
        </div>
      );
    }
  }
}

function App() { 
    const classes = useStyles();
    return (<TabbedApp classes={classes}></TabbedApp>);
} 

export default App;
