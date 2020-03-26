import React from "react";
import DataGrid from './DataGrid.js';
import './App.css';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
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
  },
}));

class TabbedApp extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tables: null,
      error: null,
      currentTabIndex: 0
    };
  }

  componentDidMount() {
    fetch("/tables")
      .then(res => res.json())
      .then(
        (result) => {
          this.setState({
            tables: result
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
    const { error, tables, currentTabIndex } = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!tables) {
      return <div>Loading data...</div>;
    } else {
    
      const handleChange = (event, newTabIndex) => {
        this.setState({currentTabIndex: newTabIndex});
      };      
      return (
        <div className={this.props.classes.root}>
          <AppBar position="static">
            <Tabs value={currentTabIndex} onChange={handleChange} aria-label="Tabs">
              <Tab label="Tables" {...a11yProps(0)} />
              <Tab label="Diagram" {...a11yProps(1)} />
            </Tabs>
          </AppBar>
          <TabPanel value={currentTabIndex} index={0}>
            <DataGrid rows={tables} title='Table'/>
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
