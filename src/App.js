import React from "react";
import { makeStyles } from '@material-ui/core/styles';
import './App.css';
import Select from './Select.js'
import SelectSchemas from './SelectSchemas.js'
import Tables from './Tables.js'

class SelectDatabases extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      databases: [],
      selectedDatabase: null,
      selectedSchema: null,      
      error: null,
    };
    this.onSelectDatabase = this.onSelectDatabase.bind(this);
    this.onSelectSchema = this.onSelectSchema.bind(this);
  }

  componentDidMount() {
    fetch("/databases")
      .then(res => res.json())
      .then(
        (results) => {
          this.setState({
            databases: results,
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
      return;
    }
    // this.props.history.push('/database/' + values.id);
    const selectedDatabase = this.state.databases.find(e => e.id === values.id);
    this.setState({selectedDatabase, selectedSchema: null});
  }  

  onSelectSchema(schema) {
    if (!schema) {
      this.setState({
        selectedSchema: null,
      });
      return;
    }

    this.setState({selectedSchema: {schema, tables: [], links: []}});
  }
  
  render() {
    const { databases, selectedDatabase, selectedSchema } = this.state;
    const classes = this.props.classes;
    const searchText = '';

    const selectedSchemaValue = selectedSchema ? selectedSchema.schema : null;
    const tabKey = 'tab-' + (selectedSchema ? selectedSchema.schema.id : 'none');
    const dbKey = 'db-' + (selectedDatabase ? selectedDatabase.id : 'none');
    
    return (
      <div>
        <Select label="databases" options={databases} onChange={this.onSelectDatabase} selectedValue={selectedDatabase}></Select>
        <SelectSchemas key={dbKey} onSelectSchema={this.onSelectSchema} selectedDatabase={this.state.selectedDatabase} selectedSchema={selectedSchemaValue}></SelectSchemas>
        <Tables key={tabKey} classes={classes} selectedSchema={selectedSchema} selectByTableId={this.selectByTableId}></Tables>
      </div>
    );
  }
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

function App() { 
    const classes = useStyles();
    return (<SelectDatabases classes={classes}></SelectDatabases>);
} 

export default App;
