import React, { useState } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import './App.css';
import Select from './Select.js'
import SelectSchemas from './SelectSchemas.js'
import Tables from './Tables.js';
import Error from './Error.js';
import { withRouter, HashRouter, Route } from "react-router-dom";

class SelectDatabases extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      databases: [],
      error: null,
    };
    this.onSelectDatabase = this.onSelectDatabase.bind(this);
  }

  onSelectDatabase(values, pushRoute) {
    if (!values) {
      this.props.setSelectedDatabase(null);
      this.props.setSelectedSchema(null);
      return;
    }

    const selectedDatabase = this.state.databases.find(e => e.id === values.id);
    if (selectedDatabase) {
      this.props.setSelectedDatabase(selectedDatabase);
      this.props.setSelectedSchema(null);
      if (pushRoute) {
        this.props.history.push('/database/' + selectedDatabase.id);
      }
    }
  };

  componentDidMount() {
    fetch("/databases")
      .then(res => res.json())
      .then(
        (results) => {
          this.setState({
            databases: results,
          }, () => {
            if (this.props.match.params.dbId) {
              this.onSelectDatabase({id: parseInt(this.props.match.params.dbId)}, false);
            }
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
    const { databases, error } = this.state;
    const searchText = '';

    if (error) {
      return <Error error={error}/>
    }

    return <Select label="databases" options={databases} onChange={values => this.onSelectDatabase(values, true)} selectedValue={this.props.selectedDatabase}/>;
  }
}

const SelectDatabasesWithRouter = withRouter(SelectDatabases);
const SelectSchemasWithRouter = withRouter(SelectSchemas);


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
    const [selectedDatabase, setSelectedDatabase] = useState(null);
    const [selectedSchema, setSelectedSchema] = useState(null);

    const tabKey = 'tab-' + (selectedSchema ? selectedSchema.schema.id : 'none');
    const dbKey = 'db-' + (selectedDatabase ? selectedDatabase.id : 'none');

    return (
      <HashRouter>
        {/* This component is always displayed, so we add path='', but leave the full path before to load the dbId if needed */}
        <Route path={['/database/:dbId', '']} render={ ({ match }) =>
          <SelectDatabasesWithRouter
            selectedDatabase={selectedDatabase}
            setSelectedDatabase={setSelectedDatabase}
            setSelectedSchema={setSelectedSchema}/>}/>
        {/* This component is always displayed when dbId is present, so we add path='/database/:dbId', but leave the full path before to load the schema if a schema is also specified */}
        <Route path={['/database/:dbId/schema/:schemaId', '/database/:dbId']} render={ ({ match }) =>
        <SelectSchemasWithRouter
            key={dbKey}
            setSelectedSchema={setSelectedSchema}
            selectedDatabase={selectedDatabase}
            selectedSchema={selectedSchema}/>
        }/>
      <Route path='/database/:dbId/schema/:schemaId' render={ ({ match }) =>
          <Tables key={tabKey} classes={classes} selectedSchema={selectedSchema} routeSchemaId={parseInt(match.params.schemaId)}/>
        }/>
      </HashRouter>
    );
}

export default App;
