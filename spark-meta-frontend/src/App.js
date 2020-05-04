import React, { useState } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import './App.css';
import Select from './helpers/Select.js'
import SelectSchemas from './schema/SelectSchemas.js'
import Tables from './table/Tables.js';
import Error from './helpers/Error.js';
import { withRouter, HashRouter, Route } from "react-router-dom";
import Container from '@material-ui/core/Container';
import CssBaseline from '@material-ui/core/CssBaseline';
import Box from '@material-ui/core/Box';


class SelectDatabases extends React.PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      databases: [],
      error: null,
    };
  }

  onSelectDatabase = (values, pushRoute) => {
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
  grow: {
    flexGrow: 1,
  },
  bar: {
    backgroundColor: "rgb(0, 99, 175)",
  }
}));

function App() {
    const classes = useStyles();
    const [selectedDatabase, setSelectedDatabase] = useState(null);
    const [selectedSchema, setSelectedSchema] = useState(null);

    const tabKey = 'tab-' + (selectedSchema ? selectedSchema.id : 'none');
    const dbKey = 'db-' + (selectedDatabase ? selectedDatabase.id : 'none');

    return (
      <React.Fragment>
        <CssBaseline />
        <Box display="flex" className={classes.bar}>
          <Box m="auto"><a href="https://eds.aphp.fr" style={{"a:textDecoration": "none"}}><img src="images/logo-eds-blanc_1.png" alt="EDS"/></a></Box>
          <Box m="auto"><h1 style={{color: "#ffffff"}}>DataDef : documentation collaborative des bases de l’EDS</h1></Box>
        </Box>
        <Container maxWidth="md">
          <p>DataDef est un outil permettant la visualisation et la documentation des bases de l’EDS. Il est actuellement en développement grâce à l’aide de bénévoles. Toute suggestion ou commentaire est bienvenue, à adresser à <a href="mailto:jean-francois.yuen@dataiku.com">jean-francois.yuen@dataiku.com</a>, <a href="mailto:romain.bey@aphp.fr">romain.bey@aphp.fr</a></p>
        </Container>
        <HashRouter>
          {/* This component is always displayed, so we add path='', but leave the full path before to load the dbId if needed */}
          <Route path={['/database/:dbId', '']} render={ ({ match }) =>
            <Container maxWidth="md">
              <SelectDatabasesWithRouter
                selectedDatabase={selectedDatabase}
                setSelectedDatabase={setSelectedDatabase}
                setSelectedSchema={setSelectedSchema}/>
            </Container>
          }/>
          {/* This component is always displayed when dbId is present, so we add path='/database/:dbId', but leave the full path before to load the schema if a schema is also specified */}
          <Route path={['/database/:dbId/schema/:schemaId', '/database/:dbId']} render={ ({ match }) =>
            <Container maxWidth="md">
              <SelectSchemasWithRouter
                  key={dbKey}
                  setSelectedSchema={setSelectedSchema}
                  selectedDatabase={selectedDatabase}
                  selectedSchema={selectedSchema}/>
            </Container>
          }/>
        <Route path='/database/:dbId/schema/:schemaId' render={ ({ match }) =>
            <Tables key={tabKey} classes={classes} selectedSchema={selectedSchema} routeSchemaId={parseInt(match.params.schemaId)}/>
          }/>
        </HashRouter>
      </React.Fragment>
    );
}

export default App;
