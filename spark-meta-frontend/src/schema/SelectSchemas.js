import React from "react";
import Select from '../helpers/Select.js'
import Error from '../helpers/Error.js';

class SelectSchemas extends React.PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      schemas: [],
      error: null,
    };
  }

  componentDidMount() {
    if (!this.props.selectedDatabase) {
      this.setState({
        schemas: []
      });
      return;
    }

    fetch("/schemas?ids_database=" + this.props.selectedDatabase.id)
    .then(res => res.json())
    .then(
      (results) => {
        this.setState({
          schemas: results,
        }, () => {
          if (this.props.match.params.schemaId) {
            this.onSelectSchema({id: parseInt(this.props.match.params.schemaId)}, false);
          } else if (results.length === 1) { // Only one schema, select it by default
            this.onSelectSchema(results[0], true);
          }
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

  onSelectSchema = (values, pushRoute) => {
    if (!values) {
      this.props.setSelectedSchema(null);
      return;
    }

    const schema = this.state.schemas.find(e => e.id === values.id);
    if (schema) {
      this.props.setSelectedSchema(schema);
      if (pushRoute) {
        this.props.history.push('/database/' + this.props.selectedDatabase.id + '/schema/' + schema.id);
      }
    }
  }

  render() {
    if (!this.props.selectedDatabase) {
      return null;
    }

    if (this.state.error) {
      return <Error error={this.state.error}/>
    }

    return <Select label="schemas"
            options={this.state.schemas}
            onChange={ values => this.onSelectSchema(values, true)}
            selectedValue={this.props.selectedSchema}>
          </Select>;
  }
}

export default SelectSchemas;