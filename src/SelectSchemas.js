import React from "react";
import './App.css';
import Select from './Select.js'

class SelectSchemas extends React.Component {
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
        });

        // Only one schema, select it by default
        if (results.length === 1) {
          this.props.onSelectSchema(results[0]);
        }
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
    if (!this.props.selectedDatabase) {
      return null;
    }

    return <Select label="schemas" 
            options={this.state.schemas} 
            onChange={ values => {
              let schema = null;
              if (values) {
                schema = this.state.schemas.find(e => e.id === values.id);
              }
            this.props.onSelectSchema(schema);
            }}
            selectedValue={this.props.selectedSchema}>
          </Select>;
  }
}

export default SelectSchemas;