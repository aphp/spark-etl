import React from "react";
import DataGrid from './DataGrid.js';
import './App.css';

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      table: null,
      attributes: null,
      error: null
    };
  }

  componentDidMount() {
    fetch("/tables")
      .then(res => res.json())
      .then(
        (result) => {
          this.setState({
            table: result
          });
        },
        (error) => {
          this.setState({
            error
          });
        }
      );
      fetch("/attributes")
      .then(res => res.json())
      .then(
        (result) => {
          this.setState({
            attributes: result
          });
        },
        (error) => {
          this.setState({
            error
          });
        }
      )      
  }
  render() {  
    const { error, attributes, table } = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!table || !attributes) {
      return <div>Loading data...</div>;
    } else {
      return (
        <div><DataGrid rows={table} attributes={attributes} title='Table'/></div>
      );
    }
  }
}

export default App;
