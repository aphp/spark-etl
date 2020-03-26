import React from "react";
import DataGrid from './DataGrid.js';
import './App.css';

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tables: null,
      error: null
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
    const { error, tables } = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!tables) {
      return <div>Loading data...</div>;
    } else {
      return (
        <div><DataGrid rows={tables} title='Table'/></div>
      );
    }
  }
}

export default App;
