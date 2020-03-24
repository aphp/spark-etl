import React from "react";
import DataGrid from './DataGrid.js';
import './App.css';

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      table: null,
      attributes: null,
      isLoaded: false,
      error: null
    };
  }

  componentDidMount() {
    fetch("/table")
      .then(res => res.json())
      .then(
        (result) => {
          this.setState({
            isLoaded: true,
            table: result
          });
        },
        (error) => {
          this.setState({
            isLoaded: true,
            error
          });
        }
      )
  }
  render() {  
    const { error, isLoaded, table } = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!isLoaded) {
      return <div>Loading data...</div>;
    } else {
      return (
        <div><DataGrid rows={table} title='Table'/></div>
      );
    }
  }
}

export default App;
