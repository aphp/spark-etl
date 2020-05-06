import React from 'react';
import Error from '../helpers/Error.js';
import CircularIndeterminate from '../helpers/CircularIndeterminate.js';

function checkStatus(response) {
  if (!response.ok) {
    throw new Error(`could not fetch image: ${response.status} - ${response.statusText}`);
  }
  return response;
}

class ImageDiagram extends React.Component {
  constructor(props) {
      super(props);
      this.state = {image: null, error: null, isLoading: true};
  }

  componentDidMount() {
    if (!this.props.schema) {
      return;
    }

    fetch("/tables-viz?ids_schema=" + this.props.schema.id)
    .then(response => checkStatus(response) && response.arrayBuffer())
    .then((buffer) => {
      const buff = new Buffer(buffer);
      this.setState({
        image: buff.toString(),
        isLoading: false,
        error: null
      });
    })
    .catch(error => {
      this.setState({
        error,
        isLoading: false,
        image: null
      });
    });
  }

  render () {
    if (this.state.error) {
      return <Error error={this.state.error}/>
    }

    if (this.state.isLoading) {
      return <CircularIndeterminate size="100px"/>;
    }

    return <div style={{overflow: "auto"}} dangerouslySetInnerHTML={{ __html: this.state.image }}></div>;
  }
}

export default ImageDiagram;