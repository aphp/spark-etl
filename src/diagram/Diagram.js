import createEngine, {
	DiagramModel,
	DefaultNodeModel,
	DefaultPortModel,
	DagreEngine,
	PathFindingLinkFactory,
} from '@projectstorm/react-diagrams';
import * as React from 'react';
import { StyledButton, WorkspaceWidget } from './WorkspaceWidget';
import { CanvasWidget } from '@projectstorm/react-canvas-core';
import { StyledCanvasWidget } from './StyledCanvasWidget';

const defaultNodeColor = 'rgb(0,192,255)';
function createNode(name) {
	return new DefaultNodeModel(name, defaultNodeColor);
}

function connectNodes(ports, nodeFrom, nodeTo) {
  const outKey = nodeFrom.options.name + '.to'; //+ columnFrom;
  if (!ports.hasOwnProperty(outKey)) {
    ports[outKey] = new DefaultPortModel(true, outKey, '');
    ports[outKey].setLocked(true);
    nodeFrom.addPort(ports[outKey]);
  }

  const toKey = nodeTo.options.name + '.from'; //+ columnFrom;
  if (!ports.hasOwnProperty(toKey)) {
    ports[toKey] = new DefaultPortModel(false, toKey, '');
    ports[toKey].setLocked(true);
    nodeTo.addPort(ports[toKey]);
  }
	return ports[outKey].link(ports[toKey]);
}

class EngineWidget extends React.Component {
	constructor(props) {
		super(props);
		this.engine = new DagreEngine({
			graph: {
				rankdir: 'RL',
				ranker: 'longest-path',
				marginx: 25,
        marginy: 25,
			},
			includeLinks: true
    });
	}

	autoDistribute = () => {
		this.engine.redistribute(this.props.model);

		this.reroute();
		this.props.engine.repaintCanvas();
	};

	componentDidMount() {
		setTimeout(() => {
      this.autoDistribute();
      this.props.engine.zoomToFit()
		}, 500);
	}

	reroute() {
		this.props.engine
			.getLinkFactories()
			.getFactory(PathFindingLinkFactory.NAME)
			.calculateRoutingMatrix();
	}

	render() {
		return (
      <WorkspaceWidget buttons={
        <div>
          <StyledButton onClick={this.autoDistribute}>Re-distribute</StyledButton>
          <StyledButton onClick={() => this.props.engine.zoomToFit()}>Zoom to fit</StyledButton>
          <StyledButton onClick={() => {
            this.props.model.setZoomLevel(this.props.model.getZoomLevel() + 2);
           	this.props.engine.repaintCanvas();}}>
               Zoom in
          </StyledButton>
          <StyledButton onClick={() => {
            this.props.model.setZoomLevel(this.props.model.getZoomLevel() - 2);
           	this.props.engine.repaintCanvas();}}>
               Zoom out
          </StyledButton>          
        </div>}>
				<StyledCanvasWidget>
					<CanvasWidget engine={this.props.engine} />
				</StyledCanvasWidget>
			</WorkspaceWidget>
		);
	}
}

class Diagram extends React.Component {
	constructor(props) {
    super(props);
    this.state = {
      model: null,
      engine: null,
      nodesIndex: {},
      selected: null
    }
  }
  
	componentDidMount() {
    let model = new DiagramModel();
    
    const nodesIndex = {};
    for (const table of this.props.tables) {
      const node = createNode(table.name);
      node.id = table.id;
      node.registerListener({
        selectionChanged: this.props.onSelected
      })
      nodesIndex[table.id] = node;
      for (const col of table.columns) {
        const port = new DefaultPortModel(true, table.name + '.' + col.name, col.name);
        port.setLocked(true);
        node.addPort(port);
      }
      model.addNode(node);
    }
  
    const ports = {}
    for (const srcTgt of this.props.links) {
      const link = connectNodes(ports, nodesIndex[srcTgt.source], nodesIndex[srcTgt.target]);
      
      // link.addLabel(nodesIndex[srcTgt.source].options.name + ' -> ' + nodesIndex[srcTgt.target].options.name)
      model.addLink(link);
    }

    // model.setLocked(true);
    let engine = createEngine({registerDefaultDeleteItemsAction: false});
    engine.setModel(model);
    this.setState({model, engine, nodesIndex});
  }
  
  render() {
    if (!this.state.engine) {
      return <div></div>;
    }

    const nodesIndex = this.state.nodesIndex;
    
    for (const table of this.props.tables) {
      if (!table._display) {
        nodesIndex[table.id].options.color = '#cccccc';
      } else {
        nodesIndex[table.id].options.color = defaultNodeColor;
      }
    }

    return <EngineWidget model={this.state.model} engine={this.state.engine} />;
  }
}

export default Diagram;