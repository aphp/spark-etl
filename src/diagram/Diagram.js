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
import ZoomAction from './ZoomActions.js';

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

const engineMarginX = 25;
const engineMarginY = 25;

class EngineWidget extends React.Component {
	constructor(props) {
    super(props);
		this.engine = new DagreEngine({
			graph: {
				rankdir: 'BT',
				ranker: 'longest-path',
				marginx: engineMarginX,
        marginy: engineMarginY,
			},
			includeLinks: true
    });
	}

	autoDistribute = () => {
		this.engine.redistribute(this.props.engine.model);

		this.reroute();
		this.props.engine.repaintCanvas();
	};

	componentDidMount() {
		setTimeout(() => {
      this.autoDistribute();
      this.zoomToFit();
		}, 500);
	}

	reroute() {
		this.props.engine
			.getLinkFactories()
			.getFactory(PathFindingLinkFactory.NAME)
			.calculateRoutingMatrix();
  }

  zoomToFit() {
    const engine = this.props.engine;
    const initialZoomLevel = 100;
    const boundingRect = this.getNodesBoundingRect();
    const width = boundingRect.maxX - boundingRect.minX + engineMarginX * 2;
    const height = boundingRect.maxY - boundingRect.minY + engineMarginY * 2;

    const xFactor = engine.canvas.clientWidth / width;
    const yFactor = engine.canvas.clientHeight / height;
    const zoomFactor = xFactor < yFactor ? xFactor : yFactor;

    engine.model.setZoomLevel(initialZoomLevel * zoomFactor);

    engine.repaintCanvas();
  }

  getNodesBoundingRect() {
    let minX = Number.POSITIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    for (const node of Object.values(this.props.engine.model.activeNodeLayer.models)) {
      if (node.position.x < minX) {
        minX = node.position.x;
      }
      if (node.position.x + node.width > maxX) {
        maxX = node.position.x + node.width;
      }
      if (node.position.y < minY) {
        minY = node.position.y;
      }
      if (node.position.y + node.height > maxY) {
        maxY = node.position.y + node.height;
      }
    }

    return {minX, minY, maxX, maxY};
  }

	render() {
		return (
      <WorkspaceWidget buttons={
        <div>
          <StyledButton onClick={this.autoDistribute}>Re-distribute</StyledButton>
          <StyledButton onClick={() => this.zoomToFit()}>Zoom to fit</StyledButton>
          <StyledButton onClick={() => {
            // Create a fake event to trigger zoom in
            const evt = {type: 'wheel', deltaY: 1};
            this.props.engine.getActionEventBus().fireAction({event: evt});
           	}}>
               Zoom in
          </StyledButton>
          <StyledButton onClick={() => {
            // Create a fake event to trigger zoom out
            const evt = {type: 'wheel', deltaY: -1};
            this.props.engine.getActionEventBus().fireAction({event: evt});
            }}>
               Zoom out
          </StyledButton>
          <StyledButton onClick={() => this.props.showHideColumns()}>{ this.props.showColumns ? 'Hide columns' : 'Show columns' }</StyledButton>
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
      engine: null,
      nodesIndex: null,
      showColumns: false
    }
    this.showHideColumns = this.showHideColumns.bind(this);
  }

  showHidePorts(showColumns) {
    for (const table of this.props.tables) {
      const node = this.state.nodesIndex[table.id];

      for (const col of table.columns) {
        const portName = table.name + '.' + col.name;

        if (showColumns) {
          const port = new DefaultPortModel(true, portName, col.name);
          port.setLocked(true);
          node.addPort(port);
        } else {
          const port = node.ports[portName];
          if (port) {
            node.removePort(port);
          }
        }
      }
    }
  }

  showHideColumns() {
    const showColumns = !this.state.showColumns;
    this.showHidePorts(showColumns);
    this.setState({showColumns});
  }

	componentDidMount() {
    let model = new DiagramModel();

    const nodesIndex = {};
    for (const table of this.props.tables) {
      const node = createNode(table.name + ` [${table.id}]`);
      node.id = table.id;
      node.registerListener({
        selectionChanged: this.props.onSelected
      })
      node.options.color = defaultNodeColor;

      if (this.props.selectedTable) {
        node.setSelected(table.id === this.props.selectedTable.tables[0].id);
      }

      nodesIndex[table.id] = node;

      model.addNode(node);
    }

    const ports = {}
    for (const srcTgt of this.props.links) {
      const link = connectNodes(ports, nodesIndex[srcTgt.source], nodesIndex[srcTgt.target]);

      // link.addLabel(nodesIndex[srcTgt.source].options.name + ' -> ' + nodesIndex[srcTgt.target].options.name)
      model.addLink(link);
    }

    // model.setLocked(true);
    let engine = createEngine({
      registerDefaultDeleteItemsAction: false,
      registerDefaultZoomCanvasAction: false
    });
    engine.getActionEventBus().registerAction(new ZoomAction());

    engine.setModel(model);
    this.setState({engine, nodesIndex}, () => {
      this.showHidePorts(this.state.showColumns);
    });
  }

  render() {
    if (!this.state.engine || this.props.tables.length === 0) {
      return null;
    }

    for (const table of this.props.tables) {
      const node = this.state.nodesIndex[table.id];
      if (!node) {
        continue;
      }
      if (!table._display) {
        node.options.color = '#cccccc';
      } else {
        node.options.color = defaultNodeColor;
      }
    }

    return <EngineWidget engine={this.state.engine} showColumns={this.state.showColumns} showHideColumns={this.showHideColumns}/>;
  }
}

export default Diagram;