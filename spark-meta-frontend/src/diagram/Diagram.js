import createEngine, {
	DiagramModel,
	DefaultNodeModel,
	DagreEngine,
	PathFindingLinkFactory,
} from '@projectstorm/react-diagrams';
import * as React from 'react';
import { StyledButton, WorkspaceWidget } from './WorkspaceWidget';
import { CanvasWidget } from '@projectstorm/react-canvas-core';
import { StyledCanvasWidget } from './StyledCanvasWidget';
import ZoomAction from './ZoomActions.js';
import CircularIndeterminate from '../helpers/CircularIndeterminate.js';
import { AdvancedLinkFactory, AdvancedPortModel } from './ArrowLink';
import { CustomNodeFactory } from './CustomNode';

const defaultNodeColor = '#fefefe';
function createNode(name) {
	return new DefaultNodeModel(name, defaultNodeColor);
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
			includeLinks: false
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

	reroute = () => {
		this.props.engine
			.getLinkFactories()
			.getFactory(PathFindingLinkFactory.NAME)
			.calculateRoutingMatrix();
  }

  zoomToFit = () =>  {
    const engine = this.props.engine;
    const initialZoomLevel = 100;
    const boundingRect = this.getNodesBoundingRect();
    const width = boundingRect.maxX - boundingRect.minX + engineMarginX * 2;
    const height = boundingRect.maxY - boundingRect.minY + engineMarginY * 2;

    const xFactor = engine.canvas.clientWidth / width;
    const yFactor = engine.canvas.clientHeight / height;
    const zoomFactor = xFactor < yFactor ? xFactor : yFactor;

    engine.model.setZoomLevel(initialZoomLevel * zoomFactor);
    engine.model.setOffset(0, 0);
    engine.repaintCanvas();
  }

  getNodesBoundingRect = () =>  {
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

  zoomIn = () =>  {
    // Create a fake event to trigger zoom in
    const evt = {type: 'wheel', deltaY: -1};
    this.props.engine.getActionEventBus().fireAction({event: evt});
  }

  zoomOut = () =>  {
    // Create a fake event to trigger zoom out
    const evt = {type: 'wheel', deltaY: 1};
    this.props.engine.getActionEventBus().fireAction({event: evt});
  }
	render() {
		return (
      <WorkspaceWidget buttons={
        <div>
          <StyledButton onClick={this.autoDistribute}>Re-distribute</StyledButton>
          <StyledButton onClick={this.zoomToFit}>Zoom to fit</StyledButton>
          <StyledButton onClick={this.zoomIn}>Zoom in</StyledButton>
          <StyledButton onClick={this.zoomOut}>Zoom out</StyledButton>
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
      showColumns: false,
      isLoading: true
    }
  }

  shouldComponentUpdate(nextProps, nextState) {
    return this.state.engine !== nextState.engine ||
    Object.keys(this.props.visibleTables).length !== Object.keys(nextProps.visibleTables).length;
  }

	componentDidMount() {
    let model = new DiagramModel();

    const nodesIndex = {};
    const ports = {}
    for (const table of this.props.tables) {
      const node = createNode(table.name + ` [${table.id}]`);
      node.id = table.id;
      table._node = node;
      node.registerListener({
        selectionChanged: this.props.onSelected
      })
      node.options.color = defaultNodeColor;

      nodesIndex[table.id] = node;

      for (const col of table.columns) {
        const portName = table.name + '.' + col.name;
        const inside = col.parents.length === 0;
        const port = new AdvancedPortModel(inside, portName, col.name);
        port.setPk(col.is_pk);
        port.setFk(col.is_fk);
        port.setLocked(true);
        node.addPort(port);
        ports[portName] = port;
      }
      model.addNode(node);
    }

    // Add ports + connect nodes
    for (const table of this.props.tables) {
      for (const src of table.children) {
        const srcPort = ports[src.table_source + '.' + src.column_source];
        const targetPort = ports[src.table_target + '.' + src.column_target];
        const link = srcPort.link(targetPort);
        model.addLink(link);
      }
    }

    // model.setLocked(true);
    let engine = createEngine({
      registerDefaultDeleteItemsAction: false,
      registerDefaultZoomCanvasAction: false,
    });
    engine.getLinkFactories().registerFactory(new AdvancedLinkFactory());
    engine.getNodeFactories().registerFactory(new CustomNodeFactory());
    // Prevent modifying links by adding points
    engine.setMaxNumberPointsPerLink(0);

    engine.getActionEventBus().registerAction(new ZoomAction());

    engine.setModel(model);
    this.setState({engine, nodesIndex, isLoading: false});
  }

  render() {
    if (!this.state.engine || this.props.tables.length === 0) {
      return null;
    }

    if (this.state.isLoading) {
      return <CircularIndeterminate size="100px"/>;
    }

    return <EngineWidget engine={this.state.engine} showColumns={this.state.showColumns}/>;
  }
}

export { Diagram, defaultNodeColor };