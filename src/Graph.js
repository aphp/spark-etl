import React from "react";
import { Graph } from "react-d3-graph";

// the graph configuration, you only need to pass down properties
// that you want to override, otherwise default ones will be used
const myConfig = {
    nodeHighlightBehavior: true,
    node: {
        color: "lightgreen",
        size: 120,
        highlightStrokeColor: "blue",
    },
    link: {
        highlightColor: "lightblue",
    },
};

const otherConfig = {
    "automaticRearrangeAfterDropNode": false,
    "collapsible": false,
    "directed": false,
    "focusAnimationDuration": 0.75,
    "focusZoom": 1,
    "height": 500,
    "highlightDegree": 1,
    "highlightOpacity": 1,
    "linkHighlightBehavior": true,
    "maxZoom": 8,
    "minZoom": 0.1,
    "nodeHighlightBehavior": true,
    "panAndZoom": false,
    "staticGraph": false,
    "staticGraphWithDragAndDrop": false,
    "width": 1000,
    "d3": {
      "alphaTarget": 0.05,
      "gravity": -100,
      "linkLength": 100,
      "linkStrength": 1,
      "disableLinkForce": false
    },
    "node": {
      "color": "#d3d3d3",
      "fontColor": "black",
      "fontSize": 8,
      "fontWeight": "normal",
      "highlightColor": "blue",
      "highlightFontSize": 8,
      "highlightFontWeight": "normal",
      "highlightStrokeColor": "SAME",
      "highlightStrokeWidth": "SAME",
      "labelProperty": "name",
      "mouseCursor": "pointer",
      "opacity": 1,
      "renderLabel": true,
      "size": 200,
      "strokeColor": "none",
      "strokeWidth": 1.5,
      "svg": "",
      "symbolType": "circle"
    },
    "link": {
      "color": "#d3d3d3",
      "fontColor": "black",
      "fontSize": 8,
      "fontWeight": "normal",
      "highlightColor": "blue",
      "highlightFontSize": 8,
      "highlightFontWeight": "normal",
      "labelProperty": "label",
      "mouseCursor": "pointer",
      "opacity": 1,
      "renderLabel": false,
      "semanticStrokeWidth": false,
      "strokeWidth": 1.5,
      "markerHeight": 6,
      "markerWidth": 6
    }
  }
;

// graph event callbacks
const onClickGraph = function() {
    window.alert(`Clicked the graph background`);
};

const onClickNode = function(nodeId) {
    window.alert(`Clicked node ${nodeId}`);
};

const onDoubleClickNode = function(nodeId) {
    window.alert(`Double clicked node ${nodeId}`);
};

const onRightClickNode = function(event, nodeId) {
    window.alert(`Right clicked node ${nodeId}`);
};

const onMouseOverNode = function(nodeId) {
    window.alert(`Mouse over node ${nodeId}`);
};

const onMouseOutNode = function(nodeId) {
    window.alert(`Mouse out node ${nodeId}`);
};

const onClickLink = function(source, target) {
    window.alert(`Clicked link between ${source} and ${target}`);
};

const onRightClickLink = function(event, source, target) {
    window.alert(`Right clicked link between ${source} and ${target}`);
};

const onMouseOverLink = function(source, target) {
    window.alert(`Mouse over in link between ${source} and ${target}`);
};

const onMouseOutLink = function(source, target) {
    window.alert(`Mouse out link between ${source} and ${target}`);
};

const onNodePositionChange = function(nodeId, x, y) {
    window.alert(`Node ${nodeId} is moved to new position. New position is x= ${x} y= ${y}`);
};

function Diagram( {data }) {
 return (<Graph
    id="graph-id" // id is mandatory, if no id is defined rd3g will throw an error
    data={data}
    config={otherConfig}
    // onClickNode={onClickNode}
    // onDoubleClickNode={onDoubleClickNode}
    // onRightClickNode={onRightClickNode}
    // onClickGraph={onClickGraph}
    // onClickLink={onClickLink} 
    // onRightClickLink={onRightClickLink}
    // onMouseOverNode={onMouseOverNode}
    // onMouseOutNode={onMouseOutNode}
    // onMouseOverLink={onMouseOverLink}
    // onMouseOutLink={onMouseOutLink}
    // onNodePositionChange={onNodePositionChange}
/>);
}

export default Diagram;
