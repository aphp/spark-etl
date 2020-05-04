import {
	DefaultNodeWidget,
	DefaultNodeModel,
	PortWidget,
  DefaultNodeFactory,
  DefaultPortLabelProps
} from '@projectstorm/react-diagrams';
import { AdvancedPortModel } from './ArrowLink';
import * as React from 'react';
import styled from '@emotion/styled';
import { PrimaryKey, ForeignKey } from '../helpers/Keys';

export class CustomNodeFactory extends DefaultNodeFactory {
	generateReactWidget(event: any) {
		return <CustomNodeWidget engine={this.engine as any} node={event.model} />;
	}

	generateModel(event: any) {
		return new DefaultNodeModel();
	}
}

const PortLabel = styled.div`
  display: flex;
  margin-top: 1px;
  align-items: center;
`;

const Label = styled.div`
  padding: 0 5px;
  flex-grow: 1;
`;

const Port = styled.div`
  width: 15px;
  height: 15px;
  background: rgba(white, 0.1);
`;


class KeyPortLabel extends React.Component<DefaultPortLabelProps> {
	render() {
    let keyLabel = <Port/>;
    if ((this.props.port as AdvancedPortModel).isPk()) {
      keyLabel = <Port><PrimaryKey width="16"/></Port>;
    } else if ((this.props.port as AdvancedPortModel).isFk()) {
      keyLabel = <Port><ForeignKey width="16"/></Port>;
    }

		const port = (
			<PortWidget engine={this.props.engine} port={this.props.port}>
				{keyLabel}
			</PortWidget>
		);
		const label = <Label>{this.props.port.getOptions().label}</Label>;

		return (
			<PortLabel>
				{this.props.port.getOptions().in ? port : label}
				{this.props.port.getOptions().in ? label : port}
			</PortLabel>
		);
	}
}

export const Node = styled.div<{ background: string; selected: boolean }>`
  background-color: ${p => p.background};
  border-radius: 5px;
  font-family: sans-serif;
  color: black;
  border: solid 2px black;
  overflow: visible;
  font-size: 14px;
  border: solid ${p => (p.selected ? '4px black' : '2px black')};
`;

export const Title = styled.div`
  background: #dddddd;
  font-size: 14px;
  font-weight: bold;
  display: flex;
  white-space: nowrap;
  justify-items: center;
`;

export const TitleName = styled.div`
  flex-grow: 1;
  padding: 5px 5px;
`;

export const Ports = styled.div`
  display: flex;
  // background-image: linear-gradient(rgba(0, 0, 0, 0.1), rgba(0, 0, 0, 0.2));
  // background-color: #fefefe;
`;

export const PortsContainer = styled.div`
  flex-grow: 1;
  display: flex;
  flex-direction: column;

  &:first-of-type {
    margin-right: 10px;
  }

  &:only-child {
    margin-right: 0px;
  }
`;

export class CustomNodeWidget extends DefaultNodeWidget {
	generatePort = (port: any) => {
		return <KeyPortLabel engine={this.props.engine} port={port} key={port.getID()} />;
  };

  render() {
		return (
			<Node
				data-default-node-name={this.props.node.getOptions().name}
				selected={this.props.node.isSelected()}
				background={this.props.node.getOptions().color as string}>
				<Title>
					<TitleName>{this.props.node.getOptions().name}</TitleName>
				</Title>
				<Ports>
					<PortsContainer>{this.props.node.getInPorts().map(this.generatePort)}</PortsContainer>
					<PortsContainer>{this.props.node.getOutPorts().map(this.generatePort)}</PortsContainer>
				</Ports>
			</Node>
		);
	}
}