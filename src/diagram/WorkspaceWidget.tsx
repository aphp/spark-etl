import * as React from 'react';
import styled from '@emotion/styled';

export interface WorkspaceWidgetProps {
	buttons?: any;
}

const Toolbar = styled.div`
  padding: 5px;
  display: flex;
  flex-shrink: 0;
`;

const Content = styled.div`
  flex-grow: 1;
  height: 100%;
`;

const Container = styled.div`
  background: rgb(220, 220, 220);
  display: flex;
  flex-direction: column;
  height: 100%;
  border-radius: 5px;
  overflow: hidden;
`;

export const StyledButton = styled.button`
	background: rgb(60, 60, 60);
	font-size: 14px;
	padding: 5px 10px;
	border: none;
	color: white;
	outline: none;
	cursor: pointer;
	margin: 2px;
	border-radius: 3px;
	&:hover {
		background: rgb(0, 192, 255);
	}
`;

export class WorkspaceWidget extends React.PureComponent<WorkspaceWidgetProps> {
	render() {
		return (
			<Container>
				<Toolbar>{this.props.buttons}</Toolbar>
				<Content>{this.props.children}</Content>
			</Container>
		);
	}
}