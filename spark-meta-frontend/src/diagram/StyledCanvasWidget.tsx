import * as React from 'react';
import styled from '@emotion/styled';

export interface StyledCanvasWidgetProps {
	color?: string;
	background?: string;
}

const Container = styled.div<{ color: string; background: string }>`
  height: 70vh;
  background-color: ${p => p.background};
  background-size: 50px 50px;
  display: flex;
  > * {
    height: 100%;
    min-height: 100%;
    width: 100%;
  }
  background-image: linear-gradient(
      0deg,
      transparent 24%,
      ${p => p.color} 25%,
      ${p => p.color} 26%,
      transparent 27%,
      transparent 54%,
      ${p => p.color} 55%,
      ${p => p.color} 56%,
      transparent 57%,
      transparent
    ),
    linear-gradient(
      90deg,
      transparent 24%,
      ${p => p.color} 25%,
      ${p => p.color} 26%,
      transparent 27%,
      transparent 54%,
      ${p => p.color} 55%,
      ${p => p.color} 56%,
      transparent 57%,
      transparent
    );
`;

export class StyledCanvasWidget extends React.Component<StyledCanvasWidgetProps> {
	render() {
		return (
			<Container
				background={this.props.background || 'rgb(240, 240, 240)'}
				color={this.props.color || 'rgba(0,0,0, 0.05)'}>
				{this.props.children}
			</Container>
		);
	}
}