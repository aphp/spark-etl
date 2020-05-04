import {
	DefaultPortModel,
	DefaultLinkFactory,
	DefaultLinkModel,
	DefaultLinkWidget
} from '@projectstorm/react-diagrams';
import { LinkWidget, PointModel } from '@projectstorm/react-diagrams-core';
import * as React from 'react';

export class AdvancedLinkModel extends DefaultLinkModel {
  bothDirection: boolean;
	constructor() {
		super({
			type: 'advanced',
			width: 2
		});
    this.bothDirection = false;
  }

  setBothDirection(dir: boolean): void {
    this.bothDirection = dir;
  }

  getBothDirection(): boolean {
    return this.bothDirection;
  }
}

export class AdvancedPortModel extends DefaultPortModel {
  _isPk: boolean;
  _isFk: boolean;

  constructor(isIn: boolean, name?: string, label?: string) {
    super(isIn, name, label);
    this._isPk = false;
    this._isFk = false;
  }
	createLinkModel(): AdvancedLinkModel {
		return new AdvancedLinkModel();
  }

  isPk(): boolean {
    return this._isPk;
  }

  setPk(b: boolean) {
    this._isPk = b;
  }

  isFk(): boolean {
    return this._isFk;
  }

  setFk(b: boolean) {
    this._isFk = b;
  }
}

const CustomLinkArrowWidget = (props: any) => {
	const { point, previousPoint } = props;

	const angle =
		90 +
		(Math.atan2(
			point.getPosition().y - previousPoint.getPosition().y,
			point.getPosition().x - previousPoint.getPosition().x
		) *
			180) /
			Math.PI;

	//translate(50, -10),
	return (
		<g className="arrow" transform={'translate(' + point.getPosition().x + ', ' + point.getPosition().y + ')'}>
			<g style={{ transform: 'rotate(' + angle + 'deg)' }}>
				<g transform={'translate(0, -3)'}>
					<polygon
						points="0,10 8,30 -8,30"
						fill={props.color}
						// onMouseLeave={() => {
						// 	this.setState({ selected: false });
						// }}
						// onMouseEnter={() => {
						// 	this.setState({ selected: true });
						// }}
						data-id={point.getID()}
						data-linkid={point.getLink().getID()}></polygon>
				</g>
			</g>
		</g>
	);
};

export class AdvancedLinkWidget extends DefaultLinkWidget {
  generateArrow(point: PointModel, previousPoint: PointModel): JSX.Element {
		return (
			<CustomLinkArrowWidget
				key={point.getID()}
				point={point as any}
				previousPoint={previousPoint as any}
				colorSelected={this.props.link.getOptions().selectedColor}
				color={this.props.link.getOptions().color}
			/>
		);
	}

	render() {
		//ensure id is present for all points on the path
		var points = this.props.link.getPoints();
		var paths = [];
		this.refPaths = [];

		//draw the multiple anchors and complex line instead
		for (let j = 0; j < points.length - 1; j++) {
			paths.push(
				this.generateLink(
					LinkWidget.generateLinePath(points[j], points[j + 1]),
					{
						'data-linkid': this.props.link.getID(),
						'data-point': j,
						onMouseDown: (event: any) => {
							this.addPointToLink(event, j + 1);
						}
					},
					j
				)
			);
		}

    //render the circles
    if ((this.props.link as AdvancedLinkModel).getBothDirection()) {
			paths.push(this.generateArrow(points[0], points[1]));
      for (let i = 2; i < points.length - 1; i++) {
        paths.push(this.generatePoint(points[i]));
      }
    } else {
      for (let i = 1; i < points.length - 1; i++) {
        paths.push(this.generatePoint(points[i]));
      }
    }

		if (this.props.link.getTargetPort() !== null) {
			paths.push(this.generateArrow(points[points.length - 1], points[points.length - 2]));
		} else {
			paths.push(this.generatePoint(points[points.length - 1]));
		}

		return <g data-default-link-test={this.props.link.getOptions().testName}>{paths}</g>;
	}
}

export class AdvancedLinkFactory extends DefaultLinkFactory {
	constructor() {
		super('advanced');
	}

	generateModel(): AdvancedLinkModel {
		return new AdvancedLinkModel();
	}

	generateReactWidget(event: any): JSX.Element {
		return <AdvancedLinkWidget link={event.model} diagramEngine={this.engine} />;
	}
}