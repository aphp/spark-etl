import React from "react";
import { withStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import Editable from './helpers/Editable.js';
import Typography from '@material-ui/core/Typography';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import Box from '@material-ui/core/Box';
import BoxLine from "./helpers/BoxLine.js";
import Card from '@material-ui/core/Card';
import CardActions from '@material-ui/core/CardActions';
import CardContent from '@material-ui/core/CardContent';
import CardActionArea from '@material-ui/core/CardActionArea';
import clsx from 'clsx';
import Collapse from '@material-ui/core/Collapse';
import IconButton from '@material-ui/core/IconButton';


class TextHightlighter extends React.PureComponent {
  constructor(props) {
    super(props)
    this.contentEditable = React.createRef();
    this.state = {
      text: props.text || ''
    }
  };

  save = evt => {
    const value = evt.target.value;
    // This is still empty text
    if (this.props.text === null && value.trim().length === 0) {
      return;
    }
    if (value !== this.props.text) {
      this.props.updateText(value);
      this.setState({text: value});
    }
  };

  onChange = evt => {
    this.setState({text: evt.target.value});
  }

  render() {
    const {highlight, editable, text} = this.props;

    let displayText;
    if (!highlight || highlight === '' || typeof text !== 'string') {
      displayText = text;
    } else {
      const parts = text.split(new RegExp(`(${highlight})`, 'gi'));
      displayText = parts.map((part, i) => {
        if (part.toLowerCase() === highlight.toLowerCase()) {
          return `<span style="background-color: orange">${part}</span>`;
        }
        return part;
      }).join('');
    }

    if (!editable || !this.props.updateText) {
      return <Typography variant="body2" display="inline" dangerouslySetInnerHTML={{ __html: displayText}}/>;
    }

    return <Editable
      text={displayText}
      childRef={this.contentEditable}
      type="input">
      <textarea
        ref={this.contentEditable}
        type="text"
        rows="5"
        style={{width: "100%"}}
        value={this.state.text}
        onChange={this.onChange}
        onBlur={this.save}/>
    </Editable>
  }
}


class AttributeTable extends React.Component {
  shouldComponentUpdate(nextProps, nextState) {
    return nextProps.table._forceUpdate || this.props.canEdit !== nextProps.canEdit;
  }

  render() {
    const {table, attributeCols, searchText, canEdit} = this.props;
    if (!table._hasColumnDisplay) {
      return null;
    }

    return (
      <TableContainer component={Paper}>
        <Table aria-label="table">
          <TableHead>
            <TableRow>
              {attributeCols.map(col => (
                <TableCell key={'table-head-' + table._key + '-' + col.name}>
                  {col.name}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {table.columns.map(tableColumn => (
              <TableRow key={'table-row-' + table._key + '-' + tableColumn._key}>
                { attributeCols.map(col => (
                  <TableCell key={'table-cell-' + tableColumn._key + '-' + col.name}>
                    <TextHightlighter
                      text={tableColumn[col.name]}
                      highlight={searchText}
                      updateText={this.props.updateText ? (text => this.updateText(col.name, text)) : null}
                      editable={col.editable && canEdit}/>
                  </TableCell>
                ))}
              </TableRow>)
            )}
          </TableBody>
        </Table>
      </TableContainer>);
  }
}


class CardData extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      expanded: props.expandedColumns || false
    }
  }

  shouldComponentUpdate(nextProps, nextState) {
    return (nextProps.table._forceUpdate ||
      this.props.canEdit !== nextProps.canEdit ||
      this.state.expanded !== nextState.expanded);
  }

  setExpanded = (e) => {
    this.setState({expanded: e});
  };

  handleExpandClick = () => {
    this.setExpanded(!this.state.expanded);
  };

  selectTableId = id => {
    this.props.selectTableId && this.props.selectTableId(this.props.table.id, true);
  }

  render() {
    const {table, canEdit, columns, attributeCols, searchText, classes} = this.props;
    if (!table._display) {
      return null;
    }

    const items = [
      {name: table.name, variant: "h5"},
      table.columns_count + " columns",
      table.count_table + " lines",
      "type: " + table.typ_table]

    return (
      <Card>
        <CardActionArea onClick={this.selectTableId}>
          <BoxLine items={items}/>
        </CardActionArea>
        <CardContent>
          { columns.filter(e => e.name.startsWith('comment_')).map(col => {
            return <div key={'cardcontent-' + table.name + '-' + col.name}>
              <Typography variant="h6">{col.name}</Typography>
              <TextHightlighter
                text={table[col.name]}
                highlight={searchText}
                updateText={this.props.updateColumns ? (text => this.props.updateColumns(table, col.name, text)) : null}
                editable={col.editable && canEdit}/>
              </div>
          })}
        </CardContent>
        <CardActions disableSpacing>
            <IconButton
              className={clsx(classes.expand, {
                [classes.expandOpen]: this.state.expanded,
              })}
              onClick={this.handleExpandClick}
              aria-expanded={this.state.expanded}
              aria-label="show more"
            >
              <ExpandMoreIcon />
            </IconButton>
          </CardActions>
          <Collapse in={this.state.expanded} timeout="auto" unmountOnExit>
        <CardContent>
          <AttributeTable
           table={table}
           columns={columns}
           attributeCols={attributeCols}
           searchText={searchText}
           updateText={this.props.updateColumns}
           canEdit={this.props.canEdit}/>
        </CardContent>
      </Collapse>
      </Card>
    );
  }
}

const StyledCardData = withStyles(theme => ({
  expand: {
    transform: 'rotate(0deg)',
    marginLeft: 'auto',
    transition: theme.transitions.create('transform', {
      duration: theme.transitions.duration.shortest,
    }),
  },
  expandOpen: {
    transform: 'rotate(180deg)',
  },
}))(CardData);

class DataGrid extends React.Component {
  shouldComponentUpdate(nextProps, nextState) {
    if (nextProps.schema && this.props.schema) {
      return this.props.schema.id === nextProps.schema.id ||
        nextProps.schema._forceUpdate || this.props.canEdit !== nextProps.canEdit;
    }
    return false;
  }

  render() {
    const { schema, searchText } = this.props;
    if (!schema || schema.tables.length === 0) {
      return null;
    }

    const { tables, tableHeaders, attributeCols } = schema;

    return (
      <div style={{backgroundColor: "#eeeeee"}}>
        {tables.map(table => (
          <Box p={1} key={'card-' + table._key}>
            <StyledCardData
              selectTableId={this.props.selectTableId}
              table={table} columns={tableHeaders}
              attributeCols={attributeCols}
              searchText={searchText}
              updateColumns={this.props.updateColumns}
              updateTables={this.props.updateTables}
              expandedColumns={this.props.expandedColumns}
              canEdit={this.props.canEdit}/>
          </Box>
        ))}
      </div>
    );
  }
}

export default DataGrid;
