import React from "react";
import { withStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import Editable from '../helpers/Editable.js';
import Typography from '@material-ui/core/Typography';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import Box from '@material-ui/core/Box';
import BoxLine from "../helpers/BoxLine.js";
import Card from '@material-ui/core/Card';
import CardActions from '@material-ui/core/CardActions';
import CardContent from '@material-ui/core/CardContent';
import clsx from 'clsx';
import Collapse from '@material-ui/core/Collapse';
import IconButton from '@material-ui/core/IconButton';
import { PrimaryKey, ForeignKey } from "../helpers/Keys.js";


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


function AttributeTable(props) {
  const {columns, attributeCols, searchText, canEdit, tableId, selectByTableId} = props;
  if (columns.length === 0) {
    return null;
  }

  const tableLinkStyle = {cursor:'pointer', color: '#3c8dbc'};

  return (
    <TableContainer component={Paper}>
      <Table aria-label="table">
        <TableHead>
          <TableRow>
            <TableCell></TableCell>  {/* For FK / PK */}
            {attributeCols.map(col => (
              <TableCell key={'table-head-' + tableId + '-' + col.name} style={{ verticalAlign: 'top' }}>
                {col.displayName}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {columns.map(tableColumn => (
            <TableRow key={'table-row-' + tableId + '-' + tableColumn.id}>
              <TableCell style={{ verticalAlign: 'top' }}>
                { tableColumn.is_pk && <PrimaryKey width="20"/>}
                { !tableColumn.is_pk && tableColumn.is_fk && <ForeignKey width="20"/>}
                { !tableColumn.is_pk && !tableColumn.is_fk && null}
              </TableCell>
              { attributeCols.map(col => (
                <TableCell key={'table-cell-' + tableColumn.id + '-' + col.name} style={{ verticalAlign: 'top' }}>
                  { col.name === 'parents' && tableColumn.parents.map(e =>
                  <div key={'table-' + tableColumn.id + '-' + e.name + '-parent-' + e.id}>
                    <b onClick={() => selectByTableId(e.table_target_id)} style={tableLinkStyle}>{e.table_target}</b>{'.' + e.column_target}
                  </div>) }
                  { col.name === 'children' && tableColumn.children.map(e => <
                    div key={'table-' + tableColumn.id + '-' + e.name + '-children-' + e.id}>
                      <b onClick={() => selectByTableId(e.table_source_id)} style={tableLinkStyle}>{e.table_source}</b>{'.' + e.column_source}
                    </div>)}
                  { col.name !== 'parents' && col.name !== 'children' && <TextHightlighter
                    key={'text-' + tableColumn.id + '-' + col.name + '-' + tableColumn[col.name]}
                    text={tableColumn[col.name]}
                    highlight={searchText}
                    updateText={props.updateText ? (text => props.updateText(tableColumn, col.name, text)) : null}
                    editable={col.editable && canEdit}/>}
                </TableCell>
              ))}
            </TableRow>)
          )}
        </TableBody>
      </Table>
    </TableContainer>);
}


function CardData (props) {
  const {table, canEdit, tableHeaders, attributeCols, searchText, classes, expandedColumns, visibleColumns} = props;
  const [expanded, setExpanded] = React.useState(expandedColumns || false);

  const items = [
    {value: table.columns_count, name: "Columns"},
    {value: table.typ_table, name: "Type"},
    {value: table.parents.length, name: "Parents"},
    {value: table.children.length, name: "Children"}
  ];

  if (table.count_table != null) {
    items.push({name: "Lines", value: table.count_table});
  }

  if (table.last_analyze) {
    items.push({name: "Last analysis", value: table.last_analyze});
  }

  if (table.last_commit_timestampz) {
    items.push({name: "Last inserted data", value: table.last_commit_timestampz});
  }

  return (
    <Box p={1}>
      <Card>
        <CardContent>
          <BoxLine items={[ {text: table.name, variant: "h4"}]}/>
          <TableContainer component={Paper}>
            <Table aria-label="table">
              <TableHead>
                {items.map((col, i) => (
                  <TableCell key={'table-head-' + table.id + '-head-' + i} style={{ verticalAlign: 'top' }}>
                    {col.name}
                  </TableCell>
                ))}
              </TableHead>
              <TableBody>
                <TableRow>
                {items.map((col, i) => (
                      <TableCell key={'table-head-' + table.id + '-val-' + i} style={{ verticalAlign: 'top' }}>
                        {col.value}
                      </TableCell>
                    ))}
                </TableRow>
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
        <CardContent>
          { tableHeaders.filter(e => e.name.startsWith('comment_')).map(col => {
            return <div key={'cardcontent-' + table.name + '-' + col.name}>
              <Typography variant="h6">{col.displayName}</Typography>
              <TextHightlighter
                key={'text-' + table.id + '-' + col.name + '-' + table[col.name]}
                text={table[col.name]}
                highlight={searchText}
                updateText={props.updateColumns ? (text => props.updateTables(table, col.name, text)) : null}
                editable={col.editable && canEdit}/>
              </div>
          })}
        </CardContent>
        { !expandedColumns && <CardActions disableSpacing>
            <IconButton
              className={clsx(classes.expand, {
                [classes.expandOpen]: expanded,
              })}
              onClick={() => setExpanded(!expanded)}
              aria-expanded={expanded}
              aria-label="show more">
              <ExpandMoreIcon />
            </IconButton>
          </CardActions> }
        <Collapse in={expanded} timeout="auto" unmountOnExit>
          <CardContent>
            <AttributeTable
              selectByTableId={props.selectByTableId}
              tableId={table.id}
              columns={visibleColumns}
              attributeCols={attributeCols}
              searchText={searchText}
              updateText={props.updateColumns}
              canEdit={props.canEdit}/>
            </CardContent>
        </Collapse>
      </Card>
    </Box>
  );
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


class DataGrid extends React.PureComponent {
  render() {
    const { tables, searchText, tableHeaders, attributeCols, visibleTables } = this.props;
    if (!tables || tables.length === 0) {
      return null;
    }

    return (
      <div style={{backgroundColor: "#eeeeee"}}>
        {tables.map(table => (
          visibleTables[table.id] ? <StyledCardData
            selectByTableId={this.props.selectByTableId}
            key={'card-' + table.id}
            table={table} tableHeaders={tableHeaders}
            visibleColumns={visibleTables[table.id]}
            attributeCols={attributeCols}
            searchText={searchText}
            updateColumns={this.props.updateColumns}
            updateTables={this.props.updateTables}
            expandedColumns={this.props.expandedColumns}
            canEdit={this.props.canEdit}/>
            : null
          ))}
      </div>
    );
  }
}

export default DataGrid;
