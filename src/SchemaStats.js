import React from 'react';
import Box from '@material-ui/core/Box';
import Typography from '@material-ui/core/Typography';

class SchemaStats extends React.Component {
  shouldComponentUpdate(nextProps, nextState) {
    return (nextProps.selectedSchema && nextProps.selectedSchema._forceUpdate);
  }

  render() {
    const { selectedSchema } = this.props;

    if (!selectedSchema) {
        return null;
    }

    const { tables, visibleTables, visibleColumns } = selectedSchema;
    const columnsCount = tables.reduce((a, b) => a + b.columns.length, 0);

    let tableText = `${tables.length} tables`;
    let columnsText = `${columnsCount} columns`;

    if (visibleTables !== undefined && visibleTables !== tables.length) {
      tableText = `${visibleTables}/${tables.length} tables shown`;
    }
    if (visibleColumns !== undefined && visibleColumns !== columnsCount) {
      columnsText = `${visibleColumns}/${columnsCount} columns shown`;
    }

    return (
      <Box display="flex" style={{ width: '100%' }}>
        <Box m="auto">
          <Typography component="div" display="inline" bgcolor="background.paper">
            {tableText}
          </Typography>
        </Box>
        <Box m="auto">
          <Typography component="div" display="inline" bgcolor="background.paper">
            {columnsText}
          </Typography>
        </Box>
    </Box>
    );
  }
}

export default SchemaStats;