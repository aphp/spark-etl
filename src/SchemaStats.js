import React from 'react';
import BoxLine from './helpers/BoxLine.js';

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

    return <BoxLine items={[tableText, columnsText]}/>;
  }
}

export default SchemaStats;