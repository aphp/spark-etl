import DataGrid from './DataGrid.js';
import React from 'react';
import { makeStyles, useTheme } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import Divider from '@material-ui/core/Divider';
import IconButton from '@material-ui/core/IconButton';
import ChevronLeftIcon from '@material-ui/icons/ChevronLeft';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';

const drawerWidth = '50%';

const useStyles = makeStyles((theme) => ({
  root: {
    display: 'flex',
  },
  appBar: {
    transition: theme.transitions.create(['margin', 'width'], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
  },
  appBarShift: {
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(['margin', 'width'], {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen,
    }),
    marginRight: drawerWidth,
  },
  title: {
    flexGrow: 1,
  },
  hide: {
    display: 'none',
  },
  drawer: {
    width: drawerWidth,
    flexShrink: 0,
  },
  drawerPaper: {
    width: drawerWidth,
  },
  drawerHeader: {
    display: 'flex',
    alignItems: 'center',
    padding: theme.spacing(0, 1),
    // necessary for content to be below app bar
    ...theme.mixins.toolbar,
    justifyContent: 'flex-start',
  },
  content: {
    flexGrow: 1,
    padding: theme.spacing(3),
    transition: theme.transitions.create('margin', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
    marginRight: -drawerWidth,
  },
  contentShift: {
    transition: theme.transitions.create('margin', {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen,
    }),
    marginRight: 0,
  },
}));


export default function TableDrawer(props) {
  const { selectByTableId,
    selectedTable,
    visibleTables,
    tableHeaders,
    updateColumns,
    attributeCols,
    canEdit,
    searchText,
    updateTables } = props;

  const classes = useStyles();
  const theme = useTheme();

  return (
    <Drawer
      className={classes.drawer}
      variant="persistent"
      anchor="right"
      open={selectedTable && Object.keys(visibleTables).length > 0}
      classes={{
        paper: classes.drawerPaper,
      }}>
      <div className={classes.drawerHeader}>
        <IconButton onClick={() => selectByTableId(null)}>
          {theme.direction === 'rtl' ? <ChevronLeftIcon /> : <ChevronRightIcon />}
        </IconButton>
      </div>
      <Divider />
      { selectedTable && <DataGrid
        selectByTableId={selectByTableId}
        tables={[selectedTable]}
        visibleTables={visibleTables}
        tableHeaders={tableHeaders}
        attributeCols={attributeCols}
        searchText={searchText}
        updateColumns={updateColumns}
        updateTables={updateTables}
        expandedColumns={true}
        canEdit={canEdit}/> }
    </Drawer>
  );
}