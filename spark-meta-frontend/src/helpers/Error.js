import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Box from '@material-ui/core/Box';
import Typography from '@material-ui/core/Typography';

const useStyles = makeStyles(theme => ({
    error: {
      color: 'red',
    },
  }));

function Error(props) {
  const { error } = props;
  const classes = useStyles();

  if (!error) {
      return null;
  }

  return (
    <Box display="flex" style={{ width: '100%' }}>
      <Box m="auto">
        <Typography component="div" bgcolor="background.paper">
          <h3 className={classes.error}>Error: {error.message}</h3>
        </Typography>
      </Box>
    </Box>);
}

export default Error;