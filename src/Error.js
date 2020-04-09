import React from 'react';
import { makeStyles } from '@material-ui/core/styles';

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

    return <h3 className={classes.error}>Error: {error.message}</h3>;
}

export default Error;