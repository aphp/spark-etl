import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import FormControl from '@material-ui/core/FormControl';
import FormGroup from '@material-ui/core/FormGroup';
import FormLabel from '@material-ui/core/FormLabel';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Checkbox from '@material-ui/core/Checkbox';

const useStyles = makeStyles((theme) => ({
  formControl: {
    margin: theme.spacing(3),
  },
  formGroup: {
    display: "flex",
    flexDirection: "row"
  }
}));

function ColumnCheckboxes({ columns, changeColVisibility }) {
  const classes = useStyles();

  return (
    <FormControl component="fieldset" className={classes.formControl}>
      <FormLabel component="legend">Columns to display</FormLabel>
      <FormGroup className={classes.formGroup}>
      {columns.map(col =>
        <FormControlLabel
          key={"col-checkbox-" + col.name + "-" + col.display}
          control={<Checkbox checked={col.display} onChange={e => changeColVisibility(col, e.target.checked)} name={col.name} color="default" />}
          label={col.displayName}
        />
      )}
      </FormGroup>
    </FormControl>
  );
}

export default ColumnCheckboxes;