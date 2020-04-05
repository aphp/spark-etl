import React from "react";
import TextField from '@material-ui/core/TextField';
import Autocomplete from '@material-ui/lab/Autocomplete';

function Select(props) {
  const { error, options, label, onChange } = props;

  if (error) {
    return <div>Error: {error.message}</div>;
  }

  if (options.length === 0) {
    return null;
  }


  return (<Autocomplete
    options={options}
    getOptionLabel={option => option.name}
    onChange={(event, newValue) => {
      if (!newValue) {
        return;
      }
      onChange(newValue);
    }}
    autoSelect autoHighlight autoComplete clearOnEscape openOnFocus
    renderInput={(params) => <TextField {...params} label={label} margin="normal" />}
  />);
}

export default Select;